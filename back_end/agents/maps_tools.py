"""Google Maps verification tools exposed to planning agents."""

from __future__ import annotations

import logging
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from back_end.agents.date_idea_agent import DateIdeaAgentToolError
from back_end.clients.maps import (
    AmbiguousPlaceMatchError,
    GoogleMapsClient,
    NoPlaceMatchError,
)
from back_end.clients.maps_hours import is_open_at_plan_time
from back_end.domain.models import (
    CandidatePlace,
    ComputedRoute,
    LatLng,
    MapsOpeningHours,
    MapsPlace,
    MapsPlaceMatch,
    PhotoAsset,
    PhotoAuthorAttribution,
    RouteRequest,
    TravelMode,
)
from back_end.llm.models import AgentTool, OpenRouterFunctionTool
from back_end.precache.rag_candidates import (
    RagCandidateRowError,
    candidate_place_from_rag_row,
)

logger = logging.getLogger(__name__)


class MapsVerifyPlaceToolError(RuntimeError):
    """Raised when verify_place receives invalid local inputs."""


@dataclass(frozen=True)
class _MatchFailure:
    reason: str


@dataclass
class MapsVerificationCache:
    """In-process cache for Maps verification during one pre-cache run."""

    cache_path: Path | None = None
    _place_matches: dict[str, MapsPlaceMatch | _MatchFailure] = field(
        default_factory=dict
    )

    def __post_init__(self) -> None:
        if self.cache_path is None or not self.cache_path.exists():
            return
        try:
            cached = pd.read_parquet(self.cache_path)
        except Exception as exc:
            logger.error("Could not read Maps verification cache %s: %s", self.cache_path, exc)
            raise
        for raw in cached.to_dict(orient="records"):
            fsq_place_id = str(raw.get("fsq_place_id") or "").strip()
            if not fsq_place_id:
                logger.error("Maps verification cache row is missing fsq_place_id: %r", raw)
                continue
            parsed = _cache_row_to_match(raw)
            if parsed is not None:
                self._place_matches[fsq_place_id] = parsed

    def get_place_match(self, fsq_place_id: str) -> MapsPlaceMatch | _MatchFailure | None:
        return self._place_matches.get(fsq_place_id)

    def put_place_match(
        self,
        fsq_place_id: str,
        value: MapsPlaceMatch | _MatchFailure,
    ) -> None:
        self._place_matches[fsq_place_id] = value
        self._flush()

    def _flush(self) -> None:
        if self.cache_path is None:
            return
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        rows = [
            _match_to_cache_row(fsq_place_id, value)
            for fsq_place_id, value in sorted(self._place_matches.items())
        ]
        pd.DataFrame(rows).to_parquet(self.cache_path, index=False)


class MapsPlaceResolver:
    """Resolve local RAG FSQ ids into Google Maps place matches."""

    def __init__(
        self,
        *,
        maps_client: GoogleMapsClient,
        rag_documents: pd.DataFrame | Mapping[str, Mapping[str, Any]],
        cache: MapsVerificationCache,
        unknown_error_cls: type[RuntimeError] = MapsVerifyPlaceToolError,
    ) -> None:
        self._maps_client = maps_client
        self._documents_by_id = _index_rag_documents(rag_documents)
        self._cache = cache
        self._unknown_error_cls = unknown_error_cls

    def has_place(self, fsq_place_id: str) -> bool:
        """Return whether the local RAG documents contain this FSQ id."""

        return fsq_place_id in self._documents_by_id

    def candidate_coordinates(self, fsq_place_id: str) -> LatLng | None:
        """Return explicit local coordinates for routing fallback, if usable."""

        row = self._documents_by_id.get(fsq_place_id)
        if row is None:
            return None
        try:
            candidate = candidate_place_from_rag_row(row)
        except RagCandidateRowError as exc:
            logger.error(
                "Cannot read local coordinates for fsq_place_id=%s: %s",
                fsq_place_id,
                exc,
            )
            return None
        return candidate.coordinates

    async def resolve_place_match(
        self,
        fsq_place_id: str,
    ) -> MapsPlaceMatch | _MatchFailure:
        cached = self._cache.get_place_match(fsq_place_id)
        if cached is not None:
            return cached

        row = self._documents_by_id.get(fsq_place_id)
        if row is None:
            logger.error("Maps resolver received unknown fsq_place_id=%s.", fsq_place_id)
            raise self._unknown_error_cls(f"Unknown fsq_place_id {fsq_place_id!r}.")

        candidate = candidate_place_from_rag_row(row)
        try:
            resolved = await self._maps_client.resolve_place_match(candidate)
        except (NoPlaceMatchError, AmbiguousPlaceMatchError) as exc:
            logger.warning(
                "Google Maps could not confidently match fsq_place_id=%s: %s",
                fsq_place_id,
                exc,
            )
            resolved = _MatchFailure(reason=str(exc))
        self._cache.put_place_match(fsq_place_id, resolved)
        return resolved


class MapsVerifyPlaceTool:
    """Verify an FSQ venue against Google Maps."""

    def __init__(
        self,
        *,
        maps_client: GoogleMapsClient,
        rag_documents: pd.DataFrame | Mapping[str, Mapping[str, Any]],
        cache: MapsVerificationCache | None = None,
    ) -> None:
        self._cache = cache or MapsVerificationCache()
        self._resolver = MapsPlaceResolver(
            maps_client=maps_client,
            rag_documents=rag_documents,
            cache=self._cache,
            unknown_error_cls=MapsVerifyPlaceToolError,
        )

    def as_agent_tool(self) -> AgentTool:
        """Return verify_place as an OpenRouter function tool."""

        return AgentTool(
            definition=OpenRouterFunctionTool(
                name="verify_place",
                description=(
                    "Verify one FSQ venue against Google Maps before finalising a plan. "
                    "Checks whether Google can confidently match it, returns operational "
                    "status, rating evidence, Maps URI, and whether it appears open at "
                    "plan_time_iso when a precise ISO datetime is supplied."
                ),
                parameters_json_schema={
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "fsq_place_id": {
                            "type": "string",
                            "description": "FSQ place id from the RAG result.",
                        },
                        "plan_time_iso": {
                            "type": ["string", "null"],
                            "description": (
                                "ISO 8601 local-or-offset datetime for the planned visit, "
                                "or null when the plan time is not precise."
                            ),
                        },
                    },
                    "required": ["fsq_place_id"],
                },
                strict=True,
            ),
            handler=self.verify,
        )

    async def verify(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Execute one Google Maps verification."""

        fsq_place_id = _required_tool_string(arguments, "fsq_place_id")
        plan_time_iso = _optional_tool_string(
            arguments.get("plan_time_iso"),
            "plan_time_iso",
        )
        cached = await self._resolver.resolve_place_match(fsq_place_id)

        if isinstance(cached, _MatchFailure):
            return _unmatched_payload(
                fsq_place_id=fsq_place_id,
                failure_reason=cached.reason,
            )

        return _matched_payload(cached, plan_time_iso=plan_time_iso)


class MapsComputeLegTool:
    """Compute a Google Maps route leg between two verified FSQ venues."""

    def __init__(
        self,
        *,
        maps_client: GoogleMapsClient,
        rag_documents: pd.DataFrame | Mapping[str, Mapping[str, Any]],
        cache: MapsVerificationCache | None = None,
    ) -> None:
        self._maps_client = maps_client
        self._cache = cache or MapsVerificationCache()
        self._resolver = MapsPlaceResolver(
            maps_client=maps_client,
            rag_documents=rag_documents,
            cache=self._cache,
            unknown_error_cls=DateIdeaAgentToolError,
        )

    def as_agent_tool(self) -> AgentTool:
        """Return compute_leg as an OpenRouter function tool."""

        return AgentTool(
            definition=OpenRouterFunctionTool(
                name="compute_leg",
                description=(
                    "Compute the actual Google Maps route between two chosen FSQ "
                    "venue stops. Use this to check whether a hop is plausible for "
                    "the selected travel mode. Returns route totals and warnings "
                    "only, not turn-by-turn legs or steps."
                ),
                parameters_json_schema={
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "from_fsq_place_id": {"type": "string"},
                        "to_fsq_place_id": {"type": "string"},
                        "travel_mode": {
                            "type": "string",
                            "enum": [mode.name for mode in TravelMode],
                        },
                        "departure_time_iso": {"type": ["string", "null"]},
                    },
                    "required": [
                        "from_fsq_place_id",
                        "to_fsq_place_id",
                        "travel_mode",
                    ],
                },
                strict=True,
            ),
            handler=self.compute_leg,
        )

    async def compute_leg(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Resolve both endpoints and compute one compact route summary."""

        from_fsq_place_id = _required_compute_tool_string(
            arguments,
            "from_fsq_place_id",
        )
        to_fsq_place_id = _required_compute_tool_string(arguments, "to_fsq_place_id")
        travel_mode = _compute_travel_mode(arguments.get("travel_mode"))
        departure_time = _compute_departure_time(arguments.get("departure_time_iso"))

        origin = await self._resolver.resolve_place_match(from_fsq_place_id)
        if isinstance(origin, _MatchFailure):
            return _route_failure_payload(
                from_fsq_place_id=from_fsq_place_id,
                to_fsq_place_id=to_fsq_place_id,
                travel_mode=travel_mode,
                failure_reason=(
                    f"from_fsq_place_id {from_fsq_place_id!r} failed place match: "
                    f"{origin.reason}"
                ),
            )

        destination = await self._resolver.resolve_place_match(to_fsq_place_id)
        if isinstance(destination, _MatchFailure):
            return _route_failure_payload(
                from_fsq_place_id=from_fsq_place_id,
                to_fsq_place_id=to_fsq_place_id,
                travel_mode=travel_mode,
                failure_reason=(
                    f"to_fsq_place_id {to_fsq_place_id!r} failed place match: "
                    f"{destination.reason}"
                ),
            )

        route = await self._maps_client.compute_route(
            RouteRequest(
                origin=origin.google_place.location,
                destination=destination.google_place.location,
                travel_mode=travel_mode,
                departure_time=departure_time,
            )
        )
        return _route_success_payload(
            from_fsq_place_id=from_fsq_place_id,
            to_fsq_place_id=to_fsq_place_id,
            travel_mode=travel_mode,
            route=route,
        )


class MapsVerifyPlanTool:
    """Verify venue and route feasibility for one complete in-progress plan."""

    def __init__(
        self,
        *,
        maps_client: GoogleMapsClient,
        rag_documents: pd.DataFrame | Mapping[str, Mapping[str, Any]],
        cache: MapsVerificationCache | None = None,
        connective_anchors_by_stop_type: Mapping[str, LatLng] | None = None,
        default_connective_anchor: LatLng | None = None,
    ) -> None:
        self._maps_client = maps_client
        self._cache = cache or MapsVerificationCache()
        self._resolver = MapsPlaceResolver(
            maps_client=maps_client,
            rag_documents=rag_documents,
            cache=self._cache,
            unknown_error_cls=DateIdeaAgentToolError,
        )
        self._connective_anchors_by_stop_type = _copy_connective_anchors(
            connective_anchors_by_stop_type or {}
        )
        self._default_connective_anchor = default_connective_anchor

    def as_agent_tool(self) -> AgentTool:
        """Return verify_plan as an OpenRouter function tool."""

        return AgentTool(
            definition=OpenRouterFunctionTool(
                name="verify_plan",
                description=(
                    "Verify a whole in-progress date plan in one call. Checks every "
                    "venue against Google Places and computes every adjacent route "
                    "leg that has resolvable endpoint coordinates. Returns feasibility "
                    "booleans and reasons; failed feasibility is not an exception."
                ),
                parameters_json_schema={
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "plan_time_iso": {"type": ["string", "null"]},
                        "transport_mode": {
                            "type": "string",
                            "enum": [mode.name for mode in TravelMode],
                        },
                        "stops": {
                            "type": "array",
                            "minItems": 1,
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "kind": {
                                        "type": "string",
                                        "enum": ["venue", "connective"],
                                    },
                                    "stop_type": {"type": "string"},
                                    "fsq_place_id": {"type": ["string", "null"]},
                                },
                                "required": ["kind", "stop_type", "fsq_place_id"],
                            },
                        },
                        "max_leg_seconds": {"type": "integer", "minimum": 1},
                    },
                    "required": [
                        "plan_time_iso",
                        "transport_mode",
                        "stops",
                        "max_leg_seconds",
                    ],
                },
                strict=True,
            ),
            handler=self.verify_plan,
        )

    async def verify_plan(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Verify all stops and legs without short-circuiting on failed feasibility."""

        plan_time_iso = _optional_verify_tool_string(
            arguments.get("plan_time_iso"),
            "plan_time_iso",
        )
        plan_time = _verify_plan_time(plan_time_iso)
        transport_mode = _verify_transport_mode(arguments.get("transport_mode"))
        max_leg_seconds = _positive_int_argument(
            arguments.get("max_leg_seconds"),
            "max_leg_seconds",
        )
        stops = _verify_plan_stops(arguments.get("stops"))

        stops_verification: list[dict[str, Any]] = []
        endpoint_locations: list[LatLng | None] = []
        summary_reasons: list[str] = []
        all_venues_matched = True
        all_open_at_plan_time = True

        for stop_index, stop in enumerate(stops, start=1):
            if stop["kind"] == "connective":
                anchor = self._connective_anchor_for(stop["stop_type"])
                stops_verification.append(
                    _connective_plan_stop_payload(
                        stop_type=stop["stop_type"],
                        anchor=anchor,
                    )
                )
                endpoint_locations.append(anchor)
                continue

            verification, location = await self._verify_venue_for_plan(
                stop=stop,
                stop_index=stop_index,
                plan_time_iso=plan_time_iso,
                summary_reasons=summary_reasons,
            )
            stops_verification.append(verification)
            endpoint_locations.append(location)
            if verification["ok"] is not True:
                all_venues_matched = False
            if verification["open_at_plan_time"] is not True:
                all_open_at_plan_time = False

        if plan_time_iso is None and any(stop["kind"] == "venue" for stop in stops):
            all_open_at_plan_time = False
            summary_reasons.append(
                "plan_time_iso is null; venue opening at plan time cannot be confirmed."
            )

        legs: list[dict[str, Any]] = []
        all_legs_under_threshold = True
        for leg_index in range(len(stops) - 1):
            leg = await self._compute_plan_leg(
                from_stop=stops[leg_index],
                to_stop=stops[leg_index + 1],
                from_location=endpoint_locations[leg_index],
                to_location=endpoint_locations[leg_index + 1],
                from_stop_index=leg_index + 1,
                to_stop_index=leg_index + 2,
                transport_mode=transport_mode,
                plan_time=plan_time,
                max_leg_seconds=max_leg_seconds,
            )
            legs.append(leg)
            if leg["under_threshold"] is not True:
                all_legs_under_threshold = False
                failure_reason = leg.get("failure_reason")
                if isinstance(failure_reason, str) and failure_reason:
                    summary_reasons.append(f"leg {leg_index + 1}: {failure_reason}")

        return {
            "stops_verification": stops_verification,
            "legs": legs,
            "feasibility": {
                "all_venues_matched": all_venues_matched,
                "all_open_at_plan_time": all_open_at_plan_time,
                "all_legs_under_threshold": all_legs_under_threshold,
                "summary_reasons": summary_reasons,
            },
        }

    async def _verify_venue_for_plan(
        self,
        *,
        stop: dict[str, Any],
        stop_index: int,
        plan_time_iso: str | None,
        summary_reasons: list[str],
    ) -> tuple[dict[str, Any], LatLng | None]:
        fsq_place_id = stop["fsq_place_id"]
        if not self._resolver.has_place(fsq_place_id):
            reason = f"Unknown fsq_place_id {fsq_place_id!r}."
            logger.error("verify_plan stop %d failed: %s", stop_index, reason)
            summary_reasons.append(f"stop {stop_index}: {reason}")
            return (
                _plan_unmatched_payload(
                    stop_type=stop["stop_type"],
                    fsq_place_id=fsq_place_id,
                    failure_reason=reason,
                    fallback_location=None,
                ),
                None,
            )

        fallback_location = self._resolver.candidate_coordinates(fsq_place_id)
        try:
            resolved = await self._resolver.resolve_place_match(fsq_place_id)
        except RagCandidateRowError as exc:
            reason = str(exc)
            logger.error(
                "verify_plan stop %d fsq_place_id=%s failed local candidate parsing: %s",
                stop_index,
                fsq_place_id,
                reason,
            )
            summary_reasons.append(f"stop {stop_index}: {reason}")
            return (
                _plan_unmatched_payload(
                    stop_type=stop["stop_type"],
                    fsq_place_id=fsq_place_id,
                    failure_reason=reason,
                    fallback_location=fallback_location,
                ),
                fallback_location,
            )

        if isinstance(resolved, _MatchFailure):
            summary_reasons.append(f"stop {stop_index}: {resolved.reason}")
            return (
                _plan_unmatched_payload(
                    stop_type=stop["stop_type"],
                    fsq_place_id=fsq_place_id,
                    failure_reason=resolved.reason,
                    fallback_location=fallback_location,
                ),
                fallback_location,
            )

        payload = _plan_matched_payload(
            stop_type=stop["stop_type"],
            match=resolved,
            plan_time_iso=plan_time_iso,
        )
        if payload["ok"] is not True:
            reason = payload["failure_reason"]
            if isinstance(reason, str) and reason:
                summary_reasons.append(f"stop {stop_index}: {reason}")
        elif payload["open_at_plan_time"] is not True:
            reason = payload["open_failure_reason"]
            if isinstance(reason, str) and reason:
                summary_reasons.append(f"stop {stop_index}: {reason}")
        return payload, resolved.google_place.location

    async def _compute_plan_leg(
        self,
        *,
        from_stop: dict[str, Any],
        to_stop: dict[str, Any],
        from_location: LatLng | None,
        to_location: LatLng | None,
        from_stop_index: int,
        to_stop_index: int,
        transport_mode: TravelMode,
        plan_time: datetime | None,
        max_leg_seconds: int,
    ) -> dict[str, Any]:
        if from_location is None or to_location is None:
            missing: list[str] = []
            if from_location is None:
                missing.append("from endpoint")
            if to_location is None:
                missing.append("to endpoint")
            reason = f"Cannot compute route; {' and '.join(missing)} has no coordinates."
            logger.error(
                "verify_plan skipped leg %d->%d: %s",
                from_stop_index,
                to_stop_index,
                reason,
            )
            return _plan_leg_skipped_payload(
                from_stop=from_stop,
                to_stop=to_stop,
                from_stop_index=from_stop_index,
                to_stop_index=to_stop_index,
                transport_mode=transport_mode,
                failure_reason=reason,
            )

        route = await self._maps_client.compute_route(
            RouteRequest(
                origin=from_location,
                destination=to_location,
                travel_mode=transport_mode,
                departure_time=plan_time,
            )
        )
        threshold_duration = route.duration_seconds
        if threshold_duration is None:
            threshold_duration = route.static_duration_seconds

        if threshold_duration is None:
            under_threshold = False
            failure_reason = "Route response did not include a duration."
            logger.error(
                "verify_plan leg %d->%d had no duration in the route response.",
                from_stop_index,
                to_stop_index,
            )
        elif threshold_duration > max_leg_seconds:
            under_threshold = False
            failure_reason = (
                f"duration {threshold_duration:.0f}s exceeds threshold "
                f"{max_leg_seconds}s."
            )
        else:
            under_threshold = True
            failure_reason = None

        return _plan_leg_success_payload(
            from_stop=from_stop,
            to_stop=to_stop,
            from_stop_index=from_stop_index,
            to_stop_index=to_stop_index,
            transport_mode=transport_mode,
            route=route,
            threshold_duration_seconds=threshold_duration,
            under_threshold=under_threshold,
            failure_reason=failure_reason,
        )

    def _connective_anchor_for(self, stop_type: str) -> LatLng | None:
        return self._connective_anchors_by_stop_type.get(
            stop_type,
            self._default_connective_anchor,
        )


def _index_rag_documents(
    rag_documents: pd.DataFrame | Mapping[str, Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    if isinstance(rag_documents, pd.DataFrame):
        if "fsq_place_id" not in rag_documents.columns:
            raise MapsVerifyPlaceToolError("RAG documents are missing fsq_place_id.")
        return {
            str(row["fsq_place_id"]): row.to_dict()
            for _, row in rag_documents.iterrows()
            if str(row.get("fsq_place_id", "")).strip()
        }
    return {
        str(place_id): {"fsq_place_id": str(place_id), **dict(row)}
        for place_id, row in rag_documents.items()
    }


def _copy_connective_anchors(
    anchors_by_stop_type: Mapping[str, LatLng],
) -> dict[str, LatLng]:
    copied: dict[str, LatLng] = {}
    for raw_stop_type, anchor in anchors_by_stop_type.items():
        stop_type = _compute_non_empty_string(raw_stop_type, "connective stop_type")
        if not isinstance(anchor, LatLng):
            raise ValueError(
                f"Connective anchor for stop_type={stop_type!r} must be a LatLng."
            )
        copied[stop_type] = anchor
    return copied


def _matched_payload(
    match: MapsPlaceMatch,
    *,
    plan_time_iso: str | None,
) -> dict[str, Any]:
    place = match.google_place
    opening_hours = place.regular_opening_hours
    weekday_descriptions = (
        list(opening_hours.weekday_descriptions) if opening_hours is not None else []
    )
    open_at_plan_time: bool | None
    if plan_time_iso is None:
        open_at_plan_time = None
    elif place.business_status == "CLOSED_PERMANENTLY":
        open_at_plan_time = False
    else:
        open_at_plan_time = is_open_at_plan_time(opening_hours, plan_time_iso)

    return {
        "fsq_place_id": match.candidate_place.fsq_place_id,
        "matched": True,
        "google_place_id": place.place_id,
        "display_name": place.display_name,
        "formatted_address": place.formatted_address,
        "google_maps_uri": place.google_maps_uri,
        "website_uri": place.website_uri,
        "business_status": place.business_status or "UNKNOWN",
        "rating": place.rating,
        "user_rating_count": place.user_rating_count,
        "match_kind": match.match_kind,
        "weekday_descriptions": weekday_descriptions,
        "open_at_plan_time": open_at_plan_time,
        "primary_photo": _photo_payload(place.photos[0]) if place.photos else None,
        "photos": _photo_list_payload(place.photos, max_items=3),
        "failure_reason": None,
    }


def _plan_matched_payload(
    *,
    stop_type: str,
    match: MapsPlaceMatch,
    plan_time_iso: str | None,
) -> dict[str, Any]:
    place = match.google_place
    base = _matched_payload(match, plan_time_iso=plan_time_iso)
    business_status = place.business_status or "UNKNOWN"
    open_at_plan_time = base["open_at_plan_time"]
    open_failure_reason: str | None = None
    ok = True
    failure_reason: str | None = None

    if business_status != "UNKNOWN" and business_status != "OPERATIONAL":
        ok = False
        failure_reason = f"venue business_status is {business_status}."
        open_at_plan_time = False
        open_failure_reason = failure_reason
        logger.error(
            "verify_plan rejected fsq_place_id=%s due to business_status=%s.",
            match.candidate_place.fsq_place_id,
            business_status,
        )
    elif plan_time_iso is None:
        open_failure_reason = (
            "plan_time_iso is null; opening at plan time cannot be confirmed."
        )
    elif open_at_plan_time is None:
        open_failure_reason = (
            "Google opening-hours data did not confirm this venue is open at plan time."
        )
    elif open_at_plan_time is False:
        open_failure_reason = "Venue is not open at plan time."

    return {
        "kind": "venue",
        "stop_type": stop_type,
        "ok": ok,
        **base,
        "business_status": business_status,
        "open_at_plan_time": open_at_plan_time,
        "failure_reason": failure_reason,
        "open_failure_reason": open_failure_reason,
        "straight_line_distance_meters": round(
            match.straight_line_distance_meters,
            1,
        ),
        "name_similarity": round(match.name_similarity, 6),
        "location": _latlng_payload(place.location),
    }


def _plan_unmatched_payload(
    *,
    stop_type: str,
    fsq_place_id: str,
    failure_reason: str,
    fallback_location: LatLng | None,
) -> dict[str, Any]:
    payload = {
        "kind": "venue",
        "stop_type": stop_type,
        "ok": False,
        **_unmatched_payload(
            fsq_place_id=fsq_place_id,
            failure_reason=failure_reason,
        ),
        "open_at_plan_time": False,
        "open_failure_reason": failure_reason,
    }
    if fallback_location is not None:
        payload["location"] = _latlng_payload(fallback_location)
        payload["location_source"] = "local_candidate"
    return payload


def _connective_plan_stop_payload(
    *,
    stop_type: str,
    anchor: LatLng | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "kind": "connective",
        "stop_type": stop_type,
        "fsq_place_id": None,
        "ok": True,
    }
    if anchor is not None:
        payload["location"] = _latlng_payload(anchor)
        payload["location_source"] = "connective_anchor"
    return payload


def _unmatched_payload(
    *,
    fsq_place_id: str,
    failure_reason: str,
) -> dict[str, Any]:
    return {
        "fsq_place_id": fsq_place_id,
        "matched": False,
        "google_place_id": None,
        "display_name": None,
        "formatted_address": None,
        "google_maps_uri": None,
        "website_uri": None,
        "business_status": "UNKNOWN",
        "rating": None,
        "user_rating_count": None,
        "match_kind": None,
        "weekday_descriptions": [],
        "open_at_plan_time": None,
        "primary_photo": None,
        "photos": [],
        "failure_reason": failure_reason,
    }


def _photo_list_payload(
    photos: tuple[PhotoAsset, ...],
    *,
    max_items: int | None = None,
) -> list[dict[str, Any]]:
    payloads = [_photo_payload(photo) for photo in photos]
    if max_items is not None:
        return payloads[:max_items]
    return payloads


def _photo_payload(photo: PhotoAsset) -> dict[str, Any]:
    return {
        "name": photo.name,
        "width_px": photo.width_px,
        "height_px": photo.height_px,
        "author_attributions": [
            _photo_author_attribution_payload(attr)
            for attr in photo.author_attributions
        ],
    }


def _photo_author_attribution_payload(
    attribution: PhotoAuthorAttribution,
) -> dict[str, Any]:
    return {
        "display_name": attribution.display_name,
        "uri": attribution.uri,
        "photo_uri": attribution.photo_uri,
    }


def _plan_leg_success_payload(
    *,
    from_stop: dict[str, Any],
    to_stop: dict[str, Any],
    from_stop_index: int,
    to_stop_index: int,
    transport_mode: TravelMode,
    route: ComputedRoute,
    threshold_duration_seconds: float | None,
    under_threshold: bool,
    failure_reason: str | None,
) -> dict[str, Any]:
    return {
        "from_stop_index": from_stop_index,
        "to_stop_index": to_stop_index,
        "from_kind": from_stop["kind"],
        "to_kind": to_stop["kind"],
        "from_fsq_place_id": from_stop["fsq_place_id"],
        "to_fsq_place_id": to_stop["fsq_place_id"],
        "transport_mode": transport_mode.value,
        "status": "computed",
        "distance_meters": route.distance_meters,
        "duration_seconds": route.duration_seconds,
        "static_duration_seconds": route.static_duration_seconds,
        "duration_for_threshold_seconds": threshold_duration_seconds,
        "under_threshold": under_threshold,
        "warnings": list(route.warnings),
        "failure_reason": failure_reason,
    }


def _plan_leg_skipped_payload(
    *,
    from_stop: dict[str, Any],
    to_stop: dict[str, Any],
    from_stop_index: int,
    to_stop_index: int,
    transport_mode: TravelMode,
    failure_reason: str,
) -> dict[str, Any]:
    return {
        "from_stop_index": from_stop_index,
        "to_stop_index": to_stop_index,
        "from_kind": from_stop["kind"],
        "to_kind": to_stop["kind"],
        "from_fsq_place_id": from_stop["fsq_place_id"],
        "to_fsq_place_id": to_stop["fsq_place_id"],
        "transport_mode": transport_mode.value,
        "status": "skipped",
        "distance_meters": None,
        "duration_seconds": None,
        "static_duration_seconds": None,
        "duration_for_threshold_seconds": None,
        "under_threshold": False,
        "warnings": [],
        "failure_reason": failure_reason,
    }


def _latlng_payload(point: LatLng) -> dict[str, float]:
    return {
        "latitude": point.latitude,
        "longitude": point.longitude,
    }


def _route_success_payload(
    *,
    from_fsq_place_id: str,
    to_fsq_place_id: str,
    travel_mode: TravelMode,
    route: ComputedRoute,
) -> dict[str, Any]:
    return {
        "from_fsq_place_id": from_fsq_place_id,
        "to_fsq_place_id": to_fsq_place_id,
        "travel_mode": travel_mode.value,
        "distance_meters": route.distance_meters,
        "duration_seconds": route.duration_seconds,
        "static_duration_seconds": route.static_duration_seconds,
        "warnings": list(route.warnings),
        "failure_reason": None,
    }


def _route_failure_payload(
    *,
    from_fsq_place_id: str,
    to_fsq_place_id: str,
    travel_mode: TravelMode,
    failure_reason: str,
) -> dict[str, Any]:
    logger.error(
        "Skipping compute_leg from_fsq_place_id=%s to_fsq_place_id=%s "
        "travel_mode=%s because endpoint verification failed: %s",
        from_fsq_place_id,
        to_fsq_place_id,
        travel_mode.value,
        failure_reason,
    )
    return {
        "from_fsq_place_id": from_fsq_place_id,
        "to_fsq_place_id": to_fsq_place_id,
        "travel_mode": travel_mode.value,
        "distance_meters": None,
        "duration_seconds": None,
        "static_duration_seconds": None,
        "warnings": [],
        "failure_reason": failure_reason,
    }


def _match_to_cache_row(
    fsq_place_id: str,
    value: MapsPlaceMatch | _MatchFailure,
) -> dict[str, Any]:
    if isinstance(value, _MatchFailure):
        return {
            "fsq_place_id": fsq_place_id,
            "status": "failed",
            "failure_reason": value.reason,
            "match_kind": None,
            "candidate_name": None,
            "candidate_address": None,
            "candidate_latitude": None,
            "candidate_longitude": None,
            "candidate_locality": None,
            "candidate_region": None,
            "candidate_postcode": None,
            "google_place_id": None,
            "google_resource_name": None,
            "google_display_name": None,
            "google_formatted_address": None,
            "google_maps_uri": None,
            "website_uri": None,
            "google_latitude": None,
            "google_longitude": None,
            "business_status": None,
            "rating": None,
            "user_rating_count": None,
            "regular_opening_hours_json": None,
            "photos_json": None,
            "straight_line_distance_meters": None,
            "name_similarity": None,
        }

    candidate = value.candidate_place
    place = value.google_place
    return {
        "fsq_place_id": fsq_place_id,
        "status": "matched",
        "failure_reason": None,
        "match_kind": value.match_kind,
        "candidate_name": candidate.name,
        "candidate_address": candidate.address,
        "candidate_latitude": candidate.latitude,
        "candidate_longitude": candidate.longitude,
        "candidate_locality": candidate.locality,
        "candidate_region": candidate.region,
        "candidate_postcode": candidate.postcode,
        "google_place_id": place.place_id,
        "google_resource_name": place.resource_name,
        "google_display_name": place.display_name,
        "google_formatted_address": place.formatted_address,
        "google_maps_uri": place.google_maps_uri,
        "website_uri": place.website_uri,
        "google_latitude": place.location.latitude,
        "google_longitude": place.location.longitude,
        "business_status": place.business_status,
        "rating": place.rating,
        "user_rating_count": place.user_rating_count,
        "regular_opening_hours_json": _opening_hours_to_cache_json(
            place.regular_opening_hours
        ),
        "photos_json": _photos_to_cache_json(place.photos),
        "straight_line_distance_meters": value.straight_line_distance_meters,
        "name_similarity": value.name_similarity,
    }


def _cache_row_to_match(raw: Mapping[str, Any]) -> MapsPlaceMatch | _MatchFailure | None:
    status = _cache_optional_str(raw.get("status"))
    if status == "failed":
        return _MatchFailure(
            reason=_cache_optional_str(raw.get("failure_reason"))
            or "Cached Maps match failure."
        )
    if status != "matched":
        logger.error("Maps verification cache row has invalid status=%r.", status)
        return None

    try:
        fsq_place_id = _cache_required_str(raw, "fsq_place_id")
        return MapsPlaceMatch(
            candidate_place=CandidatePlace(
                fsq_place_id=fsq_place_id,
                name=_cache_required_str(raw, "candidate_name"),
                address=_cache_optional_str(raw.get("candidate_address")),
                latitude=_cache_optional_float(raw.get("candidate_latitude")),
                longitude=_cache_optional_float(raw.get("candidate_longitude")),
                locality=_cache_optional_str(raw.get("candidate_locality")),
                region=_cache_optional_str(raw.get("candidate_region")),
                postcode=_cache_optional_str(raw.get("candidate_postcode")),
            ),
            google_place=MapsPlace(
                place_id=_cache_required_str(raw, "google_place_id"),
                resource_name=(
                    _cache_optional_str(raw.get("google_resource_name"))
                    or f"places/{_cache_required_str(raw, 'google_place_id')}"
                ),
                display_name=_cache_required_str(raw, "google_display_name"),
                location=LatLng(
                    latitude=_cache_required_float(raw, "google_latitude"),
                    longitude=_cache_required_float(raw, "google_longitude"),
                ),
                formatted_address=_cache_optional_str(
                    raw.get("google_formatted_address")
                ),
                google_maps_uri=_cache_optional_str(raw.get("google_maps_uri")),
                website_uri=_cache_optional_str(raw.get("website_uri")),
                business_status=_cache_optional_str(raw.get("business_status")),
                rating=_cache_optional_float(raw.get("rating")),
                user_rating_count=_cache_optional_int(raw.get("user_rating_count")),
                regular_opening_hours=_opening_hours_from_cache_json(
                    raw.get("regular_opening_hours_json")
                ),
                photos=_photos_from_cache_json(raw.get("photos_json")),
            ),
            straight_line_distance_meters=_cache_required_float(
                raw,
                "straight_line_distance_meters",
            ),
            name_similarity=_cache_required_float(raw, "name_similarity"),
            match_kind=_cache_optional_str(raw.get("match_kind")) or "cached",
        )
    except (TypeError, ValueError) as exc:
        logger.error("Invalid Maps verification cache row %r: %s", raw, exc)
        return None


def _opening_hours_to_cache_json(opening_hours: MapsOpeningHours | None) -> str | None:
    if opening_hours is None:
        return None
    return json.dumps(
        {
            "open_now": opening_hours.open_now,
            "weekday_descriptions": list(opening_hours.weekday_descriptions),
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def _opening_hours_from_cache_json(value: Any) -> MapsOpeningHours | None:
    text = _cache_optional_str(value)
    if text is None:
        return None
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("regular_opening_hours_json must contain an object.")
    descriptions = parsed.get("weekday_descriptions", [])
    if not isinstance(descriptions, list) or not all(
        isinstance(item, str) for item in descriptions
    ):
        raise ValueError("weekday_descriptions must be a list of strings.")
    open_now = parsed.get("open_now")
    if open_now is not None and not isinstance(open_now, bool):
        raise ValueError("open_now must be a bool or null.")
    return MapsOpeningHours(
        open_now=open_now,
        weekday_descriptions=tuple(descriptions),
    )


def _photos_to_cache_json(photos: tuple[PhotoAsset, ...]) -> str | None:
    if not photos:
        return None
    return json.dumps(
        [_photo_payload(photo) for photo in photos],
        separators=(",", ":"),
        sort_keys=True,
    )


def _photos_from_cache_json(value: Any) -> tuple[PhotoAsset, ...]:
    text = _cache_optional_str(value)
    if text is None:
        return ()
    parsed = json.loads(text)
    if not isinstance(parsed, list):
        raise ValueError("photos_json must contain a list.")
    photos: list[PhotoAsset] = []
    for item in parsed:
        if not isinstance(item, dict):
            raise ValueError("photos_json entries must be objects.")
        raw_attributions = item.get("author_attributions", [])
        if not isinstance(raw_attributions, list):
            raise ValueError("photo author_attributions must be a list.")
        attributions: list[PhotoAuthorAttribution] = []
        for raw_attr in raw_attributions:
            if not isinstance(raw_attr, dict):
                raise ValueError("photo author attribution entries must be objects.")
            attributions.append(
                PhotoAuthorAttribution(
                    display_name=_cache_optional_str(raw_attr.get("display_name")),
                    uri=_cache_optional_str(raw_attr.get("uri")),
                    photo_uri=_cache_optional_str(raw_attr.get("photo_uri")),
                )
            )
        photos.append(
            PhotoAsset(
                name=_cache_required_str(item, "name"),
                width_px=_cache_optional_int(item.get("width_px")),
                height_px=_cache_optional_int(item.get("height_px")),
                author_attributions=tuple(attributions),
            )
        )
    return tuple(photos)


def _cache_required_str(raw: Mapping[str, Any], field_name: str) -> str:
    value = _cache_optional_str(raw.get(field_name))
    if value is None:
        raise ValueError(f"{field_name} is required.")
    return value


def _cache_optional_str(value: Any) -> str | None:
    if value is None or _is_nan(value):
        return None
    text = str(value).strip()
    return text or None


def _cache_required_float(raw: Mapping[str, Any], field_name: str) -> float:
    value = _cache_optional_float(raw.get(field_name))
    if value is None:
        raise ValueError(f"{field_name} is required.")
    return value


def _cache_optional_float(value: Any) -> float | None:
    if value is None or _is_nan(value):
        return None
    return float(value)


def _cache_optional_int(value: Any) -> int | None:
    if value is None or _is_nan(value):
        return None
    return int(value)


def _is_nan(value: Any) -> bool:
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    try:
        return bool(value != value)
    except Exception:
        return False


def _required_tool_string(payload: dict[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise MapsVerifyPlaceToolError(f"{field_name} must be a non-empty string.")
    return value.strip()


def _optional_tool_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise MapsVerifyPlaceToolError(f"{field_name} must be null or a non-empty string.")
    return value.strip()


def _required_compute_tool_string(
    payload: dict[str, Any],
    field_name: str,
) -> str:
    value = payload.get(field_name)
    return _compute_non_empty_string(value, field_name)


def _compute_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DateIdeaAgentToolError(f"{field_name} must be a non-empty string.")
    return value.strip()


def _compute_travel_mode(value: Any) -> TravelMode:
    raw_mode = _compute_non_empty_string(value, "travel_mode")
    try:
        return TravelMode[raw_mode]
    except KeyError as exc:
        allowed = ", ".join(mode.name for mode in TravelMode)
        raise DateIdeaAgentToolError(
            f"travel_mode must be one of {allowed}, got {raw_mode!r}."
        ) from exc


def _compute_departure_time(value: Any) -> datetime | None:
    if value is None:
        return None
    raw_time = _compute_non_empty_string(value, "departure_time_iso")
    normalized = f"{raw_time[:-1]}+00:00" if raw_time.endswith("Z") else raw_time
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise DateIdeaAgentToolError(
            f"departure_time_iso must be a valid ISO datetime, got {raw_time!r}."
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DateIdeaAgentToolError(
            "departure_time_iso must include an explicit timezone offset."
        )
    return parsed


def _optional_verify_tool_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    return _compute_non_empty_string(value, field_name)


def _verify_plan_time(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        return _compute_departure_time(value)
    except DateIdeaAgentToolError as exc:
        raise DateIdeaAgentToolError(
            str(exc).replace("departure_time_iso", "plan_time_iso")
        ) from exc


def _verify_transport_mode(value: Any) -> TravelMode:
    try:
        return _compute_travel_mode(value)
    except DateIdeaAgentToolError as exc:
        raise DateIdeaAgentToolError(
            str(exc).replace("travel_mode", "transport_mode")
        ) from exc


def _positive_int_argument(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DateIdeaAgentToolError(f"{field_name} must be an integer.")
    if value <= 0:
        raise DateIdeaAgentToolError(f"{field_name} must be positive.")
    return value


def _verify_plan_stops(value: Any) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list) or not value:
        raise DateIdeaAgentToolError("stops must be a non-empty array.")
    return tuple(
        _verify_plan_stop(raw_stop, stop_index=stop_index)
        for stop_index, raw_stop in enumerate(value)
    )


def _verify_plan_stop(value: Any, *, stop_index: int) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DateIdeaAgentToolError(f"stops[{stop_index}] must be an object.")
    kind = _compute_non_empty_string(value.get("kind"), f"stops[{stop_index}].kind")
    if kind not in {"venue", "connective"}:
        raise DateIdeaAgentToolError(
            f"stops[{stop_index}].kind must be 'venue' or 'connective'."
        )
    stop_type = _compute_non_empty_string(
        value.get("stop_type"),
        f"stops[{stop_index}].stop_type",
    )
    raw_place_id = value.get("fsq_place_id")
    if kind == "venue":
        fsq_place_id = _compute_non_empty_string(
            raw_place_id,
            f"stops[{stop_index}].fsq_place_id",
        )
    else:
        if raw_place_id is not None:
            raise DateIdeaAgentToolError(
                f"stops[{stop_index}].fsq_place_id must be null for connective stops."
            )
        fsq_place_id = None
    return {
        "kind": kind,
        "stop_type": stop_type,
        "fsq_place_id": fsq_place_id,
    }


__all__ = [
    "MapsComputeLegTool",
    "MapsPlaceResolver",
    "MapsVerificationCache",
    "MapsVerifyPlaceTool",
    "MapsVerifyPlanTool",
]
