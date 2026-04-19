"""Plan loading, ranking, and transformation for the local frontend API."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd

from back_end.api.models import (
    BookingContextPayload,
    DateTemplatePayload,
    DateTemplateStopPayload,
    PlanPayload,
    PlanStop,
    TransportLeg,
)
from back_end.precache.asset_sync import DEFAULT_FRONTEND_API_OUTPUT_PATH, DEFAULT_FRONTEND_IMAGES_DIR
from back_end.query.location import TypedLocationResolver, _haversine_km
from back_end.catalog.repository import PlacesRepository
from back_end.query.settings import load_query_settings
from back_end.query.errors import LocationAmbiguityError, LocationResolutionError
from back_end.rag.retriever import RagRetrieverError, load_date_templates
from back_end.rag.settings import load_rag_settings

logger = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[2]

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_TIME_RE = re.compile(r"^(?P<hour>\d{1,2}):(?P<minute>\d{2})$")
_TRANSPORT_ALIASES = {
    "walking": "WALK",
    "walk": "WALK",
    "public_transport": "TRANSIT",
    "transit": "TRANSIT",
    "driving": "DRIVE",
    "drive": "DRIVE",
}
_STATIC_ROUTE = "/static/precache-images"
_E164_PHONE_RE = re.compile(r"^\+[1-9]\d{6,14}$")


class FrontendApiError(RuntimeError):
    """Raised when the local frontend API cannot serve a safe response."""


@dataclass(frozen=True)
class _ApiPlanRecord:
    plan_id: str
    template_id: str
    bucket_id: str
    plan_title: str
    bucket_label: str
    hero_image_relative_path: str | None
    payload: dict[str, Any]
    generated_at_utc: str | None
    source_written_at_utc: str | None
    template_duration_hours: float | None
    vibe_tokens: frozenset[str]
    search_tokens: frozenset[str]
    center_latitude: float | None
    center_longitude: float | None
    plan_hour_local: int | None
    transport_mode: str | None


@dataclass(frozen=True)
class _Snapshot:
    source_mtime_ns: int
    records: tuple[_ApiPlanRecord, ...]
    by_plan_id: dict[str, _ApiPlanRecord]
    bucket_counts: tuple[tuple[str, int], ...]
    template_counts: tuple[tuple[str, int], ...]
    vibe_counts: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class _TemplateSnapshot:
    source_mtime_ns: int
    templates: tuple[DateTemplatePayload, ...]


class FrontendPlanService:
    """Serve local precache plans with refresh-on-change behavior."""

    def __init__(
        self,
        *,
        plans_api_path: Path | str = DEFAULT_FRONTEND_API_OUTPUT_PATH,
        assets_dir: Path | str = DEFAULT_FRONTEND_IMAGES_DIR,
        date_templates_path: Path | str | None = None,
        location_resolver: TypedLocationResolver | None = None,
    ) -> None:
        self._plans_api_path = _repo_path(plans_api_path)
        self._assets_dir = _repo_path(assets_dir)
        resolved_templates_path = (
            load_rag_settings().date_templates_path
            if date_templates_path is None
            else date_templates_path
        )
        self._date_templates_path = _repo_path(resolved_templates_path)
        self._lock = asyncio.Lock()
        self._templates_lock = asyncio.Lock()
        self._snapshot: _Snapshot | None = None
        self._template_snapshot: _TemplateSnapshot | None = None
        if location_resolver is None:
            repository = PlacesRepository(load_query_settings())
            location_resolver = TypedLocationResolver(repository)
        self._location_resolver = location_resolver

    @property
    def plans_api_path(self) -> Path:
        return self._plans_api_path

    @property
    def assets_dir(self) -> Path:
        return self._assets_dir

    @property
    def date_templates_path(self) -> Path:
        return self._date_templates_path

    async def health(self) -> dict[str, Any]:
        snapshot = await self._maybe_snapshot()
        image_count = 0
        if self._assets_dir.exists():
            image_count = sum(1 for _ in self._assets_dir.rglob("*") if _.is_file())
        return {
            "status": "ok" if snapshot is not None else "degraded",
            "plansReady": snapshot is not None,
            "plansCount": 0 if snapshot is None else len(snapshot.records),
            "assetsReady": self._assets_dir.exists(),
            "imagesCount": image_count,
            "source": {
                "plansApiPath": str(self._plans_api_path),
                "assetsDir": str(self._assets_dir),
                "plansApiExists": self._plans_api_path.exists(),
            },
        }

    async def list_templates(self) -> list[DateTemplatePayload]:
        snapshot = await self._require_template_snapshot()
        return list(snapshot.templates)

    async def metadata(self) -> dict[str, Any]:
        snapshot = await self._require_snapshot()
        return {
            "totalPlans": len(snapshot.records),
            "buckets": [
                {"id": key, "label": key.replace("_", " ").title(), "count": count}
                for key, count in snapshot.bucket_counts
            ],
            "templates": [
                {"id": key, "label": key.replace("_", " ").title(), "count": count}
                for key, count in snapshot.template_counts
            ],
            "vibes": [{"id": key, "label": key.title(), "count": count} for key, count in snapshot.vibe_counts],
        }

    async def get_plan(self, *, plan_id: str, public_base_url: str) -> PlanPayload:
        snapshot = await self._require_snapshot()
        record = snapshot.by_plan_id.get(plan_id)
        if record is None:
            raise KeyError(plan_id)
        return self._to_plan_payload(
            record=record,
            public_base_url=public_base_url,
            requested_budget=None,
            requested_party_size=None,
        )

    async def list_plans(
        self,
        *,
        limit: int,
        bucket_id: str | None,
        template_id: str | None,
        vibe: str | None,
        public_base_url: str,
    ) -> list[PlanPayload]:
        snapshot = await self._require_snapshot()
        records = list(snapshot.records)
        if bucket_id:
            records = [record for record in records if record.bucket_id == bucket_id]
        if template_id:
            records = [record for record in records if record.template_id == template_id]
        if vibe:
            tokens = _tokenize(vibe)
            records = [record for record in records if tokens & record.vibe_tokens]
        records = records[:limit]
        return [
            self._to_plan_payload(
                record=record,
                public_base_url=public_base_url,
                requested_budget=None,
                requested_party_size=None,
            )
            for record in records
        ]

    async def generate(
        self,
        *,
        request: dict[str, Any],
        public_base_url: str,
    ) -> tuple[list[PlanPayload], list[str], dict[str, int]]:
        snapshot = await self._require_snapshot()
        warnings: list[str] = []
        location_text = str(request["location"]).strip()
        vibe_text = str(request["vibe"]).strip()
        radius_km = float(request["radiusKm"])
        requested_transport = _TRANSPORT_ALIASES.get(str(request["transportMode"]).strip().lower())
        requested_hour = _parse_start_hour(request.get("startTime"))
        requested_duration_minutes = int(request["durationMinutes"])
        requested_budget = _optional_text(request.get("budget"))
        requested_party_size = _optional_int(request.get("partySize"))
        limit = int(request["limit"])

        if requested_budget:
            warnings.append(
                "Budget filtering is not applied because the local cached plans do not include reliable price data."
            )

        resolved_location = await self._resolve_location(location_text=location_text, warnings=warnings)
        vibe_tokens = _tokenize(vibe_text)
        ranked: list[tuple[float, _ApiPlanRecord]] = []

        for record in snapshot.records:
            distance_km = _distance_to_record(record, resolved_location.anchor_latitude, resolved_location.anchor_longitude)
            if distance_km is None:
                logger.error("Plan %s has no usable coordinates and will be skipped.", record.plan_id)
                continue
            if distance_km > radius_km:
                continue
            vibe_overlap = len(vibe_tokens & record.vibe_tokens)
            text_overlap = len(vibe_tokens & record.search_tokens)
            vibe_score = vibe_overlap * 4.0 + min(text_overlap, 6) * 0.5
            distance_score = max(0.0, radius_km - distance_km)
            transport_score = 0.0
            if requested_transport and record.transport_mode:
                if requested_transport == record.transport_mode:
                    transport_score = 1.5
                else:
                    transport_score = -0.75
            time_score = 0.0
            if requested_hour is not None and record.plan_hour_local is not None:
                time_score = max(0.0, 3.0 - abs(requested_hour - record.plan_hour_local) * 0.75)
            duration_score = 0.0
            if record.template_duration_hours is not None:
                duration_score = max(
                    0.0,
                    2.0 - abs(int(record.template_duration_hours * 60) - requested_duration_minutes) / 90.0,
                )
            total_score = vibe_score + distance_score + transport_score + time_score + duration_score
            if total_score <= 0:
                continue
            ranked.append((total_score, record))

        ranked.sort(key=lambda item: (-item[0], item[1].plan_title, item[1].plan_id))
        matched_records = [record for _, record in ranked]
        returned_records = matched_records[:limit]
        return (
            [
                self._to_plan_payload(
                    record=record,
                    public_base_url=public_base_url,
                    requested_budget=requested_budget,
                    requested_party_size=requested_party_size,
                )
                for record in returned_records
            ],
            warnings,
            {
                "matchedCount": len(matched_records),
                "returnedCount": len(returned_records),
                "totalAvailable": len(snapshot.records),
            },
        )

    async def _maybe_snapshot(self) -> _Snapshot | None:
        try:
            return await self._require_snapshot()
        except FrontendApiError:
            return None

    async def _require_snapshot(self) -> _Snapshot:
        if not self._plans_api_path.exists():
            logger.error("Plans API parquet is missing at %s.", self._plans_api_path)
            raise FrontendApiError(
                "Frontend plans parquet is missing. Run the precache asset sync first."
            )
        stat_result = self._plans_api_path.stat()
        cached = self._snapshot
        if cached is not None and cached.source_mtime_ns == stat_result.st_mtime_ns:
            return cached
        async with self._lock:
            cached = self._snapshot
            stat_result = self._plans_api_path.stat()
            if cached is not None and cached.source_mtime_ns == stat_result.st_mtime_ns:
                return cached
            snapshot = await asyncio.to_thread(self._load_snapshot, stat_result.st_mtime_ns)
            self._snapshot = snapshot
            return snapshot

    async def _require_template_snapshot(self) -> _TemplateSnapshot:
        if not self._date_templates_path.exists():
            logger.error("Date templates YAML is missing at %s.", self._date_templates_path)
            raise FrontendApiError(
                "Date templates YAML is missing. Restore config/date_templates.yaml."
            )
        stat_result = self._date_templates_path.stat()
        cached = self._template_snapshot
        if cached is not None and cached.source_mtime_ns == stat_result.st_mtime_ns:
            return cached
        async with self._templates_lock:
            cached = self._template_snapshot
            stat_result = self._date_templates_path.stat()
            if cached is not None and cached.source_mtime_ns == stat_result.st_mtime_ns:
                return cached
            snapshot = await asyncio.to_thread(
                self._load_template_snapshot,
                stat_result.st_mtime_ns,
            )
            self._template_snapshot = snapshot
            return snapshot

    async def _resolve_location(self, *, location_text: str, warnings: list[str]) -> Any:
        try:
            return await asyncio.to_thread(self._location_resolver.resolve, location_text)
        except LocationAmbiguityError:
            if "," in location_text or location_text.strip().isdigit():
                raise
            fallback = f"{location_text}, NSW"
            resolved = await asyncio.to_thread(self._location_resolver.resolve, fallback)
            warning = (
                f"Location {location_text!r} was ambiguous in the local dataset, so the API assumed NSW and searched {fallback!r}."
            )
            logger.warning(warning)
            warnings.append(warning)
            return resolved
        except LocationResolutionError:
            raise

    def _load_snapshot(self, source_mtime_ns: int) -> _Snapshot:
        df = pd.read_parquet(self._plans_api_path)
        required_columns = {
            "plan_id",
            "template_id",
            "bucket_id",
            "plan_title",
            "bucket_label",
            "hero_image_relative_path",
            "api_payload_json",
            "generated_at_utc",
            "source_written_at_utc",
        }
        missing = sorted(required_columns - set(df.columns))
        if missing:
            logger.error("Plans API parquet %s is missing columns %s.", self._plans_api_path, missing)
            raise FrontendApiError(
                f"Frontend plans parquet is invalid. Missing columns: {missing}."
            )
        records: list[_ApiPlanRecord] = []
        bucket_counts: dict[str, int] = {}
        template_counts: dict[str, int] = {}
        vibe_counts: dict[str, int] = {}
        for row in df.itertuples(index=False):
            row_map = row._asdict()
            raw_payload = row_map.get("api_payload_json")
            if not isinstance(raw_payload, str) or not raw_payload.strip():
                logger.error("Plan row %s has blank api_payload_json; skipping.", row_map.get("plan_id"))
                continue
            try:
                payload = json.loads(raw_payload)
            except json.JSONDecodeError as exc:
                logger.error("Plan row %s has invalid api_payload_json; skipping.", row_map.get("plan_id"))
                logger.exception(exc)
                continue
            record = _record_from_row(row_map=row_map, payload=payload)
            records.append(record)
            bucket_counts[record.bucket_id] = bucket_counts.get(record.bucket_id, 0) + 1
            template_counts[record.template_id] = template_counts.get(record.template_id, 0) + 1
            for vibe in record.vibe_tokens:
                vibe_counts[vibe] = vibe_counts.get(vibe, 0) + 1
        by_plan_id = {record.plan_id: record for record in records}
        return _Snapshot(
            source_mtime_ns=source_mtime_ns,
            records=tuple(records),
            by_plan_id=by_plan_id,
            bucket_counts=tuple(sorted(bucket_counts.items())),
            template_counts=tuple(sorted(template_counts.items())),
            vibe_counts=tuple(sorted(vibe_counts.items())),
        )

    def _load_template_snapshot(self, source_mtime_ns: int) -> _TemplateSnapshot:
        try:
            raw_templates = load_date_templates(self._date_templates_path)
        except (FileNotFoundError, RagRetrieverError) as exc:
            logger.error(
                "Failed to load date templates from %s: %s",
                self._date_templates_path,
                exc,
            )
            raise FrontendApiError(
                f"Date templates could not be loaded from {self._date_templates_path}."
            ) from exc

        templates: list[DateTemplatePayload] = []
        for index, raw_template in enumerate(raw_templates):
            if not isinstance(raw_template, dict):
                logger.error("Template row %s is not a mapping and will be skipped.", index)
                continue
            try:
                templates.append(_template_payload_from_mapping(raw_template))
            except FrontendApiError as exc:
                template_id = _optional_text(raw_template.get("id")) or f"index={index}"
                logger.error("Template %s is invalid: %s", template_id, exc)
                raise

        if not templates:
            logger.error("No valid templates were loaded from %s.", self._date_templates_path)
            raise FrontendApiError("Date templates YAML did not contain any valid templates.")

        return _TemplateSnapshot(
            source_mtime_ns=source_mtime_ns,
            templates=tuple(templates),
        )

    def _to_plan_payload(
        self,
        *,
        record: _ApiPlanRecord,
        public_base_url: str,
        requested_budget: str | None,
        requested_party_size: int | None,
    ) -> PlanPayload:
        payload = record.payload
        stops_data = payload.get("stops")
        if not isinstance(stops_data, list):
            raise FrontendApiError(f"Plan {record.plan_id} is missing stop data.")
        legs = payload.get("legs") if isinstance(payload.get("legs"), list) else []
        plan_time_iso = _optional_text(payload.get("plan_time_iso"))
        stop_times = _estimate_stop_times(
            plan_time_iso=plan_time_iso,
            stop_count=len(stops_data),
        )
        stop_times_iso = _estimate_stop_times_iso(
            plan_time_iso=_optional_text(payload.get("plan_time_iso")),
            stop_count=len(stops_data),
        )
        stops: list[PlanStop] = []
        for index, stop in enumerate(stops_data):
            if not isinstance(stop, dict):
                logger.error("Plan %s contains a non-object stop payload.", record.plan_id)
                continue
            transport_text = None
            if index < len(legs) and isinstance(legs[index], dict):
                transport_text = _format_leg_transport(legs[index])
            stops.append(
                PlanStop(
                    id=_optional_text(stop.get("fsq_place_id"))
                    or _optional_text(stop.get("google_place_id"))
                    or f"{record.plan_id}-stop-{index+1}",
                    kind=_stop_kind(stop),
                    stopType=_optional_text(stop.get("stop_type")) or "venue",
                    name=_required_text(stop.get("name"), field_name="stop.name"),
                    description=_optional_text(stop.get("llm_description")) or "",
                    whyItFits=_optional_text(stop.get("why_it_fits")),
                    time=None if index >= len(stop_times) else stop_times[index],
                    transport=transport_text,
                    mapsUrl=_optional_text(stop.get("google_maps_uri")),
                    address=_optional_text(stop.get("address")),
                    phoneNumber=_extract_booking_phone_number(
                        stop,
                        plan_id=record.plan_id,
                        stop_index=index,
                    ),
                )
            )
        booking_context = _build_booking_context(
            plan_id=record.plan_id,
            plan_time_iso=plan_time_iso,
            stops_data=stops_data,
            stop_times_iso=stop_times_iso,
            requested_party_size=requested_party_size,
        )
        return PlanPayload(
            id=record.plan_id,
            title=record.plan_title,
            hook=_optional_text(payload.get("plan_hook")) or "A cached date plan.",
            summary=_optional_text(payload.get("template_description"))
            or _optional_text(payload.get("plan_hook")),
            vibes=_string_list(payload.get("vibe")),
            templateHint=_optional_text(payload.get("template_title")) or record.template_id,
            templateId=record.template_id,
            heroImageUrl=_absolute_image_url(
                relative_path=record.hero_image_relative_path,
                public_base_url=public_base_url,
            ),
            durationLabel=_format_duration_label(record.template_duration_hours, len(stops)),
            costBand=requested_budget or "Unspecified",
            weather=None,
            mapsVerificationNeeded=_maps_verification_needed(payload.get("feasibility")),
            constraintsConsidered=[],
            stops=stops,
            transportLegs=[_transport_leg_payload(leg) for leg in legs if isinstance(leg, dict)],
            bookingContext=booking_context,
            source="api",
        )


def _record_from_row(*, row_map: dict[str, Any], payload: dict[str, Any]) -> _ApiPlanRecord:
    stops = payload.get("stops")
    locations = []
    if isinstance(stops, list):
        for stop in stops:
            if not isinstance(stop, dict):
                continue
            location = stop.get("location")
            if not isinstance(location, dict):
                continue
            latitude = _optional_float(location.get("latitude"))
            longitude = _optional_float(location.get("longitude"))
            if latitude is None or longitude is None:
                continue
            locations.append((latitude, longitude))
    center_latitude = None
    center_longitude = None
    if locations:
        center_latitude = sum(latitude for latitude, _ in locations) / len(locations)
        center_longitude = sum(longitude for _, longitude in locations) / len(locations)
    return _ApiPlanRecord(
        plan_id=_required_text(row_map.get("plan_id"), field_name="plan_id"),
        template_id=_required_text(row_map.get("template_id"), field_name="template_id"),
        bucket_id=_required_text(row_map.get("bucket_id"), field_name="bucket_id"),
        plan_title=_required_text(row_map.get("plan_title"), field_name="plan_title"),
        bucket_label=_required_text(row_map.get("bucket_label"), field_name="bucket_label"),
        hero_image_relative_path=_optional_text(row_map.get("hero_image_relative_path")),
        payload=payload,
        generated_at_utc=_optional_text(row_map.get("generated_at_utc")),
        source_written_at_utc=_optional_text(row_map.get("source_written_at_utc")),
        template_duration_hours=_optional_float(payload.get("template_duration_hours")),
        vibe_tokens=frozenset(_tokenize(" ".join(_string_list(payload.get("vibe"))))),
        search_tokens=frozenset(_tokenize(_optional_text(payload.get("search_text")))),
        center_latitude=center_latitude,
        center_longitude=center_longitude,
        plan_hour_local=_plan_hour(_optional_text(payload.get("plan_time_iso"))),
        transport_mode=_optional_text(payload.get("transport_mode")),
    )


def _template_payload_from_mapping(raw_template: dict[str, Any]) -> DateTemplatePayload:
    stops_raw = raw_template.get("stops")
    if not isinstance(stops_raw, list):
        raise FrontendApiError("template.stops must be a list.")

    vibes = _string_list(raw_template.get("vibe"))
    if not vibes:
        raise FrontendApiError("template.vibe must contain at least one entry.")

    stops: list[DateTemplateStopPayload] = []
    for index, raw_stop in enumerate(stops_raw):
        if not isinstance(raw_stop, dict):
            raise FrontendApiError(f"template.stops[{index}] must be a mapping.")
        stops.append(
            DateTemplateStopPayload(
                type=_required_text(raw_stop.get("type"), field_name=f"template.stops[{index}].type"),
                kind="connective"
                if _optional_text(raw_stop.get("kind")) == "connective"
                else "venue",
                note=_optional_text(raw_stop.get("note")),
            )
        )

    meaningful_variations = _optional_int(raw_template.get("meaningful_variations"))
    if meaningful_variations is None or meaningful_variations < 0:
        raise FrontendApiError("template.meaningful_variations must be a non-negative integer.")

    duration_hours = _optional_float(raw_template.get("duration_hours"))
    if duration_hours is None or duration_hours <= 0:
        raise FrontendApiError("template.duration_hours must be a positive number.")

    weather_sensitive = raw_template.get("weather_sensitive")
    if not isinstance(weather_sensitive, bool):
        raise FrontendApiError("template.weather_sensitive must be a boolean.")

    return DateTemplatePayload(
        id=_required_text(raw_template.get("id"), field_name="template.id"),
        title=_required_text(raw_template.get("title"), field_name="template.title"),
        vibes=vibes,
        timeOfDay=_required_text(raw_template.get("time_of_day"), field_name="template.time_of_day"),
        durationHours=duration_hours,
        meaningfulVariations=meaningful_variations,
        weatherSensitive=weather_sensitive,
        description=_required_text(raw_template.get("description"), field_name="template.description"),
        stops=stops,
    )


def _distance_to_record(record: _ApiPlanRecord, latitude: float, longitude: float) -> float | None:
    if record.center_latitude is None or record.center_longitude is None:
        return None
    return _haversine_km(latitude, longitude, record.center_latitude, record.center_longitude)


def _transport_leg_payload(leg: dict[str, Any]) -> TransportLeg:
    mode = _optional_text(leg.get("transport_mode")) or "Unknown"
    return TransportLeg(
        mode=mode.title(),
        durationText=_format_duration_text(_optional_float(leg.get("duration_seconds"))),
    )


def _format_leg_transport(leg: dict[str, Any]) -> str | None:
    duration_text = _format_duration_text(_optional_float(leg.get("duration_seconds")))
    mode = _optional_text(leg.get("transport_mode"))
    if mode is None and duration_text == "Unknown":
        return None
    clean_mode = "Unknown" if mode is None else mode.title()
    return f"{clean_mode} · {duration_text}"


def _format_vibe_line(value: Any) -> str:
    vibes = _string_list(value)
    if not vibes:
        return "Maps-verified local date plan"
    return " · ".join(item.replace("_", " ").title() for item in vibes)


def _format_duration_label(duration_hours: float | None, stop_count: int) -> str:
    if duration_hours is not None and duration_hours > 0:
        total_minutes = int(round(duration_hours * 60))
    else:
        total_minutes = max(90, stop_count * 60)
    hours = total_minutes // 60
    minutes = total_minutes % 60
    if minutes == 0:
        return f"{hours} hours" if hours != 1 else "1 hour"
    if hours == 0:
        return f"{minutes} mins"
    return f"{hours}h {minutes}m"


def _format_duration_text(duration_seconds: float | None) -> str:
    if duration_seconds is None or duration_seconds <= 0:
        return "Unknown"
    minutes = int(round(duration_seconds / 60.0))
    if minutes < 60:
        return f"{minutes} min"
    hours = minutes // 60
    remainder = minutes % 60
    if remainder == 0:
        return f"{hours} hr"
    return f"{hours} hr {remainder} min"


def _maps_verification_needed(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    flags = (
        value.get("all_legs_under_threshold"),
        value.get("all_open_at_plan_time"),
        value.get("all_venues_matched"),
    )
    normalized = [flag for flag in flags if isinstance(flag, bool)]
    if not normalized:
        return False
    return not all(normalized)


def _stop_kind(stop: dict[str, Any]) -> str:
    kind = _optional_text(stop.get("kind"))
    if kind == "connective":
        return "connective"
    return "venue"


def _estimate_stop_times(*, plan_time_iso: str | None, stop_count: int) -> list[str]:
    if plan_time_iso is None or stop_count <= 0:
        return []
    parsed = pd.Timestamp(plan_time_iso)
    out = []
    for index in range(stop_count):
        out.append((parsed + pd.Timedelta(minutes=index * 75)).strftime("%-I:%M %p"))
    return out


def _estimate_stop_times_iso(*, plan_time_iso: str | None, stop_count: int) -> list[str]:
    if plan_time_iso is None or stop_count <= 0:
        return []
    parsed = pd.Timestamp(plan_time_iso)
    out: list[str] = []
    for index in range(stop_count):
        out.append((parsed + pd.Timedelta(minutes=index * 75)).isoformat())
    return out


def _build_booking_context(
    *,
    plan_id: str,
    plan_time_iso: str | None,
    stops_data: list[Any],
    stop_times_iso: list[str],
    requested_party_size: int | None,
) -> BookingContextPayload | None:
    chosen_index: int | None = None
    chosen_stop: dict[str, Any] | None = None

    for index, stop in enumerate(stops_data):
        if not isinstance(stop, dict):
            continue
        stop_type = (_optional_text(stop.get("stop_type")) or "").casefold()
        if "restaurant" in stop_type:
            chosen_index = index
            chosen_stop = stop
            break

    if chosen_stop is None:
        for index, stop in enumerate(stops_data):
            if not isinstance(stop, dict):
                continue
            signals = {item.casefold() for item in _string_list(stop.get("booking_signals"))}
            if {"booking", "third_party_booking"} & signals:
                chosen_index = index
                chosen_stop = stop
                break

    if chosen_stop is None:
        return None

    restaurant_name = _optional_text(chosen_stop.get("name"))
    if restaurant_name is None:
        logger.error("Plan %s booking candidate stop is missing a name.", plan_id)
        return None

    arrival_iso = None
    if chosen_index is not None and chosen_index < len(stop_times_iso):
        arrival_iso = stop_times_iso[chosen_index]
    elif plan_time_iso is not None:
        arrival_iso = plan_time_iso

    return BookingContextPayload(
        planId=plan_id,
        restaurantName=restaurant_name,
        restaurantPhoneNumber=_extract_booking_phone_number(
            chosen_stop,
            plan_id=plan_id,
            stop_index=chosen_index,
        ),
        restaurantAddress=_optional_text(chosen_stop.get("address")),
        suggestedArrivalTimeIso=arrival_iso,
        partySize=requested_party_size or 2,
    )


def _extract_booking_phone_number(
    stop: dict[str, Any],
    *,
    plan_id: str,
    stop_index: int | None,
) -> str | None:
    for key in (
        "phone_number",
        "restaurant_phone_number",
        "international_phone_number",
        "formatted_phone_number",
        "national_phone_number",
    ):
        value = _optional_text(stop.get(key))
        if value is None:
            continue
        if _E164_PHONE_RE.fullmatch(value):
            return value
        logger.warning(
            "Plan %s stop %s has non-E.164 phone number in %s; omitting unsafe prefill: %r",
            plan_id,
            stop_index,
            key,
            value,
        )
        return None
    return None


def _plan_hour(plan_time_iso: str | None) -> int | None:
    if plan_time_iso is None:
        return None
    try:
        return int(pd.Timestamp(plan_time_iso).hour)
    except Exception:
        return None


def _parse_start_hour(value: Any) -> int | None:
    text = _optional_text(value)
    if text is None:
        return None
    match = _TIME_RE.fullmatch(text)
    if match is None:
        return None
    hour = int(match.group("hour"))
    minute = int(match.group("minute"))
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return hour


def _absolute_image_url(*, relative_path: str | None, public_base_url: str) -> str | None:
    if relative_path is None:
        return None
    return urljoin(public_base_url, f"{_STATIC_ROUTE}/{relative_path}")


def _tokenize(text: str | None) -> set[str]:
    if text is None:
        return set()
    return set(_TOKEN_RE.findall(text.casefold()))


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, (list, tuple)):
        out: list[str] = []
        for item in value:
            text = _optional_text(item)
            if text is not None:
                out.append(text)
        return out
    return []


def _required_text(value: Any, *, field_name: str) -> str:
    text = _optional_text(value)
    if text is None:
        raise FrontendApiError(f"{field_name} must be a non-empty string.")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _repo_path(value: Path | str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return REPO_ROOT / path
