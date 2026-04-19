"""Google Maps Places and Routes client for backend use."""

from __future__ import annotations

import asyncio
import logging
import math
import re
from dataclasses import dataclass
from datetime import timezone
from difflib import SequenceMatcher
from typing import Any

import httpx

from back_end.domain.models import (
    CandidatePlace,
    ComputedRoute,
    LatLng,
    MapsOpeningHours,
    MapsOpeningPeriod,
    MapsOpeningPoint,
    MapsPlace,
    MapsPlaceMatch,
    PhotoAsset,
    PhotoAuthorAttribution,
    PhotoMedia,
    RouteLeg,
    RouteRequest,
    RouteStep,
    RouteStepTransitDetails,
    RouteStepTransitLine,
)
from back_end.clients.settings import MapsSettings

logger = logging.getLogger(__name__)

TEXT_SEARCH_FIELD_MASK = ",".join(
    (
        "places.id",
        "places.name",
        "places.displayName",
        "places.formattedAddress",
        "places.location",
        "places.googleMapsUri",
        "places.businessStatus",
        "places.rating",
        "places.userRatingCount",
        "places.regularOpeningHours",
        "places.photos",
        "places.postalAddress",
    )
)

PLACE_DETAILS_FIELD_MASK = ",".join(
    (
        "id",
        "name",
        "displayName",
        "formattedAddress",
        "location",
        "googleMapsUri",
        "businessStatus",
        "rating",
        "userRatingCount",
        "regularOpeningHours",
        "photos",
        "postalAddress",
    )
)

COMPUTE_ROUTES_FIELD_MASK = ",".join(
    (
        "routes.distanceMeters",
        "routes.duration",
        "routes.staticDuration",
        "routes.polyline.encodedPolyline",
        "routes.warnings",
        "routes.legs.distanceMeters",
        "routes.legs.duration",
        "routes.legs.staticDuration",
        "routes.legs.steps.distanceMeters",
        "routes.legs.steps.staticDuration",
        "routes.legs.steps.travelMode",
        "routes.legs.steps.navigationInstruction.instructions",
        "routes.legs.steps.transitDetails.stopDetails",
        "routes.legs.steps.transitDetails.headsign",
        "routes.legs.steps.transitDetails.stopCount",
        "routes.legs.steps.transitDetails.transitLine.name",
        "routes.legs.steps.transitDetails.transitLine.nameShort",
        "routes.legs.steps.transitDetails.transitLine.vehicle.type",
    )
)

RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
_WHITESPACE_RE = re.compile(r"\s+")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_REGION_ALIASES = {
    "nsw": "new south wales",
    "new south wales": "new south wales",
    "vic": "victoria",
    "victoria": "victoria",
    "qld": "queensland",
    "queensland": "queensland",
    "sa": "south australia",
    "south australia": "south australia",
    "wa": "western australia",
    "western australia": "western australia",
    "tas": "tasmania",
    "tasmania": "tasmania",
    "nt": "northern territory",
    "northern territory": "northern territory",
    "act": "australian capital territory",
    "australian capital territory": "australian capital territory",
}


class MapsClientError(RuntimeError):
    """Base class for Maps client errors."""


class MapsUpstreamError(MapsClientError):
    """Raised when Google Maps rejects a request or returns invalid HTTP."""


class MapsResponseSchemaError(MapsClientError):
    """Raised when Google Maps returns an unexpected response shape."""


class PlaceMatchError(MapsClientError):
    """Base class for place-match failures."""


class NoPlaceMatchError(PlaceMatchError):
    """Raised when no confident place match can be found."""


class AmbiguousPlaceMatchError(PlaceMatchError):
    """Raised when multiple plausible place matches survive validation."""


@dataclass(frozen=True)
class _ScoredPlaceCandidate:
    place: MapsPlace
    exact_name: bool
    name_similarity: float
    straight_line_distance_meters: float
    postcode_match: bool
    locality_match: bool
    region_match: bool

    @property
    def sort_key(self) -> tuple[int, int, int, float, float]:
        return (
            1 if self.exact_name else 0,
            1 if self.postcode_match else 0,
            1 if self.locality_match else 0,
            self.name_similarity,
            -self.straight_line_distance_meters,
        )


class GoogleMapsClient:
    """Purpose-built client for Date Night's Google Maps usage."""

    def __init__(
        self,
        settings: MapsSettings,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = settings
        self._http_client = http_client or httpx.AsyncClient()
        self._owns_http_client = http_client is None

    async def __aenter__(self) -> "GoogleMapsClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_http_client:
            await self._http_client.aclose()

    def close(self) -> None:
        """Compatibility shim for non-async callers.

        Networking is async. Prefer ``await client.aclose()`` or
        ``async with GoogleMapsClient(...)`` in async code.
        """

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self.aclose())
            return
        raise RuntimeError(
            "GoogleMapsClient.close() was called inside a running event loop. "
            "Use 'await client.aclose()' instead."
        )

    async def search_text_places(
        self, candidate: CandidatePlace
    ) -> tuple[MapsPlace, ...]:
        """Search Google Places for a local candidate."""

        coordinates = candidate.coordinates
        if coordinates is None:
            raise NoPlaceMatchError(
                f"Candidate {candidate.fsq_place_id} is missing coordinates; "
                "Google place matching requires latitude and longitude."
            )

        query_parts = [candidate.name]
        if candidate.locality:
            query_parts.append(candidate.locality)
        if candidate.region:
            query_parts.append(candidate.region)
        if candidate.postcode:
            query_parts.append(candidate.postcode)
        query = ", ".join(query_parts)

        bias_radius = max(
            self._settings.text_search_location_bias_radius_meters,
            self._settings.max_match_distance_meters,
        )

        body = {
            "textQuery": query,
            "pageSize": self._settings.text_search_result_limit,
            "locationBias": {
                "circle": {
                    "center": {
                        "latitude": coordinates.latitude,
                        "longitude": coordinates.longitude,
                    },
                    "radius": bias_radius,
                }
            },
        }

        payload = await self._request_json(
            "POST",
            f"{self._settings.places_base_url}/places:searchText",
            field_mask=TEXT_SEARCH_FIELD_MASK,
            json=body,
        )
        raw_places = payload.get("places", [])
        if not isinstance(raw_places, list):
            raise MapsResponseSchemaError(
                "Text Search response did not contain a list in 'places'."
            )
        return tuple(self._parse_place(place) for place in raw_places)

    async def resolve_place_match(self, candidate: CandidatePlace) -> MapsPlaceMatch:
        """Resolve a local place into a single confident Google place match."""

        places = await self.search_text_places(candidate)
        if not places:
            raise NoPlaceMatchError(
                f"No Google Places results found for candidate {candidate.fsq_place_id}."
            )

        coordinates = candidate.coordinates
        if coordinates is None:
            raise NoPlaceMatchError(
                f"Candidate {candidate.fsq_place_id} is missing coordinates."
            )

        scored_candidates: list[_ScoredPlaceCandidate] = []
        rejection_reasons: list[str] = []

        for place in places:
            distance_meters = _haversine_meters(coordinates, place.location)
            if distance_meters > self._settings.max_match_distance_meters:
                rejection_reasons.append(
                    f"{place.place_id}: distance {distance_meters:.1f}m exceeds "
                    f"threshold {self._settings.max_match_distance_meters:.1f}m"
                )
                continue

            if not _address_compatible(candidate, place):
                rejection_reasons.append(
                    f"{place.place_id}: address mismatch against candidate locality/"
                    "region/postcode"
                )
                continue

            normalized_candidate_name = _normalize_name(candidate.name)
            normalized_google_name = _normalize_name(place.display_name)
            exact_name = normalized_candidate_name == normalized_google_name
            name_similarity = SequenceMatcher(
                None, normalized_candidate_name, normalized_google_name
            ).ratio()
            if not exact_name and name_similarity < self._settings.min_name_similarity:
                rejection_reasons.append(
                    f"{place.place_id}: name similarity {name_similarity:.3f} below "
                    f"threshold {self._settings.min_name_similarity:.3f}"
                )
                continue

            scored_candidates.append(
                _ScoredPlaceCandidate(
                    place=place,
                    exact_name=exact_name,
                    name_similarity=name_similarity,
                    straight_line_distance_meters=distance_meters,
                    postcode_match=_postcode_match(candidate, place),
                    locality_match=_locality_match(candidate, place),
                    region_match=_region_match(candidate, place),
                )
            )

        if not scored_candidates:
            logger.error(
                "No confident Google place match for fsq_place_id=%s name=%r. "
                "Rejected results: %s",
                candidate.fsq_place_id,
                candidate.name,
                "; ".join(rejection_reasons) or "no candidate results",
            )
            raise NoPlaceMatchError(
                f"No confident Google place match for candidate "
                f"{candidate.fsq_place_id}."
            )

        scored_candidates.sort(key=lambda item: item.sort_key, reverse=True)
        best = scored_candidates[0]
        if len(scored_candidates) > 1:
            second = scored_candidates[1]
            if _is_ambiguous(best, second):
                raise AmbiguousPlaceMatchError(
                    f"Ambiguous Google place matches for candidate "
                    f"{candidate.fsq_place_id}: {best.place.place_id} and "
                    f"{second.place.place_id} are both plausible."
                )

        return MapsPlaceMatch(
            candidate_place=candidate,
            google_place=best.place,
            straight_line_distance_meters=best.straight_line_distance_meters,
            name_similarity=best.name_similarity,
        )

    async def get_place_details(self, place_id: str) -> MapsPlace:
        """Fetch place details for a Google place ID."""

        payload = await self._request_json(
            "GET",
            f"{self._settings.places_base_url}/places/{place_id}",
            field_mask=PLACE_DETAILS_FIELD_MASK,
        )
        return self._parse_place(payload)

    async def get_photo_media(
        self,
        photo_name: str,
        *,
        max_width_px: int | None = None,
        max_height_px: int | None = None,
    ) -> PhotoMedia:
        """Resolve a photo reference to a short-lived photo URI.

        This uses ``skipHttpRedirect=true`` so the backend gets JSON rather than
        a direct image redirect. The returned ``photo_uri`` is the safe handoff
        to later server-side logic; photo names themselves are not durable.
        """

        width = max_width_px or self._settings.default_photo_max_width_px
        height = max_height_px or self._settings.default_photo_max_height_px
        payload = await self._request_json(
            "GET",
            f"{self._settings.places_base_url}/{photo_name}/media",
            params={
                "maxWidthPx": width,
                "maxHeightPx": height,
                "skipHttpRedirect": "true",
                "key": self._settings.api_key,
            },
            include_api_key_header=False,
        )
        name = payload.get("name")
        photo_uri = payload.get("photoUri")
        if not isinstance(name, str) or not isinstance(photo_uri, str):
            raise MapsResponseSchemaError(
                "Photo media response is missing 'name' or 'photoUri'."
            )
        return PhotoMedia(name=name, photo_uri=photo_uri)

    async def compute_route(self, route_request: RouteRequest) -> ComputedRoute:
        """Compute a single route between two coordinates."""

        body: dict[str, Any] = {
            "origin": _waypoint(route_request.origin),
            "destination": _waypoint(route_request.destination),
            "travelMode": route_request.travel_mode.value,
        }
        if route_request.departure_time is not None:
            body["departureTime"] = _to_rfc3339(route_request.departure_time)
        if route_request.arrival_time is not None:
            body["arrivalTime"] = _to_rfc3339(route_request.arrival_time)

        payload = await self._request_json(
            "POST",
            f"{self._settings.routes_base_url}:computeRoutes",
            field_mask=COMPUTE_ROUTES_FIELD_MASK,
            json=body,
        )

        routes = payload.get("routes")
        if not isinstance(routes, list) or not routes:
            raise MapsResponseSchemaError(
                "Compute Routes response did not contain any routes."
            )
        route = routes[0]
        if not isinstance(route, dict):
            raise MapsResponseSchemaError("Compute Routes returned a non-object route.")

        legs_raw = route.get("legs", [])
        if not isinstance(legs_raw, list):
            raise MapsResponseSchemaError("Route 'legs' field is not a list.")

        legs = tuple(self._parse_route_leg(leg) for leg in legs_raw)
        polyline = route.get("polyline", {})
        encoded_polyline = None
        if isinstance(polyline, dict):
            encoded_polyline = _optional_str(polyline.get("encodedPolyline"))

        warnings_raw = route.get("warnings", [])
        if warnings_raw is None:
            warnings_raw = []
        if not isinstance(warnings_raw, list) or any(
            not isinstance(item, str) for item in warnings_raw
        ):
            raise MapsResponseSchemaError("Route warnings must be a list of strings.")

        return ComputedRoute(
            distance_meters=_optional_int(route.get("distanceMeters")),
            duration_seconds=_parse_duration_seconds(route.get("duration")),
            static_duration_seconds=_parse_duration_seconds(route.get("staticDuration")),
            polyline=encoded_polyline,
            warnings=tuple(warnings_raw),
            legs=legs,
        )

    def _parse_route_leg(self, raw_leg: dict[str, Any]) -> RouteLeg:
        if not isinstance(raw_leg, dict):
            raise MapsResponseSchemaError("Route leg must be an object.")
        raw_steps = raw_leg.get("steps", [])
        if raw_steps is None:
            raw_steps = []
        if not isinstance(raw_steps, list):
            raise MapsResponseSchemaError("Route leg steps must be a list.")
        return RouteLeg(
            distance_meters=_optional_int(raw_leg.get("distanceMeters")),
            duration_seconds=_parse_duration_seconds(raw_leg.get("duration")),
            static_duration_seconds=_parse_duration_seconds(
                raw_leg.get("staticDuration")
            ),
            steps=tuple(self._parse_route_step(step) for step in raw_steps),
        )

    def _parse_route_step(self, raw_step: dict[str, Any]) -> RouteStep:
        if not isinstance(raw_step, dict):
            raise MapsResponseSchemaError("Route step must be an object.")

        navigation_instruction = raw_step.get("navigationInstruction", {})
        instruction = None
        if isinstance(navigation_instruction, dict):
            instruction = _optional_str(navigation_instruction.get("instructions"))

        raw_transit_details = raw_step.get("transitDetails")
        transit_details = None
        if raw_transit_details is not None:
            if not isinstance(raw_transit_details, dict):
                raise MapsResponseSchemaError(
                    "Route step transitDetails must be an object when present."
                )
            line = raw_transit_details.get("transitLine", {})
            parsed_line = None
            if isinstance(line, dict):
                vehicle = line.get("vehicle", {})
                parsed_line = RouteStepTransitLine(
                    name=_optional_str(line.get("name")),
                    short_name=_optional_str(line.get("nameShort")),
                    vehicle_type=_optional_str(
                        vehicle.get("type") if isinstance(vehicle, dict) else None
                    ),
                )
            transit_details = RouteStepTransitDetails(
                arrival_stop_name=_nested_str(
                    raw_transit_details, ("stopDetails", "arrivalStop", "name")
                ),
                departure_stop_name=_nested_str(
                    raw_transit_details, ("stopDetails", "departureStop", "name")
                ),
                headsign=_optional_str(raw_transit_details.get("headsign")),
                stop_count=_optional_int(raw_transit_details.get("stopCount")),
                line=parsed_line,
            )

        return RouteStep(
            distance_meters=_optional_int(raw_step.get("distanceMeters")),
            static_duration_seconds=_parse_duration_seconds(raw_step.get("staticDuration")),
            instruction=instruction,
            travel_mode=_optional_str(raw_step.get("travelMode")),
            transit_details=transit_details,
        )

    def _parse_place(self, raw_place: dict[str, Any]) -> MapsPlace:
        if not isinstance(raw_place, dict):
            raise MapsResponseSchemaError("Place payload must be a JSON object.")

        place_id = raw_place.get("id")
        resource_name = raw_place.get("name")
        display_name = _nested_str(raw_place, ("displayName", "text"))

        if not isinstance(place_id, str) or not isinstance(resource_name, str):
            raise MapsResponseSchemaError(
                "Place payload is missing required 'id' or 'name' fields."
            )
        if not isinstance(display_name, str) or display_name.strip() == "":
            raise MapsResponseSchemaError(
                f"Place {place_id} is missing displayName.text."
            )

        location = raw_place.get("location")
        if not isinstance(location, dict):
            raise MapsResponseSchemaError(f"Place {place_id} is missing location.")
        latitude = location.get("latitude")
        longitude = location.get("longitude")
        if not isinstance(latitude, (int, float)) or not isinstance(
            longitude, (int, float)
        ):
            raise MapsResponseSchemaError(
                f"Place {place_id} has invalid location coordinates."
            )

        opening_hours = self._parse_opening_hours(raw_place.get("regularOpeningHours"))
        photos = self._parse_photos(raw_place.get("photos"))
        postal_address = raw_place.get("postalAddress")
        postal_locality = None
        postal_region = None
        postal_postcode = None
        if postal_address is not None:
            if not isinstance(postal_address, dict):
                raise MapsResponseSchemaError(
                    f"Place {place_id} has non-object postalAddress."
                )
            postal_locality = _optional_str(postal_address.get("locality"))
            postal_region = _optional_str(postal_address.get("administrativeArea"))
            postal_postcode = _optional_str(postal_address.get("postalCode"))

        return MapsPlace(
            place_id=place_id,
            resource_name=resource_name,
            display_name=display_name,
            location=LatLng(latitude=float(latitude), longitude=float(longitude)),
            formatted_address=_optional_str(raw_place.get("formattedAddress")),
            google_maps_uri=_optional_str(raw_place.get("googleMapsUri")),
            business_status=_optional_str(raw_place.get("businessStatus")),
            rating=_optional_float(raw_place.get("rating")),
            user_rating_count=_optional_int(raw_place.get("userRatingCount")),
            regular_opening_hours=opening_hours,
            photos=photos,
            postal_locality=postal_locality,
            postal_region=postal_region,
            postal_postcode=postal_postcode,
        )

    def _parse_opening_hours(
        self, raw_hours: Any
    ) -> MapsOpeningHours | None:
        if raw_hours is None:
            return None
        if not isinstance(raw_hours, dict):
            raise MapsResponseSchemaError("regularOpeningHours must be an object.")
        weekday_descriptions = raw_hours.get("weekdayDescriptions", [])
        if weekday_descriptions is None:
            weekday_descriptions = []
        if not isinstance(weekday_descriptions, list) or any(
            not isinstance(item, str) for item in weekday_descriptions
        ):
            raise MapsResponseSchemaError(
                "regularOpeningHours.weekdayDescriptions must be a list of strings."
            )
        open_now = raw_hours.get("openNow")
        if open_now is not None and not isinstance(open_now, bool):
            raise MapsResponseSchemaError("regularOpeningHours.openNow must be a bool.")
        periods = self._parse_opening_periods(raw_hours.get("periods"))
        return MapsOpeningHours(
            open_now=open_now,
            weekday_descriptions=tuple(weekday_descriptions),
            periods=periods,
        )

    def _parse_opening_periods(self, raw_periods: Any) -> tuple[MapsOpeningPeriod, ...]:
        if raw_periods is None:
            return ()
        if not isinstance(raw_periods, list):
            raise MapsResponseSchemaError(
                "regularOpeningHours.periods must be a list when present."
            )
        periods: list[MapsOpeningPeriod] = []
        for raw_period in raw_periods:
            if not isinstance(raw_period, dict):
                raise MapsResponseSchemaError(
                    "regularOpeningHours.periods entries must be objects."
                )
            raw_open = raw_period.get("open")
            if not isinstance(raw_open, dict):
                raise MapsResponseSchemaError(
                    "regularOpeningHours.periods entry is missing open."
                )
            raw_close = raw_period.get("close")
            if raw_close is not None and not isinstance(raw_close, dict):
                raise MapsResponseSchemaError(
                    "regularOpeningHours.periods close must be an object when present."
                )
            periods.append(
                MapsOpeningPeriod(
                    open=self._parse_opening_point(raw_open, label="open"),
                    close=self._parse_opening_point(raw_close, label="close")
                    if raw_close is not None
                    else None,
                )
            )
        return tuple(periods)

    def _parse_opening_point(
        self,
        raw_point: dict[str, Any],
        *,
        label: str,
    ) -> MapsOpeningPoint:
        day = raw_point.get("day")
        hour = raw_point.get("hour")
        minute = raw_point.get("minute")
        if isinstance(day, bool) or not isinstance(day, int) or not 0 <= day <= 6:
            raise MapsResponseSchemaError(
                f"regularOpeningHours.periods {label}.day must be an integer 0-6."
            )
        if isinstance(hour, bool) or not isinstance(hour, int) or not 0 <= hour <= 23:
            raise MapsResponseSchemaError(
                f"regularOpeningHours.periods {label}.hour must be an integer 0-23."
            )
        if (
            isinstance(minute, bool)
            or not isinstance(minute, int)
            or not 0 <= minute <= 59
        ):
            raise MapsResponseSchemaError(
                f"regularOpeningHours.periods {label}.minute must be an integer 0-59."
            )
        return MapsOpeningPoint(day=day, hour=hour, minute=minute)

    def _parse_photos(self, raw_photos: Any) -> tuple[PhotoAsset, ...]:
        if raw_photos is None:
            return ()
        if not isinstance(raw_photos, list):
            raise MapsResponseSchemaError("photos must be a list when present.")
        photos: list[PhotoAsset] = []
        for raw_photo in raw_photos:
            if not isinstance(raw_photo, dict):
                raise MapsResponseSchemaError("photo entry must be an object.")
            name = raw_photo.get("name")
            if not isinstance(name, str):
                raise MapsResponseSchemaError("photo entry is missing name.")
            raw_attributions = raw_photo.get("authorAttributions", [])
            if raw_attributions is None:
                raw_attributions = []
            if not isinstance(raw_attributions, list):
                raise MapsResponseSchemaError(
                    "photo authorAttributions must be a list."
                )
            attributions = tuple(
                PhotoAuthorAttribution(
                    display_name=_optional_str(attr.get("displayName"))
                    if isinstance(attr, dict)
                    else None,
                    uri=_optional_str(attr.get("uri")) if isinstance(attr, dict) else None,
                    photo_uri=_optional_str(attr.get("photoUri"))
                    if isinstance(attr, dict)
                    else None,
                )
                for attr in raw_attributions
            )
            photos.append(
                PhotoAsset(
                    name=name,
                    width_px=_optional_int(raw_photo.get("widthPx")),
                    height_px=_optional_int(raw_photo.get("heightPx")),
                    author_attributions=attributions,
                )
            )
        return tuple(photos)

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        field_mask: str | None = None,
        include_api_key_header: bool = True,
        **kwargs: Any,
    ) -> dict[str, Any]:
        response = await self._request(
            method,
            url,
            field_mask=field_mask,
            include_api_key_header=include_api_key_header,
            **kwargs,
        )
        try:
            payload = response.json()
        except ValueError as exc:
            logger.error(
                "Google Maps returned non-JSON payload for %s %s. body=%r",
                method,
                url,
                response.text[:500],
            )
            raise MapsResponseSchemaError(
                f"Google Maps returned non-JSON payload for {method} {url}."
            ) from exc
        if not isinstance(payload, dict):
            raise MapsResponseSchemaError(
                f"Google Maps returned non-object JSON for {method} {url}."
            )
        return payload

    async def _request(
        self,
        method: str,
        url: str,
        *,
        field_mask: str | None = None,
        include_api_key_header: bool = True,
        **kwargs: Any,
    ) -> httpx.Response:
        headers = dict(kwargs.pop("headers", {}))
        if include_api_key_header:
            headers["X-Goog-Api-Key"] = self._settings.api_key
        if field_mask:
            headers["X-Goog-FieldMask"] = field_mask
        headers.setdefault("Content-Type", "application/json")

        attempts = self._settings.retry_count + 1
        last_exception: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                response = await self._http_client.request(
                    method,
                    url,
                    headers=headers,
                    timeout=self._settings.timeout_seconds,
                    **kwargs,
                )
            except httpx.HTTPError as exc:
                logger.error(
                    "Google Maps request failed on attempt %d/%d for %s %s: %s",
                    attempt,
                    attempts,
                    method,
                    url,
                    exc,
                )
                last_exception = exc
                if attempt < attempts:
                    continue
                raise MapsUpstreamError(
                    f"Google Maps request failed for {method} {url}: {exc}"
                ) from exc

            if response.status_code < 400:
                return response

            body_preview = response.text[:500]
            logger.error(
                "Google Maps returned status=%d on attempt %d/%d for %s %s. body=%r",
                response.status_code,
                attempt,
                attempts,
                method,
                url,
                body_preview,
            )
            if response.status_code in RETRYABLE_STATUS_CODES and attempt < attempts:
                continue
            raise MapsUpstreamError(
                f"Google Maps returned HTTP {response.status_code} for "
                f"{method} {url}: {body_preview}"
            )

        raise MapsUpstreamError(
            f"Google Maps request failed for {method} {url}: {last_exception}"
        )


def _normalize_name(value: str) -> str:
    normalized = _NON_ALNUM_RE.sub(" ", value.casefold())
    normalized = _WHITESPACE_RE.sub(" ", normalized).strip()
    return normalized


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    raise MapsResponseSchemaError(f"Expected string or null, got {type(value).__name__}.")


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise MapsResponseSchemaError("Expected int or null, got bool.")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    raise MapsResponseSchemaError(f"Expected int or null, got {type(value).__name__}.")


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise MapsResponseSchemaError("Expected float or null, got bool.")
    if isinstance(value, (int, float)):
        return float(value)
    raise MapsResponseSchemaError(
        f"Expected float-compatible number or null, got {type(value).__name__}."
    )


def _nested_str(payload: dict[str, Any], path: tuple[str, ...]) -> str | None:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return _optional_str(current)


def _parse_duration_seconds(value: Any) -> float | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.endswith("s"):
        raise MapsResponseSchemaError(
            f"Expected Google duration string ending in 's', got {value!r}."
        )
    try:
        return float(value[:-1])
    except ValueError as exc:
        raise MapsResponseSchemaError(
            f"Could not parse Google duration string {value!r}."
        ) from exc


def _waypoint(point: LatLng) -> dict[str, Any]:
    return {
        "location": {
            "latLng": {
                "latitude": point.latitude,
                "longitude": point.longitude,
            }
        }
    }


def _to_rfc3339(value: Any) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _postcode_match(candidate: CandidatePlace, place: MapsPlace) -> bool:
    return bool(
        candidate.postcode
        and place.postal_postcode
        and candidate.postcode.strip() == place.postal_postcode.strip()
    )


def _locality_match(candidate: CandidatePlace, place: MapsPlace) -> bool:
    return bool(
        candidate.locality
        and place.postal_locality
        and _normalize_name(candidate.locality) == _normalize_name(place.postal_locality)
    )


def _region_match(candidate: CandidatePlace, place: MapsPlace) -> bool:
    return bool(
        candidate.region
        and place.postal_region
        and _canonical_region(candidate.region) == _canonical_region(place.postal_region)
    )


def _address_compatible(candidate: CandidatePlace, place: MapsPlace) -> bool:
    if candidate.postcode and place.postal_postcode:
        if candidate.postcode.strip() != place.postal_postcode.strip():
            return False
    if candidate.locality and place.postal_locality:
        if _normalize_name(candidate.locality) != _normalize_name(place.postal_locality):
            return False
    if candidate.region and place.postal_region:
        if _canonical_region(candidate.region) != _canonical_region(place.postal_region):
            return False
    return True


def _canonical_region(value: str) -> str:
    normalized = _normalize_name(value)
    return _REGION_ALIASES.get(normalized, normalized)


def _is_ambiguous(
    best: _ScoredPlaceCandidate, second: _ScoredPlaceCandidate
) -> bool:
    if best.sort_key[:3] != second.sort_key[:3]:
        return False
    similarity_gap = abs(best.name_similarity - second.name_similarity)
    distance_gap = abs(
        best.straight_line_distance_meters - second.straight_line_distance_meters
    )
    return similarity_gap <= 0.01 and distance_gap <= 25.0


def _haversine_meters(point_a: LatLng, point_b: LatLng) -> float:
    """Return great-circle distance in meters."""

    radius = 6_371_000.0
    lat1 = math.radians(point_a.latitude)
    lat2 = math.radians(point_b.latitude)
    delta_lat = math.radians(point_b.latitude - point_a.latitude)
    delta_lon = math.radians(point_b.longitude - point_a.longitude)

    sin_lat = math.sin(delta_lat / 2.0)
    sin_lon = math.sin(delta_lon / 2.0)
    a_value = (
        sin_lat * sin_lat
        + math.cos(lat1) * math.cos(lat2) * sin_lon * sin_lon
    )
    c_value = 2.0 * math.atan2(math.sqrt(a_value), math.sqrt(1.0 - a_value))
    return radius * c_value
