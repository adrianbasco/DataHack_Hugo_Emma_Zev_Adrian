"""Typed backend models shared across filtering and Maps integration."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


@dataclass(frozen=True)
class LatLng:
    """Latitude/longitude pair."""

    latitude: float
    longitude: float


@dataclass(frozen=True)
class CandidatePlace:
    """Place from the local parquet candidate pool."""

    fsq_place_id: str
    name: str
    latitude: float | None
    longitude: float | None
    address: str | None = None
    locality: str | None = None
    region: str | None = None
    postcode: str | None = None
    fsq_category_ids: tuple[str, ...] = field(default_factory=tuple)

    @property
    def coordinates(self) -> LatLng | None:
        if self.latitude is None or self.longitude is None:
            return None
        return LatLng(latitude=self.latitude, longitude=self.longitude)


@dataclass(frozen=True)
class PhotoAuthorAttribution:
    """Required attribution that may accompany a photo."""

    display_name: str | None
    uri: str | None
    photo_uri: str | None


@dataclass(frozen=True)
class PhotoAsset:
    """Photo reference returned by Google Places."""

    name: str
    width_px: int | None
    height_px: int | None
    author_attributions: tuple[PhotoAuthorAttribution, ...] = ()


@dataclass(frozen=True)
class PhotoMedia:
    """Resolved media response for a photo reference."""

    name: str
    photo_uri: str


@dataclass(frozen=True)
class MapsOpeningPoint:
    """One opening-hours boundary from Google Places."""

    day: int
    hour: int
    minute: int


@dataclass(frozen=True)
class MapsOpeningPeriod:
    """One regular opening-hours interval from Google Places."""

    open: MapsOpeningPoint
    close: MapsOpeningPoint | None


@dataclass(frozen=True)
class MapsOpeningHours:
    """Subset of opening-hours data used by the backend."""

    open_now: bool | None
    weekday_descriptions: tuple[str, ...]
    periods: tuple[MapsOpeningPeriod, ...] = ()


@dataclass(frozen=True)
class MapsPlace:
    """Parsed Google Places response."""

    place_id: str
    resource_name: str
    display_name: str
    location: LatLng
    formatted_address: str | None = None
    google_maps_uri: str | None = None
    business_status: str | None = None
    rating: float | None = None
    user_rating_count: int | None = None
    regular_opening_hours: MapsOpeningHours | None = None
    photos: tuple[PhotoAsset, ...] = ()
    postal_locality: str | None = None
    postal_region: str | None = None
    postal_postcode: str | None = None


@dataclass(frozen=True)
class MapsPlaceMatch:
    """Confident match from a local candidate to a Google place."""

    candidate_place: CandidatePlace
    google_place: MapsPlace
    straight_line_distance_meters: float
    name_similarity: float
    match_kind: str = "coord_name"


class TravelMode(str, Enum):
    """Supported route travel modes."""

    BICYCLE = "BICYCLE"
    DRIVE = "DRIVE"
    WALK = "WALK"
    TRANSIT = "TRANSIT"


@dataclass(frozen=True)
class RouteStepTransitLine:
    """Transit line metadata for a step."""

    name: str | None
    short_name: str | None
    vehicle_type: str | None


@dataclass(frozen=True)
class RouteStepTransitDetails:
    """Transit-specific detail attached to a route step."""

    arrival_stop_name: str | None
    departure_stop_name: str | None
    headsign: str | None
    stop_count: int | None
    line: RouteStepTransitLine | None


@dataclass(frozen=True)
class RouteStep:
    """Single route step."""

    distance_meters: int | None
    static_duration_seconds: float | None
    instruction: str | None
    travel_mode: str | None
    transit_details: RouteStepTransitDetails | None


@dataclass(frozen=True)
class RouteLeg:
    """Route leg from one waypoint to the next."""

    distance_meters: int | None
    duration_seconds: float | None
    static_duration_seconds: float | None
    steps: tuple[RouteStep, ...]


@dataclass(frozen=True)
class ComputedRoute:
    """Top-level route object returned by Google Routes."""

    distance_meters: int | None
    duration_seconds: float | None
    static_duration_seconds: float | None
    polyline: str | None
    warnings: tuple[str, ...]
    legs: tuple[RouteLeg, ...]


@dataclass(frozen=True)
class RouteRequest:
    """Normalized request for one route lookup."""

    origin: LatLng
    destination: LatLng
    travel_mode: TravelMode
    departure_time: datetime | None = None
    arrival_time: datetime | None = None


class WeatherExposure(str, Enum):
    """How exposed a plan or stop is to outdoor weather."""

    INDOOR = "indoor"
    MIXED = "mixed"
    OUTDOOR = "outdoor"
    ACTIVE_OUTDOOR = "active_outdoor"


class WeatherAssessmentStatus(str, Enum):
    """Outcome of a weather evaluation."""

    SAFE = "safe"
    REJECT = "reject"
    INSUFFICIENT_DATA = "insufficient_data"
    UPSTREAM_FAILURE = "upstream_failure"


class WeatherRiskKind(str, Enum):
    """Weather risks that can invalidate a weather-sensitive itinerary."""

    HEAVY_RAIN = "heavy_rain"
    THUNDERSTORM = "thunderstorm"
    EXTREME_HEAT = "extreme_heat"
    STRONG_WIND = "strong_wind"


@dataclass(frozen=True)
class WeatherForecastPoint:
    """One hourly forecast point normalized from the upstream provider."""

    starts_at: datetime
    temperature_c: float | None
    apparent_temperature_c: float | None
    precipitation_mm: float | None
    precipitation_probability_pct: int | None
    weather_code: int | None
    wind_speed_kph: float | None
    wind_gusts_kph: float | None
    is_day: bool | None


@dataclass(frozen=True)
class WeatherForecast:
    """Hourly forecast returned by the weather provider."""

    coordinates: LatLng
    timezone: str
    timezone_abbreviation: str | None
    utc_offset_seconds: int
    points: tuple[WeatherForecastPoint, ...]


@dataclass(frozen=True)
class WeatherRisk:
    """A concrete weather risk identified during a window assessment."""

    kind: WeatherRiskKind
    starts_at: datetime
    message: str
    observed_value: float | int | None = None
    threshold_value: float | int | None = None


@dataclass(frozen=True)
class WeatherCheckFailure:
    """Explicit failure state for the weather layer."""

    reason: str
    message: str


@dataclass(frozen=True)
class WeatherWindowAssessment:
    """Decision about whether weather should block a plan or stop."""

    exposure: WeatherExposure
    start_at: datetime
    end_at: datetime
    status: WeatherAssessmentStatus
    should_reject: bool
    summary: str
    considered_points: tuple[WeatherForecastPoint, ...]
    risks: tuple[WeatherRisk, ...] = ()
    failure: WeatherCheckFailure | None = None
