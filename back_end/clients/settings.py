"""Runtime settings for the Date Night backend."""

from __future__ import annotations

import os
from dataclasses import dataclass


class MapsConfigurationError(RuntimeError):
    """Raised when Maps-backed code is configured incorrectly."""


class WeatherConfigurationError(RuntimeError):
    """Raised when weather-backed code is configured incorrectly."""


def _read_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise MapsConfigurationError(
            f"{name} must be a float, got {raw!r}."
        ) from exc
    if value <= 0:
        raise MapsConfigurationError(f"{name} must be positive, got {value}.")
    return value


def _read_optional_float(
    name: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float | None:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except ValueError as exc:
        raise WeatherConfigurationError(
            f"{name} must be a float, got {raw!r}."
        ) from exc
    if minimum is not None and value < minimum:
        raise WeatherConfigurationError(
            f"{name} must be >= {minimum}, got {value}."
        )
    if maximum is not None and value > maximum:
        raise WeatherConfigurationError(
            f"{name} must be <= {maximum}, got {value}."
        )
    return value


def _read_optional_int(
    name: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int | None:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise WeatherConfigurationError(
            f"{name} must be an integer, got {raw!r}."
        ) from exc
    if minimum is not None and value < minimum:
        raise WeatherConfigurationError(
            f"{name} must be >= {minimum}, got {value}."
        )
    if maximum is not None and value > maximum:
        raise WeatherConfigurationError(
            f"{name} must be <= {maximum}, got {value}."
        )
    return value


def _read_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise MapsConfigurationError(
            f"{name} must be an integer, got {raw!r}."
        ) from exc
    if value < minimum:
        raise MapsConfigurationError(
            f"{name} must be >= {minimum}, got {value}."
        )
    return value


@dataclass(frozen=True)
class MapsSettings:
    """Settings for Google Maps Places and Routes API calls."""

    api_key: str
    places_base_url: str = "https://places.googleapis.com/v1"
    routes_base_url: str = "https://routes.googleapis.com/directions/v2"
    timeout_seconds: float = 10.0
    retry_count: int = 1
    text_search_result_limit: int = 5
    text_search_location_bias_radius_meters: float = 500.0
    max_match_distance_meters: float = 250.0
    min_name_similarity: float = 0.92
    min_place_rating: float = 3.8
    min_user_rating_count: int = 0
    default_photo_max_width_px: int = 1200
    default_photo_max_height_px: int = 900

    @classmethod
    def from_env(cls) -> "MapsSettings":
        """Load Maps settings from environment variables.

        Fails loudly when the backend is configured to use Google Maps but no
        API key is present.
        """

        api_key = os.getenv("MAPS_API_KEY")
        if api_key is None or api_key.strip() == "":
            raise MapsConfigurationError(
                "MAPS_API_KEY is required for Google Maps client calls."
            )

        return cls(
            api_key=api_key,
            timeout_seconds=_read_float("MAPS_TIMEOUT_SECONDS", 10.0),
            retry_count=_read_int("MAPS_RETRY_COUNT", 1, minimum=0),
            text_search_result_limit=_read_int(
                "MAPS_TEXT_SEARCH_RESULT_LIMIT", 5, minimum=1
            ),
            text_search_location_bias_radius_meters=_read_float(
                "MAPS_TEXT_SEARCH_BIAS_RADIUS_METERS", 500.0
            ),
            max_match_distance_meters=_read_float(
                "MAPS_MAX_MATCH_DISTANCE_METERS", 250.0
            ),
            min_name_similarity=_read_float("MAPS_MIN_NAME_SIMILARITY", 0.92),
            min_place_rating=_read_float("MAPS_MIN_PLACE_RATING", 3.8),
            min_user_rating_count=_read_int(
                "MAPS_MIN_USER_RATING_COUNT", 0, minimum=0
            ),
            default_photo_max_width_px=_read_int(
                "MAPS_DEFAULT_PHOTO_MAX_WIDTH_PX", 1200, minimum=1
            ),
            default_photo_max_height_px=_read_int(
                "MAPS_DEFAULT_PHOTO_MAX_HEIGHT_PX", 900, minimum=1
            ),
        )


@dataclass(frozen=True)
class WeatherSettings:
    """Settings for the Open-Meteo hourly forecast client."""

    forecast_base_url: str = "https://api.open-meteo.com/v1"
    timeout_seconds: float = 10.0
    retry_count: int = 1
    max_forecast_days: int = 14
    hourly_precipitation_mm_threshold: float = 3.0
    precipitation_probability_threshold_pct: int = 70
    extreme_heat_c_threshold: float = 32.0
    strong_wind_kph_threshold: float = 40.0

    @classmethod
    def from_env(cls) -> "WeatherSettings":
        """Load weather settings from environment variables.

        Open-Meteo does not require an API key for the public forecast endpoint,
        so configuration focuses on explicit policy thresholds and timeouts.
        """

        max_forecast_days = _read_optional_int(
            "WEATHER_MAX_FORECAST_DAYS",
            minimum=1,
            maximum=16,
        )
        precipitation_probability_threshold_pct = _read_optional_int(
            "WEATHER_PRECIPITATION_PROBABILITY_THRESHOLD_PCT",
            minimum=0,
            maximum=100,
        )
        return cls(
            forecast_base_url=(
                os.getenv("WEATHER_FORECAST_BASE_URL") or "https://api.open-meteo.com/v1"
            ),
            timeout_seconds=_read_float("WEATHER_TIMEOUT_SECONDS", 10.0),
            retry_count=_read_int("WEATHER_RETRY_COUNT", 1, minimum=0),
            max_forecast_days=max_forecast_days or 14,
            hourly_precipitation_mm_threshold=(
                _read_optional_float(
                    "WEATHER_HOURLY_PRECIPITATION_MM_THRESHOLD",
                    minimum=0.0,
                )
                or 3.0
            ),
            precipitation_probability_threshold_pct=(
                precipitation_probability_threshold_pct or 70
            ),
            extreme_heat_c_threshold=(
                _read_optional_float(
                    "WEATHER_EXTREME_HEAT_C_THRESHOLD",
                    minimum=-100.0,
                )
                or 32.0
            ),
            strong_wind_kph_threshold=(
                _read_optional_float(
                    "WEATHER_STRONG_WIND_KPH_THRESHOLD",
                    minimum=0.0,
                )
                or 40.0
            ),
        )
