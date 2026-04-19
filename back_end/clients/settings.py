"""Runtime settings for the Date Night backend."""

from __future__ import annotations

import os
from dataclasses import dataclass


class MapsConfigurationError(RuntimeError):
    """Raised when Maps-backed code is configured incorrectly."""


class WeatherConfigurationError(RuntimeError):
    """Raised when weather-backed code is configured incorrectly."""


class OpenRouterConfigurationError(RuntimeError):
    """Raised when OpenRouter-backed code is configured incorrectly."""


class BraveConfigurationError(RuntimeError):
    """Raised when Brave-backed code is configured incorrectly."""


class BlandAIConfigurationError(RuntimeError):
    """Raised when Bland AI-backed code is configured incorrectly."""


DEFAULT_BLAND_AI_BOOKING_PHONE_NUMBER = "+61491114073"


def bland_ai_booking_phone_number_from_env() -> str:
    """Return the configured safe outbound booking target."""

    return (
        _read_optional_string("BLAND_AI_BOOKING_PHONE_NUMBER")
        or DEFAULT_BLAND_AI_BOOKING_PHONE_NUMBER
    )


def _read_float(
    name: str,
    default: float,
    *,
    error_cls: type[RuntimeError] = MapsConfigurationError,
) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise error_cls(f"{name} must be a float, got {raw!r}.") from exc
    if value <= 0:
        raise error_cls(f"{name} must be positive, got {value}.")
    return value


def _read_optional_float(
    name: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    error_cls: type[RuntimeError] = WeatherConfigurationError,
) -> float | None:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except ValueError as exc:
        raise error_cls(f"{name} must be a float, got {raw!r}.") from exc
    if minimum is not None and value < minimum:
        raise error_cls(f"{name} must be >= {minimum}, got {value}.")
    if maximum is not None and value > maximum:
        raise error_cls(f"{name} must be <= {maximum}, got {value}.")
    return value


def _read_optional_int(
    name: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
    error_cls: type[RuntimeError] = WeatherConfigurationError,
) -> int | None:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise error_cls(f"{name} must be an integer, got {raw!r}.") from exc
    if minimum is not None and value < minimum:
        raise error_cls(f"{name} must be >= {minimum}, got {value}.")
    if maximum is not None and value > maximum:
        raise error_cls(f"{name} must be <= {maximum}, got {value}.")
    return value


def _read_int(
    name: str,
    default: int,
    *,
    minimum: int = 0,
    error_cls: type[RuntimeError] = MapsConfigurationError,
) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise error_cls(f"{name} must be an integer, got {raw!r}.") from exc
    if value < minimum:
        raise error_cls(f"{name} must be >= {minimum}, got {value}.")
    return value


def _read_optional_string(name: str) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def _read_bool(
    name: str,
    default: bool,
    *,
    error_cls: type[RuntimeError] = MapsConfigurationError,
) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise error_cls(f"{name} must be a boolean, got {raw!r}.")


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
            timeout_seconds=_read_float(
                "WEATHER_TIMEOUT_SECONDS",
                10.0,
                error_cls=WeatherConfigurationError,
            ),
            retry_count=_read_int(
                "WEATHER_RETRY_COUNT",
                1,
                minimum=0,
                error_cls=WeatherConfigurationError,
            ),
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


@dataclass(frozen=True)
class OpenRouterSettings:
    """Settings for OpenRouter chat completions and tool use."""

    api_key: str
    base_url: str = "https://openrouter.ai/api/v1"
    default_model: str | None = None
    timeout_seconds: float = 45.0
    retry_count: int = 1
    max_tool_round_trips: int = 8
    http_referer: str | None = None
    app_title: str | None = None

    @classmethod
    def from_env(cls) -> "OpenRouterSettings":
        """Load OpenRouter settings from environment variables."""

        api_key = _read_optional_string("OPENROUTER_API_KEY")
        if api_key is None:
            raise OpenRouterConfigurationError(
                "OPENROUTER_API_KEY is required for OpenRouter client calls."
            )

        return cls(
            api_key=api_key,
            base_url=(
                _read_optional_string("OPENROUTER_BASE_URL")
                or "https://openrouter.ai/api/v1"
            ),
            default_model=_read_optional_string("OPENROUTER_MODEL"),
            timeout_seconds=_read_float(
                "OPENROUTER_TIMEOUT_SECONDS",
                45.0,
                error_cls=OpenRouterConfigurationError,
            ),
            retry_count=_read_int(
                "OPENROUTER_RETRY_COUNT",
                1,
                minimum=0,
                error_cls=OpenRouterConfigurationError,
            ),
            max_tool_round_trips=_read_int(
                "OPENROUTER_MAX_TOOL_ROUND_TRIPS",
                8,
                minimum=1,
                error_cls=OpenRouterConfigurationError,
            ),
            http_referer=_read_optional_string("OPENROUTER_HTTP_REFERER"),
            app_title=_read_optional_string("OPENROUTER_APP_TITLE"),
        )


@dataclass(frozen=True)
class BraveSettings:
    """Settings for Brave Search API calls."""

    api_key: str
    base_url: str = "https://api.search.brave.com/res/v1"
    timeout_seconds: float = 15.0
    retry_count: int = 1
    country: str = "AU"
    search_lang: str = "en"
    ui_lang: str = "en-AU"

    @classmethod
    def from_env(cls) -> "BraveSettings":
        """Load Brave Search settings from environment variables."""

        api_key = _read_optional_string("BRAVE_API_KEY")
        if api_key is None:
            raise BraveConfigurationError(
                "BRAVE_API_KEY is required for Brave Search client calls."
            )

        return cls(
            api_key=api_key,
            base_url=(
                _read_optional_string("BRAVE_BASE_URL")
                or "https://api.search.brave.com/res/v1"
            ),
            timeout_seconds=_read_float(
                "BRAVE_TIMEOUT_SECONDS",
                15.0,
                error_cls=BraveConfigurationError,
            ),
            retry_count=_read_int(
                "BRAVE_RETRY_COUNT",
                1,
                minimum=0,
                error_cls=BraveConfigurationError,
            ),
            country=_read_optional_string("BRAVE_COUNTRY") or "AU",
            search_lang=_read_optional_string("BRAVE_SEARCH_LANG") or "en",
            ui_lang=_read_optional_string("BRAVE_UI_LANG") or "en-AU",
        )


@dataclass(frozen=True)
class BlandAISettings:
    """Settings for Bland AI outbound restaurant booking calls."""

    api_key: str
    booking_phone_number: str = DEFAULT_BLAND_AI_BOOKING_PHONE_NUMBER
    base_url: str = "https://api.bland.ai/v1"
    timeout_seconds: float = 20.0
    status_retry_count: int = 1
    default_voice: str | None = None
    language: str = "en-AU"
    timezone: str = "Australia/Sydney"
    model: str = "base"
    max_duration_minutes: int = 8
    record_calls: bool = False

    @classmethod
    def from_env(cls) -> "BlandAISettings":
        """Load Bland AI settings from environment variables."""

        api_key = _read_optional_string("BLAND_AI_API_KEY")
        if api_key is None:
            raise BlandAIConfigurationError(
                "BLAND_AI_API_KEY is required for Bland AI client calls."
            )

        return cls(
            api_key=api_key,
            booking_phone_number=bland_ai_booking_phone_number_from_env(),
            base_url=(
                _read_optional_string("BLAND_AI_BASE_URL")
                or "https://api.bland.ai/v1"
            ),
            timeout_seconds=_read_float(
                "BLAND_AI_TIMEOUT_SECONDS",
                20.0,
                error_cls=BlandAIConfigurationError,
            ),
            status_retry_count=_read_int(
                "BLAND_AI_STATUS_RETRY_COUNT",
                1,
                minimum=0,
                error_cls=BlandAIConfigurationError,
            ),
            default_voice=_read_optional_string("BLAND_AI_DEFAULT_VOICE"),
            language=_read_optional_string("BLAND_AI_LANGUAGE") or "en-AU",
            timezone=_read_optional_string("BLAND_AI_TIMEZONE") or "Australia/Sydney",
            model=_read_optional_string("BLAND_AI_MODEL") or "base",
            max_duration_minutes=_read_int(
                "BLAND_AI_MAX_DURATION_MINUTES",
                8,
                minimum=1,
                error_cls=BlandAIConfigurationError,
            ),
            record_calls=_read_bool(
                "BLAND_AI_RECORD_CALLS",
                False,
                error_cls=BlandAIConfigurationError,
            ),
        )
