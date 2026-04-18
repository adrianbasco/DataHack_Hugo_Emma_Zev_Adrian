"""Weather forecast client for backend itinerary validation."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from back_end.domain.models import LatLng, WeatherForecast, WeatherForecastPoint
from back_end.clients.settings import WeatherSettings

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
HOURLY_VARIABLES = (
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "precipitation_probability",
    "weather_code",
    "wind_speed_10m",
    "wind_gusts_10m",
    "is_day",
)


class WeatherClientError(RuntimeError):
    """Base class for weather client failures."""


class WeatherUpstreamError(WeatherClientError):
    """Raised when the provider rejects a request or returns invalid HTTP."""


class WeatherResponseSchemaError(WeatherClientError):
    """Raised when the provider returns an unexpected payload shape."""


class ForecastRangeError(WeatherClientError):
    """Raised when a requested forecast window cannot be satisfied."""


class WeatherClient:
    """Purpose-built hourly forecast client backed by Open-Meteo."""

    def __init__(
        self,
        settings: WeatherSettings,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = settings
        self._http_client = http_client or httpx.AsyncClient()
        self._owns_http_client = http_client is None

    async def __aenter__(self) -> "WeatherClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_http_client:
            await self._http_client.aclose()

    def close(self) -> None:
        """Compatibility shim for non-async callers."""

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self.aclose())
            return
        raise RuntimeError(
            "WeatherClient.close() was called inside a running event loop. "
            "Use 'await client.aclose()' instead."
        )

    async def get_hourly_forecast(
        self,
        coordinates: LatLng,
        *,
        start_at: datetime,
        end_at: datetime,
    ) -> WeatherForecast:
        """Fetch a normalized hourly forecast for the requested UTC window."""

        start_utc, end_utc = _normalize_window(start_at, end_at)
        self._validate_forecast_range(start_utc, end_utc)

        query_start = _floor_to_hour(start_utc)
        query_end = _ceil_to_hour(end_utc)
        if query_end <= query_start:
            query_end = query_start + timedelta(hours=1)

        payload = await self._request_json(
            "GET",
            f"{self._settings.forecast_base_url}/forecast",
            params={
                "latitude": coordinates.latitude,
                "longitude": coordinates.longitude,
                "hourly": ",".join(HOURLY_VARIABLES),
                "start_hour": query_start.strftime("%Y-%m-%dT%H:%M"),
                "end_hour": query_end.strftime("%Y-%m-%dT%H:%M"),
                "timezone": "GMT",
                "temperature_unit": "celsius",
                "wind_speed_unit": "kmh",
                "precipitation_unit": "mm",
            },
        )
        return self._parse_forecast(payload, coordinates)

    def _validate_forecast_range(
        self,
        start_at: datetime,
        end_at: datetime,
    ) -> None:
        now_utc = datetime.now(timezone.utc)
        latest_allowed = now_utc + timedelta(days=self._settings.max_forecast_days)
        if start_at > latest_allowed or end_at > latest_allowed:
            raise ForecastRangeError(
                "Requested weather window exceeds the configured forecast horizon "
                f"of {self._settings.max_forecast_days} days."
            )

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, object],
    ) -> dict[str, Any]:
        attempts = self._settings.retry_count + 1
        last_response: httpx.Response | None = None

        for attempt_index in range(attempts):
            response = await self._http_client.request(
                method,
                url,
                params=params,
                timeout=self._settings.timeout_seconds,
            )
            last_response = response
            if response.status_code < 400:
                break
            if (
                response.status_code in RETRYABLE_STATUS_CODES
                and attempt_index < self._settings.retry_count
            ):
                logger.warning(
                    "Weather API request failed with status=%s on attempt %s/%s; retrying.",
                    response.status_code,
                    attempt_index + 1,
                    attempts,
                )
                continue

            logger.error(
                "Weather API request failed with status=%s body=%r",
                response.status_code,
                response.text[:500],
            )
            raise WeatherUpstreamError(
                f"Weather API request failed with status {response.status_code}."
            )

        if last_response is None:
            raise WeatherUpstreamError("Weather API request did not produce a response.")

        try:
            payload = last_response.json()
        except ValueError as exc:
            logger.error("Weather API returned non-JSON response: %r", last_response.text[:500])
            raise WeatherResponseSchemaError(
                "Weather API returned a non-JSON response."
            ) from exc

        if not isinstance(payload, dict):
            raise WeatherResponseSchemaError(
                "Weather API returned a top-level payload that was not an object."
            )
        if payload.get("error") is True:
            reason = payload.get("reason")
            logger.error("Weather API returned explicit error payload: %r", payload)
            raise WeatherUpstreamError(
                f"Weather API rejected the request: {reason or 'unknown reason'}."
            )
        return payload

    def _parse_forecast(
        self,
        payload: dict[str, Any],
        coordinates: LatLng,
    ) -> WeatherForecast:
        timezone_name = payload.get("timezone")
        utc_offset_seconds = payload.get("utc_offset_seconds")
        hourly = payload.get("hourly")

        if not isinstance(timezone_name, str) or not timezone_name:
            raise WeatherResponseSchemaError(
                "Weather API response did not include a valid timezone."
            )
        if not isinstance(utc_offset_seconds, int):
            raise WeatherResponseSchemaError(
                "Weather API response did not include utc_offset_seconds as an integer."
            )
        if not isinstance(hourly, dict):
            raise WeatherResponseSchemaError(
                "Weather API response did not include an hourly object."
            )

        times = self._require_list(hourly, "time")
        temperature = self._require_list(hourly, "temperature_2m")
        apparent_temperature = self._require_list(hourly, "apparent_temperature")
        precipitation = self._require_list(hourly, "precipitation")
        precipitation_probability = self._require_list(
            hourly, "precipitation_probability"
        )
        weather_code = self._require_list(hourly, "weather_code")
        wind_speed = self._require_list(hourly, "wind_speed_10m")
        wind_gusts = self._require_list(hourly, "wind_gusts_10m")
        is_day = self._require_list(hourly, "is_day")

        expected_length = len(times)
        arrays = {
            "temperature_2m": temperature,
            "apparent_temperature": apparent_temperature,
            "precipitation": precipitation,
            "precipitation_probability": precipitation_probability,
            "weather_code": weather_code,
            "wind_speed_10m": wind_speed,
            "wind_gusts_10m": wind_gusts,
            "is_day": is_day,
        }
        for name, values in arrays.items():
            if len(values) != expected_length:
                raise WeatherResponseSchemaError(
                    f"Hourly weather field {name!r} had {len(values)} values, "
                    f"expected {expected_length}."
                )

        points: list[WeatherForecastPoint] = []
        for index, raw_time in enumerate(times):
            if not isinstance(raw_time, str):
                raise WeatherResponseSchemaError(
                    f"Hourly weather field 'time' contained a non-string value at index {index}."
                )

            try:
                starts_at = datetime.strptime(raw_time, "%Y-%m-%dT%H:%M").replace(
                    tzinfo=timezone.utc
                )
            except ValueError as exc:
                raise WeatherResponseSchemaError(
                    f"Hourly weather time value {raw_time!r} was not ISO8601 hour format."
                ) from exc

            points.append(
                WeatherForecastPoint(
                    starts_at=starts_at,
                    temperature_c=_coerce_float(temperature[index]),
                    apparent_temperature_c=_coerce_float(apparent_temperature[index]),
                    precipitation_mm=_coerce_float(precipitation[index]),
                    precipitation_probability_pct=_coerce_int(
                        precipitation_probability[index]
                    ),
                    weather_code=_coerce_int(weather_code[index]),
                    wind_speed_kph=_coerce_float(wind_speed[index]),
                    wind_gusts_kph=_coerce_float(wind_gusts[index]),
                    is_day=_coerce_bool(is_day[index]),
                )
            )

        return WeatherForecast(
            coordinates=coordinates,
            timezone=timezone_name,
            timezone_abbreviation=payload.get("timezone_abbreviation")
            if isinstance(payload.get("timezone_abbreviation"), str)
            else None,
            utc_offset_seconds=utc_offset_seconds,
            points=tuple(points),
        )

    @staticmethod
    def _require_list(payload: dict[str, Any], key: str) -> list[Any]:
        value = payload.get(key)
        if not isinstance(value, list):
            raise WeatherResponseSchemaError(
                f"Weather API response did not include list field {key!r}."
            )
        return value


def _normalize_window(
    start_at: datetime,
    end_at: datetime,
) -> tuple[datetime, datetime]:
    if start_at.tzinfo is None or end_at.tzinfo is None:
        raise ForecastRangeError(
            "Weather lookups require timezone-aware datetimes."
        )
    start_utc = start_at.astimezone(timezone.utc)
    end_utc = end_at.astimezone(timezone.utc)
    if end_utc <= start_utc:
        raise ForecastRangeError("Weather lookup end_at must be after start_at.")
    return start_utc, end_utc


def _floor_to_hour(value: datetime) -> datetime:
    return value.replace(minute=0, second=0, microsecond=0)


def _ceil_to_hour(value: datetime) -> datetime:
    floored = _floor_to_hour(value)
    if floored == value:
        return value
    return floored + timedelta(hours=1)


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise WeatherResponseSchemaError(
            f"Expected numeric weather value, got boolean {value!r}."
        )
    if not isinstance(value, (int, float)):
        raise WeatherResponseSchemaError(
            f"Expected numeric weather value, got {value!r}."
        )
    return float(value)


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise WeatherResponseSchemaError(
            f"Expected integer weather value, got boolean {value!r}."
        )
    if not isinstance(value, (int, float)):
        raise WeatherResponseSchemaError(
            f"Expected integer weather value, got {value!r}."
        )
    if int(value) != value:
        raise WeatherResponseSchemaError(
            f"Expected integer weather value, got non-integral {value!r}."
        )
    return int(value)


def _coerce_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in {0, 1}:
        return bool(value)
    raise WeatherResponseSchemaError(
        f"Expected boolean-ish weather value, got {value!r}."
    )
