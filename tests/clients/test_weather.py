from __future__ import annotations

import json
import os
import unittest
from datetime import datetime, timedelta, timezone

import httpx

from back_end.domain.models import (
    LatLng,
    WeatherAssessmentStatus,
    WeatherExposure,
    WeatherForecast,
    WeatherForecastPoint,
    WeatherRiskKind,
)
from back_end.clients.settings import WeatherConfigurationError, WeatherSettings
from back_end.clients.weather import (
    ForecastRangeError,
    WeatherClient,
    WeatherResponseSchemaError,
    WeatherUpstreamError,
)
from back_end.services.weather import WeatherEvaluationService


def _make_response(
    request: httpx.Request,
    status_code: int,
    payload: dict | None = None,
) -> httpx.Response:
    headers = {"Content-Type": "application/json"}
    content = json.dumps(payload or {}).encode("utf-8")
    return httpx.Response(
        status_code=status_code,
        headers=headers,
        content=content,
        request=request,
    )


class WeatherSettingsTests(unittest.TestCase):
    def test_from_env_rejects_invalid_probability_threshold(self) -> None:
        old_value = os.environ.get("WEATHER_PRECIPITATION_PROBABILITY_THRESHOLD_PCT")
        os.environ["WEATHER_PRECIPITATION_PROBABILITY_THRESHOLD_PCT"] = "120"
        self.addCleanup(
            self._restore_env,
            "WEATHER_PRECIPITATION_PROBABILITY_THRESHOLD_PCT",
            old_value,
        )

        with self.assertRaises(WeatherConfigurationError):
            WeatherSettings.from_env()

    @staticmethod
    def _restore_env(name: str, value: str | None) -> None:
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value


class WeatherClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.settings = WeatherSettings(retry_count=1)
        self.coordinates = LatLng(latitude=-37.8136, longitude=144.9631)
        self.start_at = datetime(2026, 4, 18, 8, 30, tzinfo=timezone.utc)
        self.end_at = datetime(2026, 4, 18, 11, 0, tzinfo=timezone.utc)

    async def test_get_hourly_forecast_sends_expected_query_params(self) -> None:
        captured: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured.update(dict(request.url.params))
            return _make_response(request, 200, _forecast_payload())

        client = WeatherClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        forecast = await client.get_hourly_forecast(
            self.coordinates,
            start_at=self.start_at,
            end_at=self.end_at,
        )

        self.assertEqual("GMT", captured["timezone"])
        self.assertEqual("-37.8136", captured["latitude"])
        self.assertEqual("144.9631", captured["longitude"])
        self.assertIn("precipitation_probability", captured["hourly"])
        self.assertEqual("2026-04-18T08:00", captured["start_hour"])
        self.assertEqual("2026-04-18T11:00", captured["end_hour"])
        self.assertEqual(3, len(forecast.points))

    async def test_get_hourly_forecast_parses_nullable_values(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            payload = _forecast_payload()
            payload["hourly"]["precipitation_probability"][1] = None
            return _make_response(request, 200, payload)

        client = WeatherClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        forecast = await client.get_hourly_forecast(
            self.coordinates,
            start_at=self.start_at,
            end_at=self.end_at,
        )

        self.assertIsNone(forecast.points[1].precipitation_probability_pct)
        self.assertEqual(95, forecast.points[2].weather_code)

    async def test_get_hourly_forecast_retries_retryable_status_codes(self) -> None:
        attempts = {"count": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            attempts["count"] += 1
            if attempts["count"] == 1:
                return _make_response(request, 503, {"error": True, "reason": "temporary"})
            return _make_response(request, 200, _forecast_payload())

        client = WeatherClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        forecast = await client.get_hourly_forecast(
            self.coordinates,
            start_at=self.start_at,
            end_at=self.end_at,
        )

        self.assertEqual(2, attempts["count"])
        self.assertEqual(3, len(forecast.points))

    async def test_get_hourly_forecast_raises_on_non_retryable_status(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(request, 400, {"error": True, "reason": "bad request"})

        client = WeatherClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(WeatherUpstreamError):
            await client.get_hourly_forecast(
                self.coordinates,
                start_at=self.start_at,
                end_at=self.end_at,
            )

    async def test_get_hourly_forecast_raises_on_mismatched_hourly_lengths(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            payload = _forecast_payload()
            payload["hourly"]["wind_gusts_10m"] = [20.0, 25.0]
            return _make_response(request, 200, payload)

        client = WeatherClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(WeatherResponseSchemaError):
            await client.get_hourly_forecast(
                self.coordinates,
                start_at=self.start_at,
                end_at=self.end_at,
            )

    async def test_get_hourly_forecast_requires_timezone_aware_datetimes(self) -> None:
        client = WeatherClient(self.settings, http_client=httpx.AsyncClient())
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(ForecastRangeError):
            await client.get_hourly_forecast(
                self.coordinates,
                start_at=datetime(2026, 4, 18, 8, 0),
                end_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
            )

    async def test_get_hourly_forecast_rejects_window_beyond_horizon(self) -> None:
        client = WeatherClient(
            WeatherSettings(max_forecast_days=1),
            http_client=httpx.AsyncClient(),
        )
        self.addAsyncCleanup(client.aclose)

        future = datetime.now(timezone.utc) + timedelta(days=2)
        with self.assertRaises(ForecastRangeError):
            await client.get_hourly_forecast(
                self.coordinates,
                start_at=future,
                end_at=future + timedelta(hours=2),
            )


class WeatherEvaluationServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.settings = WeatherSettings(
            hourly_precipitation_mm_threshold=3.0,
            precipitation_probability_threshold_pct=70,
            extreme_heat_c_threshold=32.0,
            strong_wind_kph_threshold=40.0,
        )
        self.service = WeatherEvaluationService(self.settings)
        self.start_at = datetime(2026, 4, 18, 8, 30, tzinfo=timezone.utc)
        self.end_at = datetime(2026, 4, 18, 10, 30, tzinfo=timezone.utc)

    def test_assess_forecast_rejects_outdoor_window_for_thunderstorm(self) -> None:
        assessment = self.service.assess_forecast(
            _forecast(
                [
                    _point("2026-04-18T08:00", weather_code=3),
                    _point("2026-04-18T09:00", weather_code=95),
                    _point("2026-04-18T10:00", weather_code=3),
                ]
            ),
            exposure=WeatherExposure.OUTDOOR,
            start_at=self.start_at,
            end_at=self.end_at,
        )

        self.assertEqual(WeatherAssessmentStatus.REJECT, assessment.status)
        self.assertTrue(assessment.should_reject)
        self.assertEqual(WeatherRiskKind.THUNDERSTORM, assessment.risks[0].kind)

    def test_assess_forecast_rejects_outdoor_window_for_missing_fields(self) -> None:
        assessment = self.service.assess_forecast(
            _forecast(
                [
                    _point("2026-04-18T08:00", apparent_temperature_c=None),
                    _point("2026-04-18T09:00"),
                ]
            ),
            exposure=WeatherExposure.OUTDOOR,
            start_at=self.start_at,
            end_at=self.end_at,
        )

        self.assertEqual(WeatherAssessmentStatus.INSUFFICIENT_DATA, assessment.status)
        self.assertTrue(assessment.should_reject)
        self.assertEqual("forecast_missing_fields", assessment.failure.reason)

    def test_assess_forecast_allows_indoor_window_even_without_points(self) -> None:
        assessment = self.service.assess_forecast(
            _forecast([]),
            exposure=WeatherExposure.INDOOR,
            start_at=self.start_at,
            end_at=self.end_at,
        )

        self.assertEqual(WeatherAssessmentStatus.SAFE, assessment.status)
        self.assertFalse(assessment.should_reject)

    def test_assess_forecast_rejects_active_outdoor_for_strong_wind(self) -> None:
        assessment = self.service.assess_forecast(
            _forecast([_point("2026-04-18T08:00", wind_gusts_kph=45.0)]),
            exposure=WeatherExposure.ACTIVE_OUTDOOR,
            start_at=self.start_at,
            end_at=self.end_at,
        )

        self.assertEqual(WeatherAssessmentStatus.REJECT, assessment.status)
        self.assertEqual(WeatherRiskKind.STRONG_WIND, assessment.risks[0].kind)

    async def test_evaluate_window_returns_non_blocking_failure_for_indoor_plan(self) -> None:
        service = WeatherEvaluationService(
            self.settings,
            weather_client=_FailingWeatherClient("provider unavailable"),
        )

        assessment = await service.evaluate_window(
            LatLng(latitude=-37.8136, longitude=144.9631),
            exposure=WeatherExposure.INDOOR,
            start_at=self.start_at,
            end_at=self.end_at,
        )

        self.assertEqual(WeatherAssessmentStatus.UPSTREAM_FAILURE, assessment.status)
        self.assertFalse(assessment.should_reject)
        self.assertEqual("weather_upstream_failure", assessment.failure.reason)

    async def test_evaluate_window_returns_blocking_failure_for_outdoor_plan(self) -> None:
        service = WeatherEvaluationService(
            self.settings,
            weather_client=_FailingWeatherClient("provider unavailable"),
        )

        assessment = await service.evaluate_window(
            LatLng(latitude=-37.8136, longitude=144.9631),
            exposure=WeatherExposure.OUTDOOR,
            start_at=self.start_at,
            end_at=self.end_at,
        )

        self.assertEqual(WeatherAssessmentStatus.UPSTREAM_FAILURE, assessment.status)
        self.assertTrue(assessment.should_reject)


class _FailingWeatherClient:
    def __init__(self, message: str) -> None:
        self._message = message

    async def get_hourly_forecast(
        self,
        coordinates: LatLng,
        *,
        start_at: datetime,
        end_at: datetime,
    ) -> WeatherForecast:
        raise WeatherUpstreamError(self._message)


def _forecast_payload() -> dict:
    return {
        "latitude": -37.8136,
        "longitude": 144.9631,
        "generationtime_ms": 1.2,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "hourly": {
            "time": [
                "2026-04-18T08:00",
                "2026-04-18T09:00",
                "2026-04-18T10:00",
            ],
            "temperature_2m": [19.5, 21.0, 23.0],
            "apparent_temperature": [18.9, 20.6, 24.8],
            "precipitation": [0.0, 1.2, 4.5],
            "precipitation_probability": [10, 55, 90],
            "weather_code": [3, 61, 95],
            "wind_speed_10m": [12.0, 14.0, 18.0],
            "wind_gusts_10m": [20.0, 25.0, 30.0],
            "is_day": [1, 1, 1],
        },
    }


def _forecast(points: list[WeatherForecastPoint]) -> WeatherForecast:
    return WeatherForecast(
        coordinates=LatLng(latitude=-37.8136, longitude=144.9631),
        timezone="GMT",
        timezone_abbreviation="GMT",
        utc_offset_seconds=0,
        points=tuple(points),
    )


def _point(
    starts_at: str,
    *,
    temperature_c: float | None = 20.0,
    apparent_temperature_c: float | None = 20.0,
    precipitation_mm: float | None = 0.0,
    precipitation_probability_pct: int | None = 10,
    weather_code: int | None = 3,
    wind_speed_kph: float | None = 15.0,
    wind_gusts_kph: float | None = 20.0,
    is_day: bool | None = True,
) -> WeatherForecastPoint:
    return WeatherForecastPoint(
        starts_at=datetime.strptime(starts_at, "%Y-%m-%dT%H:%M").replace(
            tzinfo=timezone.utc
        ),
        temperature_c=temperature_c,
        apparent_temperature_c=apparent_temperature_c,
        precipitation_mm=precipitation_mm,
        precipitation_probability_pct=precipitation_probability_pct,
        weather_code=weather_code,
        wind_speed_kph=wind_speed_kph,
        wind_gusts_kph=wind_gusts_kph,
        is_day=is_day,
    )
