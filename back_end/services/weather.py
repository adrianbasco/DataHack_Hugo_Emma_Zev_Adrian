"""Weather evaluation logic for weather-sensitive itineraries."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from back_end.domain.models import (
    LatLng,
    WeatherAssessmentStatus,
    WeatherCheckFailure,
    WeatherExposure,
    WeatherForecast,
    WeatherForecastPoint,
    WeatherRisk,
    WeatherRiskKind,
    WeatherWindowAssessment,
)
from back_end.clients.settings import WeatherSettings
from back_end.clients.weather import (
    ForecastRangeError,
    WeatherClient,
    WeatherClientError,
)

logger = logging.getLogger(__name__)

THUNDERSTORM_CODES = frozenset({95, 96, 99})


class WeatherEvaluationService:
    """Apply explicit weather rejection policy to a forecast window."""

    def __init__(
        self,
        settings: WeatherSettings,
        *,
        weather_client: WeatherClient | None = None,
    ) -> None:
        self._settings = settings
        self._weather_client = weather_client

    async def evaluate_window(
        self,
        coordinates: LatLng,
        *,
        exposure: WeatherExposure,
        start_at: datetime,
        end_at: datetime,
    ) -> WeatherWindowAssessment:
        """Fetch forecast data and evaluate whether it should block a plan."""

        client = self._weather_client or WeatherClient(self._settings)
        owns_client = self._weather_client is None
        try:
            forecast = await client.get_hourly_forecast(
                coordinates,
                start_at=start_at,
                end_at=end_at,
            )
            return self.assess_forecast(
                forecast,
                exposure=exposure,
                start_at=start_at,
                end_at=end_at,
            )
        except WeatherClientError as exc:
            should_reject = exposure is not WeatherExposure.INDOOR
            logger.error(
                "Weather evaluation failed for exposure=%s window=%s->%s: %s",
                exposure.value,
                start_at.isoformat(),
                end_at.isoformat(),
                exc,
            )
            reason = (
                "forecast_range_invalid"
                if isinstance(exc, ForecastRangeError)
                else "weather_upstream_failure"
            )
            summary = (
                "Weather forecast was unavailable; rejecting this weather-sensitive window."
                if should_reject
                else "Weather forecast was unavailable, but indoor plans are not blocked by weather."
            )
            return WeatherWindowAssessment(
                exposure=exposure,
                start_at=_as_utc(start_at),
                end_at=_as_utc(end_at),
                status=WeatherAssessmentStatus.UPSTREAM_FAILURE,
                should_reject=should_reject,
                summary=summary,
                considered_points=(),
                failure=WeatherCheckFailure(reason=reason, message=str(exc)),
            )
        finally:
            if owns_client:
                await client.aclose()

    def assess_forecast(
        self,
        forecast: WeatherForecast,
        *,
        exposure: WeatherExposure,
        start_at: datetime,
        end_at: datetime,
    ) -> WeatherWindowAssessment:
        """Evaluate a normalized forecast against the weather policy."""

        start_utc = _as_utc(start_at)
        end_utc = _as_utc(end_at)
        window_points = tuple(
            point for point in forecast.points if _point_overlaps_window(point, start_utc, end_utc)
        )

        if exposure is WeatherExposure.INDOOR:
            return WeatherWindowAssessment(
                exposure=exposure,
                start_at=start_utc,
                end_at=end_utc,
                status=WeatherAssessmentStatus.SAFE,
                should_reject=False,
                summary="Indoor plans are not blocked by weather policy.",
                considered_points=window_points,
            )

        if not window_points:
            logger.error(
                "Weather assessment had no forecast points for exposure=%s window=%s->%s.",
                exposure.value,
                start_utc.isoformat(),
                end_utc.isoformat(),
            )
            return WeatherWindowAssessment(
                exposure=exposure,
                start_at=start_utc,
                end_at=end_utc,
                status=WeatherAssessmentStatus.INSUFFICIENT_DATA,
                should_reject=True,
                summary="Weather forecast did not cover the requested time window.",
                considered_points=(),
                failure=WeatherCheckFailure(
                    reason="forecast_window_uncovered",
                    message="No hourly forecast points overlapped the requested window.",
                ),
            )

        missing_fields = _find_missing_required_fields(window_points)
        if missing_fields:
            logger.error(
                "Weather assessment is missing required forecast fields for exposure=%s: %s",
                exposure.value,
                ", ".join(missing_fields),
            )
            return WeatherWindowAssessment(
                exposure=exposure,
                start_at=start_utc,
                end_at=end_utc,
                status=WeatherAssessmentStatus.INSUFFICIENT_DATA,
                should_reject=True,
                summary=(
                    "Weather forecast was incomplete for the requested time window, "
                    "so the plan cannot be weather-verified."
                ),
                considered_points=window_points,
                failure=WeatherCheckFailure(
                    reason="forecast_missing_fields",
                    message=(
                        "Missing required hourly forecast fields: "
                        + ", ".join(missing_fields)
                    ),
                ),
            )

        risks: list[WeatherRisk] = []
        for point in window_points:
            point_risks = self._risks_for_point(point, exposure=exposure)
            risks.extend(point_risks)

        if risks:
            summary = _summarize_risks(risks)
            return WeatherWindowAssessment(
                exposure=exposure,
                start_at=start_utc,
                end_at=end_utc,
                status=WeatherAssessmentStatus.REJECT,
                should_reject=True,
                summary=summary,
                considered_points=window_points,
                risks=tuple(risks),
            )

        return WeatherWindowAssessment(
            exposure=exposure,
            start_at=start_utc,
            end_at=end_utc,
            status=WeatherAssessmentStatus.SAFE,
            should_reject=False,
            summary="Forecast passed the configured weather checks for this window.",
            considered_points=window_points,
        )

    def _risks_for_point(
        self,
        point: WeatherForecastPoint,
        *,
        exposure: WeatherExposure,
    ) -> tuple[WeatherRisk, ...]:
        risks: list[WeatherRisk] = []
        if point.weather_code in THUNDERSTORM_CODES:
            risks.append(
                WeatherRisk(
                    kind=WeatherRiskKind.THUNDERSTORM,
                    starts_at=point.starts_at,
                    observed_value=point.weather_code,
                    threshold_value=None,
                    message="Thunderstorm conditions forecast for this hour.",
                )
            )

        if point.precipitation_mm is not None and (
            point.precipitation_mm >= self._settings.hourly_precipitation_mm_threshold
        ):
            risks.append(
                WeatherRisk(
                    kind=WeatherRiskKind.HEAVY_RAIN,
                    starts_at=point.starts_at,
                    observed_value=point.precipitation_mm,
                    threshold_value=self._settings.hourly_precipitation_mm_threshold,
                    message=(
                        "Hourly precipitation exceeds the configured heavy-rain threshold."
                    ),
                )
            )

        if point.apparent_temperature_c is not None and (
            point.apparent_temperature_c >= self._settings.extreme_heat_c_threshold
        ):
            risks.append(
                WeatherRisk(
                    kind=WeatherRiskKind.EXTREME_HEAT,
                    starts_at=point.starts_at,
                    observed_value=point.apparent_temperature_c,
                    threshold_value=self._settings.extreme_heat_c_threshold,
                    message="Apparent temperature exceeds the configured heat threshold.",
                )
            )

        if (
            exposure is WeatherExposure.ACTIVE_OUTDOOR
            and point.wind_gusts_kph is not None
            and point.wind_gusts_kph >= self._settings.strong_wind_kph_threshold
        ):
            risks.append(
                WeatherRisk(
                    kind=WeatherRiskKind.STRONG_WIND,
                    starts_at=point.starts_at,
                    observed_value=point.wind_gusts_kph,
                    threshold_value=self._settings.strong_wind_kph_threshold,
                    message="Wind gusts exceed the configured threshold for active outdoor plans.",
                )
            )

        if (
            point.precipitation_probability_pct is not None
            and point.precipitation_probability_pct
            >= self._settings.precipitation_probability_threshold_pct
            and point.precipitation_mm is not None
            and point.precipitation_mm > 0.0
            and not any(risk.kind is WeatherRiskKind.HEAVY_RAIN for risk in risks)
        ):
            risks.append(
                WeatherRisk(
                    kind=WeatherRiskKind.HEAVY_RAIN,
                    starts_at=point.starts_at,
                    observed_value=point.precipitation_probability_pct,
                    threshold_value=self._settings.precipitation_probability_threshold_pct,
                    message="Rain probability is high enough to reject this outdoor hour.",
                )
            )

        return tuple(risks)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ForecastRangeError(
            "Weather evaluation requires timezone-aware datetimes."
        )
    return value.astimezone(timezone.utc)


def _point_overlaps_window(
    point: WeatherForecastPoint,
    start_at: datetime,
    end_at: datetime,
) -> bool:
    point_end = point.starts_at + timedelta(hours=1)
    return point.starts_at < end_at and point_end > start_at


def _find_missing_required_fields(points: tuple[WeatherForecastPoint, ...]) -> tuple[str, ...]:
    missing: set[str] = set()
    for point in points:
        if point.apparent_temperature_c is None:
            missing.add("apparent_temperature_c")
        if point.precipitation_mm is None:
            missing.add("precipitation_mm")
        if point.precipitation_probability_pct is None:
            missing.add("precipitation_probability_pct")
        if point.weather_code is None:
            missing.add("weather_code")
        if point.wind_gusts_kph is None:
            missing.add("wind_gusts_kph")
    return tuple(sorted(missing))


def _summarize_risks(risks: list[WeatherRisk]) -> str:
    risk_kinds = sorted({risk.kind.value for risk in risks})
    joined = ", ".join(risk_kinds)
    return f"Forecast rejected this window due to: {joined}."
