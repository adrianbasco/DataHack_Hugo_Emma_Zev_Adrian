"""Deterministic plan-time resolution for precache planner cells.

This module resolves a template-level ``time_of_day`` slot into one or more
concrete timezone-aware ISO datetimes for a specific location bucket.

The resolver is intentionally strict: it does not silently fall back from an
unknown template slot, a naive reference datetime, or an impossible date range.
Callers should treat resolution failures as configuration errors and surface
them loudly.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from back_end.precache.models import LocationBucket

logger = logging.getLogger(__name__)

DEFAULT_DAYS_AHEAD_MIN = 1
DEFAULT_DAYS_AHEAD_MAX = 21
DEFAULT_MAX_CANDIDATES = 3

_TIME_OF_DAY_HOURS = {
    "morning": 10,
    "midday": 12,
    "afternoon": 14,
    "evening": 19,
    "night": 21,
    "late_night": 21,
    "flexible": 18,
}
_WEEKDAY_NAMES = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)
_SUPPORTED_WEEKDAY_BIASES = {"default", "weekday", "weekend"}


class PlanTimeResolverError(ValueError):
    """Raised when a plan time cannot be resolved deterministically."""


@dataclass(frozen=True)
class PlanTimeCandidate:
    """One concrete candidate plan time for a template/bucket pair."""

    plan_time_iso: str
    day_of_week: str
    reason: str


def bucket_timezone(bucket: LocationBucket) -> ZoneInfo:
    """Return the canonical timezone for a bucket.

    Buckets are Sydney-only today, but this function is a deliberate seam for
    future city-specific timezone routing.
    """

    try:
        return ZoneInfo("Australia/Sydney")
    except ZoneInfoNotFoundError as exc:
        logger.error(
            "Could not load timezone for bucket_id=%r: Australia/Sydney is unavailable.",
            getattr(bucket, "bucket_id", None),
        )
        raise PlanTimeResolverError(
            "Australia/Sydney timezone data is unavailable for plan-time resolution."
        ) from exc


def resolve_plan_time(
    template: Mapping[str, object],
    bucket: LocationBucket,
    reference_now: datetime,
    **kwargs: object,
) -> PlanTimeCandidate:
    """Resolve the first candidate plan time for one template/bucket pair."""

    return resolve_plan_time_candidates(
        template=template,
        bucket=bucket,
        reference_now=reference_now,
        **kwargs,
    )[0]


def resolve_plan_time_candidates(
    template: Mapping[str, object],
    bucket: LocationBucket,
    reference_now: datetime,
    *,
    weekday_bias: str = "default",
    days_ahead_min: int = DEFAULT_DAYS_AHEAD_MIN,
    days_ahead_max: int = DEFAULT_DAYS_AHEAD_MAX,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
) -> tuple[PlanTimeCandidate, ...]:
    """Resolve one or more deterministic plan times for a template/bucket pair."""

    if not isinstance(template, Mapping):
        logger.error(
            "template must be a Mapping for bucket_id=%r, got %s.",
            getattr(bucket, "bucket_id", None),
            type(template).__name__,
        )
        raise PlanTimeResolverError("template must be a Mapping.")
    template_id = _require_nonempty_string(template.get("id"), "template.id")
    time_of_day = _normalize_time_of_day(template.get("time_of_day"), template_id)
    localized_now = _normalize_reference_now(reference_now, bucket)
    timezone = bucket_timezone(bucket)
    _validate_search_window(
        weekday_bias=weekday_bias,
        days_ahead_min=days_ahead_min,
        days_ahead_max=days_ahead_max,
        max_candidates=max_candidates,
        template_id=template_id,
    )

    target_hour = _TIME_OF_DAY_HOURS[time_of_day]
    preferred_weekdays = _preferred_weekdays(time_of_day, weekday_bias)
    candidates: list[PlanTimeCandidate] = []

    for day_offset in range(days_ahead_min, days_ahead_max + 1):
        candidate_date = (localized_now + timedelta(days=day_offset)).date()
        candidate_dt = datetime(
            year=candidate_date.year,
            month=candidate_date.month,
            day=candidate_date.day,
            hour=target_hour,
            minute=0,
            second=0,
            tzinfo=timezone,
        )
        if candidate_dt <= localized_now:
            continue
        weekday_index = candidate_dt.weekday()
        if weekday_index not in preferred_weekdays:
            continue
        plan_time_iso = _validated_plan_time_iso(candidate_dt, template_id)
        candidates.append(
            PlanTimeCandidate(
                plan_time_iso=plan_time_iso,
                day_of_week=_WEEKDAY_NAMES[weekday_index],
                reason=_build_reason(
                    time_of_day=time_of_day,
                    weekday_bias=weekday_bias,
                    template_id=template_id,
                    timezone_name=timezone.key,
                    weekday_index=weekday_index,
                    day_offset=day_offset,
                ),
            )
        )
        if len(candidates) >= max_candidates:
            break

    if not candidates:
        logger.error(
            "Could not resolve plan times for template_id=%r within %s-%s days, "
            "time_of_day=%r, weekday_bias=%r.",
            template_id,
            days_ahead_min,
            days_ahead_max,
            time_of_day,
            weekday_bias,
        )
        raise PlanTimeResolverError(
            "No plan-time candidates matched the requested slot and date window."
        )

    return tuple(candidates)


def _normalize_reference_now(reference_now: datetime, bucket: LocationBucket) -> datetime:
    if not isinstance(reference_now, datetime):
        logger.error(
            "reference_now must be a datetime, got %s for bucket_id=%r.",
            type(reference_now).__name__,
            getattr(bucket, "bucket_id", None),
        )
        raise PlanTimeResolverError("reference_now must be a datetime instance.")
    if reference_now.tzinfo is None or reference_now.utcoffset() is None:
        logger.error(
            "reference_now must be timezone-aware for bucket_id=%r, got %r.",
            getattr(bucket, "bucket_id", None),
            reference_now,
        )
        raise PlanTimeResolverError("reference_now must be timezone-aware.")
    return reference_now.astimezone(bucket_timezone(bucket))


def _validate_search_window(
    *,
    weekday_bias: str,
    days_ahead_min: int,
    days_ahead_max: int,
    max_candidates: int,
    template_id: str,
) -> None:
    if weekday_bias not in _SUPPORTED_WEEKDAY_BIASES:
        logger.error(
            "Unsupported weekday_bias=%r for template_id=%r; expected one of %s.",
            weekday_bias,
            template_id,
            sorted(_SUPPORTED_WEEKDAY_BIASES),
        )
        raise PlanTimeResolverError(
            f"weekday_bias must be one of {sorted(_SUPPORTED_WEEKDAY_BIASES)!r}."
        )
    if not isinstance(days_ahead_min, int) or not isinstance(days_ahead_max, int):
        logger.error(
            "days_ahead_min/max must be integers for template_id=%r; got %r/%r.",
            template_id,
            days_ahead_min,
            days_ahead_max,
        )
        raise PlanTimeResolverError("days_ahead_min and days_ahead_max must be integers.")
    if days_ahead_min < 0:
        logger.error(
            "days_ahead_min must be >= 0 for template_id=%r, got %s.",
            template_id,
            days_ahead_min,
        )
        raise PlanTimeResolverError("days_ahead_min must be >= 0.")
    if days_ahead_max < days_ahead_min:
        logger.error(
            "days_ahead_max must be >= days_ahead_min for template_id=%r; got %s < %s.",
            template_id,
            days_ahead_max,
            days_ahead_min,
        )
        raise PlanTimeResolverError("days_ahead_max must be >= days_ahead_min.")
    if not isinstance(max_candidates, int) or max_candidates <= 0:
        logger.error(
            "max_candidates must be a positive integer for template_id=%r, got %r.",
            template_id,
            max_candidates,
        )
        raise PlanTimeResolverError("max_candidates must be a positive integer.")


def _normalize_time_of_day(value: object, template_id: str) -> str:
    if not isinstance(value, str) or not value.strip():
        logger.error(
            "template_id=%r must define a non-empty time_of_day, got %r.",
            template_id,
            value,
        )
        raise PlanTimeResolverError("template.time_of_day must be a non-empty string.")
    normalized = value.strip().casefold()
    if normalized not in _TIME_OF_DAY_HOURS:
        logger.error(
            "Unsupported time_of_day=%r for template_id=%r; expected one of %s.",
            value,
            template_id,
            sorted(_TIME_OF_DAY_HOURS),
        )
        raise PlanTimeResolverError(
            f"Unsupported template time_of_day={value!r}."
        )
    return normalized


def _preferred_weekdays(time_of_day: str, weekday_bias: str) -> tuple[int, ...]:
    if weekday_bias == "weekday":
        return (0, 1, 2, 3, 4)
    if weekday_bias == "weekend":
        return (5, 6)
    if time_of_day in {"evening", "night", "late_night", "flexible"}:
        return (4, 5, 6)
    return (5, 6)


def _validated_plan_time_iso(candidate_dt: datetime, template_id: str) -> str:
    plan_time_iso = candidate_dt.isoformat(timespec="seconds")
    if plan_time_iso.endswith("Z"):
        logger.error(
            "Resolved plan_time_iso unexpectedly ended with Z for template_id=%r: %r.",
            template_id,
            plan_time_iso,
        )
        raise PlanTimeResolverError("plan_time_iso must include an explicit offset.")
    try:
        parsed = datetime.fromisoformat(plan_time_iso)
    except ValueError as exc:
        logger.error(
            "Resolved plan_time_iso is not valid ISO for template_id=%r: %r.",
            template_id,
            plan_time_iso,
        )
        raise PlanTimeResolverError(
            "Resolved plan_time_iso is not valid ISO 8601."
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        logger.error(
            "Resolved plan_time_iso is missing an explicit offset for template_id=%r: %r.",
            template_id,
            plan_time_iso,
        )
        raise PlanTimeResolverError("plan_time_iso must include an explicit offset.")
    return plan_time_iso


def _build_reason(
    *,
    time_of_day: str,
    weekday_bias: str,
    template_id: str,
    timezone_name: str,
    weekday_index: int,
    day_offset: int,
) -> str:
    hour = _TIME_OF_DAY_HOURS[time_of_day]
    return (
        f"Resolved template_id={template_id!r} time_of_day={time_of_day!r} "
        f"to {hour:02d}:00 in {timezone_name} and selected the next "
        f"{_WEEKDAY_NAMES[weekday_index]} {day_offset} day(s) ahead "
        f"using weekday_bias={weekday_bias!r}."
    )


def _require_nonempty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        logger.error("%s must be a non-empty string, got %r.", field_name, value)
        raise PlanTimeResolverError(f"{field_name} must be a non-empty string.")
    return value.strip()
