"""Opening-hours parsing for Google Maps weekday description strings."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime

from back_end.domain.models import MapsOpeningHours

logger = logging.getLogger(__name__)

_WEEKDAY_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}
_DASH_RE = re.compile(r"\s*[-\u2010-\u2015]\s*")
_SPACE_RE = re.compile(r"\s+")
_TIME_RE = re.compile(
    r"^(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<period>AM|PM)?$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class _TimeToken:
    hour: int
    minute: int
    period: str | None


@dataclass(frozen=True)
class _Interval:
    start_minute: int
    end_minute: int


def is_open_at_plan_time(
    opening_hours: MapsOpeningHours | None,
    plan_time_iso: str,
) -> bool | None:
    """Return whether Google weekday descriptions say a venue is open then."""

    if opening_hours is None:
        logger.error("Cannot check opening hours because regular_opening_hours is absent.")
        return None
    plan_time = _parse_plan_time(plan_time_iso)
    if plan_time is None:
        return None
    schedule = _parse_weekday_descriptions(opening_hours.weekday_descriptions)
    if schedule is None:
        return None
    weekday = plan_time.weekday()
    minute = plan_time.hour * 60 + plan_time.minute
    todays_intervals = schedule.get(weekday)
    if todays_intervals is None:
        logger.error(
            "Cannot check opening hours for weekday=%s because Google descriptions "
            "did not include that day. descriptions=%r",
            weekday,
            opening_hours.weekday_descriptions,
        )
        return None
    if any(interval.start_minute <= minute < interval.end_minute for interval in todays_intervals):
        return True

    previous_weekday = (weekday - 1) % 7
    previous_intervals = schedule.get(previous_weekday, ())
    rollover_minute = minute + 24 * 60
    if any(
        interval.end_minute > 24 * 60
        and interval.start_minute <= rollover_minute < interval.end_minute
        for interval in previous_intervals
    ):
        return True
    return False


def _parse_plan_time(plan_time_iso: str) -> datetime | None:
    if not isinstance(plan_time_iso, str) or not plan_time_iso.strip():
        logger.error("plan_time_iso must be a non-empty ISO 8601 datetime string.")
        return None
    normalized = plan_time_iso.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        logger.error("Could not parse plan_time_iso=%r as ISO 8601 datetime.", plan_time_iso)
        return None


def _parse_weekday_descriptions(
    weekday_descriptions: tuple[str, ...],
) -> dict[int, tuple[_Interval, ...]] | None:
    schedule: dict[int, tuple[_Interval, ...]] = {}
    for raw_description in weekday_descriptions:
        description = _normalize_text(raw_description)
        if ":" not in description:
            logger.error("Unparseable Google opening-hours description: %r", raw_description)
            return None
        day_text, hours_text = description.split(":", 1)
        weekday = _WEEKDAY_INDEX.get(day_text.strip().casefold())
        if weekday is None:
            logger.error("Unparseable Google opening-hours weekday: %r", raw_description)
            return None
        hours_text = hours_text.strip()
        if hours_text.casefold() == "closed":
            schedule[weekday] = ()
            continue
        if hours_text.casefold() == "open 24 hours":
            schedule[weekday] = (_Interval(start_minute=0, end_minute=24 * 60),)
            continue

        intervals: list[_Interval] = []
        for raw_range in hours_text.split(","):
            interval = _parse_range(raw_range.strip(), raw_description=raw_description)
            if interval is None:
                return None
            intervals.append(interval)
        schedule[weekday] = tuple(intervals)
    return schedule


def _parse_range(raw_range: str, *, raw_description: str) -> _Interval | None:
    parts = _DASH_RE.split(raw_range, maxsplit=1)
    if len(parts) != 2:
        logger.error("Unparseable Google opening-hours range: %r", raw_description)
        return None
    start_token = _parse_time_token(parts[0])
    end_token = _parse_time_token(parts[1])
    if start_token is None or end_token is None:
        logger.error("Unparseable Google opening-hours time: %r", raw_description)
        return None
    start_minute, end_minute = _infer_range_minutes(start_token, end_token)
    if start_minute is None or end_minute is None:
        logger.error("Ambiguous Google opening-hours range: %r", raw_description)
        return None
    if end_minute <= start_minute:
        end_minute += 24 * 60
    return _Interval(start_minute=start_minute, end_minute=end_minute)


def _parse_time_token(value: str) -> _TimeToken | None:
    match = _TIME_RE.match(_normalize_text(value))
    if match is None:
        return None
    hour = int(match.group("hour"))
    minute = int(match.group("minute") or 0)
    period = match.group("period")
    if hour < 1 or hour > 12 or minute < 0 or minute > 59:
        return None
    return _TimeToken(hour=hour, minute=minute, period=period.upper() if period else None)


def _infer_range_minutes(
    start: _TimeToken,
    end: _TimeToken,
) -> tuple[int | None, int | None]:
    if start.period and end.period:
        return _to_minutes(start, start.period), _to_minutes(end, end.period)
    if not start.period and not end.period:
        return None, None
    if not start.period and end.period:
        end_minute = _to_minutes(end, end.period)
        return _best_missing_period_start(start, end_minute), end_minute
    if start.period and not end.period:
        start_minute = _to_minutes(start, start.period)
        return start_minute, _best_missing_period_end(end, start_minute)
    return None, None


def _best_missing_period_start(token: _TimeToken, end_minute: int) -> int:
    candidates = [_to_minutes(token, "AM"), _to_minutes(token, "PM")]
    return min(candidates, key=lambda start: _range_duration(start, end_minute))


def _best_missing_period_end(token: _TimeToken, start_minute: int) -> int:
    candidates = [_to_minutes(token, "AM"), _to_minutes(token, "PM")]
    return min(candidates, key=lambda end: _range_duration(start_minute, end))


def _range_duration(start_minute: int, end_minute: int) -> int:
    duration = end_minute - start_minute
    if duration <= 0:
        duration += 24 * 60
    return duration


def _to_minutes(token: _TimeToken, period: str) -> int:
    hour = token.hour % 12
    if period == "PM":
        hour += 12
    return hour * 60 + token.minute


def _normalize_text(value: str) -> str:
    text = value.replace("\u00a0", " ").replace("\u202f", " ")
    return _SPACE_RE.sub(" ", text).strip()
