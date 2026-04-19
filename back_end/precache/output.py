"""Parquet output storage for generated pre-cache date plans and failures."""

from __future__ import annotations

import hashlib
import json
import logging
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from back_end.agents.precache_planner import PrecachePlannerFailure

logger = logging.getLogger(__name__)

DEFAULT_PRECACHE_OUTPUT_PATH = Path("data/precache/plans.parquet")
DEFAULT_PRECACHE_FAILURE_OUTPUT_PATH = Path("data/precache/failures.parquet")

OUTPUT_COLUMNS: tuple[str, ...] = (
    "plan_id",
    "bucket_id",
    "bucket_label",
    "bucket_latitude",
    "bucket_longitude",
    "bucket_radius_km",
    "bucket_transport_mode",
    "bucket_tags_json",
    "bucket_metadata_json",
    "template_id",
    "template_title",
    "vibe",
    "time_of_day",
    "weather_sensitive",
    "template_duration_hours",
    "template_description",
    "template_metadata_json",
    "plan_title",
    "plan_hook",
    "plan_time_iso",
    "stops_json",
    "search_text",
    "card_json",
    "fsq_place_ids_sorted",
    "fsq_place_id_count",
    "verification_json",
    "generated_at_utc",
    "written_at_utc",
    "model",
)

FAILURE_OUTPUT_COLUMNS: tuple[str, ...] = (
    "failure_id",
    "bucket_id",
    "template_id",
    "plan_time_iso",
    "attempt_index",
    "reason",
    "detail",
    "rejected_ideas_json",
    "signature",
    "tool_executions_count",
    "model",
    "generated_at_utc",
    "written_at_utc",
)

REQUIRED_BUCKET_METADATA: tuple[str, ...] = (
    "label",
    "latitude",
    "longitude",
    "radius_km",
    "transport_mode",
)
REQUIRED_TEMPLATE_METADATA: tuple[str, ...] = (
    "title",
    "vibe",
    "time_of_day",
    "weather_sensitive",
)


class PrecacheOutputError(RuntimeError):
    """Raised when pre-cache output cannot be written or read safely."""


@dataclass(frozen=True)
class PrecachePlanOutput:
    """One generated date plan ready for parquet output."""

    bucket_id: str
    template_id: str
    bucket_metadata: Mapping[str, Any]
    template_metadata: Mapping[str, Any]
    plan_title: str
    plan_hook: str
    plan_time_iso: str
    stops: Sequence[Mapping[str, Any]]
    search_text: str
    card_payload: Mapping[str, Any]
    verification: Mapping[str, Any]
    generated_at_utc: datetime | str
    model: str
    written_at_utc: datetime | str | None = None


@dataclass(frozen=True)
class PrecacheWriteResult:
    """Summary for an append-or-dedupe parquet write."""

    output_path: Path
    written_count: int
    replaced_count: int
    total_count: int
    plan_ids: tuple[str, ...]


@dataclass(frozen=True)
class PrecacheFailureOutput:
    """One planner failure enriched with the cell metadata needed for storage."""

    bucket_id: str
    template_id: str
    plan_time_iso: str
    attempt_index: int
    reason: str
    detail: str
    rejected_ideas: Sequence[str] = ()
    signature: str | None = None
    tool_executions_count: int = 0
    model: str | None = None
    generated_at_utc: datetime | str | None = None
    written_at_utc: datetime | str | None = None


@dataclass(frozen=True)
class PrecacheFailureWriteResult:
    """Summary for an append-or-dedupe failure parquet write."""

    output_path: Path
    written_count: int
    replaced_count: int
    total_count: int
    failure_ids: tuple[str, ...]


def make_plan_id(
    *,
    bucket_id: str,
    template_id: str,
    fsq_place_ids: Sequence[str],
) -> str:
    """Return the stable plan id for a bucket, template, and venue signature."""

    clean_bucket_id = _required_text(bucket_id, "bucket_id")
    clean_template_id = _required_text(template_id, "template_id")
    ids_sorted = _sorted_fsq_place_ids(fsq_place_ids)
    payload = {
        "bucket_id": clean_bucket_id,
        "template_id": clean_template_id,
        "fsq_place_ids": ids_sorted,
    }
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()


def make_failure_id(
    *,
    bucket_id: str,
    template_id: str,
    plan_time_iso: str,
    attempt_index: int,
) -> str:
    """Return the stable failure id for one bucket/template/time/attempt cell."""

    clean_bucket_id = _required_text(bucket_id, "bucket_id")
    clean_template_id = _required_text(template_id, "template_id")
    normalized_plan_time_iso = _normalize_iso_timestamp_preserving_offset(
        plan_time_iso,
        field_name="plan_time_iso",
    )
    clean_attempt_index = _required_nonnegative_int(attempt_index, "attempt_index")
    payload = {
        "bucket_id": clean_bucket_id,
        "template_id": clean_template_id,
        "plan_time_iso": normalized_plan_time_iso,
        "attempt_index": clean_attempt_index,
    }
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()


def build_precache_failure_output(
    *,
    bucket_id: str,
    template_id: str,
    plan_time_iso: str,
    attempt_index: int,
    failure: "PrecachePlannerFailure",
    generated_at_utc: datetime | str | None = None,
    written_at_utc: datetime | str | None = None,
) -> PrecacheFailureOutput:
    """Wrap one ``PrecachePlannerFailure`` with the metadata needed for parquet output."""

    if not hasattr(failure, "reason") or not hasattr(failure, "detail"):
        raise PrecacheOutputError(
            "failure must expose PrecachePlannerFailure-like reason/detail attributes."
        )

    rejected_ideas = getattr(failure, "rejected_ideas", ())
    tool_executions = getattr(failure, "tool_executions", ())
    signature = getattr(failure, "signature", None)
    model = getattr(failure, "model", None)
    return PrecacheFailureOutput(
        bucket_id=bucket_id,
        template_id=template_id,
        plan_time_iso=plan_time_iso,
        attempt_index=attempt_index,
        reason=_required_text(getattr(failure, "reason"), "failure.reason"),
        detail=_required_text(getattr(failure, "detail"), "failure.detail"),
        rejected_ideas=tuple(_string_list(rejected_ideas, field_name="failure.rejected_ideas")),
        signature=_optional_text(signature),
        tool_executions_count=_required_nonnegative_int(
            len(tool_executions),
            "failure.tool_executions_count",
        ),
        model=_optional_text(model),
        generated_at_utc=generated_at_utc or _timestamp_now(),
        written_at_utc=written_at_utc,
    )


def fsq_place_ids_sorted_signature(fsq_place_ids: Sequence[str]) -> str:
    """Return a deterministic JSON signature for a plan's grounded FSQ ids."""

    return _json_dumps(_sorted_fsq_place_ids(fsq_place_ids))


def append_precache_plans(
    plans: Sequence[PrecachePlanOutput],
    *,
    output_path: Path | str = DEFAULT_PRECACHE_OUTPUT_PATH,
) -> PrecacheWriteResult:
    """Append generated plans to parquet, replacing existing rows by plan_id."""

    if not plans:
        raise ValueError("plans must contain at least one PrecachePlanOutput.")

    path = Path(output_path)
    _require_parquet_path(path, label="pre-cache output")
    written_at_utc = _timestamp_now()
    rows = [_record_to_row(plan, written_at_utc=written_at_utc) for plan in plans]
    plan_ids = tuple(str(row["plan_id"]) for row in rows)
    duplicate_plan_ids = sorted(_duplicates(plan_ids))
    if duplicate_plan_ids:
        logger.error(
            "Incoming pre-cache plans contain duplicate plan_ids=%s.",
            duplicate_plan_ids,
        )
        raise PrecacheOutputError(
            f"Incoming pre-cache plans contain duplicate plan_ids {duplicate_plan_ids}."
        )

    new_df = pd.DataFrame(rows, columns=list(OUTPUT_COLUMNS))
    if path.exists():
        existing_df = read_precache_output(path)
        duplicate_mask = existing_df["plan_id"].isin(plan_ids)
        replaced_count = int(duplicate_mask.sum())
        combined_df = pd.concat(
            [existing_df.loc[~duplicate_mask], new_df],
            ignore_index=True,
        )
    else:
        replaced_count = 0
        combined_df = new_df

    path.parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_parquet(path, index=False)
    logger.info(
        "Wrote %d pre-cache plans to %s; replaced=%d total=%d.",
        len(new_df),
        path,
        replaced_count,
        len(combined_df),
    )
    return PrecacheWriteResult(
        output_path=path,
        written_count=len(new_df),
        replaced_count=replaced_count,
        total_count=len(combined_df),
        plan_ids=plan_ids,
    )


def append_precache_failures(
    failures: Sequence[PrecacheFailureOutput],
    *,
    output_path: Path | str = DEFAULT_PRECACHE_FAILURE_OUTPUT_PATH,
) -> PrecacheFailureWriteResult:
    """Append planner failures to parquet, replacing existing rows by failure_id."""

    if not failures:
        raise ValueError("failures must contain at least one PrecacheFailureOutput.")

    path = Path(output_path)
    _require_parquet_path(path, label="pre-cache failure output")
    written_at_utc = _timestamp_now()
    rows = [_failure_record_to_row(failure, written_at_utc=written_at_utc) for failure in failures]
    failure_ids = tuple(str(row["failure_id"]) for row in rows)
    duplicate_failure_ids = sorted(_duplicates(failure_ids))
    if duplicate_failure_ids:
        logger.error(
            "Incoming pre-cache failures contain duplicate failure_ids=%s.",
            duplicate_failure_ids,
        )
        raise PrecacheOutputError(
            "Incoming pre-cache failures contain duplicate failure_ids "
            f"{duplicate_failure_ids}."
        )

    new_df = pd.DataFrame(rows, columns=list(FAILURE_OUTPUT_COLUMNS))
    if path.exists():
        existing_df = read_precache_failures(path)
        duplicate_mask = existing_df["failure_id"].isin(failure_ids)
        replaced_count = int(duplicate_mask.sum())
        combined_df = pd.concat(
            [existing_df.loc[~duplicate_mask], new_df],
            ignore_index=True,
        )
    else:
        replaced_count = 0
        combined_df = new_df

    path.parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_parquet(path, index=False)
    logger.info(
        "Wrote %d pre-cache failures to %s; replaced=%d total=%d.",
        len(new_df),
        path,
        replaced_count,
        len(combined_df),
    )
    return PrecacheFailureWriteResult(
        output_path=path,
        written_count=len(new_df),
        replaced_count=replaced_count,
        total_count=len(combined_df),
        failure_ids=failure_ids,
    )


def read_precache_output(
    output_path: Path | str = DEFAULT_PRECACHE_OUTPUT_PATH,
) -> pd.DataFrame:
    """Read the generated plan parquet output after validating its schema."""

    path = Path(output_path)
    _require_parquet_path(path, label="pre-cache output")
    if not path.exists():
        raise FileNotFoundError(f"Pre-cache output parquet not found at {path}.")

    df = pd.read_parquet(path)
    df = _upgrade_output_schema(df)
    _validate_output_schema(df, source=path)
    return df.loc[:, list(OUTPUT_COLUMNS)].copy()


def read_precache_failures(
    output_path: Path | str = DEFAULT_PRECACHE_FAILURE_OUTPUT_PATH,
) -> pd.DataFrame:
    """Read the generated failure parquet output after validating its schema."""

    path = Path(output_path)
    _require_parquet_path(path, label="pre-cache failure output")
    if not path.exists():
        raise FileNotFoundError(f"Pre-cache failure parquet not found at {path}.")

    df = pd.read_parquet(path)
    _validate_failure_output_schema(df, source=path)
    return df.loc[:, list(FAILURE_OUTPUT_COLUMNS)].copy()


def summarize_failures_by_reason(
    output_path: Path | str = DEFAULT_PRECACHE_FAILURE_OUTPUT_PATH,
) -> dict[str, int]:
    """Return failure counts by reason for run-summary reporting."""

    path = Path(output_path)
    _require_parquet_path(path, label="pre-cache failure output")
    if not path.exists():
        logger.info("No pre-cache failure parquet exists at %s; returning empty summary.", path)
        return {}

    df = read_precache_failures(path)
    if df.empty:
        return {}
    counts = df["reason"].astype(str).value_counts(sort=False).sort_index().to_dict()
    return {str(reason): int(count) for reason, count in counts.items()}


def read_existing_signatures(
    bucket_id: str,
    template_id: str,
    *,
    output_path: Path | str = DEFAULT_PRECACHE_OUTPUT_PATH,
) -> set[str]:
    """Read existing FSQ-id signatures for one bucket/template pair."""

    clean_bucket_id = _required_text(bucket_id, "bucket_id")
    clean_template_id = _required_text(template_id, "template_id")
    path = Path(output_path)
    _require_parquet_path(path, label="pre-cache output")
    if not path.exists():
        logger.info("No pre-cache output parquet exists at %s; no signatures loaded.", path)
        return set()

    df = read_precache_output(path)
    matching = df.loc[
        (df["bucket_id"] == clean_bucket_id)
        & (df["template_id"] == clean_template_id),
        "fsq_place_ids_sorted",
    ]
    return set(str(signature) for signature in matching)


def _record_to_row(
    record: PrecachePlanOutput,
    *,
    written_at_utc: str,
) -> dict[str, Any]:
    bucket_id = _required_text(record.bucket_id, "bucket_id")
    template_id = _required_text(record.template_id, "template_id")
    model = _required_text(record.model, "model")
    bucket_metadata = _validated_metadata(
        record.bucket_metadata,
        required_fields=REQUIRED_BUCKET_METADATA,
        label="bucket_metadata",
    )
    template_metadata = _validated_metadata(
        record.template_metadata,
        required_fields=REQUIRED_TEMPLATE_METADATA,
        label="template_metadata",
    )
    stops = _validated_stops(record.stops)
    fsq_place_ids = _venue_fsq_place_ids(stops)
    fsq_signature = fsq_place_ids_sorted_signature(fsq_place_ids)
    plan_id = make_plan_id(
        bucket_id=bucket_id,
        template_id=template_id,
        fsq_place_ids=fsq_place_ids,
    )
    vibe_values = tuple(_string_list(template_metadata["vibe"], field_name="template.vibe"))
    if not vibe_values:
        raise PrecacheOutputError("template_metadata.vibe must contain at least one value.")

    row = {
        "plan_id": plan_id,
        "bucket_id": bucket_id,
        "bucket_label": _required_text(bucket_metadata["label"], "bucket_metadata.label"),
        "bucket_latitude": _required_float(
            bucket_metadata["latitude"],
            "bucket_metadata.latitude",
        ),
        "bucket_longitude": _required_float(
            bucket_metadata["longitude"],
            "bucket_metadata.longitude",
        ),
        "bucket_radius_km": _required_positive_float(
            bucket_metadata["radius_km"],
            "bucket_metadata.radius_km",
        ),
        "bucket_transport_mode": _required_text(
            bucket_metadata["transport_mode"],
            "bucket_metadata.transport_mode",
        ),
        "bucket_tags_json": _json_dumps(
            _string_list(bucket_metadata.get("tags", ()), field_name="bucket_metadata.tags")
        ),
        "bucket_metadata_json": _json_dumps(bucket_metadata),
        "template_id": template_id,
        "template_title": _required_text(
            template_metadata["title"],
            "template_metadata.title",
        ),
        "vibe": _json_dumps(vibe_values),
        "time_of_day": _required_text(
            template_metadata["time_of_day"],
            "template_metadata.time_of_day",
        ),
        "weather_sensitive": _required_bool(
            template_metadata["weather_sensitive"],
            "template_metadata.weather_sensitive",
        ),
        "template_duration_hours": _optional_float(
            template_metadata.get("duration_hours"),
            "template_metadata.duration_hours",
        ),
        "template_description": _optional_text(template_metadata.get("description")),
        "template_metadata_json": _json_dumps(template_metadata),
        "plan_title": _required_text(record.plan_title, "plan_title"),
        "plan_hook": _required_text(record.plan_hook, "plan_hook"),
        "plan_time_iso": _normalize_iso_timestamp_preserving_offset(
            record.plan_time_iso,
            field_name="plan_time_iso",
        ),
        "stops_json": _json_dumps(stops),
        "search_text": _required_text(record.search_text, "search_text"),
        "card_json": _json_dumps(
            _validated_metadata(record.card_payload, required_fields=(), label="card_payload")
        ),
        "fsq_place_ids_sorted": fsq_signature,
        "fsq_place_id_count": len(fsq_place_ids),
        "verification_json": _json_dumps(
            _validated_metadata(record.verification, required_fields=(), label="verification")
        ),
        "generated_at_utc": _normalize_timestamp(
            record.generated_at_utc,
            field_name="generated_at_utc",
        ),
        "written_at_utc": _normalize_timestamp(
            record.written_at_utc,
            field_name="written_at_utc",
        )
        if record.written_at_utc is not None
        else written_at_utc,
        "model": model,
    }
    return row


def _upgrade_output_schema(df: pd.DataFrame) -> pd.DataFrame:
    upgraded = df.copy()
    defaults: dict[str, Any] = {
        "plan_title": None,
        "plan_hook": None,
        "plan_time_iso": None,
        "search_text": None,
        "card_json": None,
    }
    for column, default in defaults.items():
        if column not in upgraded.columns:
            upgraded[column] = default
    return upgraded


def _failure_record_to_row(
    record: PrecacheFailureOutput,
    *,
    written_at_utc: str,
) -> dict[str, Any]:
    bucket_id = _required_text(record.bucket_id, "bucket_id")
    template_id = _required_text(record.template_id, "template_id")
    plan_time_iso = _normalize_iso_timestamp_preserving_offset(
        record.plan_time_iso,
        field_name="plan_time_iso",
    )
    attempt_index = _required_nonnegative_int(record.attempt_index, "attempt_index")
    reason = _required_text(record.reason, "reason")
    detail = _required_text(record.detail, "detail")
    rejected_ideas = _string_list(record.rejected_ideas, field_name="rejected_ideas")
    signature = _optional_text(record.signature)
    tool_executions_count = _required_nonnegative_int(
        record.tool_executions_count,
        "tool_executions_count",
    )
    model = _optional_text(record.model)
    generated_at_utc = (
        _normalize_timestamp(record.generated_at_utc, field_name="generated_at_utc")
        if record.generated_at_utc is not None
        else _timestamp_now()
    )
    failure_id = make_failure_id(
        bucket_id=bucket_id,
        template_id=template_id,
        plan_time_iso=plan_time_iso,
        attempt_index=attempt_index,
    )
    row = {
        "failure_id": failure_id,
        "bucket_id": bucket_id,
        "template_id": template_id,
        "plan_time_iso": plan_time_iso,
        "attempt_index": attempt_index,
        "reason": reason,
        "detail": detail,
        "rejected_ideas_json": _json_dumps(rejected_ideas),
        "signature": signature,
        "tool_executions_count": tool_executions_count,
        "model": model,
        "generated_at_utc": generated_at_utc,
        "written_at_utc": _normalize_timestamp(
            record.written_at_utc,
            field_name="written_at_utc",
        )
        if record.written_at_utc is not None
        else written_at_utc,
    }
    return row


def _validate_output_schema(df: pd.DataFrame, *, source: Path) -> None:
    actual = tuple(str(column) for column in df.columns)
    missing = sorted(set(OUTPUT_COLUMNS) - set(actual))
    extra = sorted(set(actual) - set(OUTPUT_COLUMNS))
    if missing or extra:
        logger.error(
            "Pre-cache output parquet %s has invalid schema missing=%s extra=%s.",
            source,
            missing,
            extra,
        )
        raise PrecacheOutputError(
            f"Pre-cache output parquet {source} has invalid schema. "
            f"Missing columns: {missing}. Extra columns: {extra}."
        )
    if df["plan_id"].duplicated().any():
        duplicates = sorted(
            df.loc[df["plan_id"].duplicated(), "plan_id"].astype(str).unique()
        )
        logger.error(
            "Pre-cache output parquet %s contains duplicate plan_ids=%s.",
            source,
            duplicates,
        )
        raise PrecacheOutputError(
            f"Pre-cache output parquet {source} contains duplicate plan_ids {duplicates}."
        )


def _validate_failure_output_schema(df: pd.DataFrame, *, source: Path) -> None:
    actual = tuple(str(column) for column in df.columns)
    missing = sorted(set(FAILURE_OUTPUT_COLUMNS) - set(actual))
    extra = sorted(set(actual) - set(FAILURE_OUTPUT_COLUMNS))
    if missing or extra:
        logger.error(
            "Pre-cache failure parquet %s has invalid schema missing=%s extra=%s.",
            source,
            missing,
            extra,
        )
        raise PrecacheOutputError(
            f"Pre-cache failure parquet {source} has invalid schema. "
            f"Missing columns: {missing}. Extra columns: {extra}."
        )
    if df["failure_id"].duplicated().any():
        duplicates = sorted(
            df.loc[df["failure_id"].duplicated(), "failure_id"].astype(str).unique()
        )
        logger.error(
            "Pre-cache failure parquet %s contains duplicate failure_ids=%s.",
            source,
            duplicates,
        )
        raise PrecacheOutputError(
            f"Pre-cache failure parquet {source} contains duplicate failure_ids {duplicates}."
        )


def _require_parquet_path(path: Path, *, label: str) -> None:
    if path.suffix != ".parquet":
        raise ValueError(f"Expected {label} parquet path, got {path}.")


def _validated_metadata(
    value: Mapping[str, Any],
    *,
    required_fields: tuple[str, ...],
    label: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PrecacheOutputError(f"{label} must be a mapping.")
    metadata = dict(value)
    missing = sorted(field_name for field_name in required_fields if field_name not in metadata)
    if missing:
        logger.error("%s is missing required fields=%s.", label, missing)
        raise PrecacheOutputError(f"{label} is missing required fields {missing}.")
    _assert_json_serializable(metadata, field_name=label)
    return metadata


def _validated_stops(stops: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    if isinstance(stops, (str, bytes)) or not isinstance(stops, Sequence):
        raise PrecacheOutputError("stops must be a non-empty sequence of mappings.")
    if not stops:
        raise PrecacheOutputError("stops must contain at least one stop.")

    normalized: list[dict[str, Any]] = []
    for index, stop in enumerate(stops):
        if not isinstance(stop, Mapping):
            raise PrecacheOutputError(f"stops[{index}] must be a mapping.")
        normalized_stop = dict(stop)
        kind = normalized_stop.get("kind")
        fsq_place_id = normalized_stop.get("fsq_place_id")
        if kind == "venue" and not _optional_text(fsq_place_id):
            logger.error("Venue stop at index=%d is missing fsq_place_id.", index)
            raise PrecacheOutputError(f"Venue stop at index {index} is missing fsq_place_id.")
        _assert_json_serializable(normalized_stop, field_name=f"stops[{index}]")
        normalized.append(normalized_stop)

    fsq_place_ids = _venue_fsq_place_ids(tuple(normalized))
    if not fsq_place_ids:
        raise PrecacheOutputError("stops must contain at least one fsq_place_id.")
    duplicate_ids = sorted(_duplicates(fsq_place_ids))
    if duplicate_ids:
        logger.error("Plan stops contain duplicate fsq_place_ids=%s.", duplicate_ids)
        raise PrecacheOutputError(f"Plan stops contain duplicate fsq_place_ids {duplicate_ids}.")
    return tuple(normalized)


def _venue_fsq_place_ids(stops: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    ids: list[str] = []
    for stop in stops:
        fsq_place_id = _optional_text(stop.get("fsq_place_id"))
        if fsq_place_id:
            ids.append(fsq_place_id)
    return tuple(ids)


def _sorted_fsq_place_ids(fsq_place_ids: Sequence[str]) -> tuple[str, ...]:
    if isinstance(fsq_place_ids, (str, bytes)) or not isinstance(fsq_place_ids, Sequence):
        raise PrecacheOutputError("fsq_place_ids must be a non-empty sequence of strings.")
    ids = tuple(_required_text(value, "fsq_place_id") for value in fsq_place_ids)
    if not ids:
        raise PrecacheOutputError("fsq_place_ids must contain at least one id.")
    duplicate_ids = sorted(_duplicates(ids))
    if duplicate_ids:
        raise PrecacheOutputError(f"fsq_place_ids contains duplicate ids {duplicate_ids}.")
    return tuple(sorted(ids))


def _duplicates(values: Sequence[str]) -> set[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return duplicates


def _required_text(value: Any, field_name: str) -> str:
    text = _optional_text(value)
    if text is None:
        raise PrecacheOutputError(f"{field_name} must be a non-empty string.")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if bool(value != value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    return text or None


def _required_float(value: Any, field_name: str) -> float:
    parsed = _optional_float(value, field_name)
    if parsed is None:
        raise PrecacheOutputError(f"{field_name} must be numeric.")
    return parsed


def _required_positive_float(value: Any, field_name: str) -> float:
    parsed = _required_float(value, field_name)
    if parsed <= 0:
        raise PrecacheOutputError(f"{field_name} must be positive.")
    return parsed


def _optional_float(value: Any, field_name: str) -> float | None:
    if value is None:
        return None
    try:
        if bool(value != value):
            return None
    except Exception:
        pass
    if isinstance(value, bool):
        raise PrecacheOutputError(f"{field_name} must be numeric, got bool.")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise PrecacheOutputError(f"{field_name} must be numeric.") from exc
    if not math.isfinite(parsed):
        raise PrecacheOutputError(f"{field_name} must be finite.")
    return parsed


def _required_nonnegative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise PrecacheOutputError(f"{field_name} must be a non-negative integer.")
    if isinstance(value, float):
        if not value.is_integer():
            raise PrecacheOutputError(f"{field_name} must be a non-negative integer.")
        parsed = int(value)
        exact_match = True
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise PrecacheOutputError(f"{field_name} must be a non-negative integer.")
        if text.startswith("+"):
            text = text[1:]
        if not text.isdigit():
            raise PrecacheOutputError(f"{field_name} must be a non-negative integer.")
        parsed = int(text)
        exact_match = True
    else:
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise PrecacheOutputError(f"{field_name} must be a non-negative integer.") from exc
        try:
            exact_match = value == parsed
        except Exception:
            exact_match = True
    if not exact_match:
        raise PrecacheOutputError(f"{field_name} must be a non-negative integer.")
    if parsed < 0:
        raise PrecacheOutputError(f"{field_name} must be a non-negative integer.")
    return parsed


def _required_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise PrecacheOutputError(f"{field_name} must be a boolean.")
    return value


def _string_list(value: Any, *, field_name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        raw_values = [value]
    else:
        try:
            raw_values = list(value)
        except TypeError as exc:
            raise PrecacheOutputError(f"{field_name} must be a string or sequence.") from exc
    return tuple(str(item).strip() for item in raw_values if str(item).strip())


def _normalize_timestamp(value: datetime | str, *, field_name: str) -> str:
    if isinstance(value, datetime):
        timestamp = value
        if timestamp.tzinfo is None:
            logger.error("%s must be timezone-aware.", field_name)
            raise PrecacheOutputError(f"{field_name} must be timezone-aware.")
        return timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise PrecacheOutputError(f"{field_name} must be a non-empty timestamp.")
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            logger.error("%s is not a valid ISO timestamp: %r.", field_name, value)
            raise PrecacheOutputError(f"{field_name} must be a valid ISO timestamp.") from exc
        if parsed.tzinfo is None:
            logger.error("%s must include timezone info: %r.", field_name, value)
            raise PrecacheOutputError(f"{field_name} must include timezone info.")
        return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")
    raise PrecacheOutputError(f"{field_name} must be a timezone-aware datetime or ISO string.")


def _normalize_iso_timestamp_preserving_offset(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise PrecacheOutputError(f"{field_name} must be a non-empty ISO timestamp.")
    text = value.strip()
    if not text:
        raise PrecacheOutputError(f"{field_name} must be a non-empty ISO timestamp.")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        logger.error("%s is not a valid ISO timestamp: %r.", field_name, value)
        raise PrecacheOutputError(f"{field_name} must be a valid ISO timestamp.") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        logger.error("%s must include timezone info: %r.", field_name, value)
        raise PrecacheOutputError(f"{field_name} must include timezone info.")
    return parsed.isoformat()


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _assert_json_serializable(value: Any, *, field_name: str) -> None:
    try:
        _json_dumps(value)
    except (TypeError, ValueError) as exc:
        logger.error("%s is not JSON serializable.", field_name)
        raise PrecacheOutputError(f"{field_name} must be JSON serializable.") from exc


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
