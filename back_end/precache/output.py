"""Parquet output storage for generated pre-cache date plans."""

from __future__ import annotations

import hashlib
import json
import logging
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_PRECACHE_OUTPUT_PATH = Path("data/precache/plans.parquet")

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
    "stops_json",
    "fsq_place_ids_sorted",
    "fsq_place_id_count",
    "verification_json",
    "generated_at_utc",
    "written_at_utc",
    "model",
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
    stops: Sequence[Mapping[str, Any]]
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


def read_precache_output(
    output_path: Path | str = DEFAULT_PRECACHE_OUTPUT_PATH,
) -> pd.DataFrame:
    """Read the generated plan parquet output after validating its schema."""

    path = Path(output_path)
    _require_parquet_path(path, label="pre-cache output")
    if not path.exists():
        raise FileNotFoundError(f"Pre-cache output parquet not found at {path}.")

    df = pd.read_parquet(path)
    _validate_output_schema(df, source=path)
    return df.loc[:, list(OUTPUT_COLUMNS)].copy()


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
            bucket_metadata["latitude"], "bucket_metadata.latitude"
        ),
        "bucket_longitude": _required_float(
            bucket_metadata["longitude"], "bucket_metadata.longitude"
        ),
        "bucket_radius_km": _required_positive_float(
            bucket_metadata["radius_km"], "bucket_metadata.radius_km"
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
        "stops_json": _json_dumps(stops),
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
