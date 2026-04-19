"""Build location-scoped candidate pools from RAG document parquet."""

from __future__ import annotations

import logging
import math
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from back_end.precache.models import (
    CandidatePoolPlace,
    LocationBucket,
    LocationCandidatePool,
)
from back_end.rag.retriever import CONNECTIVE_STOP_TYPES, STOP_TYPE_KEYWORDS

logger = logging.getLogger(__name__)

REQUIRED_RAG_DOCUMENT_COLUMNS: tuple[str, ...] = (
    "fsq_place_id",
    "name",
    "latitude",
    "longitude",
    "crawl4ai_quality_score",
    "crawl4ai_template_stop_tags",
    "fsq_category_labels",
)


class PrecacheCandidatePoolError(RuntimeError):
    """Raised when a pre-cache candidate pool cannot be built safely."""


def load_location_buckets(path: Path | str) -> tuple[LocationBucket, ...]:
    """Load explicit location buckets from YAML."""

    bucket_path = Path(path)
    if not bucket_path.exists():
        raise FileNotFoundError(f"Location buckets file not found at {bucket_path}.")
    raw = yaml.safe_load(bucket_path.read_text(encoding="utf-8"))
    raw_buckets = raw.get("buckets") if isinstance(raw, dict) else None
    if not isinstance(raw_buckets, list) or not raw_buckets:
        raise PrecacheCandidatePoolError(
            f"Location bucket YAML {bucket_path} must contain a non-empty buckets list."
        )
    return tuple(_parse_bucket(item, source=bucket_path) for item in raw_buckets)


def build_location_candidate_pool(
    *,
    rag_documents_path: Path | str,
    bucket: LocationBucket,
    max_candidates: int = 250,
) -> LocationCandidatePool:
    """Build a scoped candidate pool for one location bucket."""

    if max_candidates <= 0:
        raise ValueError("max_candidates must be positive.")
    if bucket.radius_km <= 0:
        raise ValueError("bucket.radius_km must be positive.")

    path = Path(rag_documents_path)
    if path.suffix != ".parquet":
        raise ValueError(f"Expected RAG documents parquet path, got {path}.")
    if not path.exists():
        raise FileNotFoundError(f"RAG documents parquet not found at {path}.")

    documents = _load_candidate_pool_documents(path)
    return build_location_candidate_pool_from_documents(
        rag_documents=documents,
        bucket=bucket,
        max_candidates=max_candidates,
    )


def build_location_candidate_pool_from_documents(
    *,
    rag_documents: pd.DataFrame,
    bucket: LocationBucket,
    max_candidates: int = 250,
) -> LocationCandidatePool:
    """Build a scoped candidate pool from an already loaded RAG document frame.

    This avoids repeatedly re-reading the same parquet when building multiple
    bucket pools in one process, which keeps the precache dry-run path fast and
    deterministic.
    """

    if max_candidates <= 0:
        raise ValueError("max_candidates must be positive.")
    if bucket.radius_km <= 0:
        raise ValueError("bucket.radius_km must be positive.")

    documents = _validated_candidate_pool_documents(rag_documents, source="DataFrame")

    with_coords = documents.dropna(subset=["latitude", "longitude"]).copy()
    dropped = len(documents) - len(with_coords)
    if dropped:
        logger.warning(
            "Dropped %d RAG documents without coordinates for bucket=%s.",
            dropped,
            bucket.bucket_id,
        )

    if with_coords.empty:
        return LocationCandidatePool(
            bucket=bucket,
            places=(),
            target_plan_count=0,
            empty_reason="No RAG documents with coordinates.",
        )

    with_coords["distance_km"] = [
        _haversine_km(
            bucket.latitude,
            bucket.longitude,
            float(latitude),
            float(longitude),
        )
        for latitude, longitude in zip(with_coords["latitude"], with_coords["longitude"])
    ]
    scoped = with_coords.loc[with_coords["distance_km"] <= bucket.radius_km].copy()
    if scoped.empty:
        return LocationCandidatePool(
            bucket=bucket,
            places=(),
            target_plan_count=0,
            empty_reason=(
                f"No RAG documents within {bucket.radius_km:.2f}km of "
                f"bucket {bucket.bucket_id!r}."
            ),
        )

    scoped["quality_numeric"] = pd.to_numeric(
        scoped["crawl4ai_quality_score"],
        errors="coerce",
    ).fillna(0)
    scoped.sort_values(
        by=["quality_numeric", "distance_km", "name", "fsq_place_id"],
        ascending=[False, True, True, True],
        inplace=True,
        kind="stable",
    )
    limited = scoped.head(max_candidates).copy()
    places = tuple(_row_to_pool_place(row) for row in limited.itertuples(index=False))
    target_count = _target_plan_count(bucket=bucket, scoped=scoped)
    logger.info(
        "Built candidate pool for bucket=%s with %d/%d places target_plans=%d.",
        bucket.bucket_id,
        len(places),
        len(scoped),
        target_count,
    )
    return LocationCandidatePool(
        bucket=bucket,
        places=places,
        target_plan_count=target_count,
        empty_reason=None,
    )


def _load_candidate_pool_documents(path: Path) -> pd.DataFrame:
    documents = pd.read_parquet(path, columns=list(REQUIRED_RAG_DOCUMENT_COLUMNS))
    return _validated_candidate_pool_documents(documents, source=path)


def _validated_candidate_pool_documents(
    documents: pd.DataFrame,
    *,
    source: Path | str,
) -> pd.DataFrame:
    _validate_columns(documents, REQUIRED_RAG_DOCUMENT_COLUMNS, source=source)
    if documents.empty:
        raise PrecacheCandidatePoolError(f"RAG documents parquet {source} is empty.")
    return documents.copy()


def plan_budget_for_pair(
    *,
    bucket: LocationBucket,
    template: Mapping[str, Any],
    candidate_pool: LocationCandidatePool,
) -> int:
    """Decide how many plans to request for one location bucket/template pair."""

    if candidate_pool.bucket.bucket_id != bucket.bucket_id:
        raise ValueError(
            "candidate_pool.bucket must match bucket when computing a plan budget."
        )
    meaningful_variations = _required_positive_int(
        template.get("meaningful_variations"),
        field_name="meaningful_variations",
        template_id=str(template.get("id") or "unknown"),
    )
    stop_type_counts = _venue_stop_type_counts(template)
    if not stop_type_counts:
        logger.error(
            "Cannot compute plan budget for bucket=%s template=%s: template has no "
            "venue stop types.",
            bucket.bucket_id,
            template.get("id", "unknown"),
        )
        return 0

    physical_capacity = _candidate_pool_physical_capacity(
        bucket=bucket,
        template=template,
        places=candidate_pool.places,
        stop_type_counts=stop_type_counts,
    )
    if physical_capacity == 0:
        return 0

    requested = min(
        max(bucket.minimum_plan_count, candidate_pool.target_plan_count),
        meaningful_variations,
        physical_capacity,
    )
    if requested < bucket.minimum_plan_count:
        logger.warning(
            "Plan budget for bucket=%s template=%s is below minimum_plan_count=%d "
            "after caps: requested=%d meaningful_variations=%d physical_capacity=%d.",
            bucket.bucket_id,
            template.get("id", "unknown"),
            bucket.minimum_plan_count,
            requested,
            meaningful_variations,
            physical_capacity,
        )
    return requested


def _parse_bucket(raw: Any, *, source: Path) -> LocationBucket:
    if not isinstance(raw, dict):
        raise PrecacheCandidatePoolError(
            f"Location bucket entry in {source} must be an object."
        )
    bucket_id = _required_string(raw, "id")
    label = _required_string(raw, "label")
    latitude = _required_float(raw, "latitude")
    longitude = _required_float(raw, "longitude")
    radius_km = _required_float(raw, "radius_km")
    transport_mode = _required_string(raw, "transport_mode")
    minimum = _optional_int(raw.get("minimum_plan_count"), default=3)
    maximum = _optional_int(raw.get("maximum_plan_count"), default=20)
    boost = _optional_int(raw.get("strategic_boost"), default=0)
    if radius_km <= 0:
        raise PrecacheCandidatePoolError(f"Bucket {bucket_id!r} radius_km must be positive.")
    if minimum < 0 or maximum < 0 or maximum < minimum:
        raise PrecacheCandidatePoolError(
            f"Bucket {bucket_id!r} must have 0 <= minimum_plan_count <= maximum_plan_count."
        )
    return LocationBucket(
        bucket_id=bucket_id,
        label=label,
        latitude=latitude,
        longitude=longitude,
        radius_km=radius_km,
        transport_mode=transport_mode,
        minimum_plan_count=minimum,
        maximum_plan_count=maximum,
        strategic_boost=boost,
        tags=tuple(_string_list(raw.get("tags"))),
    )


def _row_to_pool_place(row: Any) -> CandidatePoolPlace:
    return CandidatePoolPlace(
        fsq_place_id=str(row.fsq_place_id),
        name=str(row.name),
        latitude=float(row.latitude),
        longitude=float(row.longitude),
        distance_km=float(row.distance_km),
        quality_score=int(row.quality_numeric),
        template_stop_tags=tuple(_string_list(row.crawl4ai_template_stop_tags)),
        category_labels=tuple(_string_list(row.fsq_category_labels)),
    )


def _target_plan_count(*, bucket: LocationBucket, scoped: pd.DataFrame) -> int:
    high_quality_count = int(
        pd.to_numeric(scoped["crawl4ai_quality_score"], errors="coerce")
        .fillna(0)
        .ge(6)
        .sum()
    )
    raw_target = round(
        bucket.minimum_plan_count
        + 0.025 * len(scoped)
        + 0.10 * high_quality_count
        + bucket.strategic_boost
    )
    return max(bucket.minimum_plan_count, min(bucket.maximum_plan_count, raw_target))


def _venue_stop_type_counts(template: Mapping[str, Any]) -> Counter[str]:
    stops = template.get("stops")
    if not isinstance(stops, list) or not stops:
        template_id = template.get("id", "unknown")
        raise PrecacheCandidatePoolError(
            f"Template {template_id!r} must contain a non-empty stops list."
        )

    stop_type_counts: Counter[str] = Counter()
    for stop in stops:
        if not isinstance(stop, dict) or not str(stop.get("type") or "").strip():
            raise PrecacheCandidatePoolError(
                f"Template {template.get('id', 'unknown')!r} contains a malformed stop."
            )
        stop_type = str(stop["type"]).strip()
        if stop.get("kind") == "connective" or stop_type in CONNECTIVE_STOP_TYPES:
            continue
        stop_type_counts[stop_type] += 1
    return stop_type_counts


def _candidate_pool_physical_capacity(
    *,
    bucket: LocationBucket,
    template: Mapping[str, Any],
    places: Sequence[CandidatePoolPlace],
    stop_type_counts: Counter[str],
) -> int:
    capacities: list[int] = []
    unmapped_stop_types: list[str] = []
    missing_stop_types: list[str] = []
    insufficient_stop_types: list[str] = []
    for stop_type, required_per_plan in sorted(stop_type_counts.items()):
        if stop_type not in STOP_TYPE_KEYWORDS:
            unmapped_stop_types.append(stop_type)
            capacities.append(0)
            continue
        compatible_count = _compatible_place_count(places, stop_type)
        if compatible_count == 0:
            missing_stop_types.append(stop_type)
            capacities.append(0)
            continue
        capacity = compatible_count // required_per_plan
        if capacity == 0:
            insufficient_stop_types.append(stop_type)
        capacities.append(capacity)

    if unmapped_stop_types:
        logger.error(
            "Plan budget is 0 for bucket=%s template=%s: missing "
            "STOP_TYPE_KEYWORDS mapping for stop_type(s)=%s.",
            bucket.bucket_id,
            template.get("id", "unknown"),
            ", ".join(unmapped_stop_types),
        )
        return 0
    if missing_stop_types:
        logger.error(
            "Plan budget is 0 for bucket=%s template=%s: candidate pool has no "
            "compatible places for stop_type(s)=%s using STOP_TYPE_KEYWORDS.",
            bucket.bucket_id,
            template.get("id", "unknown"),
            ", ".join(missing_stop_types),
        )
        return 0
    if insufficient_stop_types:
        logger.error(
            "Plan budget is 0 for bucket=%s template=%s: candidate pool has "
            "places for stop_type(s)=%s but not enough distinct places to fill "
            "repeated stops in a single plan.",
            bucket.bucket_id,
            template.get("id", "unknown"),
            ", ".join(insufficient_stop_types),
        )
        return 0
    return min(capacities) if capacities else 0


def _compatible_place_count(
    places: Sequence[CandidatePoolPlace],
    stop_type: str,
) -> int:
    keywords = STOP_TYPE_KEYWORDS[stop_type]
    compatible_ids: set[str] = set()
    normalized_stop_type = stop_type.casefold()
    normalized_keywords = tuple(keyword.casefold() for keyword in keywords)
    for place in places:
        stop_tags = _casefolded_set(place.template_stop_tags)
        if normalized_stop_type in stop_tags:
            compatible_ids.add(place.fsq_place_id)
            continue
        searchable = " ".join(_casefolded_set(place.category_labels))
        if any(keyword in searchable for keyword in normalized_keywords):
            compatible_ids.add(place.fsq_place_id)
    return len(compatible_ids)


def _required_positive_int(value: Any, *, field_name: str, template_id: str) -> int:
    if isinstance(value, bool):
        raise PrecacheCandidatePoolError(
            f"Template {template_id!r} {field_name} must be a positive integer."
        )
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise PrecacheCandidatePoolError(
            f"Template {template_id!r} {field_name} must be a positive integer."
        ) from exc
    if parsed <= 0:
        raise PrecacheCandidatePoolError(
            f"Template {template_id!r} {field_name} must be positive."
        )
    return parsed


def _validate_columns(df: pd.DataFrame, required: tuple[str, ...], *, source: Path) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise PrecacheCandidatePoolError(
            f"RAG documents parquet {source} is missing required columns {missing}. "
            f"Got columns: {sorted(df.columns)}."
        )


def _haversine_km(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    radius_km = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a_value = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    )
    return radius_km * 2.0 * math.atan2(math.sqrt(a_value), math.sqrt(1.0 - a_value))


def _required_string(raw: dict[str, Any], field_name: str) -> str:
    value = raw.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise PrecacheCandidatePoolError(f"{field_name} must be a non-empty string.")
    return value.strip()


def _required_float(raw: dict[str, Any], field_name: str) -> float:
    value = raw.get(field_name)
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise PrecacheCandidatePoolError(f"{field_name} must be numeric.") from exc
    if not math.isfinite(parsed):
        raise PrecacheCandidatePoolError(f"{field_name} must be finite.")
    return parsed


def _optional_int(value: Any, *, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        raise PrecacheCandidatePoolError("Expected integer, got bool.")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise PrecacheCandidatePoolError(f"Expected integer, got {value!r}.") from exc


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        values = [value]
    else:
        try:
            values = list(value)
        except TypeError:
            values = [value]
    return [str(item).strip() for item in values if str(item).strip()]


def _casefolded_set(value: Any) -> set[str]:
    return {item.casefold() for item in _string_list(value)}
