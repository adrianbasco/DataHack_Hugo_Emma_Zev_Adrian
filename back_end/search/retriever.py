"""Parquet-backed retrieval, scoring, and reranking for cached date cards."""

from __future__ import annotations

import json
import logging
import math
import re
from collections import Counter
from dataclasses import replace
from pathlib import Path
from typing import Any

from back_end.precache.output import DEFAULT_PRECACHE_OUTPUT_PATH, read_precache_output
from back_end.query.models import FilterStageStatus, FilterStageSummary
from back_end.search.models import (
    FilteredCandidatePool,
    ResolvedLocationFilter,
    RetrieverCandidate,
    ScoreBreakdown,
    SearchResult,
    StructuredFilters,
    WeatherPreference,
)

logger = logging.getLogger(__name__)

TOKEN_PATTERN = re.compile(r"[a-z0-9']+")
BM25_K1 = 1.2
BM25_B = 0.75
DEFAULT_LOCATION_RADIUS_KM = 8.0


class PlanRetriever:
    """Retrieve and rank cached plans from the plans parquet."""

    def __init__(self, output_path: Path | str = DEFAULT_PRECACHE_OUTPUT_PATH) -> None:
        self._output_path = Path(output_path)
        self._mtime_ns: int | None = None
        self._source_row_count = 0
        self._invalid_card_count = 0
        self._candidates: tuple[RetrieverCandidate, ...] = ()
        self._term_freqs: dict[str, Counter[str]] = {}
        self._doc_freqs: Counter[str] = Counter()
        self._doc_lengths: dict[str, int] = {}
        self._avg_doc_length = 0.0

    def filter_candidates(
        self,
        *,
        filters: StructuredFilters,
        resolved_location: ResolvedLocationFilter | None,
        exclude_plan_ids: tuple[str, ...] = (),
    ) -> FilteredCandidatePool:
        self._reload_if_needed()
        current = list(self._candidates)
        stages: list[FilterStageSummary] = [
            FilterStageSummary(
                stage="ready_cards",
                before=self._source_row_count,
                after=len(current),
                rejected=self._invalid_card_count,
                status=FilterStageStatus.APPLIED,
                detail="Dropped plans without a valid cached card payload.",
            )
        ]

        current, stage = self._apply_filter(
            current,
            stage="exclude_plan_ids",
            enabled=bool(exclude_plan_ids),
            detail=(
                f"Excluded {len(exclude_plan_ids)} plan ids from context."
                if exclude_plan_ids
                else "No exclude_plan_ids supplied."
            ),
            predicate=lambda candidate: candidate.plan_id not in set(exclude_plan_ids),
        )
        stages.append(stage)

        current, stage = self._apply_filter(
            current,
            stage="time_of_day",
            enabled=filters.time_of_day is not None,
            detail=(
                f"Applied time_of_day={filters.time_of_day!r}."
                if filters.time_of_day is not None
                else "No time_of_day filter supplied."
            ),
            predicate=lambda candidate: candidate.time_of_day == filters.time_of_day,
        )
        stages.append(stage)

        current, stage = self._apply_filter(
            current,
            stage="vibes",
            enabled=bool(filters.vibes),
            detail=(
                f"Applied vibes={list(filters.vibes)}."
                if filters.vibes
                else "No vibe filter supplied."
            ),
            predicate=lambda candidate: bool(set(candidate.vibes) & set(filters.vibes)),
        )
        stages.append(stage)

        normalized_transport = _normalize_transport_mode(filters.transport_mode)
        current, stage = self._apply_filter(
            current,
            stage="transport_mode",
            enabled=normalized_transport is not None,
            detail=(
                f"Applied transport_mode={normalized_transport!r}."
                if normalized_transport is not None
                else "No transport_mode filter supplied."
            ),
            predicate=lambda candidate: _normalize_transport_mode(candidate.transport_mode)
            == normalized_transport,
        )
        stages.append(stage)

        current, stage = self._apply_filter(
            current,
            stage="weather_precheck",
            enabled=filters.weather_ok is WeatherPreference.INDOORS_ONLY,
            detail=(
                "Skipped weather-sensitive plans because the user explicitly asked for indoors only."
                if filters.weather_ok is WeatherPreference.INDOORS_ONLY
                else "No indoors-only weather precheck applied."
            ),
            predicate=lambda candidate: not candidate.weather_sensitive,
        )
        stages.append(stage)

        current, stage = self._apply_location_filter(current, resolved_location)
        stages.append(stage)

        return FilteredCandidatePool(
            candidates=tuple(current),
            filter_stage_counts=tuple(stages),
        )

    def score_and_rerank(
        self,
        *,
        candidates: tuple[RetrieverCandidate, ...],
        query_text: str | None,
        template_hints: tuple[str, ...] = (),
        limit: int = 20,
    ) -> tuple[SearchResult, ...]:
        self._reload_if_needed()
        scored: list[tuple[float, SearchResult, RetrieverCandidate]] = []
        query_terms = _tokenize(query_text or "")
        for candidate in candidates:
            lexical_score = self._bm25_score(candidate.plan_id, query_terms)
            template_bonus = _template_bonus(candidate, template_hints)
            location_bonus = _location_bonus(candidate.distance_km)
            total = lexical_score + template_bonus + location_bonus
            result = SearchResult(
                plan_id=candidate.plan_id,
                score=round(total, 6),
                match_reasons=_match_reasons(
                    candidate,
                    query_terms=query_terms,
                    template_hints=template_hints,
                ),
                score_breakdown=ScoreBreakdown(
                    lexical=round(lexical_score, 6),
                    template_bonus=round(template_bonus, 6),
                    location_bonus=round(location_bonus, 6),
                    total=round(total, 6),
                ),
                card=candidate.card,
            )
            scored.append((total, result, candidate))

        scored.sort(
            key=lambda item: (
                -item[0],
                item[2].distance_km if item[2].distance_km is not None else float("inf"),
                item[2].template_id,
                item[2].plan_id,
            )
        )
        return tuple(self._rerank_diverse(scored, limit))

    def _bm25_score(self, plan_id: str, query_terms: tuple[str, ...]) -> float:
        if not query_terms:
            return 0.0
        term_freqs = self._term_freqs.get(plan_id)
        if term_freqs is None:
            return 0.0
        doc_length = self._doc_lengths.get(plan_id, 0)
        avgdl = self._avg_doc_length or 1.0
        score = 0.0
        for term in query_terms:
            freq = term_freqs.get(term, 0)
            if freq <= 0:
                continue
            df = self._doc_freqs.get(term, 0)
            idf = math.log(1.0 + (len(self._candidates) - df + 0.5) / (df + 0.5))
            numerator = freq * (BM25_K1 + 1.0)
            denominator = freq + BM25_K1 * (1.0 - BM25_B + BM25_B * doc_length / avgdl)
            score += idf * numerator / denominator
        return score

    def _rerank_diverse(
        self,
        scored: list[tuple[float, SearchResult, RetrieverCandidate]],
        limit: int,
    ) -> list[SearchResult]:
        if limit <= 0:
            return []
        template_cap = 1 if limit <= 4 else 2 if limit <= 8 else 3
        bucket_cap = 1 if limit <= 3 else 2 if limit <= 8 else 3
        selected: list[SearchResult] = []
        seen_prefixes: set[str] = set()
        template_counts: Counter[str] = Counter()
        bucket_counts: Counter[str] = Counter()
        deferred: list[tuple[float, SearchResult, RetrieverCandidate]] = []

        for _, result, candidate in scored:
            prefix = _fsq_prefix_key(candidate.fsq_place_ids_sorted)
            if prefix in seen_prefixes:
                continue
            if template_counts[candidate.template_id] >= template_cap:
                deferred.append((result.score, result, candidate))
                continue
            if bucket_counts[candidate.bucket_id] >= bucket_cap:
                deferred.append((result.score, result, candidate))
                continue
            selected.append(result)
            seen_prefixes.add(prefix)
            template_counts[candidate.template_id] += 1
            bucket_counts[candidate.bucket_id] += 1
            if len(selected) >= limit:
                return selected

        if len(selected) >= limit:
            return selected

        for _, result, candidate in deferred:
            prefix = _fsq_prefix_key(candidate.fsq_place_ids_sorted)
            if prefix in seen_prefixes:
                continue
            selected.append(result)
            seen_prefixes.add(prefix)
            if len(selected) >= limit:
                break
        return selected

    def _apply_location_filter(
        self,
        candidates: list[RetrieverCandidate],
        resolved_location: ResolvedLocationFilter | None,
    ) -> tuple[list[RetrieverCandidate], FilterStageSummary]:
        if (
            resolved_location is None
            or resolved_location.anchor_latitude is None
            or resolved_location.anchor_longitude is None
        ):
            return (
                candidates,
                FilterStageSummary(
                    stage="location_radius",
                    before=len(candidates),
                    after=len(candidates),
                    rejected=0,
                    status=FilterStageStatus.SKIPPED,
                    detail="No resolved location anchor supplied.",
                ),
            )

        radius_km = resolved_location.radius_km or DEFAULT_LOCATION_RADIUS_KM
        before = len(candidates)
        filtered: list[RetrieverCandidate] = []
        for candidate in candidates:
            distance_km = _haversine_km(
                resolved_location.anchor_latitude,
                resolved_location.anchor_longitude,
                candidate.bucket_latitude,
                candidate.bucket_longitude,
            )
            if distance_km <= radius_km:
                filtered.append(replace(candidate, distance_km=distance_km))
        return (
            filtered,
            FilterStageSummary(
                stage="location_radius",
                before=before,
                after=len(filtered),
                rejected=before - len(filtered),
                status=FilterStageStatus.APPLIED,
                detail=(
                    f"Applied {radius_km:.1f}km radius around "
                    f"{resolved_location.resolved_label or resolved_location.text or 'context location'}."
                ),
            ),
        )

    @staticmethod
    def _apply_filter(
        candidates: list[RetrieverCandidate],
        *,
        stage: str,
        enabled: bool,
        detail: str,
        predicate: Any,
    ) -> tuple[list[RetrieverCandidate], FilterStageSummary]:
        before = len(candidates)
        if not enabled:
            return (
                candidates,
                FilterStageSummary(
                    stage=stage,
                    before=before,
                    after=before,
                    rejected=0,
                    status=FilterStageStatus.SKIPPED,
                    detail=detail,
                ),
            )
        filtered = [candidate for candidate in candidates if predicate(candidate)]
        return (
            filtered,
            FilterStageSummary(
                stage=stage,
                before=before,
                after=len(filtered),
                rejected=before - len(filtered),
                status=FilterStageStatus.APPLIED,
                detail=detail,
            ),
        )

    def _reload_if_needed(self) -> None:
        if not self._output_path.exists():
            raise FileNotFoundError(
                f"Search plans parquet was not found at {self._output_path}."
            )
        mtime_ns = self._output_path.stat().st_mtime_ns
        if self._mtime_ns == mtime_ns and self._candidates:
            return

        df = read_precache_output(self._output_path)
        source_row_count = len(df)
        invalid_card_count = 0
        candidates: list[RetrieverCandidate] = []
        term_freqs: dict[str, Counter[str]] = {}
        doc_freqs: Counter[str] = Counter()
        doc_lengths: dict[str, int] = {}

        for _, row in df.iterrows():
            candidate = _row_to_candidate(row.to_dict())
            if candidate is None:
                invalid_card_count += 1
                continue
            candidates.append(candidate)
            tokens = _tokenize(candidate.search_text)
            term_counter = Counter(tokens)
            term_freqs[candidate.plan_id] = term_counter
            doc_freqs.update(term_counter.keys())
            doc_lengths[candidate.plan_id] = len(tokens)

        self._mtime_ns = mtime_ns
        self._source_row_count = source_row_count
        self._invalid_card_count = invalid_card_count
        self._candidates = tuple(candidates)
        self._term_freqs = term_freqs
        self._doc_freqs = doc_freqs
        self._doc_lengths = doc_lengths
        self._avg_doc_length = (
            sum(doc_lengths.values()) / len(doc_lengths) if doc_lengths else 0.0
        )
        logger.info(
            "Loaded %d ready search candidates from %s (source_rows=%d invalid_cards=%d).",
            len(self._candidates),
            self._output_path,
            source_row_count,
            invalid_card_count,
        )


def _row_to_candidate(row: dict[str, Any]) -> RetrieverCandidate | None:
    card = _parse_json_object(row.get("card_json"), field_name="card_json")
    if card is None:
        logger.error("Plan %s is missing a valid card_json payload; dropping from search index.", row.get("plan_id"))
        return None

    template_metadata = _parse_json_object(row.get("template_metadata_json"), field_name="template_metadata_json") or {}
    bucket_metadata = _parse_json_object(row.get("bucket_metadata_json"), field_name="bucket_metadata_json") or {}
    vibes = tuple(_parse_string_list(row.get("vibe")))
    if not vibes:
        card_vibes = card.get("vibe")
        vibes = tuple(_parse_string_list(card_vibes))

    search_text = _coalesce_text(
        row.get("search_text"),
        card.get("search_text"),
        _fallback_search_text(row, card, template_metadata, bucket_metadata),
    )
    if search_text is None:
        logger.error("Plan %s has no search text fallback; dropping from search index.", row.get("plan_id"))
        return None

    plan_time_iso = _coalesce_text(row.get("plan_time_iso"), card.get("plan_time_iso"))
    return RetrieverCandidate(
        plan_id=str(row["plan_id"]),
        bucket_id=str(row["bucket_id"]),
        template_id=str(row["template_id"]),
        bucket_label=_coalesce_text(row.get("bucket_label"), card.get("bucket_label")) or "",
        bucket_latitude=float(row["bucket_latitude"]),
        bucket_longitude=float(row["bucket_longitude"]),
        time_of_day=str(row["time_of_day"]).strip().casefold(),
        weather_sensitive=bool(row["weather_sensitive"]),
        template_duration_hours=_optional_float(row.get("template_duration_hours")),
        template_title=_coalesce_text(row.get("template_title"), card.get("template_title")) or "",
        template_description=_coalesce_text(
            row.get("template_description"),
            card.get("template_description"),
            template_metadata.get("description"),
        ),
        search_text=search_text,
        card=card,
        vibes=vibes,
        transport_mode=_coalesce_text(row.get("bucket_transport_mode"), card.get("transport_mode")),
        plan_time_iso=plan_time_iso,
        fsq_place_ids_sorted=tuple(_parse_string_list(row.get("fsq_place_ids_sorted"))),
    )


def _fallback_search_text(
    row: dict[str, Any],
    card: dict[str, Any],
    template_metadata: dict[str, Any],
    bucket_metadata: dict[str, Any],
) -> str | None:
    parts: list[str] = []
    for value in (
        row.get("plan_title"),
        row.get("plan_hook"),
        row.get("template_title"),
        row.get("template_description"),
        row.get("bucket_label"),
        template_metadata.get("description"),
        bucket_metadata.get("label"),
        " ".join(_parse_string_list(row.get("vibe"))),
    ):
        text = _coalesce_text(value)
        if text is not None:
            parts.append(text)
    for stop in card.get("stops", []):
        if not isinstance(stop, dict):
            continue
        for key in ("name", "llm_description", "why_it_fits", "stop_type"):
            text = _coalesce_text(stop.get(key))
            if text is not None:
                parts.append(text)
    collapsed = "\n".join(dict.fromkeys(parts))
    return collapsed.strip() or None


def _parse_json_object(value: object, *, field_name: str) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        logger.error("Expected %s to be a JSON object string, got %r.", field_name, type(value).__name__)
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        logger.error("Could not decode %s for search indexing.", field_name)
        return None
    if not isinstance(parsed, dict):
        logger.error("%s decoded successfully but was not a JSON object.", field_name)
        return None
    return parsed


def _parse_string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, float) and math.isnan(value):
        return []
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                return [stripped]
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        return [stripped]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def _tokenize(text: str) -> tuple[str, ...]:
    return tuple(TOKEN_PATTERN.findall(text.casefold()))


def _coalesce_text(*values: object) -> str | None:
    for value in values:
        if value is None:
            continue
        if isinstance(value, float) and math.isnan(value):
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return float(value)


def _normalize_transport_mode(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().casefold()
    mapping = {
        "walk": "walking",
        "walking": "walking",
        "w": "walking",
        "transit": "public_transport",
        "public_transport": "public_transport",
        "public transport": "public_transport",
        "drive": "driving",
        "driving": "driving",
        "car": "driving",
    }
    return mapping.get(normalized, normalized)


def _template_bonus(candidate: RetrieverCandidate, template_hints: tuple[str, ...]) -> float:
    if not template_hints:
        return 0.0
    haystack = " ".join(
        [
            candidate.template_id,
            candidate.template_title,
            candidate.template_description or "",
            candidate.search_text,
        ]
    ).casefold()
    bonus = 0.0
    for hint in template_hints:
        normalized = hint.casefold()
        if normalized in haystack:
            bonus += 0.18
    return min(bonus, 0.72)


def _location_bonus(distance_km: float | None) -> float:
    if distance_km is None:
        return 0.0
    return max(0.0, 0.25 - min(distance_km, 25.0) * 0.01)


def _match_reasons(
    candidate: RetrieverCandidate,
    *,
    query_terms: tuple[str, ...],
    template_hints: tuple[str, ...],
) -> tuple[str, ...]:
    reasons: list[str] = []
    if query_terms:
        matched_terms = [term for term in query_terms if term in set(_tokenize(candidate.search_text))]
        if matched_terms:
            reasons.append(
                "Matched query terms: " + ", ".join(sorted(dict.fromkeys(matched_terms))[:4])
            )
    if template_hints:
        matched_hints = [
            hint
            for hint in template_hints
            if hint.casefold()
            in " ".join(
                [
                    candidate.template_id,
                    candidate.template_title,
                    candidate.template_description or "",
                    candidate.search_text,
                ]
            ).casefold()
        ]
        if matched_hints:
            reasons.append("Matched template hints: " + ", ".join(sorted(dict.fromkeys(matched_hints))))
    if candidate.distance_km is not None:
        reasons.append(f"Nearby bucket: {candidate.bucket_label} ({candidate.distance_km:.1f}km)")
    if candidate.vibes:
        reasons.append("Vibes: " + ", ".join(candidate.vibes))
    return tuple(reasons)


def _fsq_prefix_key(values: tuple[str, ...]) -> str:
    if not values:
        return ""
    prefix = values[:2]
    return "|".join(prefix)


def _haversine_km(
    origin_latitude: float,
    origin_longitude: float,
    destination_latitude: float,
    destination_longitude: float,
) -> float:
    earth_radius_km = 6371.0088
    origin_lat_rad = math.radians(origin_latitude)
    origin_lon_rad = math.radians(origin_longitude)
    destination_lat_rad = math.radians(destination_latitude)
    destination_lon_rad = math.radians(destination_longitude)
    latitude_delta = destination_lat_rad - origin_lat_rad
    longitude_delta = destination_lon_rad - origin_lon_rad
    haversine = (
        math.sin(latitude_delta / 2) ** 2
        + math.cos(origin_lat_rad)
        * math.cos(destination_lat_rad)
        * math.sin(longitude_delta / 2) ** 2
    )
    return 2 * earth_radius_km * math.asin(math.sqrt(haversine))
