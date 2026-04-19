"""Orchestrate query parsing, filter merge, weather gating, and retrieval."""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Any

from back_end.catalog.repository import PlacesRepository
from back_end.clients.settings import WeatherSettings
from back_end.domain.models import LatLng, WeatherAssessmentStatus, WeatherExposure
from back_end.query.location import TypedLocationResolver
from back_end.query.models import FilterStageStatus, FilterStageSummary
from back_end.query.settings import QuerySettings, load_query_settings
from back_end.search.models import (
    FilterSource,
    FinalParsedFilters,
    FilteredCandidatePool,
    ResolvedLocationFilter,
    RetrieverCandidate,
    SearchCoordinates,
    SearchDiagnostics,
    SearchRequest,
    SearchResponse,
    SearchResult,
    SourcedValue,
    StructuredFilters,
    WeatherGateStats,
    WeatherPreference,
)
from back_end.search.parser import QueryParser
from back_end.search.retriever import PlanRetriever
from back_end.services.weather import WeatherEvaluationService

logger = logging.getLogger(__name__)

DEFAULT_LIMIT = 20
MAX_LIMIT = 50
DEFAULT_CONTEXT_RADIUS_KM = 12.0
DEFAULT_TEXT_RADIUS_KM = 8.0
WEATHER_CACHE_TTL_SECONDS = 300.0


class SearchService:
    """Main entrypoint for cached-card retrieval."""

    def __init__(
        self,
        *,
        repository: PlacesRepository | None = None,
        query_settings: QuerySettings | None = None,
        parser: QueryParser | None = None,
        retriever: PlanRetriever | None = None,
        weather_service: WeatherEvaluationService | None = None,
    ) -> None:
        self._query_settings = query_settings or load_query_settings()
        self._repository = repository or PlacesRepository(self._query_settings)
        self._location_resolver = TypedLocationResolver(self._repository)
        self._parser = parser or QueryParser(self._repository)
        self._retriever = retriever or PlanRetriever()
        self._weather_service = weather_service or WeatherEvaluationService(WeatherSettings.from_env())
        self._weather_cache: dict[tuple[float, float, str, str, str], tuple[float, Any]] = {}

    async def search(self, request: SearchRequest) -> SearchResponse:
        warnings: list[str] = []
        auto_applied_notes: list[str] = []

        parser_output = await self._parser.parse(request.query, context=request.context)
        warnings.extend(parser_output.warnings)

        merged_filters, final_parsed = self._merge_filters(
            request=request,
            parser_output=parser_output,
            warnings=warnings,
            auto_applied_notes=auto_applied_notes,
        )

        resolved_location = self._resolve_location(
            final_parsed.location,
            warnings=warnings,
        )
        final_parsed = FinalParsedFilters(
            vibes=final_parsed.vibes,
            time_of_day=final_parsed.time_of_day,
            weather_ok=final_parsed.weather_ok,
            location=SourcedValue(
                value=resolved_location,
                source=final_parsed.location.source,
            ),
            transport_mode=final_parsed.transport_mode,
            template_hints=final_parsed.template_hints,
            free_text_residual=final_parsed.free_text_residual,
            warnings=tuple(_dedupe_strings(warnings)),
            auto_applied_notes=tuple(_dedupe_strings(auto_applied_notes)),
        )

        context = request.context or _default_context()
        limit = _validated_limit(context.limit)
        filtered = self._retriever.filter_candidates(
            filters=merged_filters,
            resolved_location=resolved_location,
            exclude_plan_ids=context.exclude_plan_ids,
        )

        weather_candidates, weather_stage, weather_stats = await self._apply_weather_gate(
            filtered.candidates,
            filters=merged_filters,
            warnings=warnings,
            auto_applied_notes=auto_applied_notes,
        )
        filter_stages = tuple([*filtered.filter_stage_counts, weather_stage])
        results = self._retriever.score_and_rerank(
            candidates=weather_candidates,
            query_text=str(final_parsed.free_text_residual.value or ""),
            template_hints=merged_filters.template_hints,
            limit=limit,
        )
        total_matched_before_limit = len(weather_candidates)
        if not results:
            (
                results,
                fallback_stages,
                fallback_weather_stats,
                fallback_matched_before_limit,
            ) = await self._closest_related_fallback(
                request=request,
                final_parsed=final_parsed,
                template_hints=merged_filters.template_hints,
                limit=limit,
                warnings=warnings,
                auto_applied_notes=auto_applied_notes,
            )
            if fallback_stages:
                filter_stages = tuple([*filter_stages, *fallback_stages])
                weather_stats = fallback_weather_stats
                total_matched_before_limit = fallback_matched_before_limit

        diagnostics = SearchDiagnostics(
            total_matched_before_limit=total_matched_before_limit,
            filter_stage_counts=filter_stages,
            weather_gate_stats=weather_stats,
            unsupported_constraints=filtered.unsupported_constraints,
            warnings=tuple(_dedupe_strings(warnings)),
        )
        return SearchResponse(
            parsed=FinalParsedFilters(
                vibes=final_parsed.vibes,
                time_of_day=final_parsed.time_of_day,
                weather_ok=final_parsed.weather_ok,
                location=final_parsed.location,
                transport_mode=final_parsed.transport_mode,
                template_hints=final_parsed.template_hints,
                free_text_residual=final_parsed.free_text_residual,
                warnings=tuple(_dedupe_strings(warnings)),
                auto_applied_notes=tuple(_dedupe_strings(auto_applied_notes)),
            ),
            results=results,
            diagnostics=diagnostics,
        )

    async def _closest_related_fallback(
        self,
        *,
        request: SearchRequest,
        final_parsed: FinalParsedFilters,
        template_hints: tuple[str, ...],
        limit: int,
        warnings: list[str],
        auto_applied_notes: list[str],
    ) -> tuple[tuple[SearchResult, ...], tuple[FilterStageSummary, ...], WeatherGateStats, int]:
        context = request.context or _default_context()
        query_text = str(final_parsed.free_text_residual.value or request.query or "")
        warning = (
            "No cached cards matched every requested filter; returning closest related cached "
            "plans with time, vibe, transport, weather, and location filters relaxed."
        )
        logger.error("%s query=%r", warning, request.query)
        warnings.append(warning)
        auto_applied_notes.append("Relaxed strict search filters to return closest related cached plans.")

        filtered = self._retriever.filter_candidates(
            filters=StructuredFilters(),
            resolved_location=None,
            exclude_plan_ids=context.exclude_plan_ids,
        )
        candidates = filtered.candidates
        weather_stage = FilterStageSummary(
            stage="weather_gate",
            before=len(candidates),
            after=len(candidates),
            rejected=0,
            status=FilterStageStatus.SKIPPED,
            detail="Weather gate skipped for closest-related fallback.",
        )
        weather_stats = WeatherGateStats()
        fallback_stage = FilterStageSummary(
            stage="closest_related_fallback",
            before=0,
            after=len(candidates),
            rejected=0,
            status=FilterStageStatus.APPLIED if candidates else FilterStageStatus.SKIPPED,
            detail=(
                "Strict search produced no results, so non-identity filters were relaxed and "
                "remaining cached plans were ranked by lexical/template closeness."
            ),
        )
        results = self._retriever.score_and_rerank(
            candidates=candidates,
            query_text=query_text,
            template_hints=template_hints,
            limit=limit,
        )
        if not results:
            logger.error(
                "Closest-related fallback also returned no cached cards for query=%r.",
                request.query,
            )
            warnings.append("Closest-related fallback returned no cached cards.")
        return (
            results,
            tuple([*filtered.filter_stage_counts, weather_stage, fallback_stage]),
            weather_stats,
            len(candidates),
        )

    def _merge_filters(
        self,
        *,
        request: SearchRequest,
        parser_output: Any,
        warnings: list[str],
        auto_applied_notes: list[str],
    ) -> tuple[StructuredFilters, FinalParsedFilters]:
        overrides = request.overrides or StructuredFilters()
        context = request.context or _default_context()
        parsed_filters = parser_output.filters

        vibes_value, vibes_source = _merge_tuple_field(overrides.vibes, parsed_filters.vibes)
        time_value, time_source = _merge_scalar_field(
            override_value=overrides.time_of_day,
            parsed_value=parsed_filters.time_of_day,
            derived_value=_derive_time_of_day(context.now_iso),
        )
        if time_source is FilterSource.DERIVED and time_value is not None:
            auto_applied_notes.append(
                f"Derived time_of_day={time_value!r} from context.now_iso."
            )

        weather_value, weather_source = _merge_scalar_field(
            override_value=overrides.weather_ok,
            parsed_value=parsed_filters.weather_ok,
            derived_value=None,
        )
        if weather_value is WeatherPreference.INDOORS_ONLY:
            auto_applied_notes.append("Applied indoors-only prefilter before weather lookup.")
        elif weather_value is not None:
            auto_applied_notes.append("Kept weather-sensitive plans eligible for forecast checks.")

        location_value, location_source = _merge_location_field(
            overrides.location,
            parsed_filters.location,
            context.user_location,
        )
        if location_source is FilterSource.DERIVED and location_value is not None:
            auto_applied_notes.append("Derived search location from context.user_location.")

        transport_value, transport_source = _merge_scalar_field(
            override_value=overrides.transport_mode,
            parsed_value=parsed_filters.transport_mode,
            derived_value=None,
        )
        template_value, template_source = _merge_tuple_field(
            overrides.template_hints,
            parsed_filters.template_hints,
        )

        free_text_residual = parser_output.free_text_residual
        if free_text_residual is not None:
            residual_value = free_text_residual
            residual_source = FilterSource.PARSED
        elif request.query and request.query.strip():
            residual_value = request.query.strip()
            residual_source = FilterSource.DERIVED
            auto_applied_notes.append("Used the raw query text for lexical scoring.")
        else:
            residual_value = None
            residual_source = FilterSource.UNSET

        merged_filters = StructuredFilters(
            vibes=vibes_value,
            time_of_day=time_value,
            weather_ok=weather_value,
            location=location_value,
            transport_mode=transport_value,
            template_hints=template_value,
        )
        final_parsed = FinalParsedFilters(
            vibes=SourcedValue(value=vibes_value, source=vibes_source),
            time_of_day=SourcedValue(value=time_value, source=time_source),
            weather_ok=SourcedValue(
                value=weather_value.value if isinstance(weather_value, WeatherPreference) else weather_value,
                source=weather_source,
            ),
            location=SourcedValue(value=location_value, source=location_source),
            transport_mode=SourcedValue(value=transport_value, source=transport_source),
            template_hints=SourcedValue(value=template_value, source=template_source),
            free_text_residual=SourcedValue(value=residual_value, source=residual_source),
            warnings=tuple(_dedupe_strings(warnings)),
            auto_applied_notes=tuple(_dedupe_strings(auto_applied_notes)),
        )
        return merged_filters, final_parsed

    def _resolve_location(
        self,
        sourced_location: SourcedValue,
        *,
        warnings: list[str],
    ) -> ResolvedLocationFilter | None:
        location_value = sourced_location.value
        if location_value is None:
            return None

        if (
            sourced_location.source is FilterSource.DERIVED
            and isinstance(location_value, LocationEnvelope)
            and location_value.coordinates is not None
        ):
            return ResolvedLocationFilter(
                text=None,
                radius_km=location_value.radius_km,
                anchor_latitude=location_value.coordinates.lat,
                anchor_longitude=location_value.coordinates.lng,
                resolved_label="context_user_location",
            )

        if not isinstance(location_value, LocationEnvelope) or location_value.location is None:
            return None

        location = location_value.location
        if location.text is None:
            return None
        try:
            resolved = self._location_resolver.resolve(location.text)
        except Exception as exc:
            logger.error("Search location resolution failed for %r: %s", location.text, exc)
            warnings.append(f"Location {location.text!r} could not be resolved: {exc}")
            return ResolvedLocationFilter(
                text=location.text,
                radius_km=location.radius_km,
                anchor_latitude=None,
                anchor_longitude=None,
                resolved_label=None,
            )

        return ResolvedLocationFilter(
            text=location.text,
            radius_km=location.radius_km,
            anchor_latitude=resolved.anchor_latitude,
            anchor_longitude=resolved.anchor_longitude,
            resolved_label=resolved.input_text,
        )

    async def _apply_weather_gate(
        self,
        candidates: tuple[RetrieverCandidate, ...],
        *,
        filters: StructuredFilters,
        warnings: list[str],
        auto_applied_notes: list[str],
    ) -> tuple[tuple[RetrieverCandidate, ...], FilterStageSummary, WeatherGateStats]:
        before = len(candidates)
        if not candidates:
            return (
                (),
                FilterStageSummary(
                    stage="weather_gate",
                    before=0,
                    after=0,
                    rejected=0,
                    status=FilterStageStatus.SKIPPED,
                    detail="No candidates remained before weather gating.",
                ),
                WeatherGateStats(),
            )

        if filters.weather_ok is WeatherPreference.INDOORS_ONLY:
            skipped_count = sum(1 for candidate in candidates if candidate.weather_sensitive)
            return (
                candidates,
                FilterStageSummary(
                    stage="weather_gate",
                    before=before,
                    after=before,
                    rejected=0,
                    status=FilterStageStatus.SKIPPED,
                    detail="Weather lookups were skipped because indoors-only was already enforced.",
                ),
                WeatherGateStats(skipped_indoors_only=skipped_count),
            )

        kept: list[RetrieverCandidate] = []
        stats = WeatherGateStats()
        grouped_candidates: dict[tuple[float, float, str, str, str], list[RetrieverCandidate]] = {}
        rejected_missing_schedule = 0

        for candidate in candidates:
            if not candidate.weather_sensitive:
                kept.append(candidate)
                continue
            window = _candidate_weather_window(candidate)
            if window is None:
                logger.error(
                    "Weather-sensitive plan %s could not build a forecast window; rejecting.",
                    candidate.plan_id,
                )
                warnings.append(
                    f"Plan {candidate.plan_id} was rejected because it could not be weather-verified."
                )
                rejected_missing_schedule += 1
                continue
            start_at, end_at = window
            exposure = _weather_exposure(candidate)
            key = (
                round(candidate.bucket_latitude, 4),
                round(candidate.bucket_longitude, 4),
                start_at.isoformat(),
                end_at.isoformat(),
                exposure.value,
            )
            grouped_candidates.setdefault(key, []).append(candidate)

        cache_hits = 0
        upstream_failures = 0
        rejected = rejected_missing_schedule
        groups = 0
        evaluated = 0

        for key, grouped in grouped_candidates.items():
            candidate = grouped[0]
            start_at, end_at = _candidate_weather_window(candidate)  # already validated above
            if start_at is None or end_at is None:
                continue
            exposure = _weather_exposure(candidate)
            groups += 1
            cache_entry = self._weather_cache.get(key)
            if cache_entry is not None and cache_entry[0] > time.monotonic():
                assessment = cache_entry[1]
                cache_hits += len(grouped)
            else:
                assessment = await self._weather_service.evaluate_window(
                    LatLng(latitude=candidate.bucket_latitude, longitude=candidate.bucket_longitude),
                    exposure=exposure,
                    start_at=start_at,
                    end_at=end_at,
                )
                self._weather_cache[key] = (
                    time.monotonic() + WEATHER_CACHE_TTL_SECONDS,
                    assessment,
                )
            for item in grouped:
                evaluated += 1
                if assessment.status is WeatherAssessmentStatus.UPSTREAM_FAILURE:
                    upstream_failures += 1
                    warnings.append(
                        "Weather upstream failure rejected at least one weather-sensitive plan."
                    )
                if assessment.should_reject:
                    rejected += 1
                    continue
                kept.append(item)

        if groups:
            auto_applied_notes.append(
                f"Applied weather gating to {evaluated} weather-sensitive candidates across {groups} forecast groups."
            )

        stats = WeatherGateStats(
            evaluated=evaluated,
            rejected=rejected,
            upstream_failures=upstream_failures,
            skipped_indoors_only=0,
            cache_hits=cache_hits,
            groups=groups,
        )
        return (
            tuple(kept),
            FilterStageSummary(
                stage="weather_gate",
                before=before,
                after=len(kept),
                rejected=before - len(kept),
                status=FilterStageStatus.APPLIED,
                detail="Evaluated weather-sensitive plans against grouped forecast windows.",
            ),
            stats,
        )


class LocationEnvelope:
    """Intermediate merged location value prior to resolution."""

    def __init__(
        self,
        *,
        location: Any | None = None,
        coordinates: SearchCoordinates | None = None,
        radius_km: float | None = None,
    ) -> None:
        self.location = location
        self.coordinates = coordinates
        self.radius_km = radius_km


def _merge_scalar_field(
    *,
    override_value: Any,
    parsed_value: Any,
    derived_value: Any,
) -> tuple[Any, FilterSource]:
    if _is_set(override_value):
        return override_value, FilterSource.OVERRIDE
    if _is_set(parsed_value):
        return parsed_value, FilterSource.PARSED
    if _is_set(derived_value):
        return derived_value, FilterSource.DERIVED
    return None, FilterSource.UNSET


def _merge_tuple_field(
    override_value: tuple[str, ...],
    parsed_value: tuple[str, ...],
) -> tuple[tuple[str, ...], FilterSource]:
    if override_value:
        return override_value, FilterSource.OVERRIDE
    if parsed_value:
        return parsed_value, FilterSource.PARSED
    return (), FilterSource.UNSET


def _merge_location_field(
    override_value: Any,
    parsed_value: Any,
    derived_coordinates: SearchCoordinates | None,
) -> tuple[LocationEnvelope | None, FilterSource]:
    if _location_is_set(override_value):
        location = override_value
        radius = location.radius_km or DEFAULT_TEXT_RADIUS_KM
        return LocationEnvelope(location=location, radius_km=radius), FilterSource.OVERRIDE
    if _location_is_set(parsed_value):
        location = parsed_value
        radius = location.radius_km or DEFAULT_TEXT_RADIUS_KM
        return LocationEnvelope(location=location, radius_km=radius), FilterSource.PARSED
    if derived_coordinates is not None:
        return (
            LocationEnvelope(
                coordinates=derived_coordinates,
                radius_km=DEFAULT_CONTEXT_RADIUS_KM,
            ),
            FilterSource.DERIVED,
        )
    return None, FilterSource.UNSET


def _derive_time_of_day(now_iso: str | None) -> str | None:
    if now_iso is None:
        return None
    try:
        parsed = datetime.fromisoformat(now_iso)
    except ValueError:
        logger.error("context.now_iso=%r was not valid ISO 8601.", now_iso)
        return None
    hour = parsed.hour
    if 5 <= hour < 11:
        return "morning"
    if 11 <= hour < 14:
        return "midday"
    if 14 <= hour < 17:
        return "afternoon"
    if 17 <= hour < 22:
        return "evening"
    return "night"


def _candidate_weather_window(
    candidate: RetrieverCandidate,
) -> tuple[datetime, datetime] | None:
    if candidate.plan_time_iso is None:
        return None
    try:
        start_at = datetime.fromisoformat(candidate.plan_time_iso)
    except ValueError:
        return None
    if start_at.tzinfo is None:
        return None
    duration_hours = candidate.template_duration_hours
    if duration_hours is None:
        return None
    end_at = start_at + timedelta(hours=duration_hours)
    return start_at, end_at


def _weather_exposure(candidate: RetrieverCandidate) -> WeatherExposure:
    text = " ".join(
        [
            candidate.template_id,
            candidate.template_title,
            candidate.template_description or "",
        ]
    ).casefold()
    if any(token in text for token in ("hike", "bike", "swim", "trail", "coastal walk")):
        return WeatherExposure.ACTIVE_OUTDOOR
    if candidate.weather_sensitive:
        return WeatherExposure.OUTDOOR
    return WeatherExposure.INDOOR


def _location_is_set(value: Any) -> bool:
    return value is not None and getattr(value, "text", None) not in {None, ""}


def _is_set(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, tuple):
        return bool(value)
    return True


def _validated_limit(limit: int | None) -> int:
    if limit is None:
        return DEFAULT_LIMIT
    if limit <= 0:
        logger.error("Search limit=%s was invalid; falling back to %d.", limit, DEFAULT_LIMIT)
        return DEFAULT_LIMIT
    return min(limit, MAX_LIMIT)


def _default_context() -> Any:
    from back_end.search.models import SearchContext

    return SearchContext()


def _dedupe_strings(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result
