"""Typed models for natural-language card retrieval."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any

from back_end.query.models import FilterStageSummary


class FilterSource(str, Enum):
    """Where the final value for a filter field came from."""

    OVERRIDE = "override"
    PARSED = "parsed"
    DERIVED = "derived"
    UNSET = "unset"


class WeatherPreference(str, Enum):
    """Explicit weather preference parsed from user intent."""

    INDOORS_ONLY = "indoors_only"
    OUTDOORS_OK = "outdoors_ok"


@dataclass(frozen=True)
class SearchCoordinates:
    """User/device coordinates supplied by ambient context."""

    lat: float
    lng: float


@dataclass(frozen=True)
class LocationInput:
    """Structured location filter from the parser or UI override."""

    text: str | None = None
    radius_km: float | None = None


@dataclass(frozen=True)
class SearchContext:
    """Ambient state the frontend already knows."""

    now_iso: str | None = None
    user_location: SearchCoordinates | None = None
    exclude_plan_ids: tuple[str, ...] = ()
    limit: int | None = None


@dataclass(frozen=True)
class StructuredFilters:
    """Typed filter schema shared by overrides and parser output."""

    vibes: tuple[str, ...] = ()
    time_of_day: str | None = None
    weather_ok: WeatherPreference | None = None
    location: LocationInput | None = None
    transport_mode: str | None = None
    template_hints: tuple[str, ...] = ()


@dataclass(frozen=True)
class SearchRequest:
    """Top-level search request."""

    query: str | None = None
    context: SearchContext | None = None
    overrides: StructuredFilters | None = None


@dataclass(frozen=True)
class ParsedQuery:
    """Parser output prior to merge with overrides and ambient context."""

    free_text_residual: str | None = None
    filters: StructuredFilters = field(default_factory=StructuredFilters)
    warnings: tuple[str, ...] = ()
    llm_attempted: bool = False
    llm_succeeded: bool = False


@dataclass(frozen=True)
class ResolvedLocationFilter:
    """Concrete location anchor used by retrieval."""

    text: str | None = None
    radius_km: float | None = None
    anchor_latitude: float | None = None
    anchor_longitude: float | None = None
    resolved_label: str | None = None


@dataclass(frozen=True)
class SourcedValue:
    """One final filter value plus its provenance."""

    value: Any
    source: FilterSource


@dataclass(frozen=True)
class FinalParsedFilters:
    """Merged filter view returned to the client for debugging/UI chips."""

    vibes: SourcedValue
    time_of_day: SourcedValue
    weather_ok: SourcedValue
    location: SourcedValue
    transport_mode: SourcedValue
    template_hints: SourcedValue
    free_text_residual: SourcedValue
    warnings: tuple[str, ...] = ()
    auto_applied_notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class WeatherGateStats:
    """Diagnostics for weather-gated candidates."""

    evaluated: int = 0
    rejected: int = 0
    upstream_failures: int = 0
    skipped_indoors_only: int = 0
    cache_hits: int = 0
    groups: int = 0


@dataclass(frozen=True)
class ScoreBreakdown:
    """Explain one result's score."""

    lexical: float
    template_bonus: float = 0.0
    location_bonus: float = 0.0
    total: float = 0.0


@dataclass(frozen=True)
class SearchResult:
    """One ranked cached card."""

    plan_id: str
    score: float
    match_reasons: tuple[str, ...]
    score_breakdown: ScoreBreakdown
    card: dict[str, Any]


@dataclass(frozen=True)
class RetrieverCandidate:
    """Normalized cached plan candidate used during ranking/weather gating."""

    plan_id: str
    bucket_id: str
    template_id: str
    bucket_label: str
    bucket_latitude: float
    bucket_longitude: float
    time_of_day: str
    weather_sensitive: bool
    template_duration_hours: float | None
    template_title: str
    template_description: str | None
    search_text: str
    card: dict[str, Any]
    vibes: tuple[str, ...]
    transport_mode: str | None
    plan_time_iso: str | None
    fsq_place_ids_sorted: tuple[str, ...]
    distance_km: float | None = None


@dataclass(frozen=True)
class FilteredCandidatePool:
    """Candidates after deterministic filtering and before weather/reranking."""

    candidates: tuple[RetrieverCandidate, ...]
    filter_stage_counts: tuple[FilterStageSummary, ...]
    unsupported_constraints: tuple[str, ...] = ()


@dataclass(frozen=True)
class SearchDiagnostics:
    """Execution details surfaced to the caller."""

    total_matched_before_limit: int
    filter_stage_counts: tuple[FilterStageSummary, ...]
    weather_gate_stats: WeatherGateStats
    unsupported_constraints: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class SearchResponse:
    """Final response payload for the search endpoint."""

    parsed: FinalParsedFilters
    results: tuple[SearchResult, ...]
    diagnostics: SearchDiagnostics

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""

        return asdict(self)
