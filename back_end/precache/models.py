"""Typed models for pre-cache location scopes and candidate pools."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class LocationBucket:
    """A stable origin or destination cluster for cache generation."""

    bucket_id: str
    label: str
    latitude: float
    longitude: float
    radius_km: float
    transport_mode: str
    minimum_plan_count: int = 3
    maximum_plan_count: int = 20
    strategic_boost: int = 0
    tags: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class CandidatePoolPlace:
    """One RAG document selected for a location bucket candidate pool."""

    fsq_place_id: str
    name: str
    latitude: float
    longitude: float
    distance_km: float
    quality_score: int
    template_stop_tags: tuple[str, ...]
    category_labels: tuple[str, ...]


@dataclass(frozen=True)
class LocationCandidatePool:
    """Bounded set of candidate FSQ ids for one location bucket."""

    bucket: LocationBucket
    places: tuple[CandidatePoolPlace, ...]
    target_plan_count: int
    empty_reason: str | None = None

    @property
    def allowed_place_ids(self) -> tuple[str, ...]:
        return tuple(place.fsq_place_id for place in self.places)

