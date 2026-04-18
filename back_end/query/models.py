"""Typed models for the deterministic parquet query stack."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class TransportMode(str, Enum):
    WALKING = "walking"
    PUBLIC_TRANSPORT = "public_transport"
    DRIVING = "driving"


class LocationType(str, Enum):
    POSTCODE = "postcode"
    LOCALITY = "locality"


class FilterStageStatus(str, Enum):
    APPLIED = "applied"
    SKIPPED = "skipped"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True)
class GenerateDatesRequest:
    """Raw caller input for the local parquet query tool."""

    location: str
    vibes: tuple[str, ...]
    radius_km: float | None = None
    budget: str | None = None
    transport_mode: str = TransportMode.DRIVING.value
    party_size: int = 2
    max_candidates: int | None = None
    dietary_constraints: str | None = None
    accessibility_constraints: str | None = None


@dataclass(frozen=True)
class NormalizedConstraints:
    """Validated query constraints."""

    location_text: str
    vibes: tuple[str, ...]
    radius_km: float
    budget: str | None
    transport_mode: TransportMode
    party_size: int
    max_candidates: int
    dietary_constraints: str | None
    accessibility_constraints: str | None


@dataclass(frozen=True)
class UnsupportedConstraint:
    """Represents a constraint the parquet stage could not apply."""

    field: str
    reason: str
    message: str


@dataclass(frozen=True)
class ResolvedLocation:
    """A typed location anchor derived from the parquet dataset."""

    input_text: str
    location_type: LocationType
    locality: str | None
    region: str | None
    postcode: str | None
    anchor_latitude: float
    anchor_longitude: float
    matched_place_count: int
    matched_regions: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class FilterStageSummary:
    """Counts for one deterministic filter stage."""

    stage: str
    before: int
    after: int
    rejected: int
    status: FilterStageStatus
    detail: str | None = None


@dataclass(frozen=True)
class PlaceRecord:
    """Normalized place record returned by the query tool."""

    fsq_place_id: str
    name: str
    latitude: float
    longitude: float
    address: str | None
    locality: str | None
    region: str | None
    postcode: str | None
    fsq_category_ids: tuple[str, ...]
    fsq_category_labels: tuple[str, ...]
    distance_km: float


@dataclass(frozen=True)
class CandidatePool:
    """Bounded candidate pool for the next backend stage."""

    request: NormalizedConstraints
    resolved_location: ResolvedLocation
    candidates: tuple[PlaceRecord, ...]
    filter_summary: tuple[FilterStageSummary, ...]
    unsupported_constraints: tuple[UnsupportedConstraint, ...]
    empty_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dictionary."""

        return asdict(self)
