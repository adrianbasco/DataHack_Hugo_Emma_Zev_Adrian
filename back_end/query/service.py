"""Deterministic place query service for the local parquet dataset."""

from __future__ import annotations

import logging

from back_end.catalog.categories import Allowlist, load_allowlist
from back_end.catalog.filters import filter_places_by_vibes
from back_end.query.location import LocationFilter, TypedLocationResolver
from back_end.catalog.repository import PlacesRepository
from back_end.query.errors import ConstraintValidationError
from back_end.query.models import (
    CandidatePool,
    FilterStageStatus,
    FilterStageSummary,
    GenerateDatesRequest,
    NormalizedConstraints,
    PlaceRecord,
    TransportMode,
    UnsupportedConstraint,
)
from back_end.query.settings import QuerySettings, load_query_settings

logger = logging.getLogger(__name__)

VALID_BUDGETS = {"$", "$$", "$$$", "$$$$"}


class ConstraintNormalizer:
    """Validate raw query input and make unsupported behavior explicit."""

    def __init__(self, settings: QuerySettings) -> None:
        self._settings = settings

    def normalize(self, request: GenerateDatesRequest) -> NormalizedConstraints:
        location_text = request.location.strip()
        if not location_text:
            raise ConstraintValidationError("location must not be empty.")

        vibes = tuple(vibe.strip() for vibe in request.vibes if vibe and vibe.strip())
        if not vibes:
            raise ConstraintValidationError("vibes must contain at least one non-empty value.")

        radius_km = (
            self._settings.default_radius_km
            if request.radius_km is None
            else float(request.radius_km)
        )
        if radius_km <= 0:
            raise ConstraintValidationError("radius_km must be positive.")

        if request.transport_mode not in {mode.value for mode in TransportMode}:
            raise ConstraintValidationError(
                f"transport_mode must be one of {[mode.value for mode in TransportMode]}."
            )

        if request.party_size <= 0:
            raise ConstraintValidationError("party_size must be positive.")

        max_candidates = (
            self._settings.default_candidate_limit
            if request.max_candidates is None
            else int(request.max_candidates)
        )
        if max_candidates <= 0:
            raise ConstraintValidationError("max_candidates must be positive.")
        if max_candidates > self._settings.max_candidate_limit:
            raise ConstraintValidationError(
                f"max_candidates must be <= {self._settings.max_candidate_limit}."
            )

        budget = request.budget.strip() if request.budget is not None else None
        if budget is not None and budget not in VALID_BUDGETS:
            raise ConstraintValidationError(
                f"budget must be one of {sorted(VALID_BUDGETS)} when provided."
            )

        return NormalizedConstraints(
            location_text=location_text,
            vibes=vibes,
            radius_km=radius_km,
            budget=budget,
            transport_mode=TransportMode(request.transport_mode),
            party_size=request.party_size,
            max_candidates=max_candidates,
            dietary_constraints=_normalize_optional_text(request.dietary_constraints),
            accessibility_constraints=_normalize_optional_text(
                request.accessibility_constraints
            ),
        )


class BudgetFilter:
    """Surface budget behavior explicitly for the local parquet stage."""

    @staticmethod
    def apply(
        budget: str | None,
        candidate_count: int,
    ) -> tuple[FilterStageSummary, tuple[UnsupportedConstraint, ...]]:
        if budget is None:
            return (
                FilterStageSummary(
                    stage="budget",
                    before=candidate_count,
                    after=candidate_count,
                    rejected=0,
                    status=FilterStageStatus.SKIPPED,
                    detail="No budget preference supplied.",
                ),
                (),
            )

        unsupported = UnsupportedConstraint(
            field="budget",
            reason="unsupported_by_dataset",
            message=(
                "Budget filtering was not applied because data/au_places.parquet does "
                "not expose a trustworthy price field."
            ),
        )
        logger.warning("Budget filtering requested for %s but unsupported by dataset.", budget)
        return (
            FilterStageSummary(
                stage="budget",
                before=candidate_count,
                after=candidate_count,
                rejected=0,
                status=FilterStageStatus.UNSUPPORTED,
                detail=unsupported.message,
            ),
            (unsupported,),
        )


class PlaceQueryService:
    """Main deterministic query tool for location-aware place search."""

    def __init__(
        self,
        repository: PlacesRepository | None = None,
        allowlist: Allowlist | None = None,
        settings: QuerySettings | None = None,
    ) -> None:
        self._settings = settings or load_query_settings()
        self._repository = repository or PlacesRepository(self._settings)
        self._allowlist = allowlist or load_allowlist(
            seed_path=self._settings.allowlist_seed_path,
            taxonomy_path=self._settings.categories_parquet_path,
        )
        self._normalizer = ConstraintNormalizer(self._settings)
        self._location_resolver = TypedLocationResolver(self._repository)

    def query(self, request: GenerateDatesRequest) -> CandidatePool:
        normalized = self._normalizer.normalize(request)
        all_places = self._repository.places_df
        open_places = self._repository.open_places_df
        logger.info("Starting place query over %d open places.", len(open_places))

        resolved_location = self._location_resolver.resolve(normalized.location_text)
        summary: list[FilterStageSummary] = [
            FilterStageSummary(
                stage="open_places",
                before=len(all_places),
                after=len(open_places),
                rejected=len(all_places) - len(open_places),
                status=FilterStageStatus.APPLIED,
                detail="Excluded places with date_closed populated.",
            )
        ]

        vibe_filtered = filter_places_by_vibes(open_places, normalized.vibes, self._allowlist)
        summary.append(
            FilterStageSummary(
                stage="vibe",
                before=len(open_places),
                after=len(vibe_filtered),
                rejected=len(open_places) - len(vibe_filtered),
                status=FilterStageStatus.APPLIED,
                detail=f"Applied vibes={list(normalized.vibes)}.",
            )
        )

        location_filtered = LocationFilter.apply_radius(
            places=vibe_filtered,
            resolved_location=resolved_location,
            radius_km=normalized.radius_km,
        )
        summary.append(
            FilterStageSummary(
                stage="location_radius",
                before=len(vibe_filtered),
                after=len(location_filtered),
                rejected=len(vibe_filtered) - len(location_filtered),
                status=FilterStageStatus.APPLIED,
                detail=(
                    f"Applied {normalized.radius_km:.2f}km radius around "
                    f"{resolved_location.input_text!r}."
                ),
            )
        )

        budget_stage, unsupported_constraints = BudgetFilter.apply(
            normalized.budget,
            len(location_filtered),
        )
        summary.append(budget_stage)

        limited = location_filtered.head(normalized.max_candidates).copy()
        summary.append(
            FilterStageSummary(
                stage="candidate_limit",
                before=len(location_filtered),
                after=len(limited),
                rejected=len(location_filtered) - len(limited),
                status=FilterStageStatus.APPLIED,
                detail=f"Capped candidates to max_candidates={normalized.max_candidates}.",
            )
        )

        candidates = tuple(_row_to_place_record(row) for _, row in limited.iterrows())
        empty_reason = None
        if not candidates:
            empty_reason = (
                f"No open places matched vibes={list(normalized.vibes)} within "
                f"{normalized.radius_km:.2f}km of {resolved_location.input_text!r}."
            )
            logger.warning(empty_reason)

        return CandidatePool(
            request=normalized,
            resolved_location=resolved_location,
            candidates=candidates,
            filter_summary=tuple(summary),
            unsupported_constraints=unsupported_constraints,
            empty_reason=empty_reason,
        )


def query_places(request: GenerateDatesRequest) -> CandidatePool:
    """Convenience entrypoint for callers that only need the default stack."""

    return PlaceQueryService().query(request)


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _row_to_place_record(row: object) -> PlaceRecord:
    data = row.to_dict()
    return PlaceRecord(
        fsq_place_id=str(data["fsq_place_id"]),
        name=str(data["name"]),
        latitude=float(data["latitude"]),
        longitude=float(data["longitude"]),
        address=_optional_string(data.get("address")),
        locality=_optional_string(data.get("locality")),
        region=_optional_string(data.get("region")),
        postcode=_optional_string(data.get("postcode")),
        fsq_category_ids=tuple(str(item) for item in data.get("fsq_category_ids", [])),
        fsq_category_labels=tuple(
            str(item) for item in data.get("fsq_category_labels", [])
        ),
        distance_km=float(data["distance_km"]),
    )


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    try:
        if value != value:
            return None
    except Exception:
        pass
    text = str(value).strip()
    return text or None
