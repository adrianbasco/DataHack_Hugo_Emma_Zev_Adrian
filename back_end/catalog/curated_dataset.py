"""Build curated, query-ready place datasets from the raw AU parquet."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import pandas as pd

from back_end.catalog.categories import DEFAULT_SEED_PATH, DEFAULT_TAXONOMY_PATH, VIBES, load_allowlist
from back_end.catalog.dataset import write_filtered_places_dataset
from back_end.catalog.filters import filter_places_by_vibes
from back_end.catalog.repository import REQUIRED_PLACE_COLUMNS, load_places_frame
from back_end.query.location import LocationFilter, TypedLocationResolver
from back_end.query.settings import REPO_ROOT, QuerySettings

logger = logging.getLogger(__name__)

DEFAULT_INPUT_PATH = REPO_ROOT / "data" / "au_places.parquet"
DEFAULT_OUTPUT_PATH = (
    REPO_ROOT / "datasets" / "sydney_date_candidates_60km_website_or_exempt.parquet"
)
DEFAULT_LOCATION_TEXT = "Sydney, NSW"
DEFAULT_RADIUS_KM = 60.0
DEFAULT_REFRESHED_SINCE = "2024-04-18"
DEFAULT_WEBSITE_EXEMPT_SEED_PATHS: tuple[str, ...] = (
    "Landmarks and Outdoors > Beach",
    "Landmarks and Outdoors > Scenic Lookout",
    "Landmarks and Outdoors > Boardwalk",
    "Landmarks and Outdoors > Botanical Garden",
    "Landmarks and Outdoors > Harbor or Marina",
    "Landmarks and Outdoors > Hiking Trail",
    "Landmarks and Outdoors > Bike Trail",
    "Travel and Transportation > Pier",
    "Dining and Drinking > Night Market",
    "Dining and Drinking > Food Truck",
    "Retail > Food and Beverage Retail > Farmers Market",
)
OUTPUT_COLUMNS: tuple[str, ...] = REQUIRED_PLACE_COLUMNS + (
    "date_refreshed",
    "website",
    "has_website",
    "is_website_exempt",
    "distance_km",
)


def build_curated_places_dataset(
    *,
    places_path: Path | str = DEFAULT_INPUT_PATH,
    taxonomy_path: Path | str = DEFAULT_TAXONOMY_PATH,
    seed_path: Path | str = DEFAULT_SEED_PATH,
    location_text: str = DEFAULT_LOCATION_TEXT,
    radius_km: float = DEFAULT_RADIUS_KM,
    refreshed_since: str = DEFAULT_REFRESHED_SINCE,
    exempt_seed_paths: Iterable[str] = DEFAULT_WEBSITE_EXEMPT_SEED_PATHS,
) -> pd.DataFrame:
    """Return a curated, website-biased candidate dataset for Sydney date places."""

    if radius_km <= 0:
        raise ValueError(f"radius_km must be positive. Got {radius_km}.")

    cutoff_ts = _parse_cutoff_timestamp(refreshed_since)
    exempt_seed_paths = _normalize_exempt_seed_paths(exempt_seed_paths)

    allowlist = load_allowlist(seed_path=seed_path, taxonomy_path=taxonomy_path)
    places = load_places_frame(places_path)
    enrichment = _load_raw_enrichment(places_path)

    merged = places.merge(enrichment, on="fsq_place_id", how="left", validate="one_to_one")
    merged["date_refreshed_ts"] = pd.to_datetime(
        merged["date_refreshed"], errors="coerce", utc=True
    )
    invalid_refresh_count = int(merged["date_refreshed_ts"].isna().sum())
    if invalid_refresh_count == len(merged):
        raise ValueError(
            "Every place has an invalid or missing date_refreshed value. "
            "Refusing to continue with a broken freshness filter."
        )
    if invalid_refresh_count:
        logger.warning(
            "Encountered %d places with invalid or missing date_refreshed values. "
            "They will be excluded by the recency filter.",
            invalid_refresh_count,
        )

    merged["has_website"] = merged["website"].fillna("").astype(str).str.strip().ne("")
    merged["is_website_exempt"] = [
        _matches_any_seed_path(category_labels, exempt_seed_paths)
        for category_labels in merged["fsq_category_labels"]
    ]

    settings = QuerySettings(places_parquet_path=Path(places_path))
    resolver = TypedLocationResolver(repository=_RepositoryProxy(settings))
    resolved_location = resolver.resolve(location_text)

    vibe_filtered = filter_places_by_vibes(merged, VIBES, allowlist)
    open_filtered = vibe_filtered.loc[vibe_filtered["date_closed"].isna()].copy()
    recent_filtered = open_filtered.loc[
        open_filtered["date_refreshed_ts"] >= cutoff_ts
    ].copy()
    radius_filtered = LocationFilter.apply_radius(
        places=recent_filtered,
        resolved_location=resolved_location,
        radius_km=radius_km,
    )
    curated = radius_filtered.loc[
        radius_filtered["has_website"] | radius_filtered["is_website_exempt"]
    ].copy()

    if curated.empty:
        raise ValueError(
            "Curated dataset filtering produced 0 rows. Refusing to write an empty "
            "dataset; investigate the location, freshness cutoff, or exemption list."
        )

    curated = curated.loc[:, list(OUTPUT_COLUMNS)].copy().reset_index(drop=True)
    logger.info(
        "Built curated dataset with %d / %d places retained for %s within %.2fkm "
        "and refreshed since %s.",
        len(curated),
        len(places),
        location_text,
        radius_km,
        cutoff_ts.date().isoformat(),
    )
    return curated


def write_curated_places_dataset(
    places: pd.DataFrame,
    output_path: Path | str = DEFAULT_OUTPUT_PATH,
    *,
    overwrite: bool = False,
) -> Path:
    """Write the curated dataset to parquet using the shared atomic writer."""

    return write_filtered_places_dataset(
        places,
        output_path=output_path,
        overwrite=overwrite,
    )


def _load_raw_enrichment(places_path: Path | str) -> pd.DataFrame:
    path = Path(places_path)
    raw = pd.read_parquet(path, columns=["fsq_place_id", "date_refreshed", "website"])

    duplicate_count = int(raw["fsq_place_id"].duplicated().sum())
    if duplicate_count:
        raise ValueError(
            f"Places parquet at {path} contains {duplicate_count} duplicate fsq_place_id "
            "values in the enrichment columns."
        )

    return raw


def _parse_cutoff_timestamp(value: str) -> pd.Timestamp:
    cutoff_ts = pd.to_datetime(value, errors="raise", utc=True)
    if pd.isna(cutoff_ts):
        raise ValueError(f"refreshed_since must be a valid date-like value. Got {value!r}.")
    return cutoff_ts


def _normalize_exempt_seed_paths(exempt_seed_paths: Iterable[str]) -> tuple[str, ...]:
    normalized = tuple(path.strip() for path in exempt_seed_paths if path and path.strip())
    if not normalized:
        raise ValueError("exempt_seed_paths must contain at least one non-empty category path.")
    return normalized


def _matches_any_seed_path(
    category_labels: object,
    seed_paths: Iterable[str],
) -> bool:
    if category_labels is None:
        return False
    labels = [str(label) for label in category_labels]
    for seed_path in seed_paths:
        for label in labels:
            if label == seed_path or label.startswith(seed_path + " > "):
                return True
    return False


class _RepositoryProxy:
    """Minimal repository shim so the location resolver can use the shared loader."""

    def __init__(self, settings: QuerySettings) -> None:
        self._settings = settings
        self._places_df: pd.DataFrame | None = None

    @property
    def open_places_df(self) -> pd.DataFrame:
        if self._places_df is None:
            self._places_df = load_places_frame(self._settings.places_parquet_path)
        return self._places_df.loc[self._places_df["date_closed"].isna()].copy()
