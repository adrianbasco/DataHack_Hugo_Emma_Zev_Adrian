"""Settings for the deterministic parquet-backed query layer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class QuerySettings:
    """Filesystem paths and defaults for local place queries."""

    places_parquet_path: Path = REPO_ROOT / "data" / "au_places.parquet"
    categories_parquet_path: Path = REPO_ROOT / "data" / "categories.parquet"
    allowlist_seed_path: Path = REPO_ROOT / "config" / "date_categories.yaml"
    default_radius_km: float = 5.0
    default_candidate_limit: int = 50
    max_candidate_limit: int = 250


def load_query_settings() -> QuerySettings:
    """Return local settings for the parquet query layer."""

    return QuerySettings()
