"""Apply the vibe allowlist to the places dataset.

The input ``places`` DataFrame is expected to come from
``data/au_places.parquet`` and must have an ``fsq_category_ids`` column of
list/array-typed values (each element a category id string).
"""

from __future__ import annotations

import logging
from typing import Iterable

import pandas as pd

from back_end.catalog.categories import Allowlist

logger = logging.getLogger(__name__)

REQUIRED_PLACE_COLS: tuple[str, ...] = ("fsq_place_id", "fsq_category_ids")


def filter_places_by_vibes(
    places: pd.DataFrame,
    vibes: Iterable[str],
    allowlist: Allowlist,
) -> pd.DataFrame:
    """Return the subset of ``places`` whose category IDs match any of ``vibes``.

    A place is kept iff at least one of its ``fsq_category_ids`` entries is
    in the union of the requested vibes' allowlists.

    Raises ``ValueError`` for a malformed ``places`` DataFrame or unknown vibes
    (the latter via ``Allowlist.ids_for``).
    """
    missing_cols = [c for c in REQUIRED_PLACE_COLS if c not in places.columns]
    if missing_cols:
        raise ValueError(
            f"places DataFrame is missing required columns {missing_cols}. "
            f"Got columns: {sorted(places.columns)}."
        )

    vibes = list(vibes)
    target_ids: frozenset[str] = allowlist.ids_for(vibes)
    if not target_ids:
        raise ValueError(
            f"Allowlist for vibes {vibes!r} is empty. This should be impossible "
            "given seed validation; investigate categories.load_allowlist()."
        )

    total = len(places)
    logger.info(
        "Filtering %d places against %d category IDs (vibes=%s)",
        total, len(target_ids), vibes,
    )

    exploded = places["fsq_category_ids"].explode()
    is_missing = exploded.isna()
    missing_count = int(is_missing.groupby(exploded.index).all().sum())
    if missing_count:
        logger.warning(
            "%d of %d places have no fsq_category_ids (will be excluded).",
            missing_count, total,
        )

    matches = exploded[exploded.isin(target_ids)]
    matching_indices = matches.index.unique()
    filtered = places.loc[matching_indices].copy()

    logger.info(
        "Kept %d / %d places (%.2f%%) for vibes=%s",
        len(filtered), total,
        (100.0 * len(filtered) / total) if total else 0.0,
        vibes,
    )
    return filtered
