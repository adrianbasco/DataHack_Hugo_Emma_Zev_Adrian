"""Build a compact date-worthy places parquet from the raw AU dump."""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path

import pandas as pd

from back_end.catalog.categories import DEFAULT_SEED_PATH, DEFAULT_TAXONOMY_PATH, VIBES, load_allowlist
from back_end.catalog.filters import filter_places_by_vibes
from back_end.catalog.repository import REQUIRED_PLACE_COLUMNS, load_places_frame
from back_end.query.settings import REPO_ROOT

logger = logging.getLogger(__name__)

DEFAULT_INPUT_PATH = REPO_ROOT / "data" / "au_places.parquet"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "data" / "au_places_date_worthy.parquet"
OUTPUT_COLUMNS: tuple[str, ...] = REQUIRED_PLACE_COLUMNS


def build_filtered_places_dataset(
    places_path: Path | str = DEFAULT_INPUT_PATH,
    taxonomy_path: Path | str = DEFAULT_TAXONOMY_PATH,
    seed_path: Path | str = DEFAULT_SEED_PATH,
) -> pd.DataFrame:
    """Return the date-worthy subset of the AU places parquet.

    The output keeps only the columns the current query pipeline actually
    consumes. This strips both irrelevant rows and unused fields from the raw
    dump while preserving the existing backend contract.
    """

    places_path = _require_parquet_path(places_path, label="places_path", must_exist=True)
    taxonomy_path = _require_parquet_path(
        taxonomy_path, label="taxonomy_path", must_exist=True
    )
    seed_path = Path(seed_path)
    if not seed_path.exists():
        raise FileNotFoundError(f"Allowlist seed YAML not found at {seed_path}.")

    allowlist = load_allowlist(seed_path=seed_path, taxonomy_path=taxonomy_path)
    places = load_places_frame(places_path)
    filtered = filter_places_by_vibes(places, VIBES, allowlist)

    if filtered.empty:
        raise ValueError(
            "Filtering produced 0 rows. Refusing to write an empty derived dataset; "
            "check the allowlist seed and taxonomy inputs."
        )

    output_df = filtered.loc[:, list(OUTPUT_COLUMNS)].copy().reset_index(drop=True)
    logger.info(
        "Built date-worthy dataset with %d / %d places (%.2f%% retained).",
        len(output_df),
        len(places),
        100.0 * len(output_df) / len(places),
    )
    return output_df


def write_filtered_places_dataset(
    places: pd.DataFrame,
    output_path: Path | str = DEFAULT_OUTPUT_PATH,
    *,
    overwrite: bool = False,
) -> Path:
    """Write the filtered dataset to parquet using an atomic replace."""

    output_path = _require_parquet_path(output_path, label="output_path")
    if not output_path.parent.exists():
        raise FileNotFoundError(
            f"Output directory {output_path.parent} does not exist. Create it explicitly first."
        )
    if output_path.exists() and not overwrite:
        raise FileExistsError(
            f"Refusing to overwrite existing file {output_path}. Pass overwrite=True to replace it."
        )

    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f"{output_path.stem}.",
            suffix=".tmp.parquet",
            dir=output_path.parent,
            delete=False,
        ) as tmp_file:
            tmp_path = Path(tmp_file.name)
        places.to_parquet(tmp_path, index=False)
        os.replace(tmp_path, output_path)
    except Exception:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()
        raise

    logger.info("Wrote %d filtered places to %s", len(places), output_path)
    return output_path


def _require_parquet_path(
    path: Path | str,
    *,
    label: str,
    must_exist: bool = False,
) -> Path:
    resolved = Path(path)
    if resolved.suffix != ".parquet":
        raise ValueError(f"{label} must point to a .parquet file. Got {resolved}.")
    if must_exist and not resolved.exists():
        raise FileNotFoundError(f"{label} parquet not found at {resolved}.")
    return resolved
