"""Build a compact date-worthy AU places parquet from the raw Foursquare dump.

Run from the repo root:

    source .venv/bin/activate
    python scripts/filter_au_places.py
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from back_end.catalog.dataset import (  # noqa: E402
    DEFAULT_INPUT_PATH,
    DEFAULT_OUTPUT_PATH,
    build_filtered_places_dataset,
    write_filtered_places_dataset,
)
from back_end.catalog.categories import DEFAULT_SEED_PATH, DEFAULT_TAXONOMY_PATH  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Filter data/au_places.parquet down to the curated date-worthy categories "
            "and write a compact derived parquet."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT_PATH,
        help="Source AU places parquet.",
    )
    parser.add_argument(
        "--taxonomy",
        type=Path,
        default=DEFAULT_TAXONOMY_PATH,
        help="Foursquare categories parquet used to expand the allowlist.",
    )
    parser.add_argument(
        "--seed",
        type=Path,
        default=DEFAULT_SEED_PATH,
        help="Curated allowlist YAML.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Destination parquet for the filtered dataset.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace the output parquet if it already exists.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("filter_au_places")

    try:
        filtered = build_filtered_places_dataset(
            places_path=args.input,
            taxonomy_path=args.taxonomy,
            seed_path=args.seed,
        )
        write_filtered_places_dataset(
            filtered,
            output_path=args.output,
            overwrite=args.overwrite,
        )
    except Exception:
        log.exception("Failed to build the filtered AU places dataset.")
        return 1

    log.info("Filtered dataset build completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
