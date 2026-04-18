"""Build a curated Sydney-focused date candidate parquet.

Run from the repo root:

    source .venv/bin/activate
    python scripts/build_sydney_date_candidates.py --overwrite
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from back_end.catalog.curated_dataset import (  # noqa: E402
    DEFAULT_INPUT_PATH,
    DEFAULT_OUTPUT_PATH,
    DEFAULT_RADIUS_KM,
    DEFAULT_REFRESHED_SINCE,
    build_curated_places_dataset,
    write_curated_places_dataset,
)
from back_end.catalog.categories import DEFAULT_SEED_PATH, DEFAULT_TAXONOMY_PATH  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Filter the raw AU parquet to recent, open Sydney date candidates within "
            "60km, requiring a website except for explicit public/outdoor exemptions."
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
        help="Curated date category YAML.",
    )
    parser.add_argument(
        "--location",
        default="Sydney, NSW",
        help="Location anchor text accepted by the local location resolver.",
    )
    parser.add_argument(
        "--radius-km",
        type=float,
        default=DEFAULT_RADIUS_KM,
        help="Radius around the resolved location anchor.",
    )
    parser.add_argument(
        "--refreshed-since",
        default=DEFAULT_REFRESHED_SINCE,
        help="Inclusive UTC freshness cutoff date, for example 2024-04-18.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Destination parquet for the curated dataset.",
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
    log = logging.getLogger("build_sydney_date_candidates")

    try:
        curated = build_curated_places_dataset(
            places_path=args.input,
            taxonomy_path=args.taxonomy,
            seed_path=args.seed,
            location_text=args.location,
            radius_km=args.radius_km,
            refreshed_since=args.refreshed_since,
        )
        write_curated_places_dataset(
            curated,
            output_path=args.output,
            overwrite=args.overwrite,
        )
    except Exception:
        log.exception("Failed to build the curated Sydney date candidates dataset.")
        return 1

    log.info("Curated Sydney date candidates dataset build completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
