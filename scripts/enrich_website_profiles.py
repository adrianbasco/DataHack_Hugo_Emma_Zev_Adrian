"""Enrich curated date candidates with website-derived profile text."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from back_end.catalog.curated_dataset import write_curated_places_dataset  # noqa: E402
from back_end.services.adaptive_website_profiles import AdaptiveWebsiteProfileClient  # noqa: E402
from back_end.services.crawl4ai_profiles import Crawl4AIWebsiteProfileClient  # noqa: E402
from back_end.services.website_profiles import WebsiteProfileClient  # noqa: E402

DEFAULT_INPUT_PATH = (
    REPO_ROOT / "datasets" / "sydney_date_candidates_60km_website_or_exempt.parquet"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Enrich the curated Sydney date candidates dataset with website-derived "
            "profile fields using either the in-repo heuristic extractor, the "
            "straight Crawl4AI pipeline, or the adaptive fallback pipeline."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT_PATH,
        help="Source curated candidate parquet.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Destination parquet. Defaults to datasets/<stem>_<backend>_profiles.parquet",
    )
    parser.add_argument(
        "--backend",
        choices=("heuristic", "crawl4ai", "adaptive"),
        default="crawl4ai",
        help="Website enrichment backend to use.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row cap, applied after filtering to places with websites.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace the output parquet if it already exists.",
    )
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> Path:
    input_path = args.input
    if not input_path.exists():
        raise FileNotFoundError(f"Input parquet not found at {input_path}.")

    places = pd.read_parquet(input_path)
    if "has_website" not in places.columns:
        raise ValueError(
            f"Input parquet at {input_path} is missing has_website. "
            "Expected the curated dataset output."
        )

    website_places = places.loc[places["has_website"]].copy()
    if args.limit is not None:
        if args.limit <= 0:
            raise ValueError(f"--limit must be positive when provided. Got {args.limit}.")
        website_places = website_places.head(args.limit).copy()

    if website_places.empty:
        raise ValueError(
            "No places with websites remained after filtering. "
            "Refusing to run a website enrichment job over 0 rows."
        )

    if args.backend == "heuristic":
        client = WebsiteProfileClient()
    elif args.backend == "crawl4ai":
        client = Crawl4AIWebsiteProfileClient()
    else:
        client = AdaptiveWebsiteProfileClient()

    try:
        enriched = await client.enrich_dataframe(website_places)
    finally:
        aclose = getattr(client, "aclose", None)
        if callable(aclose):
            await aclose()

    output_path = args.output or (
        input_path.parent / f"{input_path.stem}_{args.backend}_profiles.parquet"
    )
    write_curated_places_dataset(
        enriched,
        output_path=output_path,
        overwrite=args.overwrite,
    )
    return output_path


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("enrich_website_profiles")
    try:
        output_path = asyncio.run(_run(args))
    except Exception:
        log.exception("Failed to enrich website profiles.")
        return 1

    log.info("Wrote enriched website profiles to %s", output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
