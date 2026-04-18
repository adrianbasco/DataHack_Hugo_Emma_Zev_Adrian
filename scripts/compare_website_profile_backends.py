"""Compare heuristic website enrichment against Crawl4AI on a row subset."""

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
from back_end.services.crawl4ai_profiles import Crawl4AIWebsiteProfileClient  # noqa: E402
from back_end.services.website_profiles import WebsiteProfileClient  # noqa: E402

DEFAULT_INPUT_PATH = (
    REPO_ROOT / "datasets" / "sydney_date_candidates_60km_website_or_exempt.parquet"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the heuristic and Crawl4AI website profile extractors on the same "
            "subset and write both outputs plus a row-level comparison parquet."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT_PATH,
        help="Source curated candidate parquet.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of website rows to compare.",
    )
    parser.add_argument(
        "--output-prefix",
        default="website_profile_compare_first10",
        help="Prefix for generated comparison artifacts inside datasets/.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace output parquets if they already exist.",
    )
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> dict[str, Path]:
    places = pd.read_parquet(args.input)
    subset = places.loc[places["has_website"]].head(args.limit).copy()
    if subset.empty:
        raise ValueError("No website rows available for comparison.")

    heuristic_client = WebsiteProfileClient()
    crawl4ai_client = Crawl4AIWebsiteProfileClient()
    try:
        heuristic_df, crawl4ai_df = await asyncio.gather(
            heuristic_client.enrich_dataframe(subset),
            crawl4ai_client.enrich_dataframe(subset),
        )
    finally:
        await heuristic_client.aclose()

    comparison_df = subset.loc[:, ["fsq_place_id", "name", "website"]].copy()
    comparison_df = comparison_df.merge(
        heuristic_df[
            [
                "fsq_place_id",
                "website_enrichment_status",
                "website_page_count",
                "website_cuisines",
                "website_ambience_tags",
                "website_setting_tags",
                "website_booking_signals",
                "website_rich_profile_text",
            ]
        ],
        on="fsq_place_id",
        how="left",
        validate="one_to_one",
    )
    comparison_df = comparison_df.merge(
        crawl4ai_df[
            [
                "fsq_place_id",
                "crawl4ai_enrichment_status",
                "crawl4ai_page_count",
                "crawl4ai_cuisines",
                "crawl4ai_ambience_tags",
                "crawl4ai_setting_tags",
                "crawl4ai_booking_signals",
                "crawl4ai_rich_profile_text",
            ]
        ],
        on="fsq_place_id",
        how="left",
        validate="one_to_one",
    )
    comparison_df["website_rich_profile_len"] = (
        comparison_df["website_rich_profile_text"].fillna("").str.len()
    )
    comparison_df["crawl4ai_rich_profile_len"] = (
        comparison_df["crawl4ai_rich_profile_text"].fillna("").str.len()
    )

    datasets_dir = REPO_ROOT / "datasets"
    heuristic_path = datasets_dir / f"{args.output_prefix}_heuristic.parquet"
    crawl4ai_path = datasets_dir / f"{args.output_prefix}_crawl4ai.parquet"
    comparison_path = datasets_dir / f"{args.output_prefix}_comparison.parquet"

    write_curated_places_dataset(
        heuristic_df,
        output_path=heuristic_path,
        overwrite=args.overwrite,
    )
    write_curated_places_dataset(
        crawl4ai_df,
        output_path=crawl4ai_path,
        overwrite=args.overwrite,
    )
    write_curated_places_dataset(
        comparison_df,
        output_path=comparison_path,
        overwrite=args.overwrite,
    )
    return {
        "heuristic": heuristic_path,
        "crawl4ai": crawl4ai_path,
        "comparison": comparison_path,
    }


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("compare_website_profile_backends")
    try:
        outputs = asyncio.run(_run(args))
    except Exception:
        log.exception("Failed to compare website profile backends.")
        return 1

    for label, path in outputs.items():
        log.info("Wrote %s output to %s", label, path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
