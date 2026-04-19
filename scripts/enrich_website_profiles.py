"""Enrich curated date candidates with website-derived profile text."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import replace
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from back_end.catalog.curated_dataset import write_curated_places_dataset  # noqa: E402
from back_end.services.adaptive_website_profiles import AdaptiveWebsiteProfileClient  # noqa: E402
from back_end.services.adaptive_website_profiles import AdaptiveWebsiteProfileSettings  # noqa: E402
from back_end.services.crawl4ai_profiles import Crawl4AIProfileSettings  # noqa: E402
from back_end.services.crawl4ai_profiles import Crawl4AIWebsiteProfileClient  # noqa: E402
from back_end.services.profile_pipeline import default_run_dir  # noqa: E402
from back_end.services.profile_pipeline import ensure_run_manifest  # noqa: E402
from back_end.services.profile_pipeline import run_chunked_profile_enrichment  # noqa: E402
from back_end.services.profile_pipeline import run_layout  # noqa: E402
from back_end.services.profile_sharding import shard_places_dataframe  # noqa: E402
from back_end.services.profile_sharding import shard_suffix  # noqa: E402
from back_end.services.website_profiles import WebsiteProfileClient  # noqa: E402

DEFAULT_INPUT_PATH = (
    REPO_ROOT / "datasets" / "sydney_date_candidates_60km_website_or_exempt.parquet"
)
DEFAULT_CHUNK_SIZE = 500


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
        help="Replace output parquet or resumable run state if it already exists.",
    )
    parser.add_argument(
        "--shard-count",
        type=int,
        default=1,
        help="Deterministically split the job into this many shards.",
    )
    parser.add_argument(
        "--shard-index",
        type=int,
        default=0,
        help="0-based shard index to process when --shard-count is greater than 1.",
    )
    parser.add_argument(
        "--shard-key",
        choices=("website", "domain"),
        default="website",
        help="Identity used to keep related rows on the same shard.",
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="Resumable run directory. Defaults to a stable path under datasets/website_profile_runs/",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help="Number of website rows per durable chunk checkpoint.",
    )
    parser.add_argument(
        "--max-pages-per-site",
        type=int,
        default=None,
        help="Override Crawl4AI max pages per website.",
    )
    parser.add_argument(
        "--semaphore-count",
        type=int,
        default=None,
        help="Override Crawl4AI concurrent browser task count.",
    )
    parser.add_argument(
        "--max-session-permit",
        type=int,
        default=None,
        help="Override Crawl4AI max active sessions.",
    )
    parser.add_argument(
        "--homepage-timeout-ms",
        type=int,
        default=None,
        help="Override Crawl4AI homepage timeout in milliseconds.",
    )
    parser.add_argument(
        "--detail-timeout-ms",
        type=int,
        default=None,
        help="Override Crawl4AI detail-page timeout in milliseconds.",
    )
    parser.add_argument(
        "--retry-timeout-ms",
        type=int,
        default=None,
        help="Override Crawl4AI retry timeout in milliseconds.",
    )
    parser.add_argument(
        "--disable-retry-full-browser",
        action="store_true",
        help="Disable the heavier full-browser retry pass for failed Crawl4AI pages.",
    )
    parser.add_argument(
        "--crawl-watchdog-extra-seconds",
        type=float,
        default=None,
        help=(
            "Seconds added to each Crawl4AI page timeout before a single URL is "
            "marked crawl_watchdog_timeout."
        ),
    )
    parser.add_argument(
        "--crawl-watchdog-max-seconds",
        type=float,
        default=None,
        help=(
            "Maximum seconds any single Crawl4AI URL may hold a crawl slot before "
            "being marked crawl_watchdog_timeout."
        ),
    )
    parser.add_argument(
        "--batch-progress-interval-seconds",
        type=float,
        default=None,
        help="Seconds between no-progress warnings while a Crawl4AI URL batch is running.",
    )
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> Path:
    input_path = args.input
    if not input_path.exists():
        raise FileNotFoundError(f"Input parquet not found at {input_path}.")
    if args.chunk_size <= 0:
        raise ValueError(f"--chunk-size must be positive. Got {args.chunk_size}.")

    places = pd.read_parquet(input_path)
    if "has_website" not in places.columns:
        raise ValueError(
            f"Input parquet at {input_path} is missing has_website. "
            "Expected the curated dataset output."
        )

    website_places = places.loc[places["has_website"]].copy()
    website_places = shard_places_dataframe(
        website_places,
        shard_count=args.shard_count,
        shard_index=args.shard_index,
        shard_key=args.shard_key,
    )
    if args.limit is not None:
        if args.limit <= 0:
            raise ValueError(f"--limit must be positive when provided. Got {args.limit}.")
        website_places = website_places.head(args.limit).copy()
    if website_places.empty:
        raise ValueError(
            "No places with websites remained after filtering. "
            "Refusing to run a website enrichment job over 0 rows."
        )

    effective_run_dir = args.run_dir or default_run_dir(
        input_path=input_path,
        backend=args.backend,
        worker_count=args.shard_count,
        shard_key=args.shard_key,
        chunk_size=args.chunk_size,
    )
    logging.getLogger("enrich_website_profiles").info(
        "Prepared %d website rows for backend=%s shard=%d/%d shard_key=%s run_dir=%s.",
        len(website_places),
        args.backend,
        args.shard_index,
        args.shard_count,
        args.shard_key,
        effective_run_dir,
    )

    client = _build_client(args)
    try:
        ensure_run_manifest(
            run_dir=effective_run_dir,
            input_path=input_path,
            backend=args.backend,
            shard_count=args.shard_count,
            shard_key=args.shard_key,
            chunk_size=args.chunk_size,
            overwrite=args.overwrite,
        )
        layout = run_layout(
            run_dir=effective_run_dir,
            shard_count=args.shard_count,
            shard_index=args.shard_index,
        )
        shard_output_path = await run_chunked_profile_enrichment(
            places=website_places,
            client=client,
            layout=layout,
            chunk_size=args.chunk_size,
            shard_key=args.shard_key,
            overwrite=args.overwrite,
        )
    finally:
        aclose = getattr(client, "aclose", None)
        if callable(aclose):
            await aclose()

    if args.output is not None:
        if shard_output_path != args.output:
            shard_df = pd.read_parquet(shard_output_path)
            write_curated_places_dataset(
                shard_df,
                output_path=args.output,
                overwrite=args.overwrite,
            )
            return args.output
    return shard_output_path


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


def _build_client(args: argparse.Namespace) -> object:
    if args.backend == "heuristic":
        return WebsiteProfileClient()
    if args.backend == "crawl4ai":
        return Crawl4AIWebsiteProfileClient(settings=_crawl4ai_settings_from_args(args))
    return AdaptiveWebsiteProfileClient(settings=_adaptive_settings_from_args(args))


def _default_output_path(args: argparse.Namespace, *, run_dir: Path | None) -> Path:
    if run_dir is not None:
        layout = run_layout(
            run_dir=run_dir,
            shard_count=args.shard_count,
            shard_index=args.shard_index,
        )
        return layout.shard_output_path
    suffix = ""
    if args.shard_count > 1:
        suffix = shard_suffix(
            shard_count=args.shard_count,
            shard_index=args.shard_index,
        )
    return args.input.parent / f"{args.input.stem}_{args.backend}_profiles{suffix}.parquet"


def _crawl4ai_settings_from_args(args: argparse.Namespace) -> Crawl4AIProfileSettings:
    settings = Crawl4AIProfileSettings()
    replacements: dict[str, object] = {}
    if args.max_pages_per_site is not None:
        replacements["max_pages_per_site"] = args.max_pages_per_site
    if args.semaphore_count is not None:
        replacements["semaphore_count"] = args.semaphore_count
    if args.max_session_permit is not None:
        replacements["max_session_permit"] = args.max_session_permit
    if args.homepage_timeout_ms is not None:
        replacements["homepage_timeout_ms"] = args.homepage_timeout_ms
    if args.detail_timeout_ms is not None:
        replacements["detail_timeout_ms"] = args.detail_timeout_ms
    if args.retry_timeout_ms is not None:
        replacements["retry_timeout_ms"] = args.retry_timeout_ms
    if args.disable_retry_full_browser:
        replacements["retry_failed_with_full_browser"] = False
    if args.crawl_watchdog_extra_seconds is not None:
        if args.crawl_watchdog_extra_seconds < 0:
            raise ValueError(
                "--crawl-watchdog-extra-seconds must be non-negative when provided. "
                f"Got {args.crawl_watchdog_extra_seconds}."
            )
        replacements["crawl_watchdog_extra_seconds"] = args.crawl_watchdog_extra_seconds
    if args.crawl_watchdog_max_seconds is not None:
        if args.crawl_watchdog_max_seconds <= 0:
            raise ValueError(
                "--crawl-watchdog-max-seconds must be positive when provided. "
                f"Got {args.crawl_watchdog_max_seconds}."
            )
        replacements["crawl_watchdog_max_seconds"] = args.crawl_watchdog_max_seconds
    if args.batch_progress_interval_seconds is not None:
        if args.batch_progress_interval_seconds <= 0:
            raise ValueError(
                "--batch-progress-interval-seconds must be positive when provided. "
                f"Got {args.batch_progress_interval_seconds}."
            )
        replacements["batch_progress_interval_seconds"] = args.batch_progress_interval_seconds
    if not replacements:
        return settings
    return replace(settings, **replacements)


def _adaptive_settings_from_args(args: argparse.Namespace) -> AdaptiveWebsiteProfileSettings:
    settings = AdaptiveWebsiteProfileSettings()
    crawl4ai_settings = _crawl4ai_settings_from_args(args)
    if crawl4ai_settings == settings.crawl4ai:
        return settings
    return replace(settings, crawl4ai=crawl4ai_settings)


if __name__ == "__main__":
    raise SystemExit(main())
