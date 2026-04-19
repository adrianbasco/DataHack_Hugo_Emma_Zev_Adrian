"""Run website enrichment across multiple deterministic shards and merge the result."""

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

from back_end.services.profile_pipeline import default_run_dir  # noqa: E402
from back_end.services.profile_pipeline import ensure_run_manifest  # noqa: E402
from back_end.services.profile_pipeline import run_layout  # noqa: E402
from back_end.services.profile_pipeline import stream_merge_shard_outputs  # noqa: E402

DEFAULT_INPUT_PATH = (
    REPO_ROOT / "datasets" / "sydney_date_candidates_60km_website_or_exempt.parquet"
)
ENRICH_SCRIPT_PATH = REPO_ROOT / "scripts" / "enrich_website_profiles.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run website enrichment across multiple shard workers, then merge the "
            "result into one parquet."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT_PATH,
        help="Source curated candidate parquet.",
    )
    parser.add_argument(
        "--backend",
        choices=("heuristic", "crawl4ai", "adaptive"),
        default="crawl4ai",
        help="Website enrichment backend to use for each shard.",
    )
    parser.add_argument(
        "--worker-count",
        type=int,
        required=True,
        help="How many shard workers to run.",
    )
    parser.add_argument(
        "--shard-key",
        choices=("website", "domain"),
        default="website",
        help="Identity used to keep related rows on the same shard.",
    )
    parser.add_argument(
        "--merged-output",
        type=Path,
        default=None,
        help="Merged parquet path. Defaults to datasets/<stem>_<backend>_profiles_merged.parquet",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace shard outputs and merged output if they already exist.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional per-shard row cap, mainly for smoke tests.",
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
        "--run-dir",
        type=Path,
        default=None,
        help="Stable resumable run directory. Defaults to datasets/website_profile_runs/<job-name>",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        help="Number of website rows per durable checkpoint chunk.",
    )
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> Path:
    if args.worker_count <= 0:
        raise ValueError(f"--worker-count must be positive. Got {args.worker_count}.")
    if not args.input.exists():
        raise FileNotFoundError(f"Input parquet not found at {args.input}.")
    if args.chunk_size <= 0:
        raise ValueError(f"--chunk-size must be positive. Got {args.chunk_size}.")

    run_dir = args.run_dir or default_run_dir(
        input_path=args.input,
        backend=args.backend,
        worker_count=args.worker_count,
        shard_key=args.shard_key,
        chunk_size=args.chunk_size,
    )
    ensure_run_manifest(
        run_dir=run_dir,
        input_path=args.input,
        backend=args.backend,
        shard_count=args.worker_count,
        shard_key=args.shard_key,
        chunk_size=args.chunk_size,
        overwrite=args.overwrite,
    )
    logging.getLogger("run_website_profile_shards").info(
        "Using resumable run directory %s with %d workers and chunk size %d.",
        run_dir,
        args.worker_count,
        args.chunk_size,
    )
    shard_paths = [
        run_layout(
            run_dir=run_dir,
            shard_count=args.worker_count,
            shard_index=index,
        ).shard_output_path
        for index in range(args.worker_count)
    ]

    await asyncio.gather(
        *[
            _run_single_worker(
                args=args,
                run_dir=run_dir,
                shard_index=index,
                shard_output_path=shard_paths[index],
            )
            for index in range(args.worker_count)
        ]
    )

    for shard_index, shard_path in enumerate(shard_paths):
        if not shard_path.exists():
            raise FileNotFoundError(
                f"Shard {shard_index} did not produce an output parquet at {shard_path}."
            )
        shard_df = pd.read_parquet(shard_path)
        logging.getLogger("run_website_profile_shards").info(
            "Loaded shard %d/%d with %d rows from %s.",
            shard_index,
            args.worker_count,
            len(shard_df),
            shard_path,
        )

    merged_output = args.merged_output or (
        args.input.parent / f"{args.input.stem}_{args.backend}_profiles_merged.parquet"
    )
    if merged_output.exists() and not args.overwrite:
        logging.getLogger("run_website_profile_shards").info(
            "Merged output already exists at %s. Leaving it unchanged.",
            merged_output,
        )
        return merged_output
    stream_merge_shard_outputs(
        shard_paths=shard_paths,
        output_path=merged_output,
        overwrite=args.overwrite,
    )
    return merged_output


async def _run_single_worker(
    *,
    args: argparse.Namespace,
    run_dir: Path,
    shard_index: int,
    shard_output_path: Path,
) -> None:
    log = logging.getLogger("run_website_profile_shards")
    if shard_output_path.exists() and not args.overwrite:
        log.info(
            "Skipping shard %d because output already exists at %s.",
            shard_index,
            shard_output_path,
        )
        return

    command = [
        sys.executable,
        str(ENRICH_SCRIPT_PATH),
        "--input",
        str(args.input),
        "--backend",
        args.backend,
        "--output",
        str(shard_output_path),
        "--run-dir",
        str(run_dir),
        "--shard-count",
        str(args.worker_count),
        "--shard-index",
        str(shard_index),
        "--shard-key",
        args.shard_key,
    ]
    if args.overwrite:
        command.append("--overwrite")
    if args.limit is not None:
        command.extend(["--limit", str(args.limit)])
    if args.chunk_size is not None:
        command.extend(["--chunk-size", str(args.chunk_size)])
    if args.max_pages_per_site is not None:
        command.extend(["--max-pages-per-site", str(args.max_pages_per_site)])
    if args.semaphore_count is not None:
        command.extend(["--semaphore-count", str(args.semaphore_count)])
    if args.max_session_permit is not None:
        command.extend(["--max-session-permit", str(args.max_session_permit)])
    if args.homepage_timeout_ms is not None:
        command.extend(["--homepage-timeout-ms", str(args.homepage_timeout_ms)])
    if args.detail_timeout_ms is not None:
        command.extend(["--detail-timeout-ms", str(args.detail_timeout_ms)])
    if args.retry_timeout_ms is not None:
        command.extend(["--retry-timeout-ms", str(args.retry_timeout_ms)])
    if args.disable_retry_full_browser:
        command.append("--disable-retry-full-browser")

    log.info(
        "Launching shard %d/%d -> %s",
        shard_index,
        args.worker_count,
        shard_output_path,
    )
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=str(REPO_ROOT),
    )
    return_code = await process.wait()
    if return_code != 0:
        raise RuntimeError(
            f"Shard {shard_index} failed with exit code {return_code}. "
            f"Command: {' '.join(command)}"
        )


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("run_website_profile_shards")
    try:
        output_path = asyncio.run(_run(args))
    except Exception:
        log.exception("Failed to run website profile shard job.")
        return 1

    log.info("Merged shard output written to %s", output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
