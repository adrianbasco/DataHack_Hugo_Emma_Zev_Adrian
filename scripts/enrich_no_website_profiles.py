"""Enrich website-exempt places with Maps/Brave/LLM feature profiles."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from dataclasses import replace
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from back_end.catalog.curated_dataset import write_curated_places_dataset  # noqa: E402
from back_end.services.no_website_profiles import (  # noqa: E402
    NoWebsiteProfileClient,
    NoWebsiteProfileSettings,
)
from back_end.services.profile_pipeline import ensure_run_manifest  # noqa: E402
from back_end.services.profile_pipeline import run_chunked_profile_enrichment  # noqa: E402
from back_end.services.profile_pipeline import run_layout  # noqa: E402

DEFAULT_INPUT_PATH = (
    REPO_ROOT / "datasets" / "sydney_date_candidates_60km_website_or_exempt.parquet"
)
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "data" / "no_website_profiles"
DEFAULT_MODEL = "google/gemini-2.5-flash-lite"
DEFAULT_CHUNK_SIZE = 10
DEFAULT_PROGRESS_EVERY = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build feature-vector source text for curated places that intentionally "
            "do not have websites. Reads parquet only."
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
        help=(
            "Destination parquet. Defaults to "
            "data/no_website_profiles/outputs/<input-stem>_<strategy>_<selection>.parquet"
        ),
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help=(
            "Resumable run directory. Defaults to "
            "data/no_website_profiles/runs/<input-stem>_<strategy>_<selection>_<model>_chunks-<chunk-size>."
        ),
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=(
            "Rows per durable checkpoint chunk. Completed chunks are reused on rerun "
            "unless --overwrite is passed."
        ),
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=DEFAULT_PROGRESS_EVERY,
        help="Log cumulative progress every N processed rows.",
    )
    parser.add_argument(
        "--strategy",
        choices=("baseline", "maps", "maps-brave", "maps-brave-llm"),
        default="maps-brave-llm",
        help="Evidence stack to run.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row cap after filtering to places without websites.",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Optional offset after filtering to places without websites.",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help="OpenRouter model for the LLM extractor.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=6,
        help="Maximum concurrent place enrichments.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace output parquet if it already exists.",
    )
    parser.add_argument(
        "--verbose-http",
        action="store_true",
        help="Show noisy httpx/httpcore request logs for debugging upstream calls.",
    )
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> Path:
    if args.offset < 0:
        raise ValueError(f"--offset must be >= 0. Got {args.offset}.")
    if args.limit is not None and args.limit <= 0:
        raise ValueError(f"--limit must be positive when provided. Got {args.limit}.")
    if args.concurrency <= 0:
        raise ValueError(f"--concurrency must be positive. Got {args.concurrency}.")
    if args.chunk_size <= 0:
        raise ValueError(f"--chunk-size must be positive. Got {args.chunk_size}.")
    if args.progress_every <= 0:
        raise ValueError(f"--progress-every must be positive. Got {args.progress_every}.")
    if not args.input.exists():
        raise FileNotFoundError(f"Input parquet not found at {args.input}.")

    places = pd.read_parquet(args.input)
    if "has_website" not in places.columns:
        raise ValueError(
            f"Input parquet at {args.input} is missing has_website. "
            "Expected the curated dataset output."
        )

    no_website_places = places.loc[~places["has_website"]].copy()
    if args.offset:
        no_website_places = no_website_places.iloc[args.offset:].copy()
    if args.limit is not None:
        no_website_places = no_website_places.head(args.limit).copy()
    if no_website_places.empty:
        raise ValueError(
            "No places without websites remained after filtering. "
            "Refusing to run a no-website enrichment job over 0 rows."
        )

    settings = _settings_for_strategy(args)
    output_path = args.output or _default_output_path(args)
    run_dir = args.run_dir or _default_run_dir(args)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    run_dir.mkdir(parents=True, exist_ok=True)

    if output_path.exists() and not args.overwrite:
        logging.getLogger("enrich_no_website_profiles").info(
            "Final output already exists at %s. Leaving it untouched. "
            "Pass --overwrite to rebuild it.",
            output_path,
        )
        return output_path

    logging.getLogger("enrich_no_website_profiles").info(
        "Prepared %d no-website rows for strategy=%s model=%s concurrency=%d "
        "chunk_size=%d run_dir=%s output=%s.",
        len(no_website_places),
        args.strategy,
        settings.openrouter_model,
        settings.global_concurrency,
        args.chunk_size,
        run_dir,
        output_path,
    )

    async with NoWebsiteProfileClient(settings=settings) as client:
        ensure_run_manifest(
            run_dir=run_dir,
            input_path=args.input,
            backend=_backend_id(args),
            shard_count=1,
            shard_key="fsq_place_id",
            chunk_size=args.chunk_size,
            overwrite=args.overwrite,
        )
        layout = run_layout(
            run_dir=run_dir,
            shard_count=1,
            shard_index=0,
        )
        shard_output_path = await run_chunked_profile_enrichment(
            places=no_website_places,
            client=client,
            layout=layout,
            chunk_size=args.chunk_size,
            shard_key="fsq_place_id",
            overwrite=args.overwrite,
            progress_every=args.progress_every,
        )

    enriched = pd.read_parquet(shard_output_path)
    write_curated_places_dataset(enriched, output_path=output_path, overwrite=args.overwrite)
    return output_path


def _settings_for_strategy(args: argparse.Namespace) -> NoWebsiteProfileSettings:
    settings = NoWebsiteProfileSettings(
        global_concurrency=args.concurrency,
        openrouter_model=args.model,
    )
    if args.strategy == "baseline":
        return replace(
            settings,
            use_maps=False,
            use_brave_web=False,
            use_brave_local=False,
            use_llm=False,
        )
    if args.strategy == "maps":
        return replace(
            settings,
            use_brave_web=False,
            use_brave_local=False,
            use_llm=False,
        )
    if args.strategy == "maps-brave":
        return replace(settings, use_llm=False)
    return settings


def _default_output_path(args: argparse.Namespace) -> Path:
    suffix = _run_slug(args)
    return DEFAULT_OUTPUT_ROOT / "outputs" / f"{args.input.stem}_{suffix}.parquet"


def _default_run_dir(args: argparse.Namespace) -> Path:
    return DEFAULT_OUTPUT_ROOT / "runs" / f"{args.input.stem}_{_run_slug(args)}_chunks-{args.chunk_size}"


def _run_slug(args: argparse.Namespace) -> str:
    parts = ["no_website", args.strategy.replace("-", "_"), _selection_label(args)]
    if args.strategy == "maps-brave-llm":
        parts.append(_slug(args.model))
    return "_".join(parts)


def _backend_id(args: argparse.Namespace) -> str:
    return _run_slug(args)


def _selection_label(args: argparse.Namespace) -> str:
    if args.offset == 0 and args.limit is None:
        return "all"
    if args.offset == 0 and args.limit is not None:
        return f"first{args.limit}"
    if args.limit is None:
        return f"offset{args.offset}_all"
    return f"offset{args.offset}_limit{args.limit}"


def _slug(value: str) -> str:
    safe = "".join(char if char.isalnum() else "_" for char in value.casefold())
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe.strip("_") or "model"


def main() -> int:
    _load_repo_env()
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if not args.verbose_http:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
    log = logging.getLogger("enrich_no_website_profiles")
    try:
        output_path = asyncio.run(_run(args))
    except Exception:
        log.exception("Failed to enrich no-website profiles.")
        return 1

    log.info("No-website profiles available at %s", output_path)
    return 0


def _load_repo_env() -> None:
    """Load repo .env values without requiring the caller to export them."""

    env_path = REPO_ROOT / ".env"
    _load_env_file(env_path)


def _load_env_file(env_path: Path) -> None:
    """Load dotenv-style KEY=value pairs, preserving explicit environment."""

    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key or key in os.environ:
            continue
        os.environ[key] = value


if __name__ == "__main__":
    raise SystemExit(main())
