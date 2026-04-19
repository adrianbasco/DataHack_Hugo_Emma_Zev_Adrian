"""Build a derived RAG corpus from valid scraped Sydney place profiles.

Run from the repo root:

    source .venv/bin/activate
    python scripts/build_rag_corpus.py
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from back_end.rag.corpus import (  # noqa: E402
    build_and_write_rag_corpus,
    discover_profile_chunk_paths,
)
from back_end.rag.settings import load_rag_settings, make_run_id  # noqa: E402


def parse_args() -> argparse.Namespace:
    settings = load_rag_settings()
    parser = argparse.ArgumentParser(
        description=(
            "Build a read-only-derived RAG corpus from valid crawl4ai shard chunks. "
            "Existing pipeline data is only read, never modified."
        )
    )
    parser.add_argument(
        "--candidates",
        type=Path,
        default=settings.candidate_parquet_path,
        help="Sydney/date-worthy candidate parquet to join against.",
    )
    parser.add_argument(
        "--profile-runs-root",
        type=Path,
        default=settings.profile_runs_root,
        help="Root containing website_profile_runs chunk parquets.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Derived RAG run directory. Defaults to data/rag/runs/<run-id>.",
    )
    parser.add_argument(
        "--run-id",
        default=make_run_id("rag-corpus"),
        help="Run id stored in corpus rows and used for the default output directory.",
    )
    parser.add_argument(
        "--min-profile-quality-score",
        type=int,
        default=settings.min_profile_quality_score,
        help="Minimum crawl4ai_quality_score for semantic inclusion.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite files in the derived RAG output directory.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("build_rag_corpus")
    settings = load_rag_settings()
    output_dir = args.output_dir or settings.rag_runs_root / args.run_id

    try:
        result = build_and_write_rag_corpus(
            settings=settings,
            candidate_parquet_path=args.candidates,
            profile_chunk_paths=discover_profile_chunk_paths(args.profile_runs_root),
            output_dir=output_dir,
            run_id=args.run_id,
            min_profile_quality_score=args.min_profile_quality_score,
            overwrite=args.overwrite,
        )
    except Exception:
        log.exception("Failed to build RAG corpus.")
        return 1

    log.info("RAG corpus build completed: %s", result.documents_path)
    log.info("RAG manifest written: %s", result.manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
