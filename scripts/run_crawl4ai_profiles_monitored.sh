#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
source .venv/bin/activate

CHUNK_SIZE="${CHUNK_SIZE:-100}"
RUN_DIR="${RUN_DIR:-data/website_profile_runs/sydney_crawl4ai_local_chunks-${CHUNK_SIZE}_watchdog15}"
MAX_PAGES_PER_SITE="${MAX_PAGES_PER_SITE:-5}"
SEMAPHORE_COUNT="${SEMAPHORE_COUNT:-32}"
MAX_SESSION_PERMIT="${MAX_SESSION_PERMIT:-32}"
CRAWL_WATCHDOG_EXTRA_SECONDS="${CRAWL_WATCHDOG_EXTRA_SECONDS:-3}"
CRAWL_WATCHDOG_MAX_SECONDS="${CRAWL_WATCHDOG_MAX_SECONDS:-15}"
BATCH_PROGRESS_INTERVAL_SECONDS="${BATCH_PROGRESS_INTERVAL_SECONDS:-10}"

python scripts/enrich_website_profiles.py \
  --backend crawl4ai \
  --chunk-size "$CHUNK_SIZE" \
  --max-pages-per-site "$MAX_PAGES_PER_SITE" \
  --semaphore-count "$SEMAPHORE_COUNT" \
  --max-session-permit "$MAX_SESSION_PERMIT" \
  --crawl-watchdog-extra-seconds "$CRAWL_WATCHDOG_EXTRA_SECONDS" \
  --crawl-watchdog-max-seconds "$CRAWL_WATCHDOG_MAX_SECONDS" \
  --batch-progress-interval-seconds "$BATCH_PROGRESS_INTERVAL_SECONDS" \
  --run-dir "$RUN_DIR" \
  "$@"
