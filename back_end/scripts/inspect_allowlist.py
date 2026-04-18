"""Diagnostic: build the allowlist and report coverage against au_places.parquet.

Run from the repo root:

    .venv/bin/python -m back_end.scripts.inspect_allowlist

Prints per-vibe expanded category counts, how many Australian places survive
each vibe filter, and the top category labels within each filtered subset.
Useful for spot-checking that the curated seeds cover the right things.
"""

from __future__ import annotations

import logging
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from back_end.date_night import VIBES, filter_places_by_vibes, load_allowlist  # noqa: E402

PLACES_PATH = REPO_ROOT / "data" / "au_places.parquet"

TOP_N = 15


def _top_labels(places: pd.DataFrame, n: int) -> list[tuple[str, int]]:
    """Return the top-n primary-category labels among filtered places."""
    counter: Counter[str] = Counter()
    for labels in places["fsq_category_labels"].dropna():
        if len(labels) == 0:
            continue
        counter[labels[0]] += 1
    return counter.most_common(n)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("inspect_allowlist")

    log.info("Loading allowlist from config/date_categories.yaml")
    allowlist = load_allowlist()

    log.info("Loading places from %s", PLACES_PATH)
    if not PLACES_PATH.exists():
        log.error("Places parquet not found at %s", PLACES_PATH)
        return 1
    places = pd.read_parquet(
        PLACES_PATH,
        columns=[
            "fsq_place_id",
            "name",
            "locality",
            "region",
            "fsq_category_ids",
            "fsq_category_labels",
        ],
    )
    total = len(places)
    log.info("Loaded %d places", total)

    print()
    print("=" * 78)
    print(f"Allowlist summary — {len(allowlist.master)} unique category IDs across {len(VIBES)} vibes")
    print("=" * 78)
    for vibe in VIBES:
        ids = allowlist.by_vibe[vibe]
        print(f"  {vibe:<10s} : {len(ids):>5d} category IDs")

    print()
    print("=" * 78)
    print(f"Places coverage (starting from {total:,} AU places)")
    print("=" * 78)

    master_filtered = filter_places_by_vibes(places, list(VIBES), allowlist)
    master_pct = 100.0 * len(master_filtered) / total if total else 0.0
    print(f"  master (any vibe)    : {len(master_filtered):>8,} places  ({master_pct:5.2f}%)")

    for vibe in VIBES:
        subset = filter_places_by_vibes(places, [vibe], allowlist)
        pct = 100.0 * len(subset) / total if total else 0.0
        print(f"  {vibe:<20s} : {len(subset):>8,} places  ({pct:5.2f}%)")

    print()
    print("=" * 78)
    print(f"Top {TOP_N} primary category labels within the MASTER filtered set")
    print("=" * 78)
    for label, count in _top_labels(master_filtered, TOP_N):
        print(f"  {count:>7,}  {label}")

    print()
    print("=" * 78)
    print(f"Top {TOP_N} primary category labels PRESENT IN au_places BUT EXCLUDED from master")
    print(" (sanity check: anything here that looks date-worthy is a curation gap)")
    print("=" * 78)
    excluded = places.loc[~places.index.isin(master_filtered.index)]
    for label, count in _top_labels(excluded, TOP_N):
        print(f"  {count:>7,}  {label}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
