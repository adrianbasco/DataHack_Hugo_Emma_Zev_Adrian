"""CLI wrapper around the deterministic parquet query tool."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from back_end.query.errors import DateNightQueryError
from back_end.query.models import GenerateDatesRequest
from back_end.query.service import query_places


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Query date-worthy places from data/au_places.parquet."
    )
    parser.add_argument("--location", required=True, help="Postcode or 'locality, region'.")
    parser.add_argument(
        "--vibe",
        dest="vibes",
        action="append",
        required=True,
        help="Repeatable vibe filter, e.g. --vibe foodie --vibe romantic",
    )
    parser.add_argument("--radius-km", type=float, default=None, help="Search radius in km.")
    parser.add_argument("--budget", default=None, help="Optional budget band: $, $$, $$$, $$$$.")
    parser.add_argument(
        "--transport-mode",
        default="driving",
        help="Transport mode: walking, public_transport, driving.",
    )
    parser.add_argument("--party-size", type=int, default=2, help="Party size.")
    parser.add_argument(
        "--max-candidates",
        type=int,
        default=None,
        help="Upper bound on returned places.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )
    return parser


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    args = build_parser().parse_args()
    request = GenerateDatesRequest(
        location=args.location,
        vibes=tuple(args.vibes),
        radius_km=args.radius_km,
        budget=args.budget,
        transport_mode=args.transport_mode,
        party_size=args.party_size,
        max_candidates=args.max_candidates,
    )
    try:
        result = query_places(request)
    except DateNightQueryError as exc:
        logging.getLogger(__name__).error("Query failed: %s", exc)
        return 1

    json.dump(
        result.to_dict(),
        sys.stdout,
        indent=2 if args.pretty else None,
        ensure_ascii=True,
    )
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
