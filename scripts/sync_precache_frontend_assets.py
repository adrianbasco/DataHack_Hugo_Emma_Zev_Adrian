"""Download local plan images and export an API-facing precache snapshot."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from back_end.clients.maps import GoogleMapsClient  # noqa: E402
from back_end.precache.asset_sync import (  # noqa: E402
    DEFAULT_FRONTEND_API_OUTPUT_PATH,
    DEFAULT_FRONTEND_IMAGE_MANIFEST_PATH,
    DEFAULT_FRONTEND_IMAGES_DIR,
    sync_local_precache_assets,
)
from back_end.precache.output import DEFAULT_PRECACHE_OUTPUT_PATH  # noqa: E402
from scripts.run_precache import _build_maps_settings, _load_repo_env  # noqa: E402

logger = logging.getLogger("sync_precache_frontend_assets")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download local copies of Google photo assets referenced by cached "
            "precache plans and export an API-friendly parquet snapshot."
        )
    )
    parser.add_argument(
        "--plans-path",
        type=Path,
        default=REPO_ROOT / DEFAULT_PRECACHE_OUTPUT_PATH,
        help="Source cached plans parquet.",
    )
    parser.add_argument(
        "--assets-dir",
        type=Path,
        default=REPO_ROOT / DEFAULT_FRONTEND_IMAGES_DIR,
        help="Directory where local image files will be stored.",
    )
    parser.add_argument(
        "--image-manifest-path",
        type=Path,
        default=REPO_ROOT / DEFAULT_FRONTEND_IMAGE_MANIFEST_PATH,
        help="Parquet manifest of downloaded image assets.",
    )
    parser.add_argument(
        "--api-output-path",
        type=Path,
        default=REPO_ROOT / DEFAULT_FRONTEND_API_OUTPUT_PATH,
        help="Exported API-facing plans parquet.",
    )
    parser.add_argument(
        "--public-url-prefix",
        default=None,
        help=(
            "Optional URL prefix to embed in exported payloads, for example "
            "'/static/precache-images'."
        ),
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=8,
        help="Max concurrent image downloads.",
    )
    parser.add_argument(
        "--max-width-px",
        type=int,
        default=None,
        help="Optional override for resolved Google photo width.",
    )
    parser.add_argument(
        "--max-height-px",
        type=int,
        default=None,
        help="Optional override for resolved Google photo height.",
    )
    parser.add_argument(
        "--min-name-similarity",
        type=float,
        default=0.72,
        help="Google Maps name-match threshold reused from the precache tooling.",
    )
    parser.add_argument(
        "--max-match-distance-meters",
        type=float,
        default=350.0,
        help="Google Maps max match distance reused from the precache tooling.",
    )
    parser.add_argument(
        "--min-place-rating",
        type=float,
        default=3.5,
        help="Google Maps minimum rating reused from the precache tooling.",
    )
    parser.add_argument(
        "--min-user-rating-count",
        type=int,
        default=0,
        help="Google Maps minimum rating-count reused from the precache tooling.",
    )
    return parser.parse_args()


async def async_main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args()
    _load_repo_env()

    maps_settings = _build_maps_settings(args)
    async with GoogleMapsClient(maps_settings) as maps_client:
        summary = await sync_local_precache_assets(
            maps_client=maps_client,
            plans_path=args.plans_path,
            assets_dir=args.assets_dir,
            image_manifest_path=args.image_manifest_path,
            api_output_path=args.api_output_path,
            public_url_prefix=args.public_url_prefix,
            max_concurrency=args.max_concurrency,
            max_width_px=args.max_width_px,
            max_height_px=args.max_height_px,
        )

    print("=" * 80)
    print("PRECACHE FRONTEND ASSET SYNC")
    print("=" * 80)
    print(f"requested_asset_count : {summary.requested_asset_count}")
    print(f"downloaded_asset_count: {summary.downloaded_asset_count}")
    print(f"reused_asset_count    : {summary.reused_asset_count}")
    print(f"failed_asset_count    : {summary.failed_asset_count}")
    print(f"skipped_plan_count    : {summary.skipped_plan_count}")
    print(f"exported_plan_count   : {summary.exported_plan_count}")
    print(f"assets_dir            : {args.assets_dir}")
    print(f"image_manifest_path   : {args.image_manifest_path}")
    print(f"api_output_path       : {args.api_output_path}")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(async_main()))
