"""Async local image sync and API export for cached precache plans."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

from back_end.clients.maps import GoogleMapsClient
from back_end.precache.output import DEFAULT_PRECACHE_OUTPUT_PATH, read_precache_output

logger = logging.getLogger(__name__)

DEFAULT_FRONTEND_ROOT = Path("data/precache/frontend")
DEFAULT_FRONTEND_IMAGES_DIR = DEFAULT_FRONTEND_ROOT / "images"
DEFAULT_FRONTEND_IMAGE_MANIFEST_PATH = DEFAULT_FRONTEND_ROOT / "image_assets.parquet"
DEFAULT_FRONTEND_API_OUTPUT_PATH = DEFAULT_FRONTEND_ROOT / "plans_api.parquet"

IMAGE_ASSET_COLUMNS: tuple[str, ...] = (
    "asset_id",
    "photo_name",
    "google_place_id",
    "fsq_place_id",
    "relative_path",
    "mime_type",
    "width_px",
    "height_px",
    "source_width_px",
    "source_height_px",
    "file_size_bytes",
    "content_sha256",
    "status",
    "error",
    "downloaded_at_utc",
    "written_at_utc",
)

API_PLAN_COLUMNS: tuple[str, ...] = (
    "plan_id",
    "template_id",
    "bucket_id",
    "plan_title",
    "bucket_label",
    "hero_image_asset_id",
    "hero_image_relative_path",
    "hero_image_public_url",
    "generated_at_utc",
    "source_written_at_utc",
    "exported_at_utc",
    "api_payload_json",
)

READY_STATUS = "ready"
FAILED_STATUS = "failed"

_UNSAFE_PATH_CHARS_RE = re.compile(r"[^a-z0-9]+")
_JPEG_MAGIC = b"\xff\xd8\xff"
_PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
_GIF87A_MAGIC = b"GIF87a"
_GIF89A_MAGIC = b"GIF89a"
_RIFF_MAGIC = b"RIFF"
_WEBP_MAGIC = b"WEBP"


class PrecacheAssetSyncError(RuntimeError):
    """Raised when the local asset sync/export pipeline cannot proceed safely."""


@dataclass(frozen=True)
class RequiredPhotoAsset:
    """One photo reference that should be resolved into a local file."""

    asset_id: str
    photo_name: str
    google_place_id: str | None
    fsq_place_id: str | None
    source_width_px: int | None
    source_height_px: int | None


@dataclass(frozen=True)
class ImageAssetSyncSummary:
    """One sync/export run summary."""

    requested_asset_count: int
    downloaded_asset_count: int
    reused_asset_count: int
    failed_asset_count: int
    skipped_plan_count: int
    exported_plan_count: int


async def sync_local_precache_assets(
    *,
    maps_client: GoogleMapsClient,
    plans_path: Path | str = DEFAULT_PRECACHE_OUTPUT_PATH,
    assets_dir: Path | str = DEFAULT_FRONTEND_IMAGES_DIR,
    image_manifest_path: Path | str = DEFAULT_FRONTEND_IMAGE_MANIFEST_PATH,
    api_output_path: Path | str = DEFAULT_FRONTEND_API_OUTPUT_PATH,
    public_url_prefix: str | None = None,
    max_concurrency: int = 8,
    max_width_px: int | None = None,
    max_height_px: int | None = None,
) -> ImageAssetSyncSummary:
    """Sync referenced Google photo assets locally and export API-facing plans."""

    if max_concurrency <= 0:
        raise PrecacheAssetSyncError("max_concurrency must be positive.")

    plans_df = _read_precache_plans_with_retries(Path(plans_path))
    requested_assets, skipped_plan_ids = collect_required_photo_assets(plans_df)
    manifest_path = Path(image_manifest_path)
    assets_root = Path(assets_dir)
    api_path = Path(api_output_path)

    manifest_df = read_image_asset_manifest(manifest_path)
    existing_records = _manifest_lookup(manifest_df)
    assets_root.mkdir(parents=True, exist_ok=True)

    downloaded_count = 0
    reused_count = 0
    failed_count = 0
    updated_rows: dict[str, dict[str, Any]] = {}

    async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as download_client:
        semaphore = asyncio.Semaphore(max_concurrency)
        tasks = []
        for asset in requested_assets:
            existing = existing_records.get(asset.asset_id)
            if _is_ready_asset(existing, assets_root=assets_root):
                reused_count += 1
                continue
            tasks.append(
                asyncio.create_task(
                    _download_asset(
                        asset=asset,
                        maps_client=maps_client,
                        download_client=download_client,
                        assets_root=assets_root,
                        semaphore=semaphore,
                        max_width_px=max_width_px,
                        max_height_px=max_height_px,
                    )
                )
            )

        if tasks:
            results = await asyncio.gather(*tasks)
            for row in results:
                updated_rows[str(row["asset_id"])] = row
                if row["status"] == READY_STATUS:
                    downloaded_count += 1
                else:
                    failed_count += 1

    final_manifest = _merge_manifest_rows(
        existing_df=manifest_df,
        updated_rows=updated_rows,
    )
    _write_parquet_atomic(final_manifest, manifest_path)

    exported_df = build_frontend_api_output(
        plans_df=plans_df,
        manifest_df=final_manifest,
        public_url_prefix=public_url_prefix,
    )
    _write_parquet_atomic(exported_df, api_path)

    return ImageAssetSyncSummary(
        requested_asset_count=len(requested_assets),
        downloaded_asset_count=downloaded_count,
        reused_asset_count=reused_count,
        failed_asset_count=failed_count,
        skipped_plan_count=len(skipped_plan_ids),
        exported_plan_count=len(exported_df),
    )


def collect_required_photo_assets(
    plans_df: pd.DataFrame,
) -> tuple[tuple[RequiredPhotoAsset, ...], tuple[str, ...]]:
    """Collect unique primary-photo references from plan card payloads."""

    required_by_id: dict[str, RequiredPhotoAsset] = {}
    skipped_plan_ids: list[str] = []

    for row in plans_df.itertuples(index=False):
        row_map = row._asdict()
        plan_id = _required_text(row_map.get("plan_id"), field_name="plan_id")
        raw_card = row_map.get("card_json")
        if not isinstance(raw_card, str) or not raw_card.strip():
            logger.error("Plan %s is missing card_json; skipping export for that row.", plan_id)
            skipped_plan_ids.append(plan_id)
            continue
        try:
            card = json.loads(raw_card)
        except json.JSONDecodeError as exc:
            logger.error("Plan %s has invalid card_json and will be skipped.", plan_id)
            logger.exception(exc)
            skipped_plan_ids.append(plan_id)
            continue
        stops = card.get("stops")
        if not isinstance(stops, list):
            logger.error("Plan %s card_json has non-list stops; skipping export for that row.", plan_id)
            skipped_plan_ids.append(plan_id)
            continue

        for stop in stops:
            if not isinstance(stop, dict):
                logger.error(
                    "Plan %s contains a non-object stop payload; ignoring that stop.", plan_id
                )
                continue
            primary_photo = stop.get("primary_photo")
            if not isinstance(primary_photo, dict):
                continue
            photo_name = _optional_text(primary_photo.get("name"))
            if photo_name is None:
                logger.error(
                    "Plan %s stop %r contains a primary_photo without a name; ignoring it.",
                    plan_id,
                    stop.get("name"),
                )
                continue
            asset = RequiredPhotoAsset(
                asset_id=_asset_id(photo_name),
                photo_name=photo_name,
                google_place_id=_optional_text(stop.get("google_place_id")),
                fsq_place_id=_optional_text(stop.get("fsq_place_id")),
                source_width_px=_optional_int(primary_photo.get("width_px")),
                source_height_px=_optional_int(primary_photo.get("height_px")),
            )
            existing = required_by_id.get(asset.asset_id)
            if existing is None:
                required_by_id[asset.asset_id] = asset
                continue
            if existing.photo_name != asset.photo_name:
                logger.error(
                    "Asset id collision between photo names %r and %r.",
                    existing.photo_name,
                    asset.photo_name,
                )
                raise PrecacheAssetSyncError(
                    "Different Google photo names collapsed to the same asset id."
                )
            required_by_id[asset.asset_id] = _merge_required_assets(existing, asset)

    ordered = tuple(sorted(required_by_id.values(), key=lambda item: item.asset_id))
    return ordered, tuple(skipped_plan_ids)


def read_image_asset_manifest(path: Path | str) -> pd.DataFrame:
    """Read the durable local image manifest, or return an empty one."""

    manifest_path = Path(path)
    if not manifest_path.exists():
        return pd.DataFrame(columns=IMAGE_ASSET_COLUMNS)
    _require_parquet_path(manifest_path, label="image asset manifest")
    df = pd.read_parquet(manifest_path)
    _validate_columns(df=df, expected=IMAGE_ASSET_COLUMNS, label="image asset manifest")
    return df.loc[:, IMAGE_ASSET_COLUMNS]


def build_frontend_api_output(
    *,
    plans_df: pd.DataFrame,
    manifest_df: pd.DataFrame,
    public_url_prefix: str | None = None,
) -> pd.DataFrame:
    """Build an API-facing snapshot that points at local image assets."""

    asset_lookup = _ready_asset_lookup(manifest_df=manifest_df, public_url_prefix=public_url_prefix)
    exported_rows: list[dict[str, Any]] = []
    export_time = _timestamp_now()

    for row in plans_df.itertuples(index=False):
        row_map = row._asdict()
        plan_id = _optional_text(row_map.get("plan_id"))
        raw_card = row_map.get("card_json")
        if plan_id is None or not isinstance(raw_card, str) or not raw_card.strip():
            continue
        try:
            payload = json.loads(raw_card)
        except json.JSONDecodeError:
            continue
        stops = payload.get("stops")
        if not isinstance(stops, list):
            continue

        payload["plan_id"] = plan_id
        payload["generated_at_utc"] = _optional_text(row_map.get("generated_at_utc"))
        payload["source_written_at_utc"] = _optional_text(row_map.get("written_at_utc"))
        payload["template_duration_hours"] = _optional_float(row_map.get("template_duration_hours"))

        hero_image: dict[str, Any] | None = None
        for stop in stops:
            if not isinstance(stop, dict):
                continue
            primary_photo = stop.get("primary_photo")
            asset_payload = None
            if isinstance(primary_photo, dict):
                photo_name = _optional_text(primary_photo.get("name"))
                if photo_name is not None:
                    asset_payload = asset_lookup.get(_asset_id(photo_name))
            stop["primary_image"] = asset_payload
            if hero_image is None and asset_payload is not None:
                hero_image = asset_payload

        payload["hero_image"] = hero_image
        payload["hero_image_url"] = None if hero_image is None else hero_image.get("public_url")

        exported_rows.append(
            {
                "plan_id": plan_id,
                "template_id": _optional_text(row_map.get("template_id")),
                "bucket_id": _optional_text(row_map.get("bucket_id")),
                "plan_title": _optional_text(row_map.get("plan_title")),
                "bucket_label": _optional_text(row_map.get("bucket_label")),
                "hero_image_asset_id": None if hero_image is None else hero_image.get("asset_id"),
                "hero_image_relative_path": None
                if hero_image is None
                else hero_image.get("relative_path"),
                "hero_image_public_url": None
                if hero_image is None
                else hero_image.get("public_url"),
                "generated_at_utc": _optional_text(row_map.get("generated_at_utc")),
                "source_written_at_utc": _optional_text(row_map.get("written_at_utc")),
                "exported_at_utc": export_time,
                "api_payload_json": _json_dumps(payload),
            }
        )

    if not exported_rows:
        return pd.DataFrame(columns=API_PLAN_COLUMNS)
    df = pd.DataFrame(exported_rows)
    return df.loc[:, API_PLAN_COLUMNS]


async def _download_asset(
    *,
    asset: RequiredPhotoAsset,
    maps_client: GoogleMapsClient,
    download_client: httpx.AsyncClient,
    assets_root: Path,
    semaphore: asyncio.Semaphore,
    max_width_px: int | None,
    max_height_px: int | None,
) -> dict[str, Any]:
    async with semaphore:
        try:
            media = await maps_client.get_photo_media(
                asset.photo_name,
                max_width_px=max_width_px,
                max_height_px=max_height_px,
            )
            response = await download_client.get(media.photo_uri)
            response.raise_for_status()
            image_bytes = response.content
            if not image_bytes:
                raise PrecacheAssetSyncError(
                    f"Resolved photo {asset.photo_name!r} returned an empty image body."
                )
            extension, mime_type = _resolve_image_format(
                content_type=response.headers.get("content-type"),
                body=image_bytes,
            )
            relative_path = _relative_asset_path(asset=asset, extension=extension)
            final_path = assets_root / relative_path
            final_path.parent.mkdir(parents=True, exist_ok=True)
            await _atomic_write_bytes(final_path, image_bytes)
            downloaded_at = _timestamp_now()
            logger.info(
                "Downloaded asset %s to %s (%d bytes).",
                asset.asset_id,
                final_path,
                len(image_bytes),
            )
            return {
                "asset_id": asset.asset_id,
                "photo_name": asset.photo_name,
                "google_place_id": asset.google_place_id,
                "fsq_place_id": asset.fsq_place_id,
                "relative_path": str(relative_path.as_posix()),
                "mime_type": mime_type,
                "width_px": _optional_int(asset.source_width_px),
                "height_px": _optional_int(asset.source_height_px),
                "source_width_px": _optional_int(asset.source_width_px),
                "source_height_px": _optional_int(asset.source_height_px),
                "file_size_bytes": len(image_bytes),
                "content_sha256": hashlib.sha256(image_bytes).hexdigest(),
                "status": READY_STATUS,
                "error": None,
                "downloaded_at_utc": downloaded_at,
                "written_at_utc": downloaded_at,
            }
        except Exception as exc:
            logger.exception("Failed to download local asset for photo %s.", asset.photo_name)
            failure_time = _timestamp_now()
            return {
                "asset_id": asset.asset_id,
                "photo_name": asset.photo_name,
                "google_place_id": asset.google_place_id,
                "fsq_place_id": asset.fsq_place_id,
                "relative_path": None,
                "mime_type": None,
                "width_px": _optional_int(asset.source_width_px),
                "height_px": _optional_int(asset.source_height_px),
                "source_width_px": _optional_int(asset.source_width_px),
                "source_height_px": _optional_int(asset.source_height_px),
                "file_size_bytes": None,
                "content_sha256": None,
                "status": FAILED_STATUS,
                "error": f"{type(exc).__name__}: {exc}",
                "downloaded_at_utc": None,
                "written_at_utc": failure_time,
            }


def _merge_manifest_rows(
    *,
    existing_df: pd.DataFrame,
    updated_rows: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    rows_by_id = {
        str(row["asset_id"]): {column: row.get(column) for column in IMAGE_ASSET_COLUMNS}
        for row in existing_df.to_dict(orient="records")
    }
    rows_by_id.update(updated_rows)
    if not rows_by_id:
        return pd.DataFrame(columns=IMAGE_ASSET_COLUMNS)
    merged = pd.DataFrame(rows_by_id.values())
    merged = merged.loc[:, IMAGE_ASSET_COLUMNS]
    merged = merged.sort_values(["status", "asset_id"], kind="stable").reset_index(drop=True)
    return merged


def _manifest_lookup(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    return {
        str(row["asset_id"]): row
        for row in df.to_dict(orient="records")
        if _optional_text(row.get("asset_id")) is not None
    }


def _ready_asset_lookup(
    *,
    manifest_df: pd.DataFrame,
    public_url_prefix: str | None,
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    prefix = None
    if public_url_prefix is not None:
        clean_prefix = public_url_prefix.strip()
        prefix = clean_prefix.rstrip("/") if clean_prefix else ""
    for row in manifest_df.to_dict(orient="records"):
        if row.get("status") != READY_STATUS:
            continue
        asset_id = _optional_text(row.get("asset_id"))
        relative_path = _optional_text(row.get("relative_path"))
        if asset_id is None or relative_path is None:
            continue
        public_url = None
        if prefix is not None:
            public_url = f"{prefix}/{relative_path.lstrip('/')}" if prefix else relative_path
        out[asset_id] = {
            "asset_id": asset_id,
            "relative_path": relative_path,
            "public_url": public_url,
            "mime_type": _optional_text(row.get("mime_type")),
            "width_px": _optional_int(row.get("width_px")),
            "height_px": _optional_int(row.get("height_px")),
            "google_place_id": _optional_text(row.get("google_place_id")),
            "fsq_place_id": _optional_text(row.get("fsq_place_id")),
        }
    return out


def _is_ready_asset(existing_row: dict[str, Any] | None, *, assets_root: Path) -> bool:
    if existing_row is None:
        return False
    if existing_row.get("status") != READY_STATUS:
        return False
    relative_path = _optional_text(existing_row.get("relative_path"))
    if relative_path is None:
        logger.error("Manifest row %s is marked ready but has no relative_path.", existing_row)
        return False
    final_path = assets_root / relative_path
    if not final_path.exists():
        logger.error(
            "Manifest row asset_id=%s points to missing file %s; forcing redownload.",
            existing_row.get("asset_id"),
            final_path,
        )
        return False
    return True


def _merge_required_assets(
    first: RequiredPhotoAsset,
    second: RequiredPhotoAsset,
) -> RequiredPhotoAsset:
    return RequiredPhotoAsset(
        asset_id=first.asset_id,
        photo_name=first.photo_name,
        google_place_id=first.google_place_id or second.google_place_id,
        fsq_place_id=first.fsq_place_id or second.fsq_place_id,
        source_width_px=first.source_width_px or second.source_width_px,
        source_height_px=first.source_height_px or second.source_height_px,
    )


def _asset_id(photo_name: str) -> str:
    clean = _required_text(photo_name, field_name="photo_name")
    return hashlib.sha256(clean.encode("utf-8")).hexdigest()


def _relative_asset_path(*, asset: RequiredPhotoAsset, extension: str) -> Path:
    bucket = _safe_path_segment(asset.google_place_id or "unknown-place")
    return Path(bucket) / f"{asset.asset_id}{extension}"


def _safe_path_segment(value: str) -> str:
    lowered = value.strip().lower()
    lowered = _UNSAFE_PATH_CHARS_RE.sub("-", lowered).strip("-")
    return lowered or "unknown"


def _resolve_image_format(*, content_type: str | None, body: bytes) -> tuple[str, str]:
    normalized = (content_type or "").split(";", 1)[0].strip().lower()
    direct_map = {
        "image/jpeg": (".jpg", "image/jpeg"),
        "image/png": (".png", "image/png"),
        "image/webp": (".webp", "image/webp"),
        "image/gif": (".gif", "image/gif"),
    }
    if normalized in direct_map:
        return direct_map[normalized]
    if body.startswith(_JPEG_MAGIC):
        logger.warning("Missing/unknown content-type %r; inferred JPEG from magic bytes.", content_type)
        return ".jpg", "image/jpeg"
    if body.startswith(_PNG_MAGIC):
        logger.warning("Missing/unknown content-type %r; inferred PNG from magic bytes.", content_type)
        return ".png", "image/png"
    if body.startswith(_GIF87A_MAGIC) or body.startswith(_GIF89A_MAGIC):
        logger.warning("Missing/unknown content-type %r; inferred GIF from magic bytes.", content_type)
        return ".gif", "image/gif"
    if len(body) >= 12 and body.startswith(_RIFF_MAGIC) and body[8:12] == _WEBP_MAGIC:
        logger.warning("Missing/unknown content-type %r; inferred WEBP from magic bytes.", content_type)
        return ".webp", "image/webp"
    raise PrecacheAssetSyncError(
        f"Unsupported image content-type {content_type!r}; cannot determine file format safely."
    )


async def _atomic_write_bytes(path: Path, content: bytes) -> None:
    temp_path = path.with_name(f"{path.name}.tmp-{os.getpid()}-{int(datetime.now(UTC).timestamp())}")

    def _write() -> None:
        temp_path.write_bytes(content)
        temp_path.replace(path)

    await asyncio.to_thread(_write)


def _write_parquet_atomic(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp-{os.getpid()}")
    df.to_parquet(temp_path, index=False)
    temp_path.replace(path)
    logger.info("Wrote %d rows to %s.", len(df), path)


def _read_precache_plans_with_retries(path: Path, *, retries: int = 3, delay_seconds: float = 0.5) -> pd.DataFrame:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return read_precache_output(path)
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Attempt %d/%d to read %s failed: %s",
                attempt,
                retries,
                path,
                exc,
            )
            if attempt == retries:
                break
            time_to_sleep = delay_seconds * attempt
            logger.warning("Retrying precache parquet read in %.2f seconds.", time_to_sleep)
            time.sleep(time_to_sleep)
    assert last_error is not None
    logger.error("Failed to read precache plans from %s after %d attempts.", path, retries)
    raise PrecacheAssetSyncError(
        f"Could not read precache plans parquet at {path} after {retries} attempts."
    ) from last_error


def _validate_columns(*, df: pd.DataFrame, expected: tuple[str, ...], label: str) -> None:
    actual = tuple(str(column) for column in df.columns)
    if actual == expected:
        return
    missing = [column for column in expected if column not in actual]
    extra = [column for column in actual if column not in expected]
    logger.error("%s has invalid schema missing=%s extra=%s.", label, missing, extra)
    raise PrecacheAssetSyncError(
        f"{label} has invalid schema. Missing={missing} extra={extra}."
    )


def _require_parquet_path(path: Path, *, label: str) -> None:
    if path.suffix.lower() != ".parquet":
        raise PrecacheAssetSyncError(f"{label} must be a .parquet file, got {path}.")


def _required_text(value: Any, *, field_name: str) -> str:
    text = _optional_text(value)
    if text is None:
        raise PrecacheAssetSyncError(f"{field_name} must be a non-empty string.")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
