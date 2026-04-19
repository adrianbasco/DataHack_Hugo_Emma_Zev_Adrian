from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from back_end.precache.asset_sync import (
    API_PLAN_COLUMNS,
    IMAGE_ASSET_COLUMNS,
    READY_STATUS,
    build_frontend_api_output,
    collect_required_photo_assets,
    read_image_asset_manifest,
    sync_local_precache_assets,
)
from back_end.precache.output import OUTPUT_COLUMNS


class PrecacheAssetSyncTests(unittest.TestCase):
    def test_collect_required_photo_assets_dedupes_and_skips_invalid_rows(self) -> None:
        plans_df = _plans_dataframe(
            [
                _plan_row(
                    plan_id="plan-1",
                    card_payload=_card_payload(
                        photo_name="places/google-1/photos/photo-a",
                        google_place_id="google-1",
                        fsq_place_id="fsq-1",
                    ),
                ),
                _plan_row(
                    plan_id="plan-2",
                    card_payload=_card_payload(
                        photo_name="places/google-1/photos/photo-a",
                        google_place_id="google-1",
                        fsq_place_id="fsq-1",
                    ),
                ),
                _plan_row(plan_id="plan-3", card_json=None),
            ]
        )

        assets, skipped = collect_required_photo_assets(plans_df)

        self.assertEqual(1, len(assets))
        self.assertEqual("places/google-1/photos/photo-a", assets[0].photo_name)
        self.assertEqual(("plan-3",), skipped)

    def test_build_frontend_api_output_attaches_local_asset_payloads(self) -> None:
        plans_df = _plans_dataframe(
            [
                _plan_row(
                    plan_id="plan-1",
                    card_payload=_card_payload(
                        photo_name="places/google-1/photos/photo-a",
                        google_place_id="google-1",
                        fsq_place_id="fsq-1",
                    ),
                )
            ]
        )
        manifest_df = pd.DataFrame(
            [
                {
                    "asset_id": _asset_id("places/google-1/photos/photo-a"),
                    "photo_name": "places/google-1/photos/photo-a",
                    "google_place_id": "google-1",
                    "fsq_place_id": "fsq-1",
                    "relative_path": "google-1/asset-a.jpg",
                    "mime_type": "image/jpeg",
                    "width_px": 1200,
                    "height_px": 900,
                    "source_width_px": 1200,
                    "source_height_px": 900,
                    "file_size_bytes": 12345,
                    "content_sha256": "abc",
                    "status": READY_STATUS,
                    "error": None,
                    "downloaded_at_utc": "2026-04-19T07:00:00Z",
                    "written_at_utc": "2026-04-19T07:00:00Z",
                }
            ],
            columns=IMAGE_ASSET_COLUMNS,
        )

        exported = build_frontend_api_output(
            plans_df=plans_df,
            manifest_df=manifest_df,
            public_url_prefix="/static/precache-images",
        )

        self.assertEqual(list(API_PLAN_COLUMNS), list(exported.columns))
        self.assertEqual("google-1/asset-a.jpg", exported.iloc[0]["hero_image_relative_path"])
        self.assertEqual(
            "/static/precache-images/google-1/asset-a.jpg",
            exported.iloc[0]["hero_image_public_url"],
        )
        payload = json.loads(exported.iloc[0]["api_payload_json"])
        self.assertEqual(
            "/static/precache-images/google-1/asset-a.jpg",
            payload["hero_image_url"],
        )
        self.assertEqual(
            "google-1/asset-a.jpg",
            payload["stops"][0]["primary_image"]["relative_path"],
        )


class PrecacheAssetSyncIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_sync_local_precache_assets_redownloads_missing_ready_files_and_exports_snapshot(
        self,
    ) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addAsyncCleanup(_async_cleanup_temp_dir, temp_dir)
        root = Path(temp_dir.name)
        plans_path = root / "plans.parquet"
        assets_dir = root / "frontend" / "images"
        manifest_path = root / "frontend" / "image_assets.parquet"
        api_output_path = root / "frontend" / "plans_api.parquet"

        plans_df = _plans_dataframe(
            [
                _plan_row(
                    plan_id="plan-1",
                    card_payload=_card_payload(
                        photo_name="places/google-1/photos/photo-a",
                        google_place_id="google-1",
                        fsq_place_id="fsq-1",
                    ),
                ),
                _plan_row(
                    plan_id="plan-2",
                    card_payload=_card_payload(
                        photo_name="places/google-2/photos/photo-b",
                        google_place_id="google-2",
                        fsq_place_id="fsq-2",
                    ),
                ),
            ]
        )
        plans_df.to_parquet(plans_path, index=False)

        existing_path = assets_dir / "google-1" / "asset-a.jpg"
        existing_path.parent.mkdir(parents=True, exist_ok=True)
        existing_path.write_bytes(b"existing-image")
        manifest_df = pd.DataFrame(
            [
                {
                    "asset_id": _asset_id("places/google-1/photos/photo-a"),
                    "photo_name": "places/google-1/photos/photo-a",
                    "google_place_id": "google-1",
                    "fsq_place_id": "fsq-1",
                    "relative_path": "google-1/asset-a.jpg",
                    "mime_type": "image/jpeg",
                    "width_px": 1200,
                    "height_px": 900,
                    "source_width_px": 1200,
                    "source_height_px": 900,
                    "file_size_bytes": 12,
                    "content_sha256": "ready",
                    "status": READY_STATUS,
                    "error": None,
                    "downloaded_at_utc": "2026-04-19T07:00:00Z",
                    "written_at_utc": "2026-04-19T07:00:00Z",
                },
                {
                    "asset_id": _asset_id("places/google-2/photos/photo-b"),
                    "photo_name": "places/google-2/photos/photo-b",
                    "google_place_id": "google-2",
                    "fsq_place_id": "fsq-2",
                    "relative_path": "google-2/asset-b.jpg",
                    "mime_type": "image/jpeg",
                    "width_px": 1200,
                    "height_px": 900,
                    "source_width_px": 1200,
                    "source_height_px": 900,
                    "file_size_bytes": 12,
                    "content_sha256": "stale",
                    "status": READY_STATUS,
                    "error": None,
                    "downloaded_at_utc": "2026-04-19T07:00:00Z",
                    "written_at_utc": "2026-04-19T07:00:00Z",
                },
            ],
            columns=IMAGE_ASSET_COLUMNS,
        )
        manifest_df.to_parquet(manifest_path, index=False)

        fake_download_row = {
            "asset_id": _asset_id("places/google-2/photos/photo-b"),
            "photo_name": "places/google-2/photos/photo-b",
            "google_place_id": "google-2",
            "fsq_place_id": "fsq-2",
            "relative_path": "google-2/asset-b.jpg",
            "mime_type": "image/jpeg",
            "width_px": 1200,
            "height_px": 900,
            "source_width_px": 1200,
            "source_height_px": 900,
            "file_size_bytes": 22,
            "content_sha256": "fresh",
            "status": READY_STATUS,
            "error": None,
            "downloaded_at_utc": "2026-04-19T07:10:00Z",
            "written_at_utc": "2026-04-19T07:10:00Z",
        }

        class _FakeAsyncClient:
            async def __aenter__(self) -> "_FakeAsyncClient":
                return self

            async def __aexit__(self, exc_type, exc, tb) -> None:
                return None

        with (
            mock.patch(
                "back_end.precache.asset_sync.httpx.AsyncClient",
                return_value=_FakeAsyncClient(),
            ),
            mock.patch(
                "back_end.precache.asset_sync._download_asset",
                new=mock.AsyncMock(return_value=fake_download_row),
            ) as download_mock,
        ):
            summary = await sync_local_precache_assets(
                maps_client=object(),  # type: ignore[arg-type]
                plans_path=plans_path,
                assets_dir=assets_dir,
                image_manifest_path=manifest_path,
                api_output_path=api_output_path,
                public_url_prefix="/static/precache-images",
                max_concurrency=4,
            )

        self.assertEqual(2, summary.requested_asset_count)
        self.assertEqual(1, summary.reused_asset_count)
        self.assertEqual(1, summary.downloaded_asset_count)
        self.assertEqual(0, summary.failed_asset_count)
        self.assertEqual(2, summary.exported_plan_count)
        download_mock.assert_awaited_once()

        written_manifest = read_image_asset_manifest(manifest_path)
        self.assertEqual(2, len(written_manifest))
        written_api = pd.read_parquet(api_output_path)
        self.assertEqual(2, len(written_api))
        payloads = [json.loads(value) for value in written_api["api_payload_json"].tolist()]
        self.assertEqual(
            "/static/precache-images/google-1/asset-a.jpg",
            payloads[0]["hero_image_url"],
        )
        self.assertEqual(
            "/static/precache-images/google-2/asset-b.jpg",
            payloads[1]["hero_image_url"],
        )


def _plans_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


def _plan_row(
    *,
    plan_id: str,
    card_payload: dict[str, object] | None = None,
    card_json: str | None = None,
) -> dict[str, object]:
    resolved_card_json = card_json
    if resolved_card_json is None and card_payload is not None:
        resolved_card_json = json.dumps(card_payload, sort_keys=True)
    row: dict[str, object] = {column: None for column in OUTPUT_COLUMNS}
    row.update(
        {
            "plan_id": plan_id,
            "bucket_id": "sydney_cbd",
            "bucket_label": "Sydney CBD",
            "bucket_latitude": -33.86,
            "bucket_longitude": 151.20,
            "bucket_radius_km": 2.5,
            "bucket_transport_mode": "WALK",
            "bucket_tags_json": '["dense"]',
            "bucket_metadata_json": '{"label":"Sydney CBD"}',
            "template_id": "drinks_dinner_dessert",
            "template_title": "Drinks, dinner, dessert",
            "vibe": '["romantic"]',
            "time_of_day": "evening",
            "weather_sensitive": False,
            "template_duration_hours": 3.0,
            "template_description": "Start with drinks and end with dessert.",
            "template_metadata_json": '{"id":"drinks_dinner_dessert"}',
            "plan_title": f"Plan {plan_id}",
            "plan_hook": "A Maps-verified date night plan.",
            "plan_time_iso": "2026-04-25T19:00:00+10:00",
            "stops_json": "[]",
            "search_text": "search text",
            "card_json": resolved_card_json,
            "fsq_place_ids_sorted": '["fsq-1"]',
            "fsq_place_id_count": 1,
            "verification_json": "{}",
            "generated_at_utc": "2026-04-19T07:00:00Z",
            "written_at_utc": "2026-04-19T07:00:01Z",
            "model": "anthropic/test-model",
        }
    )
    return row


def _card_payload(
    *,
    photo_name: str,
    google_place_id: str,
    fsq_place_id: str,
) -> dict[str, object]:
    return {
        "plan_title": "Test plan",
        "plan_hook": "A Maps-verified date night plan.",
        "plan_time_iso": "2026-04-25T19:00:00+10:00",
        "bucket_id": "sydney_cbd",
        "bucket_label": "Sydney CBD",
        "template_id": "drinks_dinner_dessert",
        "template_title": "Drinks, dinner, dessert",
        "template_description": "Start with drinks and end with dessert.",
        "vibe": ["romantic"],
        "transport_mode": "WALK",
        "model": "anthropic/test-model",
        "search_text": "search text",
        "legs": [],
        "feasibility": {},
        "stops": [
            {
                "index": 1,
                "kind": "venue",
                "stop_type": "bar",
                "fsq_place_id": fsq_place_id,
                "name": "Nice Place",
                "google_place_id": google_place_id,
                "primary_photo": {
                    "name": photo_name,
                    "width_px": 1200,
                    "height_px": 900,
                    "author_attributions": [],
                },
                "photos": [
                    {
                        "name": photo_name,
                        "width_px": 1200,
                        "height_px": 900,
                        "author_attributions": [],
                    }
                ],
            }
        ],
    }


def _asset_id(photo_name: str) -> str:
    import hashlib

    return hashlib.sha256(photo_name.encode("utf-8")).hexdigest()


async def _async_cleanup_temp_dir(temp_dir: tempfile.TemporaryDirectory[str]) -> None:
    temp_dir.cleanup()
