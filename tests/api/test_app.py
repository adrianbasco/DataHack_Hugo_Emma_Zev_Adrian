from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import httpx
import pandas as pd

from back_end.api.app import create_app
from back_end.api.service import FrontendPlanService
from back_end.precache.asset_sync import API_PLAN_COLUMNS
from back_end.query.errors import LocationAmbiguityError
from back_end.query.models import LocationType, ResolvedLocation


class _FakeResolver:
    def resolve(self, location_text: str) -> ResolvedLocation:
        return ResolvedLocation(
            input_text=location_text,
            location_type=LocationType.LOCALITY,
            locality="Sydney",
            region="nsw",
            postcode=None,
            anchor_latitude=-33.8688,
            anchor_longitude=151.2093,
            matched_place_count=10,
            matched_regions=("nsw",),
        )


class _FallbackResolver:
    def resolve(self, location_text: str) -> ResolvedLocation:
        if location_text == "Sydney":
            raise LocationAmbiguityError("ambiguous")
        if location_text == "Sydney, NSW":
            return _FakeResolver().resolve(location_text)
        raise AssertionError(f"unexpected location_text {location_text!r}")


class FrontendApiAppTests(unittest.IsolatedAsyncioTestCase):
    async def test_healthz_degraded_when_snapshot_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            assets_dir = root / "images"
            service = FrontendPlanService(
                plans_api_path=root / "plans_api.parquet",
                assets_dir=assets_dir,
                location_resolver=_FakeResolver(),
            )
            app = create_app(service=service)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.get("/healthz")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual("degraded", payload["status"])
        self.assertFalse(payload["plansReady"])

    async def test_dates_endpoints_serve_absolute_images_and_filtered_generate(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            assets_dir = root / "images"
            plans_api_path = root / "plans_api.parquet"
            relative_image_path = Path("google-place-1") / "hero.jpg"
            image_path = assets_dir / relative_image_path
            image_path.parent.mkdir(parents=True, exist_ok=True)
            image_path.write_bytes(b"fake-jpeg")

            payload = {
                "plan_id": "plan-1",
                "plan_title": "Sunset Dinner",
                "template_id": "sunset_dinner",
                "template_title": "Sunset dinner",
                "template_description": "Golden hour, then dinner.",
                "template_duration_hours": 3.5,
                "bucket_id": "sydney_cbd",
                "bucket_label": "Sydney CBD",
                "vibe": ["romantic", "foodie"],
                "transport_mode": "WALK",
                "plan_time_iso": "2026-04-25T19:00:00+10:00",
                "search_text": "sunset romantic foodie cbd",
                "plan_hook": "A Maps-verified date night plan.",
                "legs": [
                    {
                        "transport_mode": "WALK",
                        "duration_seconds": 540.0,
                    }
                ],
                "stops": [
                    {
                        "fsq_place_id": "fsq-1",
                        "name": "View Bar",
                        "llm_description": "Cocktails with a view.",
                        "google_maps_uri": "https://maps.google.com/?cid=1",
                        "location": {"latitude": -33.8690, "longitude": 151.2090},
                    },
                    {
                        "fsq_place_id": "fsq-2",
                        "name": "Dinner Spot",
                        "llm_description": "Dinner afterwards.",
                        "google_maps_uri": "https://maps.google.com/?cid=2",
                        "location": {"latitude": -33.8700, "longitude": 151.2100},
                    },
                ],
            }
            df = pd.DataFrame(
                [
                    {
                        "plan_id": "plan-1",
                        "template_id": "sunset_dinner",
                        "bucket_id": "sydney_cbd",
                        "plan_title": "Sunset Dinner",
                        "bucket_label": "Sydney CBD",
                        "hero_image_asset_id": "asset-1",
                        "hero_image_relative_path": str(relative_image_path.as_posix()),
                        "hero_image_public_url": None,
                        "generated_at_utc": "2026-04-19T07:00:00Z",
                        "source_written_at_utc": "2026-04-19T07:00:01Z",
                        "exported_at_utc": "2026-04-19T07:00:02Z",
                        "api_payload_json": json.dumps(payload),
                    }
                ],
                columns=API_PLAN_COLUMNS,
            )
            df.to_parquet(plans_api_path, index=False)

            service = FrontendPlanService(
                plans_api_path=plans_api_path,
                assets_dir=assets_dir,
                location_resolver=_FakeResolver(),
            )
            app = create_app(service=service)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                list_response = await client.get("/dates")
                detail_response = await client.get("/dates/plan-1")
                generate_response = await client.post(
                    "/dates/generate",
                    json={
                        "location": "Sydney",
                        "radiusKm": 5,
                        "transportMode": "walking",
                        "vibe": "romantic foodie",
                        "budget": "$$",
                        "startTime": "19:00",
                        "durationMinutes": 210,
                        "partySize": 2,
                        "constraintsNote": "",
                        "limit": 5,
                    },
                )
                image_response = await client.get(f"/static/precache-images/{relative_image_path.as_posix()}")

        self.assertEqual(200, list_response.status_code)
        self.assertEqual(200, detail_response.status_code)
        self.assertEqual(200, generate_response.status_code)
        self.assertEqual(200, image_response.status_code)

        listed_plan = list_response.json()[0]
        self.assertEqual(
            "http://testserver/static/precache-images/google-place-1/hero.jpg",
            listed_plan["heroImageUrl"],
        )
        self.assertEqual("Sunset Dinner", detail_response.json()["plan"]["title"])
        generated = generate_response.json()
        self.assertEqual(1, generated["meta"]["matchedCount"])
        self.assertEqual("$$", generated["plans"][0]["costBand"])
        self.assertTrue(generated["warnings"])

    async def test_generate_retries_ambiguous_location_as_nsw_with_warning(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            assets_dir = root / "images"
            plans_api_path = root / "plans_api.parquet"
            payload = {
                "plan_id": "plan-1",
                "plan_title": "Sunset Dinner",
                "template_id": "sunset_dinner",
                "template_title": "Sunset dinner",
                "template_description": "Golden hour, then dinner.",
                "template_duration_hours": 3.5,
                "bucket_id": "sydney_cbd",
                "bucket_label": "Sydney CBD",
                "vibe": ["romantic"],
                "transport_mode": "WALK",
                "plan_time_iso": "2026-04-25T19:00:00+10:00",
                "search_text": "sunset romantic cbd",
                "plan_hook": "A Maps-verified date night plan.",
                "legs": [],
                "stops": [
                    {
                        "fsq_place_id": "fsq-1",
                        "name": "View Bar",
                        "llm_description": "Cocktails with a view.",
                        "google_maps_uri": "https://maps.google.com/?cid=1",
                        "location": {"latitude": -33.8690, "longitude": 151.2090},
                    }
                ],
            }
            pd.DataFrame(
                [
                    {
                        "plan_id": "plan-1",
                        "template_id": "sunset_dinner",
                        "bucket_id": "sydney_cbd",
                        "plan_title": "Sunset Dinner",
                        "bucket_label": "Sydney CBD",
                        "hero_image_asset_id": None,
                        "hero_image_relative_path": None,
                        "hero_image_public_url": None,
                        "generated_at_utc": "2026-04-19T07:00:00Z",
                        "source_written_at_utc": "2026-04-19T07:00:01Z",
                        "exported_at_utc": "2026-04-19T07:00:02Z",
                        "api_payload_json": json.dumps(payload),
                    }
                ],
                columns=API_PLAN_COLUMNS,
            ).to_parquet(plans_api_path, index=False)

            service = FrontendPlanService(
                plans_api_path=plans_api_path,
                assets_dir=assets_dir,
                location_resolver=_FallbackResolver(),
            )
            app = create_app(service=service)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.post(
                    "/dates/generate",
                    json={
                        "location": "Sydney",
                        "radiusKm": 5,
                        "transportMode": "walking",
                        "vibe": "romantic",
                        "budget": None,
                        "startTime": "19:00",
                        "durationMinutes": 180,
                        "partySize": 2,
                        "constraintsNote": "",
                        "limit": 5,
                    },
                )

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(1, payload["meta"]["matchedCount"])
        self.assertTrue(any("assumed NSW" in warning for warning in payload["warnings"]))
