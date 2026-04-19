from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import pandas as pd

from back_end.api.app import create_app
from back_end.api.service import FrontendPlanService
from back_end.precache.asset_sync import API_PLAN_COLUMNS
from back_end.query.errors import LocationAmbiguityError
from back_end.query.models import LocationType, ResolvedLocation
from back_end.services.booking import (
    BookingStatus,
    RestaurantBookingCallStatus,
    RestaurantBookingJob,
)
from back_end.clients.bland import BlandCallDetails


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


class _FakeBookingService:
    async def __aenter__(self) -> "_FakeBookingService":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def start_restaurant_booking(self, request):
        return RestaurantBookingJob(
            call_id="call_123",
            status=BookingStatus.QUEUED,
            provider="bland_ai",
            restaurant_name=request.restaurant_name,
            restaurant_phone_number=request.restaurant_phone_number,
            arrival_time=request.arrival_time,
            party_size=request.party_size,
            request_data={},
        )

    async def get_booking_status(self, call_id: str):
        return RestaurantBookingCallStatus(
            call_id=call_id,
            status=BookingStatus.CONFIRMED,
            provider_status="completed",
            queue_status="complete",
            answered_by="human",
            summary="Booking confirmed.",
            error_message=None,
            raw_details=BlandCallDetails(
                call_id=call_id,
                to="+61491114073",
                from_number=None,
                completed=True,
                queue_status="complete",
                status="completed",
                answered_by="human",
                error_message=None,
                summary="Booking confirmed.",
                disposition_tag="booking_confirmed",
                concatenated_transcript=None,
                request_data={},
                metadata={},
                raw_payload={},
            ),
        )


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

    async def test_client_errors_endpoint_logs_phone_errors(self) -> None:
        app = create_app()
        transport = httpx.ASGITransport(app=app)

        with self.assertLogs("back_end.api.app", level="ERROR") as captured:
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.post(
                    "/client-errors",
                    json={
                        "source": "api.http_error",
                        "message": "Planner search failed",
                        "stack": "Error: Planner search failed",
                        "platform": "ios",
                        "context": {"path": "/dates/search", "status": 500},
                    },
                )

        self.assertEqual(200, response.status_code)
        self.assertEqual({"status": "logged"}, response.json())
        self.assertTrue(any("Frontend client error" in line for line in captured.output))
        self.assertTrue(any("/dates/search" in line for line in captured.output))

    async def test_templates_endpoint_serves_backend_template_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            templates_path = root / "date_templates.yaml"
            _write_templates_yaml(templates_path)

            service = FrontendPlanService(
                plans_api_path=root / "plans_api.parquet",
                assets_dir=root / "images",
                date_templates_path=templates_path,
                location_resolver=_FakeResolver(),
            )
            app = create_app(service=service)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.get("/templates")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertIn("templates", payload)
        self.assertEqual(1, len(payload["templates"]))
        template = payload["templates"][0]
        self.assertEqual("sunset_dinner", template["id"])
        self.assertEqual(["romantic", "foodie"], template["vibes"])
        self.assertEqual("evening", template["timeOfDay"])
        self.assertEqual(2, len(template["stops"]))
        self.assertEqual("restaurant", template["stops"][1]["type"])
        self.assertEqual("connective", template["stops"][0]["kind"])

    async def test_templates_endpoint_returns_503_when_yaml_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = FrontendPlanService(
                plans_api_path=root / "plans_api.parquet",
                assets_dir=root / "images",
                date_templates_path=root / "missing_templates.yaml",
                location_resolver=_FakeResolver(),
            )
            app = create_app(service=service)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.get("/templates")

        self.assertEqual(503, response.status_code)
        self.assertIn("Date templates YAML is missing", response.json()["detail"])

    async def test_dates_endpoints_serve_booking_context_and_images(self) -> None:
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
                "feasibility": {
                    "all_legs_under_threshold": True,
                    "all_open_at_plan_time": True,
                    "all_venues_matched": True,
                },
                "legs": [
                    {
                        "transport_mode": "WALK",
                        "duration_seconds": 540.0,
                    }
                ],
                "stops": [
                    {
                        "fsq_place_id": "fsq-1",
                        "kind": "venue",
                        "stop_type": "cocktail_bar",
                        "name": "View Bar",
                        "llm_description": "Cocktails with a view.",
                        "google_maps_uri": "https://maps.google.com/?cid=1",
                        "address": "1 Bridge St",
                        "booking_signals": ["menu"],
                        "location": {"latitude": -33.8690, "longitude": 151.2090},
                    },
                    {
                        "fsq_place_id": "fsq-2",
                        "kind": "venue",
                        "stop_type": "restaurant",
                        "name": "Dinner Spot",
                        "llm_description": "Dinner afterwards.",
                        "why_it_fits": "The main meal stop.",
                        "google_maps_uri": "https://maps.google.com/?cid=2",
                        "address": "99 George St",
                        "phone_number": "+61290000000",
                        "booking_signals": ["third_party_booking"],
                        "location": {"latitude": -33.8700, "longitude": 151.2100},
                    },
                ],
            }
            _write_plans_api_parquet(plans_api_path, payload, relative_image_path)

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
                        "partySize": 4,
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
        self.assertEqual("Sunset Dinner", listed_plan["title"])
        self.assertEqual(
            "http://testserver/static/precache-images/google-place-1/hero.jpg",
            listed_plan["heroImageUrl"],
        )
        self.assertEqual("Sunset dinner", listed_plan["templateHint"])
        self.assertEqual("api", listed_plan["source"])
        self.assertEqual("Dinner Spot", listed_plan["bookingContext"]["restaurantName"])
        self.assertEqual("+61290000000", listed_plan["bookingContext"]["restaurantPhoneNumber"])

        detailed_plan = detail_response.json()["plan"]
        self.assertEqual("A Maps-verified date night plan.", detailed_plan["hook"])
        self.assertEqual("The main meal stop.", detailed_plan["stops"][1]["whyItFits"])
        self.assertEqual("+61290000000", detailed_plan["stops"][1]["phoneNumber"])

        generated = generate_response.json()
        self.assertEqual(1, generated["meta"]["matchedCount"])
        self.assertEqual("$$", generated["plans"][0]["costBand"])
        self.assertEqual(4, generated["plans"][0]["bookingContext"]["partySize"])
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
                        "stop_type": "restaurant",
                        "llm_description": "Cocktails with a view.",
                        "google_maps_uri": "https://maps.google.com/?cid=1",
                        "location": {"latitude": -33.8690, "longitude": 151.2090},
                    }
                ],
            }
            _write_plans_api_parquet(plans_api_path, payload, None)

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

    async def test_dates_endpoint_omits_invalid_booking_phone_prefill(self) -> None:
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
                        "name": "Dinner Spot",
                        "stop_type": "restaurant",
                        "llm_description": "Dinner afterwards.",
                        "address": "99 George St",
                        "phone_number": "02 9000 0000",
                        "location": {"latitude": -33.8690, "longitude": 151.2090},
                    }
                ],
            }
            _write_plans_api_parquet(plans_api_path, payload, None)

            service = FrontendPlanService(
                plans_api_path=plans_api_path,
                assets_dir=assets_dir,
                location_resolver=_FakeResolver(),
            )
            app = create_app(service=service)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.get("/dates/plan-1")

        self.assertEqual(200, response.status_code)
        detailed_plan = response.json()["plan"]
        self.assertIsNone(detailed_plan["bookingContext"]["restaurantPhoneNumber"])
        self.assertIsNone(detailed_plan["stops"][0]["phoneNumber"])

    async def test_booking_preview_and_booking_endpoints(self) -> None:
        with patch.dict(
            os.environ,
            {
                "BLAND_AI_API_KEY": "",
                "BLAND_AI_BOOKING_PHONE_NUMBER": "+61491114073",
            },
            clear=False,
        ):
            app = create_app(booking_service_factory=_FakeBookingService)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                preview_response = await client.post(
                    "/booking/restaurants/preview",
                    json={
                        "restaurantName": "Restaurant Hubert",
                        "restaurantPhoneNumber": "+61491114073",
                        "restaurantAddress": "15 Bligh St",
                        "arrivalTimeIso": "2026-04-24T20:15:00+10:00",
                        "partySize": 2,
                        "bookingName": "Emma",
                        "planId": "plan-1",
                    },
                )
                create_response = await client.post(
                    "/booking/restaurants",
                    json={
                        "restaurantName": "Restaurant Hubert",
                        "restaurantPhoneNumber": "+61491114073",
                        "arrivalTimeIso": "2026-04-24T20:15:00+10:00",
                        "partySize": 2,
                        "bookingName": "Emma",
                    },
                )
                status_response = await client.get("/booking/restaurants/call_123")

        self.assertEqual(200, preview_response.status_code)
        preview_payload = preview_response.json()
        self.assertEqual("Restaurant Hubert", preview_payload["bookingContext"]["restaurantName"])
        self.assertEqual("+61491114073", preview_payload["callDescription"]["phoneNumber"])
        self.assertIn("Restaurant Hubert", preview_payload["callDescription"]["task"])
        self.assertEqual("plan-1", preview_payload["callDescription"]["metadata"]["plan_id"])
        self.assertFalse(preview_payload["liveCallEnabled"])
        self.assertIn("BLAND_AI_API_KEY", preview_payload["liveCallDisabledReason"])

        self.assertEqual(200, create_response.status_code)
        create_payload = create_response.json()
        self.assertEqual("call_123", create_payload["callId"])
        self.assertEqual("queued", create_payload["status"])

        self.assertEqual(200, status_response.status_code)
        status_payload = status_response.json()
        self.assertEqual("confirmed", status_payload["status"])
        self.assertEqual("Booking confirmed.", status_payload["summary"])

    async def test_booking_preview_reports_live_call_enabled_when_configured(self) -> None:
        with patch.dict(os.environ, {"BLAND_AI_API_KEY": "test-key"}, clear=False):
            app = create_app(booking_service_factory=_FakeBookingService)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.post(
                    "/booking/restaurants/preview",
                    json={
                        "restaurantName": "Restaurant Hubert",
                        "restaurantPhoneNumber": "+61491114073",
                        "arrivalTimeIso": "2026-04-24T20:15:00+10:00",
                        "partySize": 2,
                        "bookingName": "Emma",
                    },
                )

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertTrue(payload["liveCallEnabled"])
        self.assertIsNone(payload["liveCallDisabledReason"])

    async def test_booking_preview_uses_configured_call_target_not_restaurant_phone(self) -> None:
        with patch.dict(
            os.environ,
            {
                "BLAND_AI_API_KEY": "test-key",
                "BLAND_AI_BOOKING_PHONE_NUMBER": "+61491114073",
            },
            clear=False,
        ):
            app = create_app(booking_service_factory=_FakeBookingService)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.post(
                    "/booking/restaurants/preview",
                    json={
                        "restaurantName": "Different Bistro",
                        "restaurantPhoneNumber": "+61290000000",
                        "arrivalTimeIso": "2026-04-24T20:15:00+10:00",
                        "partySize": 2,
                        "bookingName": "Emma",
                    },
                )

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual("+61491114073", payload["callDescription"]["phoneNumber"])
        self.assertEqual(
            "+61290000000",
            payload["callDescription"]["requestData"]["restaurant_phone_number"],
        )

    async def test_booking_preview_does_not_require_restaurant_phone(self) -> None:
        with patch.dict(
            os.environ,
            {
                "BLAND_AI_API_KEY": "test-key",
                "BLAND_AI_BOOKING_PHONE_NUMBER": "+61491114073",
            },
            clear=False,
        ):
            app = create_app(booking_service_factory=_FakeBookingService)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.post(
                    "/booking/restaurants/preview",
                    json={
                        "restaurantName": "Restaurant Hubert",
                        "arrivalTimeIso": "2026-04-24T20:15:00+10:00",
                        "partySize": 2,
                        "bookingName": "Emma",
                    },
                )

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual("+61491114073", payload["callDescription"]["phoneNumber"])
        self.assertNotIn(
            "restaurant_phone_number",
            payload["callDescription"]["requestData"],
        )

    async def test_create_booking_returns_503_when_bland_is_not_configured(self) -> None:
        with patch.dict(os.environ, {"BLAND_AI_API_KEY": ""}, clear=False):
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.post(
                    "/booking/restaurants",
                    json={
                        "restaurantName": "Restaurant Hubert",
                        "restaurantPhoneNumber": "+61491114073",
                        "arrivalTimeIso": "2026-04-24T20:15:00+10:00",
                        "partySize": 2,
                        "bookingName": "Emma",
                    },
                )

        self.assertEqual(503, response.status_code)
        self.assertIn("BLAND_AI_API_KEY", response.json()["detail"])

    async def test_status_lookup_returns_503_when_bland_is_not_configured(self) -> None:
        with patch.dict(os.environ, {"BLAND_AI_API_KEY": ""}, clear=False):
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.get("/booking/restaurants/call_123")

        self.assertEqual(503, response.status_code)
        self.assertIn("BLAND_AI_API_KEY", response.json()["detail"])


def _write_plans_api_parquet(
    plans_api_path: Path,
    payload: dict,
    relative_image_path: Path | None,
) -> None:
    pd.DataFrame(
        [
            {
                "plan_id": payload["plan_id"],
                "template_id": payload["template_id"],
                "bucket_id": payload["bucket_id"],
                "plan_title": payload["plan_title"],
                "bucket_label": payload["bucket_label"],
                "hero_image_asset_id": "asset-1" if relative_image_path is not None else None,
                "hero_image_relative_path": None
                if relative_image_path is None
                else str(relative_image_path.as_posix()),
                "hero_image_public_url": None,
                "generated_at_utc": "2026-04-19T07:00:00Z",
                "source_written_at_utc": "2026-04-19T07:00:01Z",
                "exported_at_utc": "2026-04-19T07:00:02Z",
                "api_payload_json": json.dumps(payload),
            }
        ],
        columns=API_PLAN_COLUMNS,
    ).to_parquet(plans_api_path, index=False)


def _write_templates_yaml(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "templates:",
                "  - id: sunset_dinner",
                "    title: Sunset dinner",
                "    vibe: [romantic, foodie]",
                "    time_of_day: evening",
                "    duration_hours: 3.5",
                "    meaningful_variations: 12",
                "    weather_sensitive: true",
                "    description: Golden hour, then dinner.",
                "    stops:",
                "      - type: scenic_lookout",
                "        kind: connective",
                "        note: sunset stop",
                "      - type: restaurant",
            ]
        ),
        encoding="utf-8",
    )
