from __future__ import annotations

import json
import logging
import unittest
from datetime import datetime
from typing import Callable
from zoneinfo import ZoneInfo

import httpx

from back_end.clients.bland import BlandAIClient
from back_end.clients.settings import BlandAISettings
from back_end.services.booking import (
    BookingRequestBuilder,
    BookingService,
    BookingStatus,
    BookingValidationError,
    RestaurantBookingRequest,
)


def _make_response(
    request: httpx.Request,
    status_code: int,
    payload: dict | None = None,
) -> httpx.Response:
    return httpx.Response(
        status_code=status_code,
        headers={"Content-Type": "application/json"},
        content=json.dumps(payload or {}).encode("utf-8"),
        request=request,
    )


class BookingRequestBuilderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = BlandAISettings(
            api_key="test-key",
            booking_phone_number="+61491114073",
            default_voice="maya",
            max_duration_minutes=6,
            record_calls=False,
        )
        self.builder = BookingRequestBuilder(self.settings)
        self.arrival_time = datetime(
            2026,
            5,
            1,
            19,
            30,
            tzinfo=ZoneInfo("Australia/Sydney"),
        )

    def test_builds_explicit_booking_call_without_alternate_time_fallback(self) -> None:
        call_request = self.builder.build(
            RestaurantBookingRequest(
                restaurant_name="Example Bistro",
                restaurant_phone_number="+61412345678",
                arrival_time=self.arrival_time,
                party_size=4,
                booking_name="Emma",
                customer_phone_number="+61487654321",
                dietary_constraints="One vegetarian diner.",
                accessibility_constraints="Step-free table preferred.",
                plan_id="plan_123",
            )
        )

        payload = call_request.to_payload()

        self.assertEqual("+61491114073", payload["phone_number"])
        self.assertTrue(payload["first_sentence"].startswith("Heyyy"))
        self.assertEqual("maya", payload["voice"])
        self.assertEqual(6, payload["max_duration"])
        self.assertEqual({"action": "hangup"}, payload["voicemail"])
        self.assertIn("You are making a restaurant reservation.", payload["task"])
        self.assertNotIn("AI assistant", payload["task"])
        self.assertIn("do not book a different time", payload["task"])
        self.assertIn("One vegetarian diner", payload["task"])
        self.assertEqual("Example Bistro", payload["request_data"]["restaurant_name"])
        self.assertEqual("+61412345678", payload["request_data"]["restaurant_phone_number"])
        self.assertEqual("Emma", payload["request_data"]["booking_name"])
        self.assertEqual("plan_123", payload["metadata"]["plan_id"])
        self.assertEqual("+61491114073", payload["metadata"]["configured_call_target"])
        self.assertIn("booking_confirmed", payload["dispositions"])

    def test_rejects_invalid_configured_call_target(self) -> None:
        builder = BookingRequestBuilder(
            BlandAISettings(
                api_key="test-key",
                booking_phone_number="+12125550123",
            )
        )

        with self.assertRaisesRegex(
            BookingValidationError,
            "BLAND_AI_BOOKING_PHONE_NUMBER",
        ):
            builder.build(
                RestaurantBookingRequest(
                    restaurant_name="Example Bistro",
                    restaurant_phone_number="+61412345678",
                    arrival_time=self.arrival_time,
                    party_size=2,
                    booking_name="Emma",
                )
            )

    def test_builds_explicit_allowed_time_window(self) -> None:
        call_request = self.builder.build(
            RestaurantBookingRequest(
                restaurant_name="Example Bistro",
                restaurant_phone_number="+61412345678",
                arrival_time=self.arrival_time,
                party_size=2,
                booking_name="Zev",
                acceptable_time_window_minutes=30,
            )
        )

        payload = call_request.to_payload()

        self.assertIn("within 30 minutes", payload["task"])
        self.assertEqual(30, payload["request_data"]["acceptable_time_window_minutes"])

    def test_builds_booking_call_without_restaurant_phone_context(self) -> None:
        call_request = self.builder.build(
            RestaurantBookingRequest(
                restaurant_name="Example Bistro",
                arrival_time=self.arrival_time,
                party_size=2,
                booking_name="Zev",
            )
        )

        payload = call_request.to_payload()

        self.assertEqual("+61491114073", payload["phone_number"])
        self.assertNotIn("restaurant_phone_number", payload["request_data"])
        self.assertIn("Restaurant: Example Bistro.", payload["task"])

    def test_rejects_naive_arrival_time(self) -> None:
        with self.assertRaises(BookingValidationError):
            self.builder.build(
                RestaurantBookingRequest(
                    restaurant_name="Example Bistro",
                    restaurant_phone_number="+61412345678",
                    arrival_time=datetime(2026, 5, 1, 19, 30),
                    party_size=2,
                    booking_name="Emma",
                )
            )

    def test_rejects_invalid_restaurant_phone_number(self) -> None:
        with self.assertRaises(BookingValidationError):
            self.builder.build(
                RestaurantBookingRequest(
                    restaurant_name="Example Bistro",
                    restaurant_phone_number="0412345678",
                    arrival_time=self.arrival_time,
                    party_size=2,
                    booking_name="Emma",
                )
            )

    def test_rejects_non_australian_restaurant_phone_number(self) -> None:
        with self.assertRaises(BookingValidationError):
            self.builder.build(
                RestaurantBookingRequest(
                    restaurant_name="Example Bistro",
                    restaurant_phone_number="+12125550123",
                    arrival_time=self.arrival_time,
                    party_size=2,
                    booking_name="Emma",
                )
            )

    def test_rejects_long_context_instead_of_sending_unsafe_prompt(self) -> None:
        with self.assertRaises(BookingValidationError):
            self.builder.build(
                RestaurantBookingRequest(
                    restaurant_name="Example Bistro",
                    restaurant_phone_number="+61412345678",
                    arrival_time=self.arrival_time,
                    party_size=2,
                    booking_name="Emma",
                    notes="x" * 2500,
                )
            )


class BookingServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.settings = BlandAISettings(
            api_key="test-key",
            booking_phone_number="+61491114073",
            base_url="https://api.test.bland.ai/v1",
            max_duration_minutes=6,
        )
        self.arrival_time = datetime(
            2026,
            5,
            1,
            19,
            30,
            tzinfo=ZoneInfo("Australia/Sydney"),
        )

    async def test_start_restaurant_booking_queues_bland_call(self) -> None:
        captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode("utf-8"))
            return _make_response(
                request,
                200,
                {
                    "status": "success",
                    "message": "Call successfully queued.",
                    "call_id": "call_123",
                    "batch_id": None,
                },
            )

        client = BlandAIClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        service = BookingService(self.settings, client=client)
        self.addAsyncCleanup(client.aclose)

        job = await service.start_restaurant_booking(
            RestaurantBookingRequest(
                restaurant_name="Example Bistro",
                restaurant_phone_number="+61412345678",
                arrival_time=self.arrival_time,
                party_size=3,
                booking_name="Adrian",
            )
        )

        self.assertEqual("call_123", job.call_id)
        self.assertEqual(BookingStatus.QUEUED, job.status)
        self.assertEqual("restaurant_booking", captured["body"]["metadata"]["purpose"])
        self.assertEqual("+61491114073", captured["body"]["phone_number"])
        self.assertEqual(
            "+61412345678",
            captured["body"]["request_data"]["restaurant_phone_number"],
        )
        self.assertEqual(3, captured["body"]["request_data"]["party_size"])

    async def test_get_booking_status_maps_confirmed_disposition(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "call_id": "call_123",
                    "completed": True,
                    "status": "completed",
                    "queue_status": "complete",
                    "answered_by": "human",
                    "summary": "Booking confirmed for 7:30 PM.",
                    "disposition_tag": "booking_confirmed",
                },
            )

        service, client = self._service_with_handler(handler)
        self.addAsyncCleanup(client.aclose)

        status = await service.get_booking_status("call_123")

        self.assertEqual(BookingStatus.CONFIRMED, status.status)
        self.assertEqual("Booking confirmed for 7:30 PM.", status.summary)

    async def test_get_booking_status_maps_no_answer(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "call_id": "call_123",
                    "completed": True,
                    "status": "no-answer",
                    "queue_status": "complete",
                    "answered_by": "no-answer",
                },
            )

        service, client = self._service_with_handler(handler)
        self.addAsyncCleanup(client.aclose)

        status = await service.get_booking_status("call_123")

        self.assertEqual(BookingStatus.NO_ANSWER, status.status)

    async def test_get_booking_status_maps_needs_human_follow_up(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "call_id": "call_123",
                    "completed": True,
                    "status": "completed",
                    "queue_status": "complete",
                    "answered_by": "human",
                    "summary": "Restaurant asked for a deposit link to finish the booking.",
                    "disposition_tag": "needs_human_follow_up",
                },
            )

        service, client = self._service_with_handler(handler)
        self.addAsyncCleanup(client.aclose)

        with self.assertLogs("back_end.services.booking", level=logging.WARNING) as logs:
            status = await service.get_booking_status("call_123")

        self.assertEqual(BookingStatus.NEEDS_HUMAN_FOLLOW_UP, status.status)
        self.assertIsNone(status.error_message)
        self.assertTrue(any("requires human follow-up" in message for message in logs.output))

    async def test_get_booking_status_marks_completed_call_without_disposition_unknown(
        self,
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "call_id": "call_123",
                    "completed": True,
                    "status": "completed",
                    "queue_status": "complete",
                    "answered_by": "human",
                    "summary": "Call completed.",
                },
            )

        service, client = self._service_with_handler(handler)
        self.addAsyncCleanup(client.aclose)

        status = await service.get_booking_status("call_123")

        self.assertEqual(BookingStatus.UNKNOWN, status.status)
        self.assertEqual(
            "Bland AI returned an unexpected booking status.",
            status.error_message,
        )

    def _service_with_handler(
        self,
        handler: Callable[[httpx.Request], httpx.Response],
    ) -> tuple[BookingService, BlandAIClient]:
        client = BlandAIClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        return BookingService(self.settings, client=client), client
