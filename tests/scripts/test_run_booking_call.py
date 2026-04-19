from __future__ import annotations

import asyncio
import contextlib
import io
import json
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from scripts import run_booking_call


class RunBookingCallCliTests(unittest.TestCase):
    def test_start_defaults_build_dry_run_payload_without_network(self) -> None:
        stdout = io.StringIO()

        with contextlib.redirect_stdout(stdout):
            exit_code = asyncio.run(run_booking_call.async_main(["start"]))

        self.assertEqual(0, exit_code)
        output = json.loads(stdout.getvalue())
        payload = output["call_payload"]

        self.assertTrue(output["dry_run"])
        self.assertTrue(output["place_call_required_for_network"])
        self.assertEqual("+61491114073", payload["phone_number"])
        self.assertEqual("Test Restaurant", payload["request_data"]["restaurant_name"])
        self.assertEqual(2, payload["request_data"]["party_size"])
        self.assertEqual(30, payload["request_data"]["acceptable_time_window_minutes"])
        self.assertIn("within 30 minutes", payload["task"])

    def test_naive_arrival_time_is_interpreted_in_requested_timezone(self) -> None:
        args = run_booking_call.parse_args(
            [
                "start",
                "--arrival-time",
                "2026-05-01T19:30:00",
                "--timezone",
                "Australia/Sydney",
            ]
        )

        request = run_booking_call.build_booking_request(
            args,
            now=datetime(2026, 4, 19, 12, 0, tzinfo=ZoneInfo("Australia/Sydney")),
        )

        self.assertEqual(
            datetime(2026, 5, 1, 19, 30, tzinfo=ZoneInfo("Australia/Sydney")),
            request.arrival_time,
        )

    def test_default_arrival_time_uses_next_friday_evening(self) -> None:
        args = run_booking_call.parse_args(["start"])

        request = run_booking_call.build_booking_request(
            args,
            now=datetime(2026, 4, 19, 12, 0, tzinfo=ZoneInfo("Australia/Sydney")),
        )

        self.assertEqual(
            datetime(2026, 4, 24, 19, 30, tzinfo=ZoneInfo("Australia/Sydney")),
            request.arrival_time,
        )

    def test_rejects_past_arrival_time(self) -> None:
        args = run_booking_call.parse_args(
            ["start", "--arrival-time", "2026-04-18T19:30:00"]
        )

        with self.assertRaisesRegex(ValueError, "future"):
            run_booking_call.build_booking_request(
                args,
                now=datetime(2026, 4, 19, 12, 0, tzinfo=ZoneInfo("Australia/Sydney")),
            )

    def test_rejects_negative_time_window(self) -> None:
        args = run_booking_call.parse_args(["start", "--time-window-minutes", "-1"])

        with self.assertRaisesRegex(ValueError, "time-window"):
            run_booking_call.build_booking_request(
                args,
                now=datetime(2026, 4, 19, 12, 0, tzinfo=ZoneInfo("Australia/Sydney")),
            )

