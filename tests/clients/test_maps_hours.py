from __future__ import annotations

import unittest

from back_end.clients.maps_hours import is_open_at_plan_time
from back_end.domain.models import MapsOpeningHours


class MapsOpeningHoursParserTests(unittest.TestCase):
    def test_closed_day_returns_false(self) -> None:
        self.assertIs(
            is_open_at_plan_time(_hours("Saturday: Closed"), "2026-04-18T19:00:00"),
            False,
        )

    def test_open_24_hours_returns_true(self) -> None:
        self.assertIs(
            is_open_at_plan_time(
                _hours("Saturday: Open 24 hours"),
                "2026-04-18T03:15:00",
            ),
            True,
        )

    def test_single_range_open(self) -> None:
        self.assertIs(
            is_open_at_plan_time(
                _hours("Saturday: 5:00 – 11:00 PM"),
                "2026-04-18T21:30:00",
            ),
            True,
        )

    def test_single_range_closed_before_open(self) -> None:
        self.assertIs(
            is_open_at_plan_time(
                _hours("Saturday: 5:00 – 11:00 PM"),
                "2026-04-18T16:59:00",
            ),
            False,
        )

    def test_single_range_end_is_exclusive(self) -> None:
        self.assertIs(
            is_open_at_plan_time(
                _hours("Saturday: 5:00 – 11:00 PM"),
                "2026-04-18T23:00:00",
            ),
            False,
        )

    def test_multi_range_midday_open(self) -> None:
        self.assertIs(
            is_open_at_plan_time(
                _hours("Saturday: 11:30 AM – 2:30 PM, 5:00 – 10:00 PM"),
                "2026-04-18T12:45:00",
            ),
            True,
        )

    def test_multi_range_gap_closed(self) -> None:
        self.assertIs(
            is_open_at_plan_time(
                _hours("Saturday: 11:30 AM – 2:30 PM, 5:00 – 10:00 PM"),
                "2026-04-18T15:30:00",
            ),
            False,
        )

    def test_overnight_range_start_day_open(self) -> None:
        self.assertIs(
            is_open_at_plan_time(
                _hours("Saturday: 11:00 PM – 2:00 AM"),
                "2026-04-18T23:30:00",
            ),
            True,
        )

    def test_overnight_range_next_day_open(self) -> None:
        hours = _hours("Saturday: 11:00 PM – 2:00 AM", "Sunday: Closed")

        self.assertIs(is_open_at_plan_time(hours, "2026-04-19T01:30:00"), True)

    def test_twelve_am_and_pm_edges(self) -> None:
        self.assertIs(
            is_open_at_plan_time(
                _hours("Saturday: 12:00 AM – 12:00 PM"),
                "2026-04-18T11:59:00",
            ),
            True,
        )
        self.assertIs(
            is_open_at_plan_time(
                _hours("Saturday: 12:00 PM – 12:00 AM"),
                "2026-04-18T23:59:00",
            ),
            True,
        )

    def test_unparseable_description_returns_none_and_logs(self) -> None:
        with self.assertLogs("back_end.clients.maps_hours", level="ERROR"):
            result = is_open_at_plan_time(
                _hours("Saturday hours unavailable"),
                "2026-04-18T19:00:00",
            )

        self.assertIsNone(result)

    def test_missing_target_day_returns_none_and_logs(self) -> None:
        with self.assertLogs("back_end.clients.maps_hours", level="ERROR"):
            result = is_open_at_plan_time(
                _hours("Friday: 5:00 – 11:00 PM"),
                "2026-04-18T19:00:00",
            )

        self.assertIsNone(result)


def _hours(*descriptions: str) -> MapsOpeningHours:
    return MapsOpeningHours(open_now=None, weekday_descriptions=tuple(descriptions))


if __name__ == "__main__":
    unittest.main()
