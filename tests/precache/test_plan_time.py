from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

from back_end.agents.precache_planner import PrecachePlannerRequest
from back_end.domain.models import TravelMode
from back_end.precache.models import LocationBucket, LocationCandidatePool
from back_end.precache.plan_time import (
    PlanTimeResolverError,
    resolve_plan_time,
    resolve_plan_time_candidates,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATES_PATH = REPO_ROOT / "config" / "date_templates.yaml"
REFERENCE_NOW = datetime(2026, 4, 19, 9, 0, tzinfo=ZoneInfo("Australia/Sydney"))
EXPECTED_DEFAULTS = {
    "morning": ("saturday", 10),
    "midday": ("saturday", 12),
    "afternoon": ("saturday", 14),
    "evening": ("friday", 19),
    "night": ("friday", 21),
    "flexible": ("friday", 18),
}


class PlanTimeResolverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        raw_config = yaml.safe_load(TEMPLATES_PATH.read_text(encoding="utf-8"))
        cls.templates = raw_config["templates"]

    def setUp(self) -> None:
        self.bucket = LocationBucket(
            bucket_id="sydney-cbd",
            label="Sydney CBD",
            latitude=-33.8688,
            longitude=151.2093,
            radius_km=2.0,
            transport_mode="walking",
        )
        self.pool = LocationCandidatePool(
            bucket=self.bucket,
            places=(),
            target_plan_count=3,
        )

    def test_all_template_time_of_day_values_resolve_to_valid_precache_plan_times(self) -> None:
        seen_time_of_day: set[str] = set()

        for template in self.templates:
            time_of_day = str(template["time_of_day"])
            expected_day, expected_hour = EXPECTED_DEFAULTS[time_of_day]

            candidate = resolve_plan_time(
                template=template,
                bucket=self.bucket,
                reference_now=REFERENCE_NOW,
            )
            parsed = datetime.fromisoformat(candidate.plan_time_iso)

            with self.subTest(template_id=template["id"], time_of_day=time_of_day):
                self.assertEqual(candidate.day_of_week, expected_day)
                self.assertEqual(parsed.hour, expected_hour)
                self.assertEqual(parsed.minute, 0)
                self.assertRegex(candidate.plan_time_iso, r"[+-]\d{2}:\d{2}$")
                self.assertNotIn("Z", candidate.plan_time_iso)
                self.assertEqual(parsed.utcoffset(), timedelta(hours=10))
                PrecachePlannerRequest(
                    bucket=self.bucket,
                    pool=self.pool,
                    template=template,
                    plan_time_iso=candidate.plan_time_iso,
                    transport_mode=TravelMode.WALK,
                    max_leg_seconds=900,
                )
                seen_time_of_day.add(time_of_day)

        self.assertEqual(seen_time_of_day, set(EXPECTED_DEFAULTS))

    def test_default_evening_resolution_returns_multiple_candidates_in_order(self) -> None:
        candidates = resolve_plan_time_candidates(
            template={"id": "sunset", "time_of_day": "evening"},
            bucket=self.bucket,
            reference_now=REFERENCE_NOW,
        )

        self.assertEqual(
            [(candidate.day_of_week, candidate.plan_time_iso) for candidate in candidates],
            [
                ("friday", "2026-04-24T19:00:00+10:00"),
                ("saturday", "2026-04-25T19:00:00+10:00"),
                ("sunday", "2026-04-26T19:00:00+10:00"),
            ],
        )

    def test_weekday_bias_can_override_weekend_skew_for_morning_templates(self) -> None:
        candidate = resolve_plan_time(
            template={"id": "weekday-breakfast", "time_of_day": "morning"},
            bucket=self.bucket,
            reference_now=REFERENCE_NOW,
            weekday_bias="weekday",
        )

        self.assertEqual(candidate.day_of_week, "monday")
        self.assertEqual(candidate.plan_time_iso, "2026-04-20T10:00:00+10:00")

    def test_invalid_time_of_day_raises_and_logs(self) -> None:
        with self.assertLogs("back_end.precache.plan_time", level="ERROR") as logs:
            with self.assertRaises(PlanTimeResolverError):
                resolve_plan_time(
                    template={"id": "broken-template", "time_of_day": "dawn"},
                    bucket=self.bucket,
                    reference_now=REFERENCE_NOW,
                )

        self.assertIn("Unsupported time_of_day", "\n".join(logs.output))

    def test_unreachable_date_window_raises_and_logs(self) -> None:
        with self.assertLogs("back_end.precache.plan_time", level="ERROR") as logs:
            with self.assertRaises(PlanTimeResolverError):
                resolve_plan_time_candidates(
                    template={"id": "weekend-only", "time_of_day": "evening"},
                    bucket=self.bucket,
                    reference_now=REFERENCE_NOW,
                    weekday_bias="weekend",
                    days_ahead_min=1,
                    days_ahead_max=1,
                )

        self.assertIn("Could not resolve plan times", "\n".join(logs.output))

    def test_naive_reference_now_is_rejected(self) -> None:
        with self.assertLogs("back_end.precache.plan_time", level="ERROR") as logs:
            with self.assertRaises(PlanTimeResolverError):
                resolve_plan_time(
                    template={"id": "naive-reference", "time_of_day": "afternoon"},
                    bucket=self.bucket,
                    reference_now=datetime(2026, 4, 19, 9, 0),
                )

        self.assertIn("reference_now must be timezone-aware", "\n".join(logs.output))

    def test_non_mapping_template_is_rejected(self) -> None:
        with self.assertLogs("back_end.precache.plan_time", level="ERROR") as logs:
            with self.assertRaises(PlanTimeResolverError):
                resolve_plan_time(
                    template=["not", "a", "mapping"],  # type: ignore[arg-type]
                    bucket=self.bucket,
                    reference_now=REFERENCE_NOW,
                )

        self.assertIn("template must be a Mapping", "\n".join(logs.output))


if __name__ == "__main__":
    unittest.main()
