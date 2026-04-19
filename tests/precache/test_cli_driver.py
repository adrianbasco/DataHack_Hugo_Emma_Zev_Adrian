from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

import scripts.run_precache as run_precache
from back_end.agents.date_idea_agent import DateIdea, DateIdeaStop
from back_end.agents.precache_planner import PrecachePlannerSuccess
from back_end.domain.models import TravelMode
from back_end.precache import cli_driver as driver
from back_end.precache.factory import PrecachePlanCell
from back_end.precache.models import (
    CandidatePoolPlace,
    LocationBucket,
    LocationCandidatePool,
)
from back_end.precache.output import (
    PrecachePlanOutput,
    fsq_place_ids_sorted_signature,
    read_precache_output,
)
from back_end.precache.plan_time import PlanTimeCandidate


class PrecacheCliDriverTests(unittest.IsolatedAsyncioTestCase):
    def test_print_plan_uses_date_idea_hook(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            run_precache._print_plan(index=1, total=3, result=_FakePlanner()._success_result())

        rendered = output.getvalue()
        self.assertIn("PLAN 1/3 — Coffee and dessert", rendered)
        self.assertIn("A quick test plan.", rendered)
        self.assertIn("Stop 1 [venue:cafe] Test Cafe", rendered)

    async def test_limit_one_writes_exactly_one_plan_and_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output_path = root / "plans.parquet"
            run_dir = root / "run"
            planner = _FakePlanner()

            summary = await driver.run(
                planner=planner,
                cells=(_cell(budget=3),),
                output_path=output_path,
                run_dir=run_dir,
                max_concurrency=1,
                retries_per_cell=3,
                limit=1,
            )

            written = read_precache_output(output_path)
            self.assertEqual(1, len(written))
            self.assertEqual(1, summary.successful_plan_count)
            self.assertEqual(1, summary.planner_call_count)
            self.assertTrue(summary.limit_reached)

            status_payload = json.loads((run_dir / "status.json").read_text(encoding="utf-8"))
            self.assertEqual("completed", status_payload["state"])
            self.assertEqual(1, status_payload["successful_plan_count"])


class _FakePlanner:
    def __init__(self) -> None:
        self.calls = 0

    async def plan(self, request) -> PrecachePlannerSuccess:
        self.calls += 1
        return self._success_result()

    def _success_result(self) -> PrecachePlannerSuccess:
        fsq_ids = ("fsq-cafe", f"fsq-dessert-{self.calls}")
        signature = fsq_place_ids_sorted_signature(fsq_ids)
        plan = PrecachePlanOutput(
            bucket_id="cbd",
            template_id="coffee_and_stroll",
            bucket_metadata={
                "label": "CBD",
                "latitude": -33.8688,
                "longitude": 151.2093,
                "radius_km": 1.0,
                "transport_mode": "walking",
                "tags": [],
            },
            template_metadata={
                "title": "Coffee and a stroll",
                "vibe": ["casual"],
                "time_of_day": "morning",
                "weather_sensitive": True,
                "duration_hours": 1.5,
                "description": "Coffee then a stroll.",
            },
            plan_title="Coffee and dessert",
            plan_hook="A quick test plan.",
            plan_time_iso="2026-04-25T19:30:00+10:00",
            stops=[
                {
                    "kind": "venue",
                    "stop_type": "cafe",
                    "fsq_place_id": "fsq-cafe",
                    "name": "Test Cafe",
                },
                {
                    "kind": "connective",
                    "stop_type": "park_or_garden",
                    "name": "Park stroll",
                    "description": "Walk nearby.",
                },
                {
                    "kind": "venue",
                    "stop_type": "dessert_shop",
                    "fsq_place_id": f"fsq-dessert-{self.calls}",
                    "name": f"Dessert {self.calls}",
                },
            ],
            search_text="Coffee and dessert around the CBD.",
            card_payload={
                "version": 1,
                "plan_title": "Coffee and dessert",
                "plan_hook": "A quick test plan.",
                "plan_time_iso": "2026-04-25T19:30:00+10:00",
                "search_text": "Coffee and dessert around the CBD.",
                "stops": [],
                "legs": [],
                "feasibility": {},
            },
            verification={
                "status": "verified",
                "feasibility": {
                    "all_venues_matched": True,
                    "all_open_at_plan_time": True,
                    "all_legs_under_threshold": True,
                },
            },
            generated_at_utc=datetime.now(UTC),
            model="fake-model",
        )
        idea = DateIdea(
            title="Coffee and dessert",
            hook="A quick test plan.",
            template_hint="coffee_and_stroll",
            stops=(
                DateIdeaStop(
                    kind="venue",
                    stop_type="cafe",
                    name="Test Cafe",
                    description="Coffee.",
                    why_it_fits="Nearby.",
                    fsq_place_id="fsq-cafe",
                ),
                DateIdeaStop(
                    kind="connective",
                    stop_type="park_or_garden",
                    name="Park stroll",
                    description="Walk nearby.",
                    why_it_fits="Fits the template.",
                ),
                DateIdeaStop(
                    kind="venue",
                    stop_type="dessert_shop",
                    name=f"Dessert {self.calls}",
                    description="Dessert.",
                    why_it_fits="Nearby.",
                    fsq_place_id=f"fsq-dessert-{self.calls}",
                ),
            ),
            maps_verification_needed=False,
        )
        return PrecachePlannerSuccess(
            plan=plan,
            idea=idea,
            signature=signature,
            verification=plan.verification,
            tool_executions=(),
            raw_output={"date_ideas": []},
            model="fake-model",
        )


def _cell(*, budget: int) -> PrecachePlanCell:
    bucket = LocationBucket(
        bucket_id="cbd",
        label="CBD",
        latitude=-33.8688,
        longitude=151.2093,
        radius_km=1.0,
        transport_mode="walking",
        minimum_plan_count=1,
        maximum_plan_count=3,
    )
    pool = LocationCandidatePool(
        bucket=bucket,
        places=(
            CandidatePoolPlace(
                fsq_place_id="fsq-cafe",
                name="Test Cafe",
                latitude=-33.8689,
                longitude=151.2094,
                distance_km=0.1,
                quality_score=9,
                template_stop_tags=("cafe",),
                category_labels=("Dining and Drinking > Cafe",),
            ),
        ),
        target_plan_count=budget,
        empty_reason=None,
    )
    template = {
        "id": "coffee_and_stroll",
        "title": "Coffee and a stroll",
        "vibe": ["casual"],
        "time_of_day": "morning",
        "weather_sensitive": True,
        "duration_hours": 1.5,
        "description": "Coffee then a stroll.",
        "stops": [
            {"type": "cafe"},
            {"type": "park_or_garden", "kind": "connective"},
        ],
    }
    return PrecachePlanCell(
        bucket=bucket,
        template=template,
        pool=pool,
        budget=budget,
        plan_time=PlanTimeCandidate(
            plan_time_iso="2026-04-25T10:00:00+10:00",
            day_of_week="saturday",
            reason="seeded test time",
        ),
        transport_mode=TravelMode.WALK,
        max_leg_seconds=1500,
    )
