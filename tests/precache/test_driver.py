from __future__ import annotations

import json
import tempfile
import unittest
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from back_end.agents.precache_planner import (
    FAILURE_REASON_DUPLICATE,
    FAILURE_REASON_EMPTY_POOL,
    FAILURE_REASON_OUTPUT_INVALID,
    PrecachePlannerConfigurationError,
    PrecachePlannerFailure,
    PrecachePlannerRequest,
    PrecachePlannerSuccess,
)
from back_end.precache.driver import (
    PrecacheDriverExecutionError,
    RunConfig,
    run_precache_driver,
)
from back_end.precache.models import LocationBucket
from back_end.precache.output import (
    PrecachePlanOutput,
    fsq_place_ids_sorted_signature,
    read_precache_failures,
    read_precache_output,
)


class FakePlanner:
    def __init__(self, responses: list[object]) -> None:
        self._responses = list(responses)
        self.requests: list[PrecachePlannerRequest] = []

    async def plan(self, request: PrecachePlannerRequest) -> object:
        self.requests.append(request)
        if not self._responses:
            raise AssertionError("Unexpected planner invocation.")
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class PrecacheDriverTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.output_path = self.root / "plans.parquet"
        self.failures_path = self.root / "failures.parquet"
        self.run_dir = self.root / "run"
        self.rag_documents_path = self.root / "rag_documents.parquet"
        _rag_documents_df().to_parquet(self.rag_documents_path, index=False)
        self.bucket = LocationBucket(
            bucket_id="cbd",
            label="CBD",
            latitude=-33.8688,
            longitude=151.2093,
            radius_km=2.0,
            transport_mode="walking",
            minimum_plan_count=1,
            maximum_plan_count=1,
            strategic_boost=0,
        )
        self.template = {
            "id": "restaurant_only",
            "title": "Restaurant only",
            "vibe": ["romantic"],
            "time_of_day": "evening",
            "duration_hours": 1.5,
            "meaningful_variations": 4,
            "weather_sensitive": False,
            "description": "Single-stop dinner date.",
            "stops": [{"type": "restaurant"}],
        }

    async def test_driver_writes_manifest_and_is_idempotent_on_rerun(self) -> None:
        planner = FakePlanner([_success(bucket_id="cbd", template_id="restaurant_only", fsq_place_id="restaurant-1")])

        first = await run_precache_driver(
            planner=planner,  # type: ignore[arg-type]
            buckets=(self.bucket,),
            templates=(self.template,),
            rag_documents_path=self.rag_documents_path,
            run_config=self._run_config(),
        )

        self.assertEqual(1, first.success_count)
        self.assertEqual(1, first.completed_cell_count)
        self.assertEqual(0, first.failure_event_count)
        written = read_precache_output(self.output_path)
        self.assertEqual(1, len(written))
        manifest = json.loads((self.run_dir / "run_manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(["cbd"], manifest["selected_bucket_ids"])
        self.assertEqual(["restaurant_only"], manifest["selected_template_ids"])
        status = self._read_status("cbd", "restaurant_only")
        self.assertEqual("completed", status["state"])
        self.assertEqual(1, status["success_count"])

        rerun_planner = FakePlanner([])
        second = await run_precache_driver(
            planner=rerun_planner,  # type: ignore[arg-type]
            buckets=(self.bucket,),
            templates=(self.template,),
            rag_documents_path=self.rag_documents_path,
            run_config=self._run_config(),
        )

        self.assertEqual(0, second.success_count)
        self.assertEqual(1, second.skipped_existing_cell_count)
        self.assertEqual(0, len(rerun_planner.requests))
        self.assertEqual(1, len(read_precache_output(self.output_path)))
        rerun_status = self._read_status("cbd", "restaurant_only")
        self.assertEqual("skipped_existing", rerun_status["state"])

    async def test_driver_records_duplicate_then_succeeds_with_new_signature(self) -> None:
        duplicate_signature = fsq_place_ids_sorted_signature(("restaurant-1",))
        planner = FakePlanner(
            [
                PrecachePlannerFailure(
                    reason=FAILURE_REASON_DUPLICATE,
                    detail="Generated plan matches an existing signature.",
                    signature=duplicate_signature,
                    model="anthropic/test-model",
                ),
                _success(bucket_id="cbd", template_id="restaurant_only", fsq_place_id="restaurant-2"),
            ]
        )

        result = await run_precache_driver(
            planner=planner,  # type: ignore[arg-type]
            buckets=(self.bucket,),
            templates=(self.template,),
            rag_documents_path=self.rag_documents_path,
            run_config=self._run_config(retries_per_cell=2),
        )

        self.assertEqual(1, result.success_count)
        self.assertEqual(1, result.failure_event_count)
        self.assertEqual(1, result.duplicate_failure_count)
        failures = read_precache_failures(self.failures_path)
        self.assertEqual([FAILURE_REASON_DUPLICATE], failures["reason"].tolist())
        self.assertEqual(
            (duplicate_signature,),
            planner.requests[1].existing_plan_signatures,
        )
        status = self._read_status("cbd", "restaurant_only")
        self.assertEqual("completed", status["state"])
        self.assertEqual(1, status["duplicate_failure_count"])

    async def test_driver_records_recoverable_failures_and_retry_exhaustion(self) -> None:
        planner = FakePlanner(
            [
                PrecachePlannerFailure(
                    reason=FAILURE_REASON_OUTPUT_INVALID,
                    detail="Output did not match the expected schema.",
                    model="anthropic/test-model",
                ),
                PrecachePlannerFailure(
                    reason=FAILURE_REASON_OUTPUT_INVALID,
                    detail="Output did not match the expected schema.",
                    model="anthropic/test-model",
                ),
                PrecachePlannerFailure(
                    reason=FAILURE_REASON_OUTPUT_INVALID,
                    detail="Output did not match the expected schema.",
                    model="anthropic/test-model",
                ),
            ]
        )

        result = await run_precache_driver(
            planner=planner,  # type: ignore[arg-type]
            buckets=(self.bucket,),
            templates=(self.template,),
            rag_documents_path=self.rag_documents_path,
            run_config=self._run_config(retries_per_cell=2),
        )

        self.assertEqual(0, result.success_count)
        self.assertEqual(1, result.exhausted_cell_count)
        self.assertEqual(4, result.failure_event_count)
        failures = read_precache_failures(self.failures_path)
        self.assertEqual(
            [
                FAILURE_REASON_OUTPUT_INVALID,
                FAILURE_REASON_OUTPUT_INVALID,
                FAILURE_REASON_OUTPUT_INVALID,
                "retry_budget_exhausted",
            ],
            failures["reason"].tolist(),
        )
        status = self._read_status("cbd", "restaurant_only")
        self.assertEqual("exhausted", status["state"])
        self.assertEqual(4, status["failure_event_count"])

    async def test_driver_raises_on_fatal_planner_configuration_error(self) -> None:
        planner = FakePlanner([PrecachePlannerConfigurationError("bad planner config")])

        with self.assertRaises(PrecachePlannerConfigurationError):
            await run_precache_driver(
                planner=planner,  # type: ignore[arg-type]
                buckets=(self.bucket,),
                templates=(self.template,),
                rag_documents_path=self.rag_documents_path,
                run_config=self._run_config(),
            )

        status = self._read_status("cbd", "restaurant_only")
        self.assertEqual("fatal", status["state"])
        self.assertFalse(self.failures_path.exists())

    async def test_driver_fail_fast_raises_on_terminal_planner_failure(self) -> None:
        planner = FakePlanner(
            [
                PrecachePlannerFailure(
                    reason=FAILURE_REASON_EMPTY_POOL,
                    detail="Candidate pool for this bucket contains no allowed places.",
                    model="anthropic/test-model",
                )
            ]
        )

        with self.assertRaises(PrecacheDriverExecutionError):
            await run_precache_driver(
                planner=planner,  # type: ignore[arg-type]
                buckets=(self.bucket,),
                templates=(self.template,),
                rag_documents_path=self.rag_documents_path,
                run_config=self._run_config(fail_fast=True),
            )

        failures = read_precache_failures(self.failures_path)
        self.assertEqual([FAILURE_REASON_EMPTY_POOL], failures["reason"].tolist())
        status = self._read_status("cbd", "restaurant_only")
        self.assertEqual("terminal_failure", status["state"])

    def _run_config(
        self,
        *,
        retries_per_cell: int = 1,
        fail_fast: bool = False,
    ) -> RunConfig:
        return RunConfig(
            output_path=self.output_path,
            failures_path=self.failures_path,
            run_dir=self.run_dir,
            max_concurrency=2,
            retries_per_cell=retries_per_cell,
            fail_fast=fail_fast,
        )

    def _read_status(self, bucket_id: str, template_id: str) -> dict[str, Any]:
        return json.loads(
            (
                self.run_dir
                / "cells"
                / bucket_id
                / f"{template_id}.json"
            ).read_text(encoding="utf-8")
        )


def _rag_documents_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fsq_place_id": "restaurant-1",
                "name": "Restaurant One",
                "latitude": -33.8688,
                "longitude": 151.2093,
                "crawl4ai_quality_score": 8,
                "crawl4ai_template_stop_tags": ["restaurant"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
            },
            {
                "fsq_place_id": "restaurant-2",
                "name": "Restaurant Two",
                "latitude": -33.8690,
                "longitude": 151.2095,
                "crawl4ai_quality_score": 7,
                "crawl4ai_template_stop_tags": ["restaurant"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
            },
        ]
    )


def _success(
    *,
    bucket_id: str,
    template_id: str,
    fsq_place_id: str,
) -> PrecachePlannerSuccess:
    plan = PrecachePlanOutput(
        bucket_id=bucket_id,
        template_id=template_id,
        bucket_metadata={
            "label": "CBD",
            "latitude": -33.8688,
            "longitude": 151.2093,
            "radius_km": 2.0,
            "transport_mode": "walking",
            "tags": ["dense"],
        },
        template_metadata={
            "title": "Restaurant only",
            "vibe": ["romantic"],
            "time_of_day": "evening",
            "weather_sensitive": False,
            "duration_hours": 1.5,
            "description": "Single-stop dinner date.",
        },
        plan_title=f"Venue {fsq_place_id}",
        plan_hook="A single-stop dinner date.",
        plan_time_iso="2026-04-25T19:30:00+10:00",
        stops=(
            {
                "kind": "venue",
                "stop_type": "restaurant",
                "fsq_place_id": fsq_place_id,
                "name": f"Venue {fsq_place_id}",
            },
        ),
        search_text=f"Venue {fsq_place_id} dinner date",
        card_payload={
            "version": 1,
            "plan_title": f"Venue {fsq_place_id}",
            "plan_hook": "A single-stop dinner date.",
            "plan_time_iso": "2026-04-25T19:30:00+10:00",
            "search_text": f"Venue {fsq_place_id} dinner date",
            "stops": [],
            "legs": [],
            "feasibility": {},
        },
        verification={"status": "verified"},
        generated_at_utc=datetime(2026, 4, 18, 10, 0, tzinfo=UTC),
        model="anthropic/test-model",
    )
    return PrecachePlannerSuccess(
        plan=plan,
        idea=_idea_stub(fsq_place_id),
        signature=fsq_place_ids_sorted_signature((fsq_place_id,)),
        verification={"status": "verified"},
        tool_executions=(),
        raw_output={"date_ideas": []},
        model="anthropic/test-model",
    )


def _idea_stub(fsq_place_id: str) -> Any:
    return type(
        "IdeaStub",
        (),
        {
            "stops": (
                type(
                    "StopStub",
                    (),
                    {"kind": "venue", "fsq_place_id": fsq_place_id},
                )(),
            )
        },
    )()


if __name__ == "__main__":
    unittest.main()
