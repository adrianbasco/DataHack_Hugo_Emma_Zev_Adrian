from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from back_end.llm.models import AgentToolExecution, OpenRouterMessage
from back_end.precache.run_state import (
    EVENT_ATTEMPT_STARTED,
    PrecacheCell,
    PrecacheRunState,
    PrecacheRunStateError,
    rebuild_precache_run_status,
)


class PrecacheRunStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.runs_root = Path(self.temp_dir.name) / "data" / "precache" / "runs"
        self.cells = (
            PrecacheCell(bucket_id="cbd", template_id="coffee_and_stroll"),
            PrecacheCell(bucket_id="cbd", template_id="dinner_then_dessert"),
            PrecacheCell(bucket_id="inner-west", template_id="gallery_then_wine"),
        )
        self.input_config = {
            "candidate_pools_path": "data/precache/candidate_pools.parquet",
            "templates_path": "config/date_templates.yaml",
            "max_retries_per_cell": 2,
        }

    def test_resolve_or_create_writes_manifest_and_initial_status(self) -> None:
        run = PrecacheRunState.resolve_or_create(
            cells=self.cells,
            input_config=self.input_config,
            planner_model="google/gemini-2.5-flash-lite",
            git_sha="abc1234",
            runs_root=self.runs_root,
            started_at_utc="2026-04-19T00:00:00Z",
        )

        self.assertTrue(run.paths.manifest_path.exists())
        self.assertTrue(run.paths.status_path.exists())
        self.assertTrue(run.paths.events_path.exists())
        self.assertTrue(run.run_id.startswith("2026-04-19T00-00-00Z-"))

        manifest = json.loads(run.paths.manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(run.run_id, manifest["run_id"])
        self.assertEqual("google/gemini-2.5-flash-lite", manifest["planner_model"])
        self.assertEqual("abc1234", manifest["git_sha"])
        self.assertEqual(3, manifest["cells_total"])
        self.assertEqual(
            [cell.to_manifest_dict() for cell in self.cells],
            manifest["cells"],
        )

        status = json.loads(run.paths.status_path.read_text(encoding="utf-8"))
        self.assertEqual("running", status["state"])
        self.assertEqual(3, status["cells_total"])
        self.assertEqual(0, status["cells_complete"])
        self.assertEqual(0, status["plans_written"])
        self.assertEqual(0, status["failures"])
        self.assertEqual(0, status["duplicates_avoided"])
        self.assertEqual(
            [cell.cell_id for cell in self.cells],
            status["pending_cell_ids"],
        )
        self.assertEqual(self.cells, run.remaining_cells())

    def test_events_drive_status_rebuild_and_resume_cursor(self) -> None:
        run = PrecacheRunState.resolve_or_create(
            cells=self.cells,
            input_config=self.input_config,
            planner_model="google/gemini-2.5-flash-lite",
            runs_root=self.runs_root,
            started_at_utc="2026-04-19T00:00:00Z",
        )

        first_cell = self.cells[0]
        second_cell = self.cells[1]

        run.record_attempt_started(first_cell, attempt_number=1, cost_usd=0.11)
        run.record_failure(
            first_cell,
            reason="verification_failed",
            signature='["restaurant-1"]',
            cost_usd=0.05,
            detail="Maps verification returned infeasible transit legs.",
        )
        run.record_retry(first_cell, reason="retry_after_verification_failure", attempt_number=2)
        run.record_success(first_cell, signature='["dessert-1","restaurant-1"]', cost_usd=0.31)
        run.record_cell_complete(first_cell, result="success")

        run.record_attempt_started(second_cell, attempt_number=1)
        run.record_duplicate_signature(
            second_cell,
            signature='["bar-1","gallery-1"]',
            reason="signature already exists in plans.parquet",
        )
        snapshot = run.record_cell_complete(
            second_cell,
            result="duplicate_signature",
            reason="signature already exists in plans.parquet",
        )

        self.assertEqual(3, snapshot.cells_total)
        self.assertEqual(2, snapshot.cells_complete)
        self.assertEqual(1, snapshot.plans_written)
        self.assertEqual(1, snapshot.failures)
        self.assertEqual(1, snapshot.duplicates_avoided)
        self.assertEqual(2, snapshot.attempts_started)
        self.assertEqual(1, snapshot.retries)
        self.assertAlmostEqual(0.47, snapshot.cost_usd_total)
        self.assertEqual(
            (
                self.cells[0].cell_id,
                self.cells[1].cell_id,
            ),
            snapshot.completed_cell_ids,
        )
        self.assertEqual((self.cells[2].cell_id,), snapshot.pending_cell_ids)
        self.assertEqual((self.cells[2],), run.remaining_cells())

        rebuilt = rebuild_precache_run_status(run.paths.run_dir)
        self.assertEqual(snapshot, rebuilt)

        event_lines = run.paths.events_path.read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(8, len(event_lines))
        self.assertEqual(EVENT_ATTEMPT_STARTED, json.loads(event_lines[0])["event_type"])

    def test_append_tool_executions_writes_jsonl_transcript(self) -> None:
        run = PrecacheRunState.resolve_or_create(
            cells=self.cells,
            input_config=self.input_config,
            planner_model="google/gemini-2.5-flash-lite",
            runs_root=self.runs_root,
            started_at_utc="2026-04-19T00:00:00Z",
        )

        transcript_path = run.append_tool_executions(
            self.cells[0],
            attempt_number=1,
            tool_executions=(
                AgentToolExecution(
                    call_id="call-1",
                    tool_name="lookup_places",
                    arguments={"query": "romantic dinner"},
                    output_text='{"places":["restaurant-1"]}',
                    tool_message=OpenRouterMessage(
                        role="tool",
                        content='{"places":["restaurant-1"]}',
                        tool_call_id="call-1",
                    ),
                ),
            ),
        )

        self.assertTrue(transcript_path.exists())
        lines = transcript_path.read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(1, len(lines))
        payload = json.loads(lines[0])
        self.assertEqual(self.cells[0].cell_id, payload["cell_id"])
        self.assertEqual("lookup_places", payload["tool_name"])
        self.assertEqual(1, payload["attempt_number"])
        self.assertEqual("call-1", payload["call_id"])

    def test_rebuild_rejects_event_for_unknown_cell(self) -> None:
        run = PrecacheRunState.resolve_or_create(
            cells=self.cells,
            input_config=self.input_config,
            planner_model="google/gemini-2.5-flash-lite",
            runs_root=self.runs_root,
            started_at_utc="2026-04-19T00:00:00Z",
        )

        run.paths.events_path.write_text(
            json.dumps(
                {
                    "event_type": "attempt_started",
                    "occurred_at_utc": "2026-04-19T00:00:01Z",
                    "cell_id": "unknown::template",
                    "bucket_id": "unknown",
                    "template_id": "template",
                }
            )
            + "\n",
            encoding="utf-8",
        )

        with self.assertRaises(PrecacheRunStateError):
            rebuild_precache_run_status(run.paths.run_dir)

    def test_interrupted_run_resumes_same_run_id_and_skips_completed_cells(self) -> None:
        seen_run_ids: list[str] = []

        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(
                self._drive_cells(
                    seen_run_ids=seen_run_ids,
                    stop_after=2,
                    started_at_utc="2026-04-19T00:00:00Z",
                )
            )

        first_run_id = seen_run_ids[0]
        resumed_processed = asyncio.run(
            self._drive_cells(
                seen_run_ids=seen_run_ids,
                stop_after=None,
                started_at_utc="2026-04-19T00:10:00Z",
            )
        )

        self.assertEqual(first_run_id, seen_run_ids[1])
        self.assertEqual([self.cells[2].cell_id], resumed_processed)

        run = PrecacheRunState.load(run_dir=self.runs_root / first_run_id)
        snapshot = run.snapshot()
        self.assertEqual("completed", snapshot.state)
        self.assertEqual(3, snapshot.cells_complete)
        self.assertEqual(3, snapshot.plans_written)
        self.assertEqual(0, len(run.remaining_cells()))

        manifest = json.loads(run.paths.manifest_path.read_text(encoding="utf-8"))
        self.assertIsNotNone(manifest["ended_at_utc"])

    async def _drive_cells(
        self,
        *,
        seen_run_ids: list[str],
        stop_after: int | None,
        started_at_utc: str,
    ) -> list[str]:
        run = PrecacheRunState.resolve_or_create(
            cells=self.cells,
            input_config=self.input_config,
            planner_model="google/gemini-2.5-flash-lite",
            runs_root=self.runs_root,
            started_at_utc=started_at_utc,
        )
        seen_run_ids.append(run.run_id)
        processed: list[str] = []
        for cell in run.remaining_cells():
            run.record_attempt_started(cell, attempt_number=1)
            run.record_success(
                cell,
                signature=json.dumps([f"signature:{cell.cell_id}"]),
                cost_usd=0.01,
            )
            run.record_cell_complete(cell, result="success")
            processed.append(cell.cell_id)
            await asyncio.sleep(0)
            if stop_after is not None and len(processed) >= stop_after:
                raise asyncio.CancelledError("simulated interruption after partial progress")

        if not run.remaining_cells():
            run.mark_finished()
        return processed
