from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from back_end.precache.output import (
    FAILURE_OUTPUT_COLUMNS,
    OUTPUT_COLUMNS,
    PrecacheFailureOutput,
    PrecacheFailureWriteResult,
    PrecacheOutputError,
    PrecachePlanOutput,
    append_precache_failures,
    append_precache_plans,
    build_precache_failure_output,
    fsq_place_ids_sorted_signature,
    make_failure_id,
    make_plan_id,
    read_precache_failures,
    read_existing_signatures,
    read_precache_output,
    summarize_failures_by_reason,
)

FAILURE_REASON_EMPTY_POOL = "empty_candidate_pool"


class PrecacheOutputTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.output_path = self.root / "plans.parquet"
        self.failure_output_path = self.root / "failures.parquet"

    def test_plan_id_and_signature_are_stable_for_sorted_fsq_ids(self) -> None:
        first = make_plan_id(
            bucket_id="cbd",
            template_id="coffee_and_stroll",
            fsq_place_ids=("fsq-b", "fsq-a"),
        )
        second = make_plan_id(
            bucket_id="cbd",
            template_id="coffee_and_stroll",
            fsq_place_ids=("fsq-a", "fsq-b"),
        )

        self.assertEqual(first, second)
        self.assertEqual(
            '["fsq-a","fsq-b"]',
            fsq_place_ids_sorted_signature(("fsq-b", "fsq-a")),
        )

    def test_failure_id_is_stable_for_same_cell(self) -> None:
        first = make_failure_id(
            bucket_id="cbd",
            template_id="coffee_and_stroll",
            plan_time_iso="2026-04-25T19:30:00+10:00",
            attempt_index=2,
        )
        second = make_failure_id(
            bucket_id="cbd",
            template_id="coffee_and_stroll",
            plan_time_iso="2026-04-25T19:30:00+10:00",
            attempt_index=2,
        )

        self.assertEqual(first, second)

    def test_append_and_read_output_dedupes_by_plan_id(self) -> None:
        first_result = append_precache_plans(
            [_plan(model="anthropic/test-model", verification={"status": "verified"})],
            output_path=self.output_path,
        )

        self.assertEqual(1, first_result.written_count)
        self.assertEqual(0, first_result.replaced_count)
        self.assertEqual(1, first_result.total_count)

        first_df = read_precache_output(self.output_path)
        self.assertEqual(list(OUTPUT_COLUMNS), list(first_df.columns))
        self.assertEqual("cbd", first_df.iloc[0]["bucket_id"])
        self.assertEqual("coffee_and_stroll", first_df.iloc[0]["template_id"])
        self.assertEqual('["casual","romantic"]', first_df.iloc[0]["vibe"])
        self.assertEqual(
            '["fsq-cafe","fsq-park"]',
            first_df.iloc[0]["fsq_place_ids_sorted"],
        )
        self.assertEqual(2, int(first_df.iloc[0]["fsq_place_id_count"]))
        self.assertEqual("verified", json.loads(first_df.iloc[0]["verification_json"])["status"])
        self.assertEqual(3, len(json.loads(first_df.iloc[0]["stops_json"])))

        second_result = append_precache_plans(
            [_plan(model="anthropic/replacement-model", verification={"status": "rechecked"})],
            output_path=self.output_path,
        )

        self.assertEqual(1, second_result.replaced_count)
        self.assertEqual(1, second_result.total_count)
        second_df = read_precache_output(self.output_path)
        self.assertEqual("anthropic/replacement-model", second_df.iloc[0]["model"])
        self.assertEqual(
            "rechecked",
            json.loads(second_df.iloc[0]["verification_json"])["status"],
        )

    def test_read_existing_signatures_filters_bucket_and_template(self) -> None:
        append_precache_plans(
            [
                _plan(bucket_id="cbd", template_id="coffee_and_stroll"),
                _plan(bucket_id="inner-west", template_id="coffee_and_stroll"),
                _plan(bucket_id="cbd", template_id="brunch_and_bookstore"),
            ],
            output_path=self.output_path,
        )

        self.assertEqual(
            {'["fsq-cafe","fsq-park"]'},
            read_existing_signatures(
                "cbd",
                "coffee_and_stroll",
                output_path=self.output_path,
            ),
        )

    def test_read_existing_signatures_returns_empty_set_for_missing_output(self) -> None:
        self.assertEqual(
            set(),
            read_existing_signatures(
                "cbd",
                "coffee_and_stroll",
                output_path=self.output_path,
            ),
        )

    def test_writer_rejects_venue_stop_without_fsq_place_id(self) -> None:
        bad_plan = _plan(
            stops=(
                {
                    "kind": "venue",
                    "stop_type": "cafe",
                    "name": "Missing FSQ",
                },
            )
        )

        with self.assertRaises(PrecacheOutputError):
            append_precache_plans([bad_plan], output_path=self.output_path)

    def test_writer_rejects_duplicate_fsq_place_ids(self) -> None:
        bad_plan = _plan(
            stops=(
                {
                    "kind": "venue",
                    "stop_type": "cafe",
                    "fsq_place_id": "fsq-cafe",
                    "name": "One",
                },
                {
                    "kind": "venue",
                    "stop_type": "bar",
                    "fsq_place_id": "fsq-cafe",
                    "name": "Two",
                },
            )
        )

        with self.assertRaises(PrecacheOutputError):
            append_precache_plans([bad_plan], output_path=self.output_path)

    def test_writer_rejects_non_parquet_output_path(self) -> None:
        with self.assertRaises(ValueError):
            append_precache_plans(
                [_plan()],
                output_path=self.root / "plans.csv",
            )

    def test_read_existing_signatures_rejects_non_parquet_output_path(self) -> None:
        with self.assertRaises(ValueError):
            read_existing_signatures(
                "cbd",
                "coffee_and_stroll",
                output_path=self.root / "plans.csv",
            )

    def test_reader_rejects_invalid_schema(self) -> None:
        pd.DataFrame([{"plan_id": "only-column"}]).to_parquet(self.output_path, index=False)

        with self.assertRaises(PrecacheOutputError):
            read_precache_output(self.output_path)

    def test_append_and_read_failures_dedupes_by_failure_id(self) -> None:
        first_result = append_precache_failures(
            [_failure_output(detail="First detail", model="anthropic/test-model")],
            output_path=self.failure_output_path,
        )

        self.assertIsInstance(first_result, PrecacheFailureWriteResult)
        self.assertEqual(1, first_result.written_count)
        self.assertEqual(0, first_result.replaced_count)
        self.assertEqual(1, first_result.total_count)

        first_df = read_precache_failures(self.failure_output_path)
        self.assertEqual(list(FAILURE_OUTPUT_COLUMNS), list(first_df.columns))
        self.assertEqual(0, int(first_df.iloc[0]["attempt_index"]))
        self.assertEqual(FAILURE_REASON_EMPTY_POOL, first_df.iloc[0]["reason"])
        self.assertEqual("First detail", first_df.iloc[0]["detail"])
        self.assertEqual([], json.loads(first_df.iloc[0]["rejected_ideas_json"]))
        self.assertEqual(0, int(first_df.iloc[0]["tool_executions_count"]))

        second_result = append_precache_failures(
            [_failure_output(detail="Replacement detail", model="anthropic/replacement-model")],
            output_path=self.failure_output_path,
        )

        self.assertEqual(1, second_result.replaced_count)
        self.assertEqual(1, second_result.total_count)
        second_df = read_precache_failures(self.failure_output_path)
        self.assertEqual("Replacement detail", second_df.iloc[0]["detail"])
        self.assertEqual("anthropic/replacement-model", second_df.iloc[0]["model"])

    def test_summarize_failures_by_reason_counts_rows(self) -> None:
        append_precache_failures(
            [
                _failure_output(attempt_index=0, reason=FAILURE_REASON_EMPTY_POOL),
                _failure_output(
                    attempt_index=1,
                    reason=FAILURE_REASON_EMPTY_POOL,
                    plan_time_iso="2026-04-26T19:30:00+10:00",
                ),
                _failure_output(
                    bucket_id="inner-west",
                    template_id="bookstore_then_bar",
                    plan_time_iso="2026-04-27T19:30:00+10:00",
                    attempt_index=0,
                    reason="agent_output_invalid",
                ),
            ],
            output_path=self.failure_output_path,
        )

        self.assertEqual(
            {
                "agent_output_invalid": 1,
                FAILURE_REASON_EMPTY_POOL: 2,
            },
            summarize_failures_by_reason(self.failure_output_path),
        )

    def test_summarize_failures_by_reason_returns_empty_dict_when_missing(self) -> None:
        self.assertEqual({}, summarize_failures_by_reason(self.failure_output_path))

    def test_failure_writer_persists_empty_candidate_pool_without_crashing(self) -> None:
        result = append_precache_failures(
            [
                _failure_output(
                    bucket_id="broken-bucket",
                    template_id="dinner_then_dessert",
                    plan_time_iso="2026-04-25T19:30:00+10:00",
                    reason=FAILURE_REASON_EMPTY_POOL,
                    detail="No candidate places survived filtering for this bucket.",
                )
            ],
            output_path=self.failure_output_path,
        )

        self.assertEqual(1, result.written_count)
        df = read_precache_failures(self.failure_output_path)
        self.assertEqual(1, len(df))
        self.assertEqual(FAILURE_REASON_EMPTY_POOL, df.iloc[0]["reason"])
        self.assertEqual("broken-bucket", df.iloc[0]["bucket_id"])

    def test_failure_reader_rejects_invalid_schema(self) -> None:
        pd.DataFrame([{"failure_id": "only-column"}]).to_parquet(
            self.failure_output_path,
            index=False,
        )

        with self.assertRaises(PrecacheOutputError):
            read_precache_failures(self.failure_output_path)

    def test_failure_writer_rejects_negative_attempt_index(self) -> None:
        with self.assertRaises(PrecacheOutputError):
            append_precache_failures(
                [_failure_output(attempt_index=-1)],
                output_path=self.failure_output_path,
            )

    def test_failure_writer_rejects_non_integer_attempt_index(self) -> None:
        with self.assertRaises(PrecacheOutputError):
            append_precache_failures(
                [_failure_output(attempt_index=1.5)],
                output_path=self.failure_output_path,
            )

    def test_failure_writer_rejects_non_parquet_output_path(self) -> None:
        with self.assertRaises(ValueError):
            append_precache_failures(
                [_failure_output()],
                output_path=self.root / "failures.csv",
            )

    def test_build_precache_failure_output_from_planner_failure(self) -> None:
        class _FailureLike:
            reason = FAILURE_REASON_EMPTY_POOL
            detail = "No candidate places survived filtering for this bucket."
            rejected_ideas = ("Idea A", "Idea B")
            tool_executions = ()
            signature = None
            model = "anthropic/test-model"

        failure = _FailureLike()

        record = build_precache_failure_output(
            bucket_id="broken-bucket",
            template_id="dinner_then_dessert",
            plan_time_iso="2026-04-25T19:30:00+10:00",
            attempt_index=3,
            failure=failure,
            generated_at_utc=datetime(2026, 4, 18, 10, 0, tzinfo=UTC),
        )

        self.assertEqual("broken-bucket", record.bucket_id)
        self.assertEqual("dinner_then_dessert", record.template_id)
        self.assertEqual(3, record.attempt_index)
        self.assertEqual(("Idea A", "Idea B"), tuple(record.rejected_ideas))
        self.assertEqual("anthropic/test-model", record.model)


def _plan(
    *,
    bucket_id: str = "cbd",
    template_id: str = "coffee_and_stroll",
    model: str = "anthropic/test-model",
    verification: dict[str, object] | None = None,
    stops: tuple[dict[str, object], ...] | None = None,
) -> PrecachePlanOutput:
    template_title = {
        "coffee_and_stroll": "Coffee and a stroll",
        "brunch_and_bookstore": "Brunch and a bookstore",
    }.get(template_id, "Template")
    return PrecachePlanOutput(
        bucket_id=bucket_id,
        template_id=template_id,
        bucket_metadata={
            "label": bucket_id.upper(),
            "latitude": -33.8688,
            "longitude": 151.2093,
            "radius_km": 2.0,
            "transport_mode": "walking",
            "tags": ["dense", "origin"],
        },
        template_metadata={
            "title": template_title,
            "vibe": ["casual", "romantic"],
            "time_of_day": "morning",
            "weather_sensitive": True,
            "duration_hours": 1.5,
            "description": "A simple test plan.",
        },
        stops=stops
        if stops is not None
        else (
            {
                "kind": "venue",
                "stop_type": "cafe",
                "fsq_place_id": "fsq-cafe",
                "name": "Cafe",
            },
            {
                "kind": "connective",
                "stop_type": "park_or_garden",
                "name": "Park stroll",
                "description": "Walk nearby.",
            },
            {
                "kind": "venue",
                "stop_type": "park",
                "fsq_place_id": "fsq-park",
                "name": "Park",
            },
        ),
        verification=verification if verification is not None else {"status": "verified"},
        generated_at_utc=datetime(2026, 4, 18, 10, 0, tzinfo=UTC),
        model=model,
    )


def _failure_output(
    *,
    bucket_id: str = "cbd",
    template_id: str = "coffee_and_stroll",
    plan_time_iso: str = "2026-04-25T19:30:00+10:00",
    attempt_index: int = 0,
    reason: str = FAILURE_REASON_EMPTY_POOL,
    detail: str = "No candidate places survived filtering for this bucket.",
    model: str | None = None,
    rejected_ideas: tuple[str, ...] = (),
    signature: str | None = None,
    tool_executions_count: int = 0,
) -> PrecacheFailureOutput:
    return PrecacheFailureOutput(
        bucket_id=bucket_id,
        template_id=template_id,
        plan_time_iso=plan_time_iso,
        attempt_index=attempt_index,
        reason=reason,
        detail=detail,
        rejected_ideas=rejected_ideas,
        signature=signature,
        tool_executions_count=tool_executions_count,
        model=model,
        generated_at_utc=datetime(2026, 4, 18, 10, 0, tzinfo=UTC),
    )


if __name__ == "__main__":
    unittest.main()
