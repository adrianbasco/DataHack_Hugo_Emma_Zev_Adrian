from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from back_end.precache.output import (
    OUTPUT_COLUMNS,
    PrecacheOutputError,
    PrecachePlanOutput,
    append_precache_plans,
    fsq_place_ids_sorted_signature,
    make_plan_id,
    read_existing_signatures,
    read_precache_output,
)


class PrecacheOutputTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.output_path = self.root / "plans.parquet"

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


if __name__ == "__main__":
    unittest.main()
