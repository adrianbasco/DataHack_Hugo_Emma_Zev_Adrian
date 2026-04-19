from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from back_end.precache.factory import build_precache_inputs
from back_end.precache.settings import PrecacheSettings


class PrecacheFactoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.documents_path = self.root / "place_documents.parquet"
        self.embeddings_path = self.root / "place_embeddings.parquet"
        self.buckets_path = self.root / "location_buckets.yaml"
        self.templates_path = self.root / "date_templates.yaml"

        pd.DataFrame(
            [
                {
                    "fsq_place_id": "fsq-cafe",
                    "name": "Test Cafe",
                    "latitude": -33.8689,
                    "longitude": 151.2094,
                    "crawl4ai_quality_score": 8,
                    "crawl4ai_template_stop_tags": ["cafe"],
                    "fsq_category_labels": ["Dining and Drinking > Cafe"],
                }
            ]
        ).to_parquet(self.documents_path, index=False)
        pd.DataFrame([{"embedding_model": "local-hashing-v1:8"}]).to_parquet(
            self.embeddings_path,
            index=False,
        )
        self.buckets_path.write_text(
            """
buckets:
  - id: cbd
    label: CBD
    latitude: -33.8688
    longitude: 151.2093
    radius_km: 1.0
    transport_mode: walking
    minimum_plan_count: 1
    maximum_plan_count: 2
""",
            encoding="utf-8",
        )
        self.templates_path.write_text(
            """
templates:
  - id: coffee_and_stroll
    title: Coffee and a stroll
    vibe: [casual]
    time_of_day: morning
    duration_hours: 1.5
    meaningful_variations: 4
    weather_sensitive: true
    description: Coffee then a stroll.
    stops:
      - type: cafe
      - type: park_or_garden
        kind: connective
""",
            encoding="utf-8",
        )
        self.settings = PrecacheSettings(
            rag_documents_path=self.documents_path,
            rag_embeddings_path=self.embeddings_path,
            location_buckets_path=self.buckets_path,
            date_templates_path=self.templates_path,
            output_path=self.root / "plans.parquet",
            runs_root=self.root / "runs",
        )

    def test_build_precache_inputs_is_deterministic_for_same_seed(self) -> None:
        first = build_precache_inputs(
            settings=self.settings,
            plan_time_seed="seed-123",
        )
        second = build_precache_inputs(
            settings=self.settings,
            plan_time_seed="seed-123",
        )

        self.assertEqual(1, len(first.cells))
        self.assertEqual(first.reference_now, second.reference_now)
        self.assertEqual(
            first.cells[0].plan_time.plan_time_iso,
            second.cells[0].plan_time.plan_time_iso,
        )
        self.assertEqual(1, first.cells[0].pool_size)
        self.assertEqual(1, first.cells[0].budget)

    def test_build_precache_inputs_rejects_unknown_bucket_filter(self) -> None:
        with self.assertRaises(ValueError):
            build_precache_inputs(
                settings=self.settings,
                bucket_ids=("missing",),
                plan_time_seed="seed-123",
            )
