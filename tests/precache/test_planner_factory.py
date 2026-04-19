from __future__ import annotations

import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from back_end.precache.planner_factory import (
    PrecachePlannerFactoryError,
    PrecachePlannerFactoryOverrides,
    PrecachePlannerFactoryResult,
    PrecacheSettings,
    build_precache_planner,
)


class PrecachePlannerFactoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.documents_path = self.root / "place_documents.parquet"
        self.embeddings_path = self.root / "place_embeddings.parquet"
        self.location_buckets_path = self.root / "location_buckets.yaml"
        self.date_templates_path = self.root / "date_templates.yaml"

        _documents_df().to_parquet(self.documents_path, index=False)
        _embeddings_df().to_parquet(self.embeddings_path, index=False)
        self.location_buckets_path.write_text(
            """
buckets:
  - id: cbd
    label: CBD
    latitude: -33.8688
    longitude: 151.2093
    radius_km: 2.0
    transport_mode: walking
    minimum_plan_count: 2
    maximum_plan_count: 8
    strategic_boost: 1
""",
            encoding="utf-8",
        )
        self.date_templates_path.write_text(
            """
templates:
  - id: dinner_and_walk
    title: Dinner and Walk
    meaningful_variations: 4
    time_of_day: evening
    vibe: [romantic]
    stops:
      - type: restaurant
      - type: scenic_lookout
        kind: connective
""",
            encoding="utf-8",
        )

    def test_precache_settings_names_missing_required_env_vars(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(PrecachePlannerFactoryError) as ctx:
                PrecacheSettings.from_env()

        self.assertIn("OPENROUTER_API_KEY", str(ctx.exception))
        self.assertIn("OPENROUTER_MODEL", str(ctx.exception))
        self.assertIn("MAPS_API_KEY", str(ctx.exception))
        self.assertIn("PRECACHE_RAG_DOCUMENTS_PATH", str(ctx.exception))
        self.assertIn("PRECACHE_RAG_EMBEDDINGS_PATH", str(ctx.exception))

    def test_build_precache_planner_rejects_missing_rag_documents_file(self) -> None:
        settings = self._settings(
            rag_documents_path=self.root / "missing_documents.parquet",
        )

        with self.assertRaises(PrecachePlannerFactoryError) as ctx:
            build_precache_planner(settings=settings)

        self.assertIn("RAG documents parquet not found", str(ctx.exception))
        self.assertIn("missing_documents.parquet", str(ctx.exception))

    def test_precache_settings_rejects_blank_required_env_var(self) -> None:
        with patch.dict(
            os.environ,
            {
                "OPENROUTER_API_KEY": "   ",
                "OPENROUTER_MODEL": "openai/gpt-4.1",
                "MAPS_API_KEY": "maps-key",
                "PRECACHE_RAG_DOCUMENTS_PATH": str(self.documents_path),
                "PRECACHE_RAG_EMBEDDINGS_PATH": str(self.embeddings_path),
            },
            clear=True,
        ):
            with self.assertRaises(PrecachePlannerFactoryError) as ctx:
                PrecacheSettings.from_env()

        self.assertIn("OPENROUTER_API_KEY", str(ctx.exception))
        self.assertIn("must not be empty", str(ctx.exception))

    def test_build_precache_planner_rejects_mixed_embedding_models(self) -> None:
        pd.DataFrame(
            [
                {
                    "fsq_place_id": "place-1",
                    "document_hash": "hash-1",
                    "embedding_model": "model-a",
                    "embedding_dimension": 2,
                    "embedding": [1.0, 0.0],
                },
                {
                    "fsq_place_id": "place-2",
                    "document_hash": "hash-2",
                    "embedding_model": "model-b",
                    "embedding_dimension": 2,
                    "embedding": [0.0, 1.0],
                },
            ]
        ).to_parquet(self.embeddings_path, index=False)
        settings = self._settings()

        with self.assertRaises(PrecachePlannerFactoryError) as ctx:
            build_precache_planner(settings=settings)

        self.assertIn("mixed embedding models", str(ctx.exception))
        self.assertIn(str(self.embeddings_path), str(ctx.exception))

    def test_build_precache_planner_returns_driver_ready_bundle(self) -> None:
        if importlib.util.find_spec("back_end.precache.output") is None:
            self.skipTest(
                "Current worktree is missing back_end.precache.output, so a real "
                "PrecachePlanner cannot be imported."
            )

        from back_end.agents.precache_planner import PrecachePlanner

        result = build_precache_planner(
            settings=self._settings(),
            overrides=PrecachePlannerFactoryOverrides(
                model="openai/gpt-4.1-mini",
                reasoning_effort="medium",
                max_tokens=1234,
                rag_default_top_k=6,
            ),
        )

        self.assertIsInstance(result, PrecachePlannerFactoryResult)
        self.assertIsInstance(result.planner, PrecachePlanner)
        self.assertEqual(["cbd"], [bucket.bucket_id for bucket in result.buckets])
        self.assertEqual(
            ["dinner_and_walk"],
            [str(template["id"]) for template in result.templates],
        )
        self.assertEqual(
            ["place-1", "place-2"],
            list(result.rag_documents_df["fsq_place_id"]),
        )

        planner = result.planner
        self.assertEqual("openai/gpt-4.1-mini", planner._model)
        self.assertEqual("medium", planner._reasoning_effort)
        self.assertEqual(1234, planner._max_tokens)
        self.assertEqual(6, planner._rag_default_top_k)

    def _settings(self, **overrides: object) -> PrecacheSettings:
        values: dict[str, object] = {
            "openrouter_api_key": "test-openrouter-key",
            "openrouter_model": "openai/gpt-4.1",
            "maps_api_key": "test-maps-key",
            "rag_documents_path": self.documents_path,
            "rag_embeddings_path": self.embeddings_path,
            "location_buckets_path": self.location_buckets_path,
            "date_templates_path": self.date_templates_path,
        }
        values.update(overrides)
        return PrecacheSettings(**values)


def _documents_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fsq_place_id": "place-1",
                "name": "Harbour Dinner",
                "document_hash": "hash-1",
                "document_text": "A romantic dinner restaurant with harbour views.",
                "crawl4ai_evidence_snippets": ["romantic dinner"],
                "latitude": -33.8688,
                "longitude": 151.2093,
                "crawl4ai_quality_score": 8,
                "crawl4ai_template_stop_tags": ["restaurant"],
                "fsq_category_labels": ["Restaurant"],
            },
            {
                "fsq_place_id": "place-2",
                "name": "Waterfront Walk",
                "document_hash": "hash-2",
                "document_text": "A scenic lookout for a waterfront stroll.",
                "crawl4ai_evidence_snippets": ["scenic lookout"],
                "latitude": -33.8690,
                "longitude": 151.2100,
                "crawl4ai_quality_score": 7,
                "crawl4ai_template_stop_tags": ["scenic_lookout"],
                "fsq_category_labels": ["Scenic Lookout"],
            },
        ]
    )


def _embeddings_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fsq_place_id": "place-1",
                "document_hash": "hash-1",
                "embedding_model": "local-hashing-v1:2",
                "embedding_dimension": 2,
                "embedding": [1.0, 0.0],
            },
            {
                "fsq_place_id": "place-2",
                "document_hash": "hash-2",
                "embedding_model": "local-hashing-v1:2",
                "embedding_dimension": 2,
                "embedding": [0.0, 1.0],
            },
        ]
    )
