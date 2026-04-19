from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from back_end.precache.candidate_pools import (
    PrecacheCandidatePoolError,
    build_location_candidate_pool,
    load_location_buckets,
    plan_budget_for_pair,
)
from back_end.precache.models import (
    CandidatePoolPlace,
    LocationBucket,
    LocationCandidatePool,
)


class CandidatePoolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.documents_path = self.root / "place_documents.parquet"
        _documents_df().to_parquet(self.documents_path, index=False)

    def test_build_location_candidate_pool_filters_by_radius_and_scores_density(self) -> None:
        pool = build_location_candidate_pool(
            rag_documents_path=self.documents_path,
            bucket=LocationBucket(
                bucket_id="cbd",
                label="CBD",
                latitude=-33.8688,
                longitude=151.2093,
                radius_km=2.0,
                transport_mode="walking",
                minimum_plan_count=2,
                maximum_plan_count=10,
                strategic_boost=1,
            ),
            max_candidates=10,
        )

        self.assertIsNone(pool.empty_reason)
        self.assertEqual(
            [place.fsq_place_id for place in pool.places],
            ["near-high-quality", "near-low-quality"],
        )
        self.assertGreaterEqual(pool.target_plan_count, 2)
        self.assertLessEqual(pool.target_plan_count, 10)

    def test_build_location_candidate_pool_returns_explicit_empty_reason(self) -> None:
        pool = build_location_candidate_pool(
            rag_documents_path=self.documents_path,
            bucket=LocationBucket(
                bucket_id="empty",
                label="Empty",
                latitude=-34.5,
                longitude=150.0,
                radius_km=0.5,
                transport_mode="walking",
            ),
        )

        self.assertEqual(pool.places, ())
        self.assertIn("No RAG documents within", pool.empty_reason or "")

    def test_load_location_buckets_rejects_bad_yaml_shape(self) -> None:
        path = self.root / "locations.yaml"
        path.write_text("buckets: []\n", encoding="utf-8")

        with self.assertRaises(PrecacheCandidatePoolError):
            load_location_buckets(path)

    def test_load_location_buckets_parses_valid_yaml(self) -> None:
        path = self.root / "locations.yaml"
        path.write_text(
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
    strategic_boost: 3
    tags: [dense, origin]
""",
            encoding="utf-8",
        )

        buckets = load_location_buckets(path)

        self.assertEqual(1, len(buckets))
        self.assertEqual("cbd", buckets[0].bucket_id)
        self.assertEqual(("dense", "origin"), buckets[0].tags)

    def test_plan_budget_floors_at_bucket_minimum(self) -> None:
        bucket = _bucket(minimum_plan_count=5)
        pool = _pool(
            bucket=bucket,
            target_plan_count=2,
            places=[
                _place(f"restaurant-{index}", category_labels=("Restaurant",))
                for index in range(8)
            ],
        )

        budget = plan_budget_for_pair(
            bucket=bucket,
            template=_template(
                meaningful_variations=12,
                stops=[{"type": "restaurant"}],
            ),
            candidate_pool=pool,
        )

        self.assertEqual(5, budget)

    def test_plan_budget_caps_at_template_meaningful_variations(self) -> None:
        bucket = _bucket(minimum_plan_count=2)
        pool = _pool(
            bucket=bucket,
            target_plan_count=10,
            places=[
                _place(f"restaurant-{index}", category_labels=("Restaurant",))
                for index in range(8)
            ],
        )

        budget = plan_budget_for_pair(
            bucket=bucket,
            template=_template(
                meaningful_variations=3,
                stops=[{"type": "restaurant"}],
            ),
            candidate_pool=pool,
        )

        self.assertEqual(3, budget)

    def test_plan_budget_caps_at_candidate_pool_physical_capacity(self) -> None:
        bucket = _bucket(minimum_plan_count=2)
        pool = _pool(
            bucket=bucket,
            target_plan_count=10,
            places=[
                _place(f"restaurant-{index}", category_labels=("Restaurant",))
                for index in range(3)
            ]
            + [
                _place(f"cafe-{index}", template_stop_tags=("cafe",))
                for index in range(5)
            ],
        )

        budget = plan_budget_for_pair(
            bucket=bucket,
            template=_template(
                meaningful_variations=12,
                stops=[{"type": "restaurant"}, {"type": "cafe"}],
            ),
            candidate_pool=pool,
        )

        self.assertEqual(3, budget)

    def test_plan_budget_returns_zero_and_logs_when_stop_type_has_no_coverage(self) -> None:
        bucket = _bucket(minimum_plan_count=2)
        pool = _pool(
            bucket=bucket,
            target_plan_count=10,
            places=[_place("restaurant-1", category_labels=("Restaurant",))],
        )

        with self.assertLogs("back_end.precache.candidate_pools", level="ERROR") as logs:
            budget = plan_budget_for_pair(
                bucket=bucket,
                template=_template(
                    meaningful_variations=12,
                    stops=[{"type": "restaurant"}, {"type": "bookstore"}],
                ),
                candidate_pool=pool,
            )

        self.assertEqual(0, budget)
        self.assertIn("Plan budget is 0", "\n".join(logs.output))
        self.assertIn("bookstore", "\n".join(logs.output))

    def test_plan_budget_returns_zero_for_repeated_stop_without_distinct_places(self) -> None:
        bucket = _bucket(minimum_plan_count=1)
        pool = _pool(
            bucket=bucket,
            target_plan_count=4,
            places=[_place("restaurant-1", category_labels=("Restaurant",))],
        )

        with self.assertLogs("back_end.precache.candidate_pools", level="ERROR") as logs:
            budget = plan_budget_for_pair(
                bucket=bucket,
                template=_template(
                    meaningful_variations=4,
                    stops=[{"type": "restaurant"}, {"type": "restaurant"}],
                ),
                candidate_pool=pool,
            )

        self.assertEqual(0, budget)
        self.assertIn("not enough distinct places", "\n".join(logs.output))

    def test_plan_budget_returns_zero_for_unmapped_stop_type(self) -> None:
        bucket = _bucket(minimum_plan_count=1)
        pool = _pool(
            bucket=bucket,
            target_plan_count=4,
            places=[_place("anything-1", category_labels=("Venue",))],
        )

        with self.assertLogs("back_end.precache.candidate_pools", level="ERROR") as logs:
            budget = plan_budget_for_pair(
                bucket=bucket,
                template=_template(
                    meaningful_variations=4,
                    stops=[{"type": "totally_unknown_stop"}],
                ),
                candidate_pool=pool,
            )

        self.assertEqual(0, budget)
        self.assertIn("missing STOP_TYPE_KEYWORDS mapping", "\n".join(logs.output))


def _documents_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fsq_place_id": "near-high-quality",
                "name": "Near Good Restaurant",
                "latitude": -33.869,
                "longitude": 151.209,
                "crawl4ai_quality_score": 8,
                "crawl4ai_template_stop_tags": ["restaurant"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
            },
            {
                "fsq_place_id": "near-low-quality",
                "name": "Near OK Cafe",
                "latitude": -33.870,
                "longitude": 151.210,
                "crawl4ai_quality_score": 2,
                "crawl4ai_template_stop_tags": ["cafe"],
                "fsq_category_labels": ["Dining and Drinking > Cafe"],
            },
            {
                "fsq_place_id": "far-away",
                "name": "Far Away Bar",
                "latitude": -33.95,
                "longitude": 151.30,
                "crawl4ai_quality_score": 9,
                "crawl4ai_template_stop_tags": ["bar"],
                "fsq_category_labels": ["Dining and Drinking > Bar"],
            },
            {
                "fsq_place_id": "missing-coords",
                "name": "Missing Coords",
                "latitude": None,
                "longitude": None,
                "crawl4ai_quality_score": 10,
                "crawl4ai_template_stop_tags": ["restaurant"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
            },
        ]
    )


def _bucket(*, minimum_plan_count: int = 2) -> LocationBucket:
    return LocationBucket(
        bucket_id="cbd",
        label="CBD",
        latitude=-33.8688,
        longitude=151.2093,
        radius_km=2.0,
        transport_mode="walking",
        minimum_plan_count=minimum_plan_count,
        maximum_plan_count=20,
    )


def _pool(
    *,
    bucket: LocationBucket,
    target_plan_count: int,
    places: list[CandidatePoolPlace],
) -> LocationCandidatePool:
    return LocationCandidatePool(
        bucket=bucket,
        places=tuple(places),
        target_plan_count=target_plan_count,
    )


def _place(
    fsq_place_id: str,
    *,
    template_stop_tags: tuple[str, ...] = (),
    category_labels: tuple[str, ...] = (),
) -> CandidatePoolPlace:
    return CandidatePoolPlace(
        fsq_place_id=fsq_place_id,
        name=fsq_place_id.replace("-", " ").title(),
        latitude=-33.8688,
        longitude=151.2093,
        distance_km=0.1,
        quality_score=8,
        template_stop_tags=template_stop_tags,
        category_labels=category_labels,
    )


def _template(
    *,
    meaningful_variations: int,
    stops: list[dict[str, str]],
) -> dict[str, object]:
    return {
        "id": "template",
        "title": "Template",
        "meaningful_variations": meaningful_variations,
        "stops": stops,
    }


if __name__ == "__main__":
    unittest.main()
