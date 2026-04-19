from __future__ import annotations

import unittest

from back_end.precache.models import CandidatePoolPlace, LocationBucket, LocationCandidatePool
from scripts.build_date_cache import (
    _bucket_score,
    _diversified_pool,
    _generation_slots_for_template,
    _target_count_for_template,
    _variant_anchor,
)


class BuildDateCacheTests(unittest.TestCase):
    def test_target_count_respects_meaningful_variations(self) -> None:
        self.assertEqual(
            1,
            _target_count_for_template(
                {"meaningful_variations": 3},
                override=0,
                minimum=1,
                maximum=4,
            ),
        )
        self.assertEqual(
            2,
            _target_count_for_template(
                {"meaningful_variations": 8},
                override=0,
                minimum=1,
                maximum=4,
            ),
        )
        self.assertEqual(
            4,
            _target_count_for_template(
                {"meaningful_variations": 20},
                override=0,
                minimum=1,
                maximum=4,
            ),
        )

    def test_bucket_score_prefers_beach_bucket_for_beach_template(self) -> None:
        template = {
            "vibe": ["outdoorsy", "romantic"],
            "stops": [{"type": "beach"}, {"type": "cafe"}],
        }
        beach_bucket = LocationBucket(
            bucket_id="bondi",
            label="Bondi",
            latitude=-33.89,
            longitude=151.27,
            radius_km=2.5,
            transport_mode="walking",
            strategic_boost=1,
            tags=("origin", "destination", "beach"),
        )
        cbd_bucket = LocationBucket(
            bucket_id="sydney_cbd",
            label="Sydney CBD",
            latitude=-33.86,
            longitude=151.20,
            radius_km=2.5,
            transport_mode="walking",
            strategic_boost=6,
            tags=("origin", "destination", "dense"),
        )

        self.assertGreater(
            _bucket_score(template, beach_bucket),
            _bucket_score(template, cbd_bucket),
        )

    def test_bucket_score_prefers_bucket_with_real_stop_supply(self) -> None:
        template = {
            "vibe": ["nightlife", "foodie", "romantic"],
            "stops": [
                {"type": "cocktail_bar"},
                {"type": "restaurant"},
                {"type": "dessert_shop"},
            ],
        }
        dense_bucket = LocationBucket(
            bucket_id="sydney_cbd",
            label="Sydney CBD",
            latitude=-33.86,
            longitude=151.20,
            radius_km=2.5,
            transport_mode="walking",
            strategic_boost=6,
            tags=("origin", "destination", "dense"),
        )
        nightlife_bucket = LocationBucket(
            bucket_id="potts",
            label="Potts",
            latitude=-33.87,
            longitude=151.22,
            radius_km=1.8,
            transport_mode="walking",
            strategic_boost=5,
            tags=("destination", "dining", "nightlife"),
        )
        dense_pool = LocationCandidatePool(
            bucket=dense_bucket,
            places=(
                CandidatePoolPlace(
                    fsq_place_id="bar-1",
                    name="Bar 1",
                    latitude=-33.86,
                    longitude=151.20,
                    distance_km=0.1,
                    quality_score=9,
                    template_stop_tags=("cocktail_bar",),
                    category_labels=("Bar",),
                ),
                CandidatePoolPlace(
                    fsq_place_id="restaurant-1",
                    name="Restaurant 1",
                    latitude=-33.861,
                    longitude=151.201,
                    distance_km=0.2,
                    quality_score=8,
                    template_stop_tags=("restaurant",),
                    category_labels=("Restaurant",),
                ),
                CandidatePoolPlace(
                    fsq_place_id="dessert-1",
                    name="Dessert 1",
                    latitude=-33.862,
                    longitude=151.202,
                    distance_km=0.3,
                    quality_score=8,
                    template_stop_tags=("dessert_shop",),
                    category_labels=("Dessert",),
                ),
                CandidatePoolPlace(
                    fsq_place_id="dessert-2",
                    name="Dessert 2",
                    latitude=-33.863,
                    longitude=151.203,
                    distance_km=0.4,
                    quality_score=7,
                    template_stop_tags=("dessert_shop",),
                    category_labels=("Dessert",),
                ),
            ),
            target_plan_count=10,
        )
        nightlife_pool = LocationCandidatePool(
            bucket=nightlife_bucket,
            places=(
                CandidatePoolPlace(
                    fsq_place_id="bar-2",
                    name="Bar 2",
                    latitude=-33.87,
                    longitude=151.22,
                    distance_km=0.1,
                    quality_score=9,
                    template_stop_tags=("cocktail_bar",),
                    category_labels=("Bar",),
                ),
                CandidatePoolPlace(
                    fsq_place_id="restaurant-2",
                    name="Restaurant 2",
                    latitude=-33.871,
                    longitude=151.221,
                    distance_km=0.2,
                    quality_score=8,
                    template_stop_tags=("restaurant",),
                    category_labels=("Restaurant",),
                ),
            ),
            target_plan_count=6,
        )

        self.assertGreater(
            _bucket_score(template, dense_bucket, pool=dense_pool),
            _bucket_score(template, nightlife_bucket, pool=nightlife_pool),
        )

    def test_generation_slots_repeat_buckets_across_cycles(self) -> None:
        bucket = LocationBucket(
            bucket_id="bondi",
            label="Bondi",
            latitude=-33.89,
            longitude=151.27,
            radius_km=2.5,
            transport_mode="walking",
            tags=("beach",),
        )
        slots = _generation_slots_for_template(
            template={"vibe": ["outdoorsy"], "stops": [{"type": "beach"}]},
            buckets=[bucket],
            prebuilt_pools={
                "bondi": LocationCandidatePool(
                    bucket=bucket,
                    places=(
                        CandidatePoolPlace(
                            fsq_place_id="beach-1",
                            name="Beach 1",
                            latitude=-33.89,
                            longitude=151.27,
                            distance_km=0.1,
                            quality_score=9,
                            template_stop_tags=("beach",),
                            category_labels=("Beach",),
                        ),
                    ),
                    target_plan_count=4,
                )
            },
            fixed_bucket=None,
            cycles=3,
            bucket_limit=4,
        )

        self.assertEqual(3, len(slots))
        self.assertEqual([0, 1, 2], [slot.cycle_index for slot in slots])

    def test_diversified_pool_keeps_local_slice(self) -> None:
        bucket = LocationBucket(
            bucket_id="cbd",
            label="CBD",
            latitude=-33.8688,
            longitude=151.2093,
            radius_km=2.5,
            transport_mode="walking",
        )
        pool = LocationCandidatePool(
            bucket=bucket,
            places=tuple(
                CandidatePoolPlace(
                    fsq_place_id=f"place-{index}",
                    name=f"Place {index}",
                    latitude=-33.8688 + ((index % 20) - 10) * 0.001,
                    longitude=151.2093 + ((index // 20) - 5) * 0.001,
                    distance_km=index * 0.1,
                    quality_score=10 - index,
                    template_stop_tags=("restaurant",),
                    category_labels=("Dining",),
                )
                for index in range(200)
            ),
            target_plan_count=10,
        )

        variant = _diversified_pool(
            pool,
            seed=123,
            keep_ratio=0.5,
            min_places=50,
        )

        self.assertEqual(100, len(variant.places))
        self.assertNotEqual(
            tuple(place.fsq_place_id for place in pool.places[:100]),
            tuple(place.fsq_place_id for place in variant.places),
        )

    def test_variant_anchor_stays_near_bucket(self) -> None:
        bucket = LocationBucket(
            bucket_id="cbd",
            label="CBD",
            latitude=-33.8688,
            longitude=151.2093,
            radius_km=2.5,
            transport_mode="walking",
        )

        lat, lng = _variant_anchor(bucket=bucket, seed=42)

        self.assertLess(abs(lat - bucket.latitude), 0.05)
        self.assertLess(abs(lng - bucket.longitude), 0.05)


if __name__ == "__main__":
    unittest.main()
