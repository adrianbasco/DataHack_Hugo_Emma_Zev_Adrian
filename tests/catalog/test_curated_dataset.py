from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

import pandas as pd

from back_end.catalog.curated_dataset import (
    DEFAULT_WEBSITE_EXEMPT_SEED_PATHS,
    OUTPUT_COLUMNS,
    build_curated_places_dataset,
)


def _taxonomy_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "category_id": "cat_restaurant",
                "category_label": "Dining and Drinking > Restaurant",
                "category_level": 1,
                "level1_category_id": "cat_restaurant",
            },
            {
                "category_id": "cat_beach",
                "category_label": "Landmarks and Outdoors > Beach",
                "category_level": 1,
                "level1_category_id": "cat_beach",
            },
            {
                "category_id": "cat_nudist_beach",
                "category_label": "Landmarks and Outdoors > Beach > Nudist Beach",
                "category_level": 2,
                "level1_category_id": "cat_beach",
                "level2_category_id": "cat_nudist_beach",
            },
            {
                "category_id": "cat_hiking",
                "category_label": "Landmarks and Outdoors > Hiking Trail",
                "category_level": 1,
                "level1_category_id": "cat_hiking",
            },
            {
                "category_id": "cat_bookstore",
                "category_label": "Retail > Bookstore",
                "category_level": 1,
                "level1_category_id": "cat_bookstore",
            },
        ]
    )


def _places_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fsq_place_id": "place-website-restaurant",
                "name": "Sydney Dinner Club",
                "latitude": -33.8700,
                "longitude": 151.2070,
                "address": "1 Harbour St",
                "locality": "Sydney",
                "region": "NSW",
                "postcode": "2000",
                "date_refreshed": "2025-08-01",
                "website": "https://dinner.example.com",
                "date_closed": None,
                "fsq_category_ids": ["cat_restaurant"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
            },
            {
                "fsq_place_id": "place-no-website-beach-descendant",
                "name": "Quiet Cove",
                "latitude": -33.8910,
                "longitude": 151.2760,
                "address": "2 Sand Rd",
                "locality": "Bondi",
                "region": "NSW",
                "postcode": "2026",
                "date_refreshed": "2025-09-15",
                "website": None,
                "date_closed": None,
                "fsq_category_ids": ["cat_nudist_beach"],
                "fsq_category_labels": ["Landmarks and Outdoors > Beach > Nudist Beach"],
            },
            {
                "fsq_place_id": "place-no-website-restaurant",
                "name": "Mystery Noodles",
                "latitude": -33.8720,
                "longitude": 151.2100,
                "address": "3 George St",
                "locality": "Sydney",
                "region": "NSW",
                "postcode": "2000",
                "date_refreshed": "2025-06-11",
                "website": None,
                "date_closed": None,
                "fsq_category_ids": ["cat_restaurant"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
            },
            {
                "fsq_place_id": "place-old-website",
                "name": "Old Cafe",
                "latitude": -33.8680,
                "longitude": 151.2050,
                "address": "4 York St",
                "locality": "Sydney",
                "region": "NSW",
                "postcode": "2000",
                "date_refreshed": "2023-01-01",
                "website": "https://old.example.com",
                "date_closed": None,
                "fsq_category_ids": ["cat_restaurant"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
            },
            {
                "fsq_place_id": "place-closed-website",
                "name": "Closed Bar",
                "latitude": -33.8690,
                "longitude": 151.2060,
                "address": "5 Pitt St",
                "locality": "Sydney",
                "region": "NSW",
                "postcode": "2000",
                "date_refreshed": "2025-07-20",
                "website": "https://closed.example.com",
                "date_closed": "2025-08-01",
                "fsq_category_ids": ["cat_restaurant"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
            },
            {
                "fsq_place_id": "place-far-website",
                "name": "Far Bookstore",
                "latitude": -32.9270,
                "longitude": 151.7760,
                "address": "6 Hunter St",
                "locality": "Newcastle",
                "region": "NSW",
                "postcode": "2300",
                "date_refreshed": "2025-08-30",
                "website": "https://far.example.com",
                "date_closed": None,
                "fsq_category_ids": ["cat_bookstore"],
                "fsq_category_labels": ["Retail > Bookstore"],
            },
        ]
    )


class CuratedDatasetTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.temp_path = Path(self.temp_dir.name)

        self.places_path = self.temp_path / "places.parquet"
        self.taxonomy_path = self.temp_path / "taxonomy.parquet"
        self.seed_path = self.temp_path / "seed.yaml"

        _places_df().to_parquet(self.places_path)
        _taxonomy_df().to_parquet(self.taxonomy_path)
        self.seed_path.write_text(
            textwrap.dedent(
                """
                vibes:
                  romantic:
                    - "Dining and Drinking > Restaurant"
                    - "Landmarks and Outdoors > Beach"
                  foodie:
                    - "Dining and Drinking > Restaurant"
                  nightlife:
                    - "Dining and Drinking > Restaurant"
                  nerdy:
                    - "Retail > Bookstore"
                  outdoorsy:
                    - "Landmarks and Outdoors > Beach"
                    - "Landmarks and Outdoors > Hiking Trail"
                  active:
                    - "Landmarks and Outdoors > Hiking Trail"
                  casual:
                    - "Dining and Drinking > Restaurant"
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

    def test_build_curated_places_dataset_requires_website_unless_exempt(self) -> None:
        curated = build_curated_places_dataset(
            places_path=self.places_path,
            taxonomy_path=self.taxonomy_path,
            seed_path=self.seed_path,
            location_text="Sydney, NSW",
            radius_km=60.0,
            refreshed_since="2024-04-18",
            exempt_seed_paths=DEFAULT_WEBSITE_EXEMPT_SEED_PATHS,
        )

        self.assertEqual(list(curated.columns), list(OUTPUT_COLUMNS))
        self.assertEqual(
            curated["fsq_place_id"].tolist(),
            ["place-website-restaurant", "place-no-website-beach-descendant"],
        )
        by_id = curated.set_index("fsq_place_id")
        self.assertTrue(bool(by_id.loc["place-website-restaurant", "has_website"]))
        self.assertFalse(bool(by_id.loc["place-website-restaurant", "is_website_exempt"]))
        self.assertFalse(bool(by_id.loc["place-no-website-beach-descendant", "has_website"]))
        self.assertTrue(bool(by_id.loc["place-no-website-beach-descendant", "is_website_exempt"]))

    def test_build_curated_places_dataset_rejects_empty_results(self) -> None:
        no_match_path = self.temp_path / "no_match.parquet"
        _places_df().iloc[[2]].to_parquet(no_match_path)

        with self.assertRaisesRegex(ValueError, "produced 0 rows"):
            build_curated_places_dataset(
                places_path=no_match_path,
                taxonomy_path=self.taxonomy_path,
                seed_path=self.seed_path,
                location_text="Sydney, NSW",
                radius_km=60.0,
                refreshed_since="2024-04-18",
                exempt_seed_paths=DEFAULT_WEBSITE_EXEMPT_SEED_PATHS,
            )


if __name__ == "__main__":
    unittest.main()
