from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

import pandas as pd

from back_end.catalog.dataset import (
    OUTPUT_COLUMNS,
    build_filtered_places_dataset,
    write_filtered_places_dataset,
)


def _taxonomy_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "category_id": "cat_romantic",
                "category_label": "Seed Romantic",
                "category_level": 1,
                "level1_category_id": "cat_romantic",
            },
            {
                "category_id": "cat_food",
                "category_label": "Seed Foodie",
                "category_level": 1,
                "level1_category_id": "cat_food",
            },
            {
                "category_id": "cat_nightlife",
                "category_label": "Seed Nightlife",
                "category_level": 1,
                "level1_category_id": "cat_nightlife",
            },
            {
                "category_id": "cat_nerdy",
                "category_label": "Seed Nerdy",
                "category_level": 1,
                "level1_category_id": "cat_nerdy",
            },
            {
                "category_id": "cat_outdoorsy",
                "category_label": "Seed Outdoorsy",
                "category_level": 1,
                "level1_category_id": "cat_outdoorsy",
            },
            {
                "category_id": "cat_active",
                "category_label": "Seed Active",
                "category_level": 1,
                "level1_category_id": "cat_active",
            },
            {
                "category_id": "cat_casual",
                "category_label": "Seed Casual",
                "category_level": 1,
                "level1_category_id": "cat_casual",
            },
            {
                "category_id": "cat_disallowed",
                "category_label": "Disallowed",
                "category_level": 1,
                "level1_category_id": "cat_disallowed",
            },
        ]
    )


def _places_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fsq_place_id": "place-1",
                "name": "Allowed Cafe",
                "latitude": -37.8136,
                "longitude": 144.9631,
                "address": "1 Main St",
                "locality": "Melbourne",
                "region": "VIC",
                "postcode": "3000",
                "fsq_category_ids": ["cat_food"],
                "fsq_category_labels": ["Seed Foodie"],
                "date_closed": None,
            },
            {
                "fsq_place_id": "place-2",
                "name": "Allowed Arcade",
                "latitude": -37.8150,
                "longitude": 144.9650,
                "address": "2 Game St",
                "locality": "Melbourne",
                "region": "VIC",
                "postcode": "3000",
                "fsq_category_ids": ["cat_active"],
                "fsq_category_labels": ["Seed Active"],
                "date_closed": None,
            },
            {
                "fsq_place_id": "place-3",
                "name": "Disallowed Office",
                "latitude": -37.8160,
                "longitude": 144.9660,
                "address": "3 Work St",
                "locality": "Melbourne",
                "region": "VIC",
                "postcode": "3000",
                "fsq_category_ids": ["cat_disallowed"],
                "fsq_category_labels": ["Disallowed"],
                "date_closed": None,
            },
        ]
    )


class FilteredDatasetTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.temp_path = Path(self.temp_dir.name)

        self.places_path = self.temp_path / "places.parquet"
        self.taxonomy_path = self.temp_path / "taxonomy.parquet"
        self.seed_path = self.temp_path / "seed.yaml"
        self.output_path = self.temp_path / "filtered.parquet"

        _places_df().to_parquet(self.places_path)
        _taxonomy_df().to_parquet(self.taxonomy_path)
        self.seed_path.write_text(
            textwrap.dedent(
                """
                vibes:
                  romantic:
                    - "Seed Romantic"
                  foodie:
                    - "Seed Foodie"
                  nightlife:
                    - "Seed Nightlife"
                  nerdy:
                    - "Seed Nerdy"
                  outdoorsy:
                    - "Seed Outdoorsy"
                  active:
                    - "Seed Active"
                  casual:
                    - "Seed Casual"
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

    def test_build_filtered_places_dataset_keeps_only_allowlisted_rows(self) -> None:
        filtered = build_filtered_places_dataset(
            places_path=self.places_path,
            taxonomy_path=self.taxonomy_path,
            seed_path=self.seed_path,
        )

        self.assertEqual(list(filtered.columns), list(OUTPUT_COLUMNS))
        self.assertEqual(filtered["fsq_place_id"].tolist(), ["place-1", "place-2"])

    def test_build_filtered_places_dataset_rejects_empty_results(self) -> None:
        only_disallowed_path = self.temp_path / "only_disallowed.parquet"
        _places_df().iloc[[2]].to_parquet(only_disallowed_path)

        with self.assertRaisesRegex(ValueError, "produced 0 rows"):
            build_filtered_places_dataset(
                places_path=only_disallowed_path,
                taxonomy_path=self.taxonomy_path,
                seed_path=self.seed_path,
            )

    def test_write_filtered_places_dataset_refuses_to_overwrite_by_default(self) -> None:
        filtered = build_filtered_places_dataset(
            places_path=self.places_path,
            taxonomy_path=self.taxonomy_path,
            seed_path=self.seed_path,
        )
        write_filtered_places_dataset(filtered, output_path=self.output_path)

        with self.assertRaisesRegex(FileExistsError, "Refusing to overwrite existing file"):
            write_filtered_places_dataset(filtered, output_path=self.output_path)

    def test_write_filtered_places_dataset_requires_parquet_output(self) -> None:
        filtered = build_filtered_places_dataset(
            places_path=self.places_path,
            taxonomy_path=self.taxonomy_path,
            seed_path=self.seed_path,
        )

        with self.assertRaisesRegex(ValueError, "must point to a .parquet file"):
            write_filtered_places_dataset(
                filtered,
                output_path=self.temp_path / "filtered.csv",
            )


if __name__ == "__main__":
    unittest.main()
