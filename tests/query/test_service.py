from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from back_end.catalog.categories import Allowlist
from back_end.catalog.repository import PlacesRepository
from back_end.query.errors import (
    ConstraintValidationError,
    DatasetValidationError,
    LocationAmbiguityError,
)
from back_end.query.models import GenerateDatesRequest
from back_end.query.service import ConstraintNormalizer, PlaceQueryService
from back_end.query.settings import QuerySettings


def _make_allowlist() -> Allowlist:
    return Allowlist(
        by_vibe={
            "foodie": frozenset({"cat_food"}),
            "romantic": frozenset({"cat_romantic"}),
            "nerdy": frozenset({"cat_nerdy"}),
        },
        master=frozenset({"cat_food", "cat_romantic", "cat_nerdy"}),
        label_by_id={},
    )


def _base_places_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fsq_place_id": "place-1",
                "name": "City Wine Bar",
                "latitude": -37.8136,
                "longitude": 144.9631,
                "address": "1 Main St",
                "locality": "Melbourne",
                "region": "VIC",
                "postcode": "3000",
                "fsq_category_ids": ["cat_food"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
                "date_closed": None,
            },
            {
                "fsq_place_id": "place-2",
                "name": "Riverside Eats",
                "latitude": -37.8250,
                "longitude": 144.9655,
                "address": "2 River Rd",
                "locality": "Southbank",
                "region": "VIC",
                "postcode": "3006",
                "fsq_category_ids": ["cat_food"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
                "date_closed": None,
            },
            {
                "fsq_place_id": "place-3",
                "name": "Museum Late Night",
                "latitude": -37.8030,
                "longitude": 144.9710,
                "address": "3 Museum Ave",
                "locality": "Melbourne",
                "region": "VIC",
                "postcode": "3000",
                "fsq_category_ids": ["cat_nerdy"],
                "fsq_category_labels": ["Arts and Entertainment > Museum"],
                "date_closed": None,
            },
            {
                "fsq_place_id": "place-4",
                "name": "Closed Bistro",
                "latitude": -37.8140,
                "longitude": 144.9620,
                "address": "4 Old St",
                "locality": "Melbourne",
                "region": "VIC",
                "postcode": "3000",
                "fsq_category_ids": ["cat_food"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
                "date_closed": "2024-01-01",
            },
            {
                "fsq_place_id": "place-5",
                "name": "Richmond NSW Cafe",
                "latitude": -33.5990,
                "longitude": 150.7510,
                "address": "5 East St",
                "locality": "Richmond",
                "region": "NSW",
                "postcode": "2753",
                "fsq_category_ids": ["cat_food"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
                "date_closed": None,
            },
            {
                "fsq_place_id": "place-6",
                "name": "Richmond VIC Cafe",
                "latitude": -37.8180,
                "longitude": 145.0010,
                "address": "6 Swan St",
                "locality": "Richmond",
                "region": "VIC",
                "postcode": "3121",
                "fsq_category_ids": ["cat_food"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
                "date_closed": None,
            },
            {
                "fsq_place_id": "place-7",
                "name": "No Coords Cafe",
                "latitude": None,
                "longitude": None,
                "address": "7 Missing Ln",
                "locality": "Melbourne",
                "region": "VIC",
                "postcode": "3000",
                "fsq_category_ids": ["cat_food"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
                "date_closed": None,
            },
        ]
    )


class QueryToolTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.temp_path = Path(self.temp_dir.name)
        self.places_path = self.temp_path / "places.parquet"
        _base_places_df().to_parquet(self.places_path)
        self.settings = QuerySettings(
            places_parquet_path=self.places_path,
            categories_parquet_path=self.temp_path / "unused_categories.parquet",
            allowlist_seed_path=self.temp_path / "unused_allowlist.yaml",
            default_radius_km=2.0,
            default_candidate_limit=50,
            max_candidate_limit=100,
        )
        self.service = PlaceQueryService(
            repository=PlacesRepository(self.settings),
            allowlist=_make_allowlist(),
            settings=self.settings,
        )

    def test_query_filters_by_location_and_includes_nearby_suburb(self) -> None:
        result = self.service.query(
            GenerateDatesRequest(
                location="3000",
                vibes=("foodie",),
                radius_km=2.0,
                max_candidates=10,
            )
        )

        self.assertEqual(
            [candidate.fsq_place_id for candidate in result.candidates],
            ["place-1", "place-2"],
        )
        self.assertIsNone(result.empty_reason)

    def test_ambiguous_locality_requires_region(self) -> None:
        with self.assertRaises(LocationAmbiguityError):
            self.service.query(
                GenerateDatesRequest(
                    location="Richmond",
                    vibes=("foodie",),
                    radius_km=2.0,
                )
            )

    def test_locality_with_region_disambiguates_cleanly(self) -> None:
        result = self.service.query(
            GenerateDatesRequest(
                location="Richmond, VIC",
                vibes=("foodie",),
                radius_km=3.0,
            )
        )

        self.assertEqual([candidate.fsq_place_id for candidate in result.candidates], ["place-6"])
        self.assertEqual(result.resolved_location.region, "VIC")

    def test_closed_places_are_excluded(self) -> None:
        result = self.service.query(
            GenerateDatesRequest(
                location="3000",
                vibes=("foodie",),
                radius_km=1.0,
            )
        )

        self.assertNotIn("place-4", [candidate.fsq_place_id for candidate in result.candidates])
        open_stage = result.filter_summary[0]
        self.assertEqual(open_stage.stage, "open_places")
        self.assertEqual(open_stage.rejected, 1)

    def test_budget_is_explicitly_unsupported(self) -> None:
        result = self.service.query(
            GenerateDatesRequest(
                location="3000",
                vibes=("foodie",),
                radius_km=1.0,
                budget="$$",
            )
        )

        self.assertEqual(len(result.unsupported_constraints), 1)
        self.assertEqual(result.unsupported_constraints[0].field, "budget")
        self.assertEqual(result.filter_summary[3].status.value, "unsupported")

    def test_empty_results_are_explicit(self) -> None:
        result = self.service.query(
            GenerateDatesRequest(
                location="3000",
                vibes=("romantic",),
                radius_km=0.3,
            )
        )

        self.assertEqual(result.candidates, ())
        self.assertIsNotNone(result.empty_reason)

    def test_constraint_normalizer_rejects_invalid_inputs(self) -> None:
        normalizer = ConstraintNormalizer(self.settings)
        with self.assertRaises(ConstraintValidationError):
            normalizer.normalize(
                GenerateDatesRequest(
                    location="3000",
                    vibes=("foodie",),
                    radius_km=-1,
                )
            )
        with self.assertRaises(ConstraintValidationError):
            normalizer.normalize(
                GenerateDatesRequest(
                    location="3000",
                    vibes=("foodie",),
                    budget="cheap",
                )
            )

    def test_repository_rejects_malformed_category_ids(self) -> None:
        bad_places_path = self.temp_path / "bad_places.parquet"
        bad_df = pd.DataFrame(
            [
                {
                    "fsq_place_id": "bad-1",
                    "name": "Bad Cafe",
                    "latitude": -37.81,
                    "longitude": 144.96,
                    "address": "1 Bad St",
                    "locality": "Melbourne",
                    "region": "VIC",
                    "postcode": "3000",
                    "fsq_category_ids": "cat_food",
                    "fsq_category_labels": "Dining and Drinking > Restaurant",
                    "date_closed": None,
                }
            ]
        )
        bad_df.to_parquet(bad_places_path)
        bad_settings = QuerySettings(
            places_parquet_path=bad_places_path,
            categories_parquet_path=self.settings.categories_parquet_path,
            allowlist_seed_path=self.settings.allowlist_seed_path,
            default_radius_km=2.0,
            default_candidate_limit=50,
            max_candidate_limit=100,
        )

        with self.assertRaises(DatasetValidationError):
            _ = PlacesRepository(bad_settings).places_df


if __name__ == "__main__":
    unittest.main()
