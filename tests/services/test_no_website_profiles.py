from __future__ import annotations

import json
import unittest

import httpx
import numpy as np
import pandas as pd

from back_end.clients.brave import BraveLocalDescription, BraveLocalResult, BraveWebResult
from back_end.clients.settings import MapsSettings
from back_end.services.no_website_profiles import (
    NoWebsiteProfileClient,
    NoWebsiteProfileSettings,
    _feature_profile_from_llm,
    _accepted_local_results,
    _heuristic_profile,
    _parse_llm_json,
    _web_result_score,
)


def _make_response(
    request: httpx.Request,
    status_code: int,
    payload: dict | None = None,
) -> httpx.Response:
    return httpx.Response(
        status_code=status_code,
        headers={"Content-Type": "application/json"},
        content=json.dumps(payload or {}).encode("utf-8"),
        request=request,
    )


class NoWebsiteProfileHelpersTests(unittest.TestCase):
    def test_heuristic_profile_for_beach_is_outdoor_and_coastal(self) -> None:
        profile = _heuristic_profile(_bondi_row(), evidence=())

        self.assertEqual("beach", profile.venue_type)
        self.assertIn("coastal", profile.setting_tags)
        self.assertIn("swimming", profile.activity_tags)
        self.assertEqual("active_outdoor", profile.weather_exposure)
        self.assertIn("external_evidence", profile.missing_data)

    def test_heuristic_profile_accepts_parquet_array_category_values(self) -> None:
        row = {
            "fsq_place_id": "marina",
            "name": "Blackwattle Bay Mooring",
            "latitude": -33.872103,
            "longitude": 151.182638,
            "locality": "Glebe",
            "region": "NSW",
            "fsq_category_labels": np.array(["Landmarks and Outdoors > Harbor or Marina"]),
        }

        profile = _heuristic_profile(row, evidence=())

        self.assertEqual("harbor_or_marina", profile.venue_type)
        self.assertIn("waterfront", profile.setting_tags)
        self.assertIn("walking", profile.activity_tags)

    def test_web_result_score_prefers_authority_source_with_name_overlap(self) -> None:
        row = _barrenjoey_row()
        official = BraveWebResult(
            title="Barrenjoey Lighthouse walk",
            url="https://www.nationalparks.nsw.gov.au/things-to-do/walks/barrenjoey",
            description="Barrenjoey Track is a scenic walk at Palm Beach.",
            extra_snippets=("Outstanding views over Pittwater.",),
        )
        generic = BraveWebResult(
            title="Sydney attractions",
            url="https://example.com/sydney",
            description="Things to do in Sydney.",
            extra_snippets=(),
        )

        self.assertGreater(_web_result_score(row, official), 0.7)
        self.assertLess(_web_result_score(row, generic), 0.45)

    def test_local_identity_gate_rejects_distant_weak_match(self) -> None:
        row = {
            "name": "Turkish Gözleme",
            "latitude": -33.871511,
            "longitude": 151.160230,
            "fsq_category_labels": ["Dining and Drinking > Food Truck"],
        }
        results = (
            BraveLocalResult(
                brave_id="loc_1",
                title="Gozleme Turkish House",
                url=None,
                provider_url=None,
                latitude=-33.8557704,
                longitude=151.1634834,
                rating=None,
                categories=(),
                price_range=None,
            ),
        )

        self.assertEqual((), _accepted_local_results(row, results))

    def test_parse_llm_json_accepts_fenced_json(self) -> None:
        payload = _parse_llm_json('```json\n{"venue_type":"beach"}\n```')

        self.assertEqual({"venue_type": "beach"}, payload)

    def test_llm_profile_rejects_too_short_feature_text(self) -> None:
        fallback = _heuristic_profile(_bondi_row(), evidence=())

        profile = _feature_profile_from_llm(
            {"feature_text": "Bondi Beach", "confidence": 0.95},
            fallback=fallback,
        )

        self.assertEqual(profile.feature_text, fallback.feature_text)


class NoWebsiteProfileClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_enrich_dataframe_combines_maps_and_brave_without_llm(self) -> None:
        captured_maps_body: dict[str, object] = {}

        def maps_handler(request: httpx.Request) -> httpx.Response:
            captured_maps_body.update(json.loads(request.content.decode("utf-8")))
            return _make_response(
                request,
                200,
                {
                    "places": [
                        {
                            "id": "maps_bondi",
                            "displayName": {"text": "Bondi Beach"},
                            "location": {
                                "latitude": -33.8909,
                                "longitude": 151.2765,
                            },
                            "types": ["beach", "natural_feature", "establishment"],
                            "primaryTypeDisplayName": {"text": "Beach"},
                            "rating": 4.6,
                            "userRatingCount": 5440,
                            "googleMapsUri": "https://maps.example/bondi",
                            "editorialSummary": {
                                "text": "Popular surf spot with waterfront eateries."
                            },
                            "reviews": [
                                {"text": {"text": "Beautiful beach for a walk."}},
                            ],
                        }
                    ]
                },
            )

        client = NoWebsiteProfileClient(
            settings=NoWebsiteProfileSettings(
                use_llm=False,
                use_brave_local=False,
                global_concurrency=1,
            ),
            maps_settings=MapsSettings(api_key="maps-key"),
            brave_client=_FakeBraveClient(),
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(maps_handler)),
        )
        self.addAsyncCleanup(client.aclose)

        enriched = await client.enrich_dataframe(pd.DataFrame([_bondi_row()]))
        row = enriched.iloc[0]

        self.assertEqual("accepted", row["no_website_maps_match_status"])
        self.assertEqual("maps_bondi", row["no_website_maps_place_id"])
        self.assertIn("brave_web_accepted", row["no_website_source_statuses"])
        self.assertIn("waterfront", row["no_website_setting_tags"])
        self.assertIn("Popular surf spot", row["no_website_feature_text"])
        self.assertEqual("Bondi Beach NSW Australia", captured_maps_body["textQuery"])


class _FakeBraveClient:
    async def search_web(self, query: str, *, count: int = 5) -> tuple[BraveWebResult, ...]:
        return (
            BraveWebResult(
                title="Bondi Beach - Official tourism guide",
                url="https://www.sydney.com/destinations/sydney/bondi-beach",
                description="Bondi Beach is a famous Sydney beach and surf spot.",
                extra_snippets=("Waterfront walking and food nearby.",),
            ),
        )

    async def search_local(
        self,
        query: str,
        *,
        latitude: float,
        longitude: float,
        radius_meters: int = 1000,
        count: int = 3,
    ) -> tuple[BraveLocalResult, ...]:
        return ()

    async def get_local_descriptions(
        self,
        brave_ids: tuple[str, ...] | list[str],
    ) -> dict[str, BraveLocalDescription]:
        return {}


def _bondi_row() -> dict[str, object]:
    return {
        "fsq_place_id": "bondi",
        "name": "Bondi Beach",
        "latitude": -33.890863,
        "longitude": 151.276490,
        "address": None,
        "locality": "Bondi Beach",
        "region": "NSW",
        "postcode": "2026",
        "fsq_category_labels": [
            "Landmarks and Outdoors > Beach",
            "Landmarks and Outdoors > Surf Spot",
        ],
    }


def _barrenjoey_row() -> dict[str, object]:
    return {
        "fsq_place_id": "barrenjoey",
        "name": "Barrenjoey Track",
        "latitude": -33.579994,
        "longitude": 151.325200,
        "locality": "Palm Beach",
        "region": "NSW",
        "postcode": "2108",
        "fsq_category_labels": ["Landmarks and Outdoors > Hiking Trail"],
    }
