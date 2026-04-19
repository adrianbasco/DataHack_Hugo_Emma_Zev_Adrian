from __future__ import annotations

import json
import os
import unittest
import dataclasses
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx

from back_end.clients.api_trace import ApiTraceLogger
from back_end.clients.maps import (
    AmbiguousPlaceMatchError,
    GoogleMapsClient,
    MapsClientError,
    MapsResponseSchemaError,
    MapsUpstreamError,
    NoPlacePhotoError,
    NoPlaceMatchError,
)
from back_end.domain.models import (
    CandidatePlace,
    LatLng,
    MapsPlace,
    PhotoAsset,
    RouteRequest,
    TravelMode,
)
from back_end.clients.settings import MapsConfigurationError, MapsSettings


def _make_response(
    request: httpx.Request,
    status_code: int,
    payload: dict | None = None,
) -> httpx.Response:
    headers = {"Content-Type": "application/json"}
    content = json.dumps(payload or {}).encode("utf-8")
    return httpx.Response(
        status_code=status_code,
        headers=headers,
        content=content,
        request=request,
    )


class MapsSettingsTests(unittest.TestCase):
    def test_from_env_requires_api_key(self) -> None:
        old_value = os.environ.pop("MAPS_API_KEY", None)
        self.addCleanup(self._restore_env, "MAPS_API_KEY", old_value)

        with self.assertRaises(MapsConfigurationError):
            MapsSettings.from_env()

    @staticmethod
    def _restore_env(name: str, value: str | None) -> None:
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value


class GoogleMapsClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.settings = MapsSettings(api_key="test-key", retry_count=1)
        self.candidate = CandidatePlace(
            fsq_place_id="fsq-1",
            name="Cute Cafe",
            latitude=-37.8136,
            longitude=144.9631,
            address="1 Swanston St",
            locality="Melbourne",
            region="VIC",
            postcode="3000",
        )

    async def test_search_text_places_sends_expected_headers_and_body(self) -> None:
        captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            captured["field_mask"] = request.headers.get("X-Goog-FieldMask")
            captured["api_key"] = request.headers.get("X-Goog-Api-Key")
            captured["body"] = json.loads(request.content.decode("utf-8"))
            return _make_response(
                request,
                200,
                {
                    "places": [
                        {
                            "id": "g-1",
                            "name": "places/g-1",
                            "displayName": {"text": "Cute Cafe"},
                            "formattedAddress": "1 Swanston St, Melbourne VIC 3000",
                            "location": {
                                "latitude": -37.81361,
                                "longitude": 144.96311,
                            },
                            "postalAddress": {
                                "locality": "Melbourne",
                                "administrativeArea": "VIC",
                                "postalCode": "3000",
                            },
                        }
                    ]
                },
            )

        client = GoogleMapsClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        places = await client.search_text_places(self.candidate)

        self.assertEqual(1, len(places))
        self.assertEqual(
            "https://places.googleapis.com/v1/places:searchText",
            captured["url"],
        )
        self.assertEqual("test-key", captured["api_key"])
        self.assertIn("places.displayName", str(captured["field_mask"]))
        self.assertEqual(
            "Cute Cafe, 1 Swanston St, Melbourne, VIC, 3000",
            captured["body"]["textQuery"],
        )

    async def test_search_text_places_writes_api_trace_file(self) -> None:
        with TemporaryDirectory() as temp_dir:
            trace_path = Path(temp_dir) / "api_trace.jsonl"

            def handler(request: httpx.Request) -> httpx.Response:
                return _make_response(
                    request,
                    200,
                    {
                        "places": [
                            {
                                "id": "g-1",
                                "name": "places/g-1",
                                "displayName": {"text": "Cute Cafe"},
                                "location": {
                                    "latitude": -37.81361,
                                    "longitude": 144.96311,
                                },
                                "postalAddress": {
                                    "locality": "Melbourne",
                                    "administrativeArea": "VIC",
                                    "postalCode": "3000",
                                },
                            }
                        ]
                    },
                )

            client = GoogleMapsClient(
                self.settings,
                http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
                trace_logger=ApiTraceLogger(trace_path),
            )
            self.addAsyncCleanup(client.aclose)

            await client.search_text_places(self.candidate)

            entries = [
                json.loads(line)
                for line in trace_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(1, len(entries))
            entry = entries[0]
            self.assertEqual("google_maps", entry["service"])
            self.assertEqual(
                "__REDACTED__",
                entry["request"]["headers"]["X-Goog-Api-Key"],
            )
            self.assertEqual(
                "Cute Cafe, 1 Swanston St, Melbourne, VIC, 3000",
                entry["request"]["body"]["textQuery"],
            )
            self.assertEqual("g-1", entry["response"]["body"]["places"][0]["id"])

    async def test_resolve_place_match_returns_best_exact_match(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "places": [
                        {
                            "id": "far-away",
                            "name": "places/far-away",
                            "displayName": {"text": "Cute Cafe"},
                            "location": {
                                "latitude": -37.8200,
                                "longitude": 144.9900,
                            },
                            "postalAddress": {
                                "locality": "Melbourne",
                                "administrativeArea": "VIC",
                                "postalCode": "3000",
                            },
                        },
                        {
                            "id": "best",
                            "name": "places/best",
                            "displayName": {"text": "Cute Cafe"},
                            "location": {
                                "latitude": -37.81362,
                                "longitude": 144.96312,
                            },
                            "formattedAddress": "1 Swanston St, Melbourne VIC 3000",
                            "googleMapsUri": "https://maps.google.com/?cid=123",
                            "businessStatus": "OPERATIONAL",
                            "rating": 4.6,
                            "userRatingCount": 312,
                            "regularOpeningHours": {
                                "openNow": True,
                                "weekdayDescriptions": ["Monday: 8:00 AM – 5:00 PM"],
                            },
                            "photos": [
                                {
                                    "name": "places/best/photos/photo-1",
                                    "widthPx": 1200,
                                    "heightPx": 800,
                                    "authorAttributions": [],
                                }
                            ],
                            "postalAddress": {
                                "locality": "Melbourne",
                                "administrativeArea": "VIC",
                                "postalCode": "3000",
                            },
                        },
                    ]
                },
            )

        client = GoogleMapsClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        match = await client.resolve_place_match(self.candidate)

        self.assertEqual("best", match.google_place.place_id)
        self.assertLess(match.straight_line_distance_meters, 50.0)
        self.assertEqual(True, match.google_place.regular_opening_hours.open_now)
        self.assertEqual(1, len(match.google_place.photos))

    async def test_resolve_place_match_accepts_region_aliases(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "places": [
                        {
                            "id": "best",
                            "name": "places/best",
                            "displayName": {"text": "Cute Cafe"},
                            "location": {
                                "latitude": -37.81362,
                                "longitude": 144.96312,
                            },
                            "postalAddress": {
                                "locality": "Melbourne",
                                "administrativeArea": "Victoria",
                                "postalCode": "3000",
                            },
                        }
                    ]
                },
            )

        client = GoogleMapsClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        match = await client.resolve_place_match(self.candidate)

        self.assertEqual("best", match.google_place.place_id)

    async def test_resolve_place_match_rejects_ambiguous_candidates(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "places": [
                        {
                            "id": "a",
                            "name": "places/a",
                            "displayName": {"text": "Cute Cafe"},
                            "location": {
                                "latitude": -37.81360,
                                "longitude": 144.96315,
                            },
                            "postalAddress": {
                                "locality": "Melbourne",
                                "administrativeArea": "VIC",
                                "postalCode": "3000",
                            },
                        },
                        {
                            "id": "b",
                            "name": "places/b",
                            "displayName": {"text": "Cute Cafe"},
                            "location": {
                                "latitude": -37.81361,
                                "longitude": 144.96316,
                            },
                            "postalAddress": {
                                "locality": "Melbourne",
                                "administrativeArea": "VIC",
                                "postalCode": "3000",
                            },
                        },
                    ]
                },
            )

        client = GoogleMapsClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(AmbiguousPlaceMatchError):
            await client.resolve_place_match(self.candidate)

    async def test_resolve_place_match_rejects_low_similarity_name(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "places": [
                        {
                            "id": "bad",
                            "name": "places/bad",
                            "displayName": {"text": "Completely Different Venue"},
                            "location": {
                                "latitude": -37.81362,
                                "longitude": 144.96312,
                            },
                            "postalAddress": {
                                "locality": "Melbourne",
                                "administrativeArea": "VIC",
                                "postalCode": "3000",
                            },
                        }
                    ]
                },
            )

        client = GoogleMapsClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(NoPlaceMatchError):
            await client.resolve_place_match(self.candidate)

    async def test_resolve_place_match_accepts_branch_name_noise_when_address_matches(self) -> None:
        candidate = CandidatePlace(
            fsq_place_id="messina",
            name="Gelato Messina",
            latitude=-33.87869,
            longitude=151.20236,
            address="3 Little Hay St",
            locality="Haymarket",
            region="NSW",
            postcode="2000",
        )

        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "places": [
                        {
                            "id": "messina-darling-square",
                            "name": "places/messina-darling-square",
                            "displayName": {"text": "Gelato Messina Darling Square"},
                            "formattedAddress": "Shop 02/3 Little Hay St, Haymarket NSW 2000",
                            "location": {
                                "latitude": -33.8786079,
                                "longitude": 151.2024991,
                            },
                            "postalAddress": {
                                "locality": "Haymarket",
                                "administrativeArea": "NSW",
                                "postalCode": "2000",
                            },
                        }
                    ]
                },
            )

        client = GoogleMapsClient(
            dataclasses.replace(self.settings, min_name_similarity=0.72),
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        match = await client.resolve_place_match(candidate)

        self.assertEqual("messina-darling-square", match.google_place.place_id)
        self.assertEqual("address_match", match.match_kind)

    async def test_resolve_place_match_accepts_max_brenner_world_square_address(self) -> None:
        candidate = CandidatePlace(
            fsq_place_id="max-world-square",
            name="Max Brenner Chocolate Bar",
            latitude=-33.877411,
            longitude=151.206912,
            address="644 George St",
            locality="Sydney",
            region="NSW",
            postcode="2000",
        )

        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "places": [
                        {
                            "id": "max-brenner-world-square",
                            "name": "places/max-brenner-world-square",
                            "displayName": {"text": "Max Brenner - World Square"},
                            "formattedAddress": (
                                "World Square Shopping Centre, Shop 1052 B/644 "
                                "George St, Sydney NSW 2000"
                            ),
                            "location": {
                                "latitude": -33.877437,
                                "longitude": 151.2068032,
                            },
                            "postalAddress": {
                                "locality": "Sydney",
                                "administrativeArea": "NSW",
                                "postalCode": "2000",
                            },
                        }
                    ]
                },
            )

        client = GoogleMapsClient(
            dataclasses.replace(self.settings, min_name_similarity=0.72),
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        match = await client.resolve_place_match(candidate)

        self.assertEqual("max-brenner-world-square", match.google_place.place_id)
        self.assertEqual("address_match", match.match_kind)

    async def test_resolve_place_match_rejects_stale_street_address(self) -> None:
        candidate = CandidatePlace(
            fsq_place_id="max-stale",
            name="Max Brenner Chocolate Bar",
            latitude=-33.865158,
            longitude=151.206886,
            address="60 Margaret St",
            locality="Sydney",
            region="NSW",
            postcode="2000",
        )

        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "places": [
                        {
                            "id": "max-brenner-world-square",
                            "name": "places/max-brenner-world-square",
                            "displayName": {"text": "Max Brenner - World Square"},
                            "formattedAddress": (
                                "World Square Shopping Centre, Shop 1052 B/644 "
                                "George St, Sydney NSW 2000"
                            ),
                            "location": {
                                "latitude": -33.877437,
                                "longitude": 151.2068032,
                            },
                            "postalAddress": {
                                "locality": "Sydney",
                                "administrativeArea": "NSW",
                                "postalCode": "2000",
                            },
                        }
                    ]
                },
            )

        client = GoogleMapsClient(
            dataclasses.replace(
                self.settings,
                min_name_similarity=0.72,
                max_match_distance_meters=350.0,
            ),
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(NoPlaceMatchError):
            await client.resolve_place_match(candidate)

    async def test_resolve_place_match_accepts_locality_drift_when_address_matches(self) -> None:
        candidate = CandidatePlace(
            fsq_place_id="belgian-cafe",
            name="Belgian Chocolate Café",
            latitude=-33.8591,
            longitude=151.2081,
            address="91 George St",
            locality="Sydney",
            region="NSW",
            postcode="2000",
        )

        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "places": [
                        {
                            "id": "belgian-cafe-google",
                            "name": "places/belgian-cafe-google",
                            "displayName": {"text": "Belgian Café"},
                            "formattedAddress": "91 George St, The Rocks NSW 2000",
                            "location": {
                                "latitude": -33.8591,
                                "longitude": 151.2081,
                            },
                            "postalAddress": {
                                "locality": "The Rocks",
                                "administrativeArea": "NSW",
                                "postalCode": "2000",
                            },
                        }
                    ]
                },
            )

        client = GoogleMapsClient(
            dataclasses.replace(self.settings, min_name_similarity=0.72),
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        match = await client.resolve_place_match(candidate)

        self.assertEqual("belgian-cafe-google", match.google_place.place_id)
        self.assertEqual("address_match", match.match_kind)

    async def test_resolve_place_match_can_fallback_when_street_differs_but_name_and_coords_match(self) -> None:
        candidate = CandidatePlace(
            fsq_place_id="rivareno",
            name="Rivareno Gelato Barangaroo",
            latitude=-33.86534,
            longitude=151.20157,
            address="33/4 Barangaroo Ave",
            locality="Barangaroo",
            region="NSW",
            postcode="2000",
        )

        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "places": [
                        {
                            "id": "rivareno-google",
                            "name": "places/rivareno-google",
                            "displayName": {"text": "Rivareno Gelato Barangaroo"},
                            "formattedAddress": "Shop 2/4 Watermans Quay, Barangaroo NSW 2000",
                            "location": {
                                "latitude": -33.86536,
                                "longitude": 151.20158,
                            },
                            "postalAddress": {
                                "locality": "Barangaroo",
                                "administrativeArea": "NSW",
                                "postalCode": "2000",
                            },
                        }
                    ]
                },
            )

        client = GoogleMapsClient(
            dataclasses.replace(self.settings, min_name_similarity=0.72),
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        match = await client.resolve_place_match(candidate)

        self.assertEqual("rivareno-google", match.google_place.place_id)
        self.assertEqual("coord_name_address_disagrees", match.match_kind)

    async def test_get_place_details_raises_on_malformed_payload(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "id": "g-1",
                    "name": "places/g-1",
                    "displayName": {"text": "Cute Cafe"},
                },
            )

        client = GoogleMapsClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(MapsResponseSchemaError):
            await client.get_place_details("g-1")

    async def test_get_photo_media_uses_skip_redirect_json_mode(self) -> None:
        captured_query: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured_query.update(dict(request.url.params))
            return _make_response(
                request,
                200,
                {
                    "name": "places/g-1/photos/photo-1/media",
                    "photoUri": "https://lh3.googleusercontent.com/photo-1",
                },
            )

        client = GoogleMapsClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        media = await client.get_photo_media("places/g-1/photos/photo-1")

        self.assertEqual(
            "https://lh3.googleusercontent.com/photo-1",
            media.photo_uri,
        )
        self.assertEqual("true", captured_query["skipHttpRedirect"])
        self.assertEqual("test-key", captured_query["key"])

    async def test_get_photo_media_rejects_malformed_photo_name(self) -> None:
        client = GoogleMapsClient(
            self.settings,
            http_client=httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda request: self.fail("unexpected HTTP request")
                )
            ),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(MapsClientError):
            await client.get_photo_media("https://example.com/not-a-photo")

    async def test_get_photo_media_rejects_zero_dimensions_without_defaulting(self) -> None:
        client = GoogleMapsClient(
            self.settings,
            http_client=httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda request: self.fail("unexpected HTTP request")
                )
            ),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(MapsClientError):
            await client.get_photo_media(
                "places/g-1/photos/photo-1",
                max_width_px=0,
            )

    async def test_get_primary_photo_media_uses_first_place_photo(self) -> None:
        captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            return _make_response(
                request,
                200,
                {
                    "name": "places/g-1/photos/primary/media",
                    "photoUri": "https://lh3.googleusercontent.com/primary",
                },
            )

        client = GoogleMapsClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)
        place = MapsPlace(
            place_id="g-1",
            resource_name="places/g-1",
            display_name="Cute Cafe",
            location=LatLng(latitude=-37.8136, longitude=144.9631),
            photos=(
                PhotoAsset(
                    name="places/g-1/photos/primary",
                    width_px=1200,
                    height_px=800,
                ),
                PhotoAsset(
                    name="places/g-1/photos/secondary",
                    width_px=1200,
                    height_px=800,
                ),
            ),
        )

        media = await client.get_primary_photo_media(
            place,
            max_width_px=640,
            max_height_px=480,
        )

        self.assertEqual("https://lh3.googleusercontent.com/primary", media.photo_uri)
        self.assertEqual(
            "https://places.googleapis.com/v1/places/g-1/photos/primary/media"
            "?maxWidthPx=640&maxHeightPx=480&skipHttpRedirect=true&key=test-key",
            captured["url"],
        )

    async def test_get_place_primary_photo_media_fetches_details_then_media(self) -> None:
        captured_paths: list[str] = []
        captured_detail_field_mask: list[str | None] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured_paths.append(request.url.path)
            if request.url.path == "/v1/places/g-1":
                captured_detail_field_mask.append(
                    request.headers.get("X-Goog-FieldMask")
                )
                return _make_response(
                    request,
                    200,
                    {
                        "id": "g-1",
                        "name": "places/g-1",
                        "displayName": {"text": "Cute Cafe"},
                        "location": {
                            "latitude": -37.8136,
                            "longitude": 144.9631,
                        },
                        "photos": [
                            {
                                "name": "places/g-1/photos/photo-1",
                                "widthPx": 1200,
                                "heightPx": 800,
                                "authorAttributions": [],
                            }
                        ],
                    },
                )
            if request.url.path == "/v1/places/g-1/photos/photo-1/media":
                return _make_response(
                    request,
                    200,
                    {
                        "name": "places/g-1/photos/photo-1/media",
                        "photoUri": "https://lh3.googleusercontent.com/photo-1",
                    },
                )
            self.fail(f"unexpected URL {request.url}")

        client = GoogleMapsClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        media = await client.get_place_primary_photo_media("g-1")

        self.assertEqual("https://lh3.googleusercontent.com/photo-1", media.photo_uri)
        self.assertEqual(
            [
                "/v1/places/g-1",
                "/v1/places/g-1/photos/photo-1/media",
            ],
            captured_paths,
        )
        self.assertIn("photos", captured_detail_field_mask[0])

    async def test_get_primary_photo_media_raises_when_place_has_no_photos(self) -> None:
        client = GoogleMapsClient(
            self.settings,
            http_client=httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda request: self.fail("unexpected HTTP request")
                )
            ),
        )
        self.addAsyncCleanup(client.aclose)
        place = MapsPlace(
            place_id="g-1",
            resource_name="places/g-1",
            display_name="No Photo Cafe",
            location=LatLng(latitude=-37.8136, longitude=144.9631),
        )

        with self.assertLogs("back_end.clients.maps", level="ERROR") as logs:
            with self.assertRaises(NoPlacePhotoError):
                await client.get_primary_photo_media(place)

        self.assertIn("has no photos", "\n".join(logs.output))

    async def test_photo_author_attributions_must_be_objects(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "places": [
                        {
                            "id": "g-1",
                            "name": "places/g-1",
                            "displayName": {"text": "Cute Cafe"},
                            "location": {
                                "latitude": -37.81361,
                                "longitude": 144.96311,
                            },
                            "photos": [
                                {
                                    "name": "places/g-1/photos/photo-1",
                                    "authorAttributions": ["not-an-object"],
                                }
                            ],
                        }
                    ]
                },
            )

        client = GoogleMapsClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(MapsResponseSchemaError):
            await client.search_text_places(self.candidate)

    async def test_compute_route_parses_legs_and_transit_steps(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode("utf-8"))
            self.assertEqual("TRANSIT", body["travelMode"])
            self.assertEqual("2026-04-18T08:00:00Z", body["departureTime"])
            return _make_response(
                request,
                200,
                {
                    "routes": [
                        {
                            "distanceMeters": 2100,
                            "duration": "1020s",
                            "staticDuration": "960s",
                            "polyline": {"encodedPolyline": "abcd"},
                            "warnings": ["Be prepared for delays"],
                            "legs": [
                                {
                                    "distanceMeters": 2100,
                                    "duration": "1020s",
                                    "staticDuration": "960s",
                                    "steps": [
                                        {
                                            "distanceMeters": 200,
                                            "staticDuration": "180s",
                                            "travelMode": "WALK",
                                            "navigationInstruction": {
                                                "instructions": "Walk to Stop A"
                                            },
                                        },
                                        {
                                            "distanceMeters": 1900,
                                            "staticDuration": "780s",
                                            "travelMode": "TRANSIT",
                                            "transitDetails": {
                                                "stopDetails": {
                                                    "arrivalStop": {"name": "Stop B"},
                                                    "departureStop": {"name": "Stop A"},
                                                },
                                                "headsign": "City",
                                                "stopCount": 5,
                                                "transitLine": {
                                                    "name": "Route 86",
                                                    "nameShort": "86",
                                                    "vehicle": {"type": "TRAM"},
                                                },
                                            },
                                        },
                                    ],
                                }
                            ],
                        }
                    ]
                },
            )

        client = GoogleMapsClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        route = await client.compute_route(
            RouteRequest(
                origin=LatLng(latitude=-37.8136, longitude=144.9631),
                destination=LatLng(latitude=-37.8100, longitude=144.9700),
                travel_mode=TravelMode.TRANSIT,
                departure_time=datetime(2026, 4, 18, 8, 0, tzinfo=timezone.utc),
            )
        )

        self.assertEqual(2100, route.distance_meters)
        self.assertEqual(1020.0, route.duration_seconds)
        self.assertEqual("abcd", route.polyline)
        self.assertEqual(1, len(route.legs))
        self.assertEqual(2, len(route.legs[0].steps))
        self.assertEqual(
            "Route 86", route.legs[0].steps[1].transit_details.line.name
        )

    async def test_compute_route_supports_bicycle_mode(self) -> None:
        captured_body: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured_body.update(json.loads(request.content.decode("utf-8")))
            return _make_response(
                request,
                200,
                {
                    "routes": [
                        {
                            "distanceMeters": 1200,
                            "duration": "360s",
                            "staticDuration": "360s",
                            "legs": [],
                        }
                    ]
                },
            )

        client = GoogleMapsClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        route = await client.compute_route(
            RouteRequest(
                origin=LatLng(latitude=-37.8136, longitude=144.9631),
                destination=LatLng(latitude=-37.8100, longitude=144.9700),
                travel_mode=TravelMode.BICYCLE,
            )
        )

        self.assertEqual("BICYCLE", captured_body["travelMode"])
        self.assertEqual(360.0, route.duration_seconds)

    async def test_request_retries_retryable_status_codes(self) -> None:
        attempts = {"count": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            attempts["count"] += 1
            if attempts["count"] == 1:
                return _make_response(request, 503, {"error": "temporary"})
            return _make_response(
                request,
                200,
                {
                    "places": [
                        {
                            "id": "g-1",
                            "name": "places/g-1",
                            "displayName": {"text": "Cute Cafe"},
                            "location": {
                                "latitude": -37.81361,
                                "longitude": 144.96311,
                            },
                            "postalAddress": {
                                "locality": "Melbourne",
                                "administrativeArea": "VIC",
                                "postalCode": "3000",
                            },
                        }
                    ]
                },
            )

        client = GoogleMapsClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        places = await client.search_text_places(self.candidate)

        self.assertEqual(2, attempts["count"])
        self.assertEqual("g-1", places[0].place_id)

    async def test_non_retryable_http_error_raises(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(request, 400, {"error": "bad request"})

        client = GoogleMapsClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(MapsUpstreamError):
            await client.search_text_places(self.candidate)
