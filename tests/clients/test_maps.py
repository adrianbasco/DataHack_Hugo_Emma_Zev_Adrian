from __future__ import annotations

import json
import os
import unittest
from datetime import datetime, timezone

import httpx

from back_end.clients.maps import (
    AmbiguousPlaceMatchError,
    GoogleMapsClient,
    MapsResponseSchemaError,
    MapsUpstreamError,
    NoPlaceMatchError,
)
from back_end.domain.models import CandidatePlace, LatLng, RouteRequest, TravelMode
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
        self.assertEqual("Cute Cafe, Melbourne, VIC, 3000", captured["body"]["textQuery"])

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
