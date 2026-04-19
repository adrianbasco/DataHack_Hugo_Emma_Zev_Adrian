from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from back_end.agents.date_idea_agent import DateIdeaAgentToolError
from back_end.agents.maps_tools import (
    MapsComputeLegTool,
    MapsVerificationCache,
    MapsVerifyPlaceTool,
    MapsVerifyPlanTool,
)
from back_end.clients.maps import (
    AmbiguousPlaceMatchError,
    MapsResponseSchemaError,
    NoPlaceMatchError,
)
from back_end.domain.models import (
    CandidatePlace,
    ComputedRoute,
    LatLng,
    MapsOpeningHours,
    MapsPlace,
    MapsPlaceMatch,
    RouteRequest,
    TravelMode,
)


class FakeMapsClient:
    def __init__(self, outcome: MapsPlaceMatch | Exception) -> None:
        self.outcome = outcome
        self.calls = 0

    async def resolve_place_match(self, candidate: CandidatePlace) -> MapsPlaceMatch:
        self.calls += 1
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return MapsPlaceMatch(
            candidate_place=candidate,
            google_place=self.outcome.google_place,
            straight_line_distance_meters=self.outcome.straight_line_distance_meters,
            name_similarity=self.outcome.name_similarity,
        )


class MapsVerifyPlaceToolTests(unittest.IsolatedAsyncioTestCase):
    async def test_permanently_closed_venue_returns_matched_closed(self) -> None:
        client = FakeMapsClient(
            _match(
                business_status="CLOSED_PERMANENTLY",
                weekday_descriptions=("Saturday: 5:00 – 11:00 PM",),
            )
        )
        tool = MapsVerifyPlaceTool(
            maps_client=client,
            rag_documents=_documents_df(),
        )

        result = await tool.verify(
            {
                "fsq_place_id": "restaurant-1",
                "plan_time_iso": "2026-04-18T20:00:00",
            }
        )

        self.assertTrue(result["matched"])
        self.assertEqual("CLOSED_PERMANENTLY", result["business_status"])
        self.assertIs(result["open_at_plan_time"], False)
        self.assertIsNone(result["failure_reason"])

    async def test_no_place_match_returns_failure_reason(self) -> None:
        client = FakeMapsClient(NoPlaceMatchError("no confident match"))
        tool = MapsVerifyPlaceTool(
            maps_client=client,
            rag_documents=_documents_df(),
        )

        result = await tool.verify(
            {"fsq_place_id": "restaurant-1", "plan_time_iso": None}
        )

        self.assertFalse(result["matched"])
        self.assertIsNone(result["google_place_id"])
        self.assertIn("no confident match", result["failure_reason"])

    async def test_parquet_cache_persists_success_and_failure(self) -> None:
        with TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "maps_cache.parquet"
            success_cache = MapsVerificationCache(cache_path=cache_path)
            success_cache.put_place_match("restaurant-1", _match(business_status="OPERATIONAL"))

            failure_client = FakeMapsClient(NoPlaceMatchError("no confident match"))
            failure_tool = MapsVerifyPlaceTool(
                maps_client=failure_client,
                rag_documents=pd.concat(
                    [
                        _documents_df(),
                        pd.DataFrame(
                            [
                                {
                                    "fsq_place_id": "dessert-1",
                                    "name": "Dessert Spot",
                                    "latitude": -33.861,
                                    "longitude": 151.201,
                                    "locality": "Sydney",
                                    "region": "NSW",
                                    "postcode": "2000",
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                ),
                cache=success_cache,
            )
            await failure_tool.verify({"fsq_place_id": "dessert-1"})

            reloaded = MapsVerificationCache(cache_path=cache_path)
            cached_success = reloaded.get_place_match("restaurant-1")
            cached_failure = reloaded.get_place_match("dessert-1")

            self.assertIsInstance(cached_success, MapsPlaceMatch)
            assert isinstance(cached_success, MapsPlaceMatch)
            self.assertEqual("google-1", cached_success.google_place.place_id)
            self.assertIsNotNone(cached_failure)
            self.assertIn("no confident match", getattr(cached_failure, "reason", ""))

    async def test_ambiguous_place_match_returns_failure_reason(self) -> None:
        client = FakeMapsClient(AmbiguousPlaceMatchError("two plausible matches"))
        tool = MapsVerifyPlaceTool(
            maps_client=client,
            rag_documents=_documents_df(),
        )

        result = await tool.verify({"fsq_place_id": "restaurant-1"})

        self.assertFalse(result["matched"])
        self.assertIn("two plausible matches", result["failure_reason"])

    async def test_opening_hours_are_checked_when_plan_time_present(self) -> None:
        client = FakeMapsClient(
            _match(
                business_status="OPERATIONAL",
                weekday_descriptions=("Saturday: 5:00 – 11:00 PM",),
            )
        )
        tool = MapsVerifyPlaceTool(
            maps_client=client,
            rag_documents=_documents_df(),
        )

        result = await tool.verify(
            {
                "fsq_place_id": "restaurant-1",
                "plan_time_iso": "2026-04-18T20:00:00",
            }
        )

        self.assertIs(result["open_at_plan_time"], True)
        self.assertEqual(["Saturday: 5:00 – 11:00 PM"], result["weekday_descriptions"])

    async def test_null_plan_time_skips_opening_hours_check(self) -> None:
        client = FakeMapsClient(_match(business_status="OPERATIONAL"))
        tool = MapsVerifyPlaceTool(
            maps_client=client,
            rag_documents=_documents_df(),
        )

        result = await tool.verify(
            {"fsq_place_id": "restaurant-1", "plan_time_iso": None}
        )

        self.assertIsNone(result["open_at_plan_time"])

    async def test_cache_suppresses_second_google_call(self) -> None:
        client = FakeMapsClient(_match(business_status="OPERATIONAL"))
        cache = MapsVerificationCache()
        tool = MapsVerifyPlaceTool(
            maps_client=client,
            rag_documents=_documents_df(),
            cache=cache,
        )

        first = await tool.verify({"fsq_place_id": "restaurant-1"})
        second = await tool.verify({"fsq_place_id": "restaurant-1"})

        self.assertTrue(first["matched"])
        self.assertTrue(second["matched"])
        self.assertEqual(1, client.calls)


class MapsComputeLegToolTests(unittest.IsolatedAsyncioTestCase):
    async def test_identical_origin_destination_returns_tiny_distance(self) -> None:
        client = FakeRouteMapsClient()
        tool = MapsComputeLegTool(
            maps_client=client,
            rag_documents=_route_documents_df(),
            cache=MapsVerificationCache(),
        )

        result = await tool.compute_leg(
            {
                "from_fsq_place_id": "restaurant-1",
                "to_fsq_place_id": "restaurant-1",
                "travel_mode": "WALK",
                "departure_time_iso": "2026-04-19T19:00:00+10:00",
            }
        )

        self.assertEqual("restaurant-1", result["from_fsq_place_id"])
        self.assertEqual("restaurant-1", result["to_fsq_place_id"])
        self.assertEqual("WALK", result["travel_mode"])
        self.assertEqual(0, result["distance_meters"])
        self.assertEqual(0.0, result["duration_seconds"])
        self.assertEqual(0.0, result["static_duration_seconds"])
        self.assertEqual(["Origin and destination are the same."], result["warnings"])
        self.assertIsNone(result["failure_reason"])
        self.assertEqual(["restaurant-1"], client.resolve_calls)
        self.assertEqual(1, len(client.route_calls))
        self.assertEqual(TravelMode.WALK, client.route_calls[0].travel_mode)

    async def test_endpoint_match_failure_returns_failure_without_route_call(self) -> None:
        client = FakeRouteMapsClient(
            failures={
                "bad-place": NoPlaceMatchError(
                    "No confident Google place match for candidate bad-place."
                )
            }
        )
        tool = MapsComputeLegTool(
            maps_client=client,
            rag_documents=_route_documents_df(),
            cache=MapsVerificationCache(),
        )

        result = await tool.compute_leg(
            {
                "from_fsq_place_id": "bad-place",
                "to_fsq_place_id": "restaurant-1",
                "travel_mode": "DRIVE",
            }
        )

        self.assertEqual("bad-place", result["from_fsq_place_id"])
        self.assertEqual("restaurant-1", result["to_fsq_place_id"])
        self.assertEqual("DRIVE", result["travel_mode"])
        self.assertIsNone(result["distance_meters"])
        self.assertIsNone(result["duration_seconds"])
        self.assertIsNone(result["static_duration_seconds"])
        self.assertEqual([], result["warnings"])
        self.assertIn("bad-place", result["failure_reason"])
        self.assertEqual(["bad-place"], client.resolve_calls)
        self.assertEqual([], client.route_calls)

    async def test_invalid_travel_mode_raises_tool_error(self) -> None:
        client = FakeRouteMapsClient()
        tool = MapsComputeLegTool(
            maps_client=client,
            rag_documents=_route_documents_df(),
            cache=MapsVerificationCache(),
        )

        with self.assertRaises(DateIdeaAgentToolError):
            await tool.compute_leg(
                {
                    "from_fsq_place_id": "restaurant-1",
                    "to_fsq_place_id": "dessert-1",
                    "travel_mode": "FERRY",
                }
            )

        self.assertEqual([], client.resolve_calls)
        self.assertEqual([], client.route_calls)

    async def test_departure_time_requires_timezone(self) -> None:
        client = FakeRouteMapsClient()
        tool = MapsComputeLegTool(
            maps_client=client,
            rag_documents=_route_documents_df(),
            cache=MapsVerificationCache(),
        )

        with self.assertRaises(DateIdeaAgentToolError):
            await tool.compute_leg(
                {
                    "from_fsq_place_id": "restaurant-1",
                    "to_fsq_place_id": "dessert-1",
                    "travel_mode": "TRANSIT",
                    "departure_time_iso": "2026-04-19T19:00:00",
                }
            )

        self.assertEqual([], client.resolve_calls)
        self.assertEqual([], client.route_calls)

    async def test_route_schema_error_propagates(self) -> None:
        client = FakeRouteMapsClient(route_error=MapsResponseSchemaError("bad route"))
        tool = MapsComputeLegTool(
            maps_client=client,
            rag_documents=_route_documents_df(),
            cache=MapsVerificationCache(),
        )

        with self.assertRaises(MapsResponseSchemaError):
            await tool.compute_leg(
                {
                    "from_fsq_place_id": "restaurant-1",
                    "to_fsq_place_id": "dessert-1",
                    "travel_mode": "TRANSIT",
                    "departure_time_iso": "2026-04-19T19:00:00Z",
                }
            )

        self.assertEqual(["restaurant-1", "dessert-1"], client.resolve_calls)
        self.assertEqual(1, len(client.route_calls))


class MapsVerifyPlanToolTests(unittest.IsolatedAsyncioTestCase):
    async def test_three_stop_plan_reports_permanently_closed_stop_two(self) -> None:
        client = FakePlanMapsClient(closed_place_ids={"dessert-1"})
        tool = MapsVerifyPlanTool(
            maps_client=client,
            rag_documents=_route_documents_df(),
            cache=MapsVerificationCache(),
        )

        result = await tool.verify_plan(
            {
                "plan_time_iso": "2026-04-18T20:00:00+10:00",
                "transport_mode": "WALK",
                "max_leg_seconds": 300,
                "stops": [
                    {
                        "kind": "venue",
                        "stop_type": "restaurant",
                        "fsq_place_id": "restaurant-1",
                    },
                    {
                        "kind": "venue",
                        "stop_type": "dessert_shop",
                        "fsq_place_id": "dessert-1",
                    },
                    {
                        "kind": "venue",
                        "stop_type": "bar",
                        "fsq_place_id": "bad-place",
                    },
                ],
            }
        )

        feasibility = result["feasibility"]
        self.assertIs(feasibility["all_venues_matched"], False)
        self.assertIs(feasibility["all_legs_under_threshold"], True)
        self.assertIn(
            "stop 2",
            " ".join(feasibility["summary_reasons"]),
        )
        self.assertIn(
            "CLOSED_PERMANENTLY",
            " ".join(feasibility["summary_reasons"]),
        )
        self.assertEqual(
            ["restaurant-1", "dessert-1", "bad-place"],
            client.resolve_calls,
        )
        self.assertEqual(2, len(client.route_calls))
        self.assertEqual(3, len(result["stops_verification"]))
        self.assertEqual(2, len(result["legs"]))
        self.assertIs(result["stops_verification"][1]["ok"], False)
        self.assertEqual(
            "CLOSED_PERMANENTLY",
            result["stops_verification"][1]["business_status"],
        )

    async def test_connective_stop_uses_explicit_anchor_for_leg_routing(self) -> None:
        client = FakePlanMapsClient()
        tool = MapsVerifyPlanTool(
            maps_client=client,
            rag_documents=_route_documents_df(),
            cache=MapsVerificationCache(),
            connective_anchors_by_stop_type={
                "harbor_or_pier": LatLng(latitude=-33.861, longitude=151.202)
            },
        )

        result = await tool.verify_plan(
            {
                "plan_time_iso": "2026-04-18T20:00:00+10:00",
                "transport_mode": "WALK",
                "max_leg_seconds": 300,
                "stops": [
                    {
                        "kind": "venue",
                        "stop_type": "restaurant",
                        "fsq_place_id": "restaurant-1",
                    },
                    {
                        "kind": "connective",
                        "stop_type": "harbor_or_pier",
                        "fsq_place_id": None,
                    },
                    {
                        "kind": "venue",
                        "stop_type": "dessert_shop",
                        "fsq_place_id": "dessert-1",
                    },
                ],
            }
        )

        self.assertIs(result["feasibility"]["all_legs_under_threshold"], True)
        self.assertEqual(2, len(client.route_calls))
        self.assertEqual("connective", result["stops_verification"][1]["kind"])
        self.assertEqual(
            "connective_anchor",
            result["stops_verification"][1]["location_source"],
        )

    async def test_connective_without_anchor_skips_leg_and_marks_unconfirmed(self) -> None:
        client = FakePlanMapsClient()
        tool = MapsVerifyPlanTool(
            maps_client=client,
            rag_documents=_route_documents_df(),
            cache=MapsVerificationCache(),
        )

        result = await tool.verify_plan(
            {
                "plan_time_iso": "2026-04-18T20:00:00+10:00",
                "transport_mode": "WALK",
                "max_leg_seconds": 300,
                "stops": [
                    {
                        "kind": "venue",
                        "stop_type": "restaurant",
                        "fsq_place_id": "restaurant-1",
                    },
                    {
                        "kind": "connective",
                        "stop_type": "harbor_or_pier",
                        "fsq_place_id": None,
                    },
                ],
            }
        )

        self.assertIs(result["feasibility"]["all_legs_under_threshold"], False)
        self.assertEqual("skipped", result["legs"][0]["status"])
        self.assertIn("coordinates", result["legs"][0]["failure_reason"])
        self.assertEqual([], client.route_calls)

    async def test_invalid_max_leg_seconds_raises_tool_error(self) -> None:
        client = FakePlanMapsClient()
        tool = MapsVerifyPlanTool(
            maps_client=client,
            rag_documents=_route_documents_df(),
            cache=MapsVerificationCache(),
        )

        with self.assertRaises(DateIdeaAgentToolError):
            await tool.verify_plan(
                {
                    "plan_time_iso": "2026-04-18T20:00:00+10:00",
                    "transport_mode": "WALK",
                    "max_leg_seconds": 0,
                    "stops": [
                        {
                            "kind": "venue",
                            "stop_type": "restaurant",
                            "fsq_place_id": "restaurant-1",
                        }
                    ],
                }
            )

        self.assertEqual([], client.resolve_calls)
        self.assertEqual([], client.route_calls)


def _documents_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fsq_place_id": "restaurant-1",
                "name": "Romantic Restaurant",
                "latitude": -33.86,
                "longitude": 151.20,
                "locality": "Sydney",
                "region": "NSW",
                "postcode": "2000",
            }
        ]
    )


def _route_documents_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fsq_place_id": "restaurant-1",
                "name": "Romantic Restaurant",
                "latitude": -33.86,
                "longitude": 151.20,
                "locality": "Sydney",
                "region": "NSW",
                "postcode": "2000",
            },
            {
                "fsq_place_id": "dessert-1",
                "name": "Dessert Bar",
                "latitude": -33.8604,
                "longitude": 151.2003,
                "locality": "Sydney",
                "region": "NSW",
                "postcode": "2000",
            },
            {
                "fsq_place_id": "bad-place",
                "name": "Gone Venue",
                "latitude": -33.861,
                "longitude": 151.201,
                "locality": "Sydney",
                "region": "NSW",
                "postcode": "2000",
            },
        ]
    )


class FakeRouteMapsClient:
    def __init__(
        self,
        *,
        failures: dict[str, Exception] | None = None,
        route_error: Exception | None = None,
    ) -> None:
        self.failures = failures or {}
        self.route_error = route_error
        self.resolve_calls: list[str] = []
        self.route_calls: list[RouteRequest] = []

    async def resolve_place_match(self, candidate: CandidatePlace) -> MapsPlaceMatch:
        self.resolve_calls.append(candidate.fsq_place_id)
        failure = self.failures.get(candidate.fsq_place_id)
        if failure is not None:
            raise failure
        place = MapsPlace(
            place_id=f"google-{candidate.fsq_place_id}",
            resource_name=f"places/google-{candidate.fsq_place_id}",
            display_name=candidate.name,
            formatted_address=None,
            google_maps_uri=None,
            location=LatLng(
                latitude=_required_coordinate(candidate.latitude),
                longitude=_required_coordinate(candidate.longitude),
            ),
            business_status="OPERATIONAL",
            rating=None,
            user_rating_count=None,
            regular_opening_hours=None,
        )
        return MapsPlaceMatch(
            candidate_place=candidate,
            google_place=place,
            straight_line_distance_meters=0.0,
            name_similarity=1.0,
        )

    async def compute_route(self, route_request: RouteRequest) -> ComputedRoute:
        self.route_calls.append(route_request)
        if self.route_error is not None:
            raise self.route_error
        if route_request.origin == route_request.destination:
            return ComputedRoute(
                distance_meters=0,
                duration_seconds=0.0,
                static_duration_seconds=0.0,
                polyline=None,
                warnings=("Origin and destination are the same.",),
                legs=(),
            )
        return ComputedRoute(
            distance_meters=75,
            duration_seconds=90.0,
            static_duration_seconds=90.0,
            polyline=None,
            warnings=(),
            legs=(),
        )


class FakePlanMapsClient:
    def __init__(self, *, closed_place_ids: set[str] | None = None) -> None:
        self.closed_place_ids = closed_place_ids or set()
        self.resolve_calls: list[str] = []
        self.route_calls: list[RouteRequest] = []

    async def resolve_place_match(self, candidate: CandidatePlace) -> MapsPlaceMatch:
        self.resolve_calls.append(candidate.fsq_place_id)
        business_status = (
            "CLOSED_PERMANENTLY"
            if candidate.fsq_place_id in self.closed_place_ids
            else "OPERATIONAL"
        )
        place = MapsPlace(
            place_id=f"google-{candidate.fsq_place_id}",
            resource_name=f"places/google-{candidate.fsq_place_id}",
            display_name=candidate.name,
            formatted_address=None,
            google_maps_uri=None,
            location=LatLng(
                latitude=_required_coordinate(candidate.latitude),
                longitude=_required_coordinate(candidate.longitude),
            ),
            business_status=business_status,
            rating=None,
            user_rating_count=None,
            regular_opening_hours=MapsOpeningHours(
                open_now=None,
                weekday_descriptions=("Saturday: 5:00 – 11:00 PM",),
            ),
        )
        return MapsPlaceMatch(
            candidate_place=candidate,
            google_place=place,
            straight_line_distance_meters=0.0,
            name_similarity=1.0,
        )

    async def compute_route(self, route_request: RouteRequest) -> ComputedRoute:
        self.route_calls.append(route_request)
        return ComputedRoute(
            distance_meters=75,
            duration_seconds=90.0,
            static_duration_seconds=90.0,
            polyline=None,
            warnings=(),
            legs=(),
        )


def _match(
    *,
    business_status: str,
    weekday_descriptions: tuple[str, ...] = ("Saturday: 5:00 – 11:00 PM",),
) -> MapsPlaceMatch:
    candidate = CandidatePlace(
        fsq_place_id="restaurant-1",
        name="Romantic Restaurant",
        latitude=-33.86,
        longitude=151.20,
        locality="Sydney",
        region="NSW",
        postcode="2000",
    )
    place = MapsPlace(
        place_id="google-1",
        resource_name="places/google-1",
        display_name="Romantic Restaurant",
        formatted_address="1 Date St, Sydney NSW 2000",
        google_maps_uri="https://maps.google.com/?cid=1",
        location=LatLng(latitude=-33.8601, longitude=151.2001),
        business_status=business_status,
        rating=4.6,
        user_rating_count=120,
        regular_opening_hours=MapsOpeningHours(
            open_now=None,
            weekday_descriptions=weekday_descriptions,
        ),
    )
    return MapsPlaceMatch(
        candidate_place=candidate,
        google_place=place,
        straight_line_distance_meters=15.0,
        name_similarity=1.0,
    )


def _required_coordinate(value: float | None) -> float:
    if value is None:
        raise AssertionError("Fake route candidate coordinates must not be None.")
    return value


if __name__ == "__main__":
    unittest.main()
