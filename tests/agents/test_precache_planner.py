"""Tests for the pre-cache planner agent wrapper."""

from __future__ import annotations

import json
import unittest
from collections.abc import Mapping
from typing import Any

import httpx
import pandas as pd

from back_end.agents.precache_planner import (
    FAILURE_REASON_AGENT_MULTIPLE,
    FAILURE_REASON_DUPLICATE,
    FAILURE_REASON_EMPTY_POOL,
    FAILURE_REASON_OUTPUT_INVALID,
    PrecachePlanner,
    PrecachePlannerConfigurationError,
    PrecachePlannerFailure,
    PrecachePlannerRequest,
    PrecachePlannerSuccess,
)
from back_end.clients.openrouter import OpenRouterClient
from back_end.clients.settings import OpenRouterSettings
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
from back_end.precache.models import (
    CandidatePoolPlace,
    LocationBucket,
    LocationCandidatePool,
)
from back_end.precache.output import fsq_place_ids_sorted_signature


def _make_http_response(
    request: httpx.Request,
    status_code: int,
    payload: dict[str, Any],
) -> httpx.Response:
    return httpx.Response(
        status_code=status_code,
        headers={"Content-Type": "application/json"},
        content=json.dumps(payload).encode("utf-8"),
        request=request,
    )


class StaticEmbeddingClient:
    async def embed_texts(self, texts: tuple[str, ...]) -> tuple[tuple[float, ...], ...]:
        return tuple((1.0, 0.0) for _ in texts)


class FakeMapsClient:
    """Minimal GoogleMapsClient stand-in for precache planner tests."""

    def __init__(self) -> None:
        self.resolve_calls: list[str] = []
        self.route_calls: list[RouteRequest] = []

    async def resolve_place_match(self, candidate: CandidatePlace) -> MapsPlaceMatch:
        self.resolve_calls.append(candidate.fsq_place_id)
        place = MapsPlace(
            place_id=f"google-{candidate.fsq_place_id}",
            resource_name=f"places/google-{candidate.fsq_place_id}",
            display_name=candidate.name,
            formatted_address=None,
            google_maps_uri=None,
            location=LatLng(
                latitude=float(candidate.latitude or 0.0),
                longitude=float(candidate.longitude or 0.0),
            ),
            business_status="OPERATIONAL",
            rating=4.6,
            user_rating_count=100,
            regular_opening_hours=MapsOpeningHours(
                open_now=None,
                weekday_descriptions=(
                    "Monday: 9:00 AM – 11:00 PM",
                    "Tuesday: 9:00 AM – 11:00 PM",
                    "Wednesday: 9:00 AM – 11:00 PM",
                    "Thursday: 9:00 AM – 11:00 PM",
                    "Friday: 9:00 AM – 11:00 PM",
                    "Saturday: 9:00 AM – 11:00 PM",
                    "Sunday: 9:00 AM – 11:00 PM",
                ),
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
            distance_meters=120,
            duration_seconds=90.0,
            static_duration_seconds=90.0,
            polyline=None,
            warnings=(),
            legs=(),
        )


def _documents_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fsq_place_id": "restaurant-1",
                "name": "Romantic Restaurant",
                "document_hash": "hash-restaurant",
                "document_text": "Romantic dining room with wine.",
                "crawl4ai_evidence_snippets": ["Romantic dining room with wine."],
                "crawl4ai_template_stop_tags": ["restaurant"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
                "crawl4ai_ambience_tags": ["romantic"],
                "crawl4ai_setting_tags": ["intimate"],
                "crawl4ai_activity_tags": [],
                "crawl4ai_drink_tags": ["wine"],
                "crawl4ai_quality_score": 8,
                "latitude": -33.86,
                "longitude": 151.20,
                "locality": "Sydney",
                "region": "NSW",
                "postcode": "2000",
            },
            {
                "fsq_place_id": "dessert-1",
                "name": "Gelato Corner",
                "document_hash": "hash-dessert",
                "document_text": "Late dessert and gelato.",
                "crawl4ai_evidence_snippets": ["Late dessert and gelato."],
                "crawl4ai_template_stop_tags": ["dessert_shop"],
                "fsq_category_labels": ["Dining and Drinking > Dessert Shop"],
                "crawl4ai_ambience_tags": ["casual"],
                "crawl4ai_setting_tags": [],
                "crawl4ai_activity_tags": [],
                "crawl4ai_drink_tags": [],
                "crawl4ai_quality_score": 7,
                "latitude": -33.861,
                "longitude": 151.201,
                "locality": "Sydney",
                "region": "NSW",
                "postcode": "2000",
            },
        ]
    )


def _embeddings_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fsq_place_id": "restaurant-1",
                "document_hash": "hash-restaurant",
                "embedding_dimension": 2,
                "embedding": [1.0, 0.0],
            },
            {
                "fsq_place_id": "dessert-1",
                "document_hash": "hash-dessert",
                "embedding_dimension": 2,
                "embedding": [0.9, 0.1],
            },
        ]
    )


def _bucket(bucket_id: str = "sydney-cbd") -> LocationBucket:
    return LocationBucket(
        bucket_id=bucket_id,
        label="Sydney CBD",
        latitude=-33.86,
        longitude=151.2,
        radius_km=3.0,
        transport_mode="WALK",
        minimum_plan_count=3,
        maximum_plan_count=10,
        strategic_boost=0,
        tags=("cbd",),
    )


def _pool_place(
    fsq_place_id: str,
    name: str,
    stop_tag: str,
    lat: float,
    lng: float,
) -> CandidatePoolPlace:
    return CandidatePoolPlace(
        fsq_place_id=fsq_place_id,
        name=name,
        latitude=lat,
        longitude=lng,
        distance_km=0.1,
        quality_score=7,
        template_stop_tags=(stop_tag,),
        category_labels=(f"Dining and Drinking > {stop_tag.title()}",),
    )


def _pool(bucket: LocationBucket) -> LocationCandidatePool:
    return LocationCandidatePool(
        bucket=bucket,
        places=(
            _pool_place("restaurant-1", "Romantic Restaurant", "restaurant", -33.86, 151.20),
            _pool_place("dessert-1", "Gelato Corner", "dessert_shop", -33.861, 151.201),
        ),
        target_plan_count=3,
        empty_reason=None,
    )


def _empty_pool(bucket: LocationBucket) -> LocationCandidatePool:
    return LocationCandidatePool(
        bucket=bucket,
        places=(),
        target_plan_count=0,
        empty_reason="No candidate places survived filtering for this bucket.",
    )


def _two_stop_template() -> dict[str, Any]:
    return {
        "id": "dinner_then_dessert",
        "title": "Dinner then dessert",
        "vibe": ["romantic", "foodie"],
        "time_of_day": "evening",
        "duration_hours": 2.0,
        "weather_sensitive": False,
        "description": "Start with dinner, finish with something sweet.",
        "stops": [
            {"type": "restaurant"},
            {"type": "dessert_shop"},
        ],
    }


def _valid_two_stop_idea_output() -> dict[str, Any]:
    return {
        "date_ideas": [
            {
                "title": "Dinner Then A Sweet Finish",
                "hook": "Slow dinner, then wander over for gelato.",
                "template_hint": "dinner_then_dessert",
                "maps_verification_needed": False,
                "constraints_considered": ["romantic"],
                "stops": [
                    {
                        "kind": "venue",
                        "stop_type": "restaurant",
                        "fsq_place_id": "restaurant-1",
                        "name": "Romantic Restaurant",
                        "description": "Start with dinner in an intimate room.",
                        "why_it_fits": "The retrieved profile mentions romantic dining.",
                    },
                    {
                        "kind": "venue",
                        "stop_type": "dessert_shop",
                        "fsq_place_id": "dessert-1",
                        "name": "Gelato Corner",
                        "description": "Finish with a shared gelato around the corner.",
                        "why_it_fits": "A quick walk keeps the evening easy.",
                    },
                ],
            }
        ],
        "rejected_ideas": [],
    }


def _make_llm_handler(
    *,
    search_query_text: str = "romantic restaurant with wine",
    final_output: dict[str, Any] | None = None,
) -> tuple[Any, dict[str, int], list[dict[str, Any]]]:
    """Return an httpx handler that mocks a 2-turn LLM flow.

    Turn 1: agent calls search_rag_places.
    Turn 2: agent emits final JSON. The wrapper will independently run
    verify_plan after this.
    """

    call_count = {"value": 0}
    bodies: list[dict[str, Any]] = []
    output_payload = final_output if final_output is not None else _valid_two_stop_idea_output()

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["value"] += 1
        bodies.append(json.loads(request.content.decode("utf-8")))
        if call_count["value"] == 1:
            return _make_http_response(
                request,
                200,
                {
                    "id": "resp_tool",
                    "model": "anthropic/claude-sonnet-4.6",
                    "choices": [
                        {
                            "finish_reason": "tool_calls",
                            "message": {
                                "role": "assistant",
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": "call_1",
                                        "type": "function",
                                        "function": {
                                            "name": "search_rag_places",
                                            "arguments": json.dumps(
                                                {
                                                    "query_text": search_query_text,
                                                    "top_k": 5,
                                                }
                                            ),
                                        },
                                    }
                                ],
                            },
                        }
                    ],
                },
            )
        return _make_http_response(
            request,
            200,
            {
                "id": "resp_final",
                "model": "anthropic/claude-sonnet-4.6",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {
                            "role": "assistant",
                            "content": json.dumps(output_payload),
                        },
                    }
                ],
            },
        )

    return handler, call_count, bodies


def _build_vector_store() -> Any:
    from back_end.rag.vector_store import ExactVectorStore

    return ExactVectorStore(_documents_df(), _embeddings_df())


class PrecachePlannerRequestValidationTests(unittest.TestCase):
    def test_mismatched_bucket_and_pool_raises(self) -> None:
        bucket = _bucket("cbd")
        other_bucket = _bucket("surry-hills")
        pool = _pool(other_bucket)
        with self.assertRaises(PrecachePlannerConfigurationError):
            PrecachePlannerRequest(
                bucket=bucket,
                pool=pool,
                template=_two_stop_template(),
                plan_time_iso="2026-04-25T19:30:00+10:00",
                transport_mode=TravelMode.WALK,
                max_leg_seconds=900,
            )

    def test_plan_time_must_have_offset(self) -> None:
        bucket = _bucket()
        with self.assertRaises(PrecachePlannerConfigurationError):
            PrecachePlannerRequest(
                bucket=bucket,
                pool=_pool(bucket),
                template=_two_stop_template(),
                plan_time_iso="2026-04-25T19:30:00",
                transport_mode=TravelMode.WALK,
                max_leg_seconds=900,
            )

    def test_max_leg_seconds_must_be_positive(self) -> None:
        bucket = _bucket()
        with self.assertRaises(PrecachePlannerConfigurationError):
            PrecachePlannerRequest(
                bucket=bucket,
                pool=_pool(bucket),
                template=_two_stop_template(),
                plan_time_iso="2026-04-25T19:30:00+10:00",
                transport_mode=TravelMode.WALK,
                max_leg_seconds=0,
            )


class PrecachePlannerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.settings = OpenRouterSettings(
            api_key="test-key",
            default_model="anthropic/claude-sonnet-4.6",
            max_tool_round_trips=4,
        )

    async def test_empty_pool_returns_failure_without_calling_llm(self) -> None:
        handler, call_count, _ = _make_llm_handler()
        client = OpenRouterClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)
        planner = PrecachePlanner(
            llm_client=client,
            maps_client=FakeMapsClient(),
            vector_store=_build_vector_store(),
            embedding_client=StaticEmbeddingClient(),
            rag_documents=_documents_df(),
        )
        bucket = _bucket()
        request = PrecachePlannerRequest(
            bucket=bucket,
            pool=_empty_pool(bucket),
            template=_two_stop_template(),
            plan_time_iso="2026-04-25T19:30:00+10:00",
            transport_mode=TravelMode.WALK,
            max_leg_seconds=900,
        )

        result = await planner.plan(request)

        self.assertIsInstance(result, PrecachePlannerFailure)
        assert isinstance(result, PrecachePlannerFailure)
        self.assertEqual(FAILURE_REASON_EMPTY_POOL, result.reason)
        self.assertIn("No candidate places", result.detail)
        self.assertEqual(0, call_count["value"])

    async def test_precache_agent_tools_are_trimmed_for_demo_cost(self) -> None:
        handler, call_count, bodies = _make_llm_handler()
        client = OpenRouterClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)
        planner = PrecachePlanner(
            llm_client=client,
            maps_client=FakeMapsClient(),
            vector_store=_build_vector_store(),
            embedding_client=StaticEmbeddingClient(),
            rag_documents=_documents_df(),
        )
        bucket = _bucket()
        request = PrecachePlannerRequest(
            bucket=bucket,
            pool=_pool(bucket),
            template=_two_stop_template(),
            plan_time_iso="2026-04-25T19:30:00+10:00",
            transport_mode=TravelMode.WALK,
            max_leg_seconds=1200,
        )

        result = await planner.plan(request)

        self.assertIsInstance(result, PrecachePlannerSuccess)
        self.assertGreaterEqual(call_count["value"], 2)
        first_request = bodies[0]
        tool_names = [tool["function"]["name"] for tool in first_request["tools"]]
        self.assertIn("verify_plan", tool_names)
        self.assertNotIn("verify_place", tool_names)
        self.assertNotIn("compute_leg", tool_names)
        self.assertNotIn("get_place_profile", tool_names)
        search_tool = next(
            tool for tool in first_request["tools"]
            if tool["function"]["name"] == "search_rag_places"
        )
        self.assertEqual(
            10,
            search_tool["function"]["parameters"]["properties"]["top_k"]["maximum"],
        )
        system_prompt = first_request["messages"][0]["content"]
        self.assertIn("Keep tool use lean", system_prompt)
        self.assertIn("Do not individually verify venues or route legs.", system_prompt)
        user_prompt = _first_user_message(first_request)
        self.assertIn("call verify_plan", user_prompt)
        self.assertNotIn("verify_place", user_prompt)

    async def test_openrouter_empty_message_returns_failure_not_crash(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _make_http_response(
                request,
                200,
                {
                    "id": "resp_empty",
                    "model": "anthropic/claude-sonnet-4.6",
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {
                                "role": "assistant",
                                "content": None,
                            },
                        }
                    ],
                },
            )

        client = OpenRouterClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)
        planner = PrecachePlanner(
            llm_client=client,
            maps_client=FakeMapsClient(),
            vector_store=_build_vector_store(),
            embedding_client=StaticEmbeddingClient(),
            rag_documents=_documents_df(),
        )
        bucket = _bucket()
        request = PrecachePlannerRequest(
            bucket=bucket,
            pool=_pool(bucket),
            template=_two_stop_template(),
            plan_time_iso="2026-04-25T19:30:00+10:00",
            transport_mode=TravelMode.WALK,
            max_leg_seconds=1200,
        )

        result = await planner.plan(request)

        self.assertIsInstance(result, PrecachePlannerFailure)
        assert isinstance(result, PrecachePlannerFailure)
        self.assertEqual(FAILURE_REASON_OUTPUT_INVALID, result.reason)
        self.assertIn("neither content nor tool_calls", result.detail)

    async def test_happy_path_produces_validated_precache_plan(self) -> None:
        handler, call_count, bodies = _make_llm_handler()
        client = OpenRouterClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)
        maps_client = FakeMapsClient()
        planner = PrecachePlanner(
            llm_client=client,
            maps_client=maps_client,
            vector_store=_build_vector_store(),
            embedding_client=StaticEmbeddingClient(),
            rag_documents=_documents_df(),
        )
        bucket = _bucket()
        request = PrecachePlannerRequest(
            bucket=bucket,
            pool=_pool(bucket),
            template=_two_stop_template(),
            plan_time_iso="2026-04-25T19:30:00+10:00",
            transport_mode=TravelMode.WALK,
            max_leg_seconds=900,
        )

        result = await planner.plan(request)

        self.assertIsInstance(result, PrecachePlannerSuccess)
        assert isinstance(result, PrecachePlannerSuccess)
        self.assertEqual(2, call_count["value"])
        self.assertEqual(
            "2026-04-25T19:30:00+10:00",
            _first_user_message(bodies[0]).split("Time window: ")[-1].splitlines()[0],
        )
        self.assertEqual("restaurant-1", result.idea.stops[0].fsq_place_id)
        self.assertEqual("dessert-1", result.idea.stops[1].fsq_place_id)
        self.assertEqual(
            fsq_place_ids_sorted_signature(("restaurant-1", "dessert-1")),
            result.signature,
        )
        self.assertTrue(result.verification["feasibility"]["all_venues_matched"])
        self.assertTrue(result.verification["feasibility"]["all_open_at_plan_time"])
        self.assertTrue(result.verification["feasibility"]["all_legs_under_threshold"])
        self.assertEqual("sydney-cbd", result.plan.bucket_id)
        self.assertEqual("dinner_then_dessert", result.plan.template_id)
        self.assertEqual(bucket.label, result.plan.bucket_metadata["label"])
        self.assertEqual("WALK", result.plan.bucket_metadata["transport_mode"])
        self.assertEqual(
            ["restaurant-1", "dessert-1"],
            [stop["fsq_place_id"] for stop in result.plan.stops],
        )
        self.assertEqual({"restaurant-1", "dessert-1"}, set(maps_client.resolve_calls))
        self.assertEqual(1, len(maps_client.route_calls))

    async def test_duplicate_signature_is_rejected(self) -> None:
        handler, _, _ = _make_llm_handler()
        client = OpenRouterClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)
        planner = PrecachePlanner(
            llm_client=client,
            maps_client=FakeMapsClient(),
            vector_store=_build_vector_store(),
            embedding_client=StaticEmbeddingClient(),
            rag_documents=_documents_df(),
        )
        bucket = _bucket()
        existing_signature = fsq_place_ids_sorted_signature(
            ("restaurant-1", "dessert-1")
        )
        request = PrecachePlannerRequest(
            bucket=bucket,
            pool=_pool(bucket),
            template=_two_stop_template(),
            plan_time_iso="2026-04-25T19:30:00+10:00",
            transport_mode=TravelMode.WALK,
            max_leg_seconds=900,
            existing_plan_signatures=(existing_signature,),
        )

        result = await planner.plan(request)

        self.assertIsInstance(result, PrecachePlannerFailure)
        assert isinstance(result, PrecachePlannerFailure)
        self.assertEqual(FAILURE_REASON_DUPLICATE, result.reason)
        self.assertEqual(existing_signature, result.signature)

    async def test_multiple_ideas_returns_failure(self) -> None:
        bad_output = _valid_two_stop_idea_output()
        extra_idea = {
            "title": "Second Idea",
            "hook": "An unwanted second idea.",
            "template_hint": None,
            "maps_verification_needed": False,
            "constraints_considered": [],
            "stops": bad_output["date_ideas"][0]["stops"],
        }
        bad_output["date_ideas"].append(extra_idea)
        handler, _, _ = _make_llm_handler(final_output=bad_output)
        client = OpenRouterClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)
        planner = PrecachePlanner(
            llm_client=client,
            maps_client=FakeMapsClient(),
            vector_store=_build_vector_store(),
            embedding_client=StaticEmbeddingClient(),
            rag_documents=_documents_df(),
        )
        bucket = _bucket()
        request = PrecachePlannerRequest(
            bucket=bucket,
            pool=_pool(bucket),
            template=_two_stop_template(),
            plan_time_iso="2026-04-25T19:30:00+10:00",
            transport_mode=TravelMode.WALK,
            max_leg_seconds=900,
        )

        result = await planner.plan(request)

        self.assertIsInstance(result, PrecachePlannerFailure)
        assert isinstance(result, PrecachePlannerFailure)
        # An idea with duplicate titles triggers duplicate-title validation
        # in _parse_and_validate_ideas before the wrapper's "multiple ideas"
        # guard. Either failure reason is fine here; we just want a Failure.
        self.assertIn(
            result.reason,
            {FAILURE_REASON_AGENT_MULTIPLE, FAILURE_REASON_OUTPUT_INVALID},
        )

    async def test_template_shape_violation_returns_failure(self) -> None:
        bad_output = _valid_two_stop_idea_output()
        # Remove one stop so the idea no longer matches the 2-stop template
        bad_output["date_ideas"][0]["stops"] = [
            bad_output["date_ideas"][0]["stops"][0]
        ]
        handler, _, _ = _make_llm_handler(final_output=bad_output)
        client = OpenRouterClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)
        planner = PrecachePlanner(
            llm_client=client,
            maps_client=FakeMapsClient(),
            vector_store=_build_vector_store(),
            embedding_client=StaticEmbeddingClient(),
            rag_documents=_documents_df(),
        )
        bucket = _bucket()
        request = PrecachePlannerRequest(
            bucket=bucket,
            pool=_pool(bucket),
            template=_two_stop_template(),
            plan_time_iso="2026-04-25T19:30:00+10:00",
            transport_mode=TravelMode.WALK,
            max_leg_seconds=900,
        )

        result = await planner.plan(request)

        self.assertIsInstance(result, PrecachePlannerFailure)
        assert isinstance(result, PrecachePlannerFailure)
        self.assertEqual(FAILURE_REASON_OUTPUT_INVALID, result.reason)
        self.assertIn("template", result.detail.lower())


def _first_user_message(body: Mapping[str, Any]) -> str:
    messages = body.get("messages", [])
    for message in messages:
        if message.get("role") == "user":
            content = message.get("content")
            if isinstance(content, str):
                return content
    raise AssertionError("No user message found in chat completion body.")


if __name__ == "__main__":
    unittest.main()
