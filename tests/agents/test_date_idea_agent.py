from __future__ import annotations

import json
import unittest

import httpx
import pandas as pd

from back_end.agents.date_idea_agent import (
    DateIdeaAgent,
    DateIdeaAgentOutputError,
    DateIdeaAgentToolError,
    DateIdeaRequest,
    RagPlaceSearchTool,
    _parse_and_validate_ideas,
    _retrieved_places_by_id,
)
from back_end.agents.maps_tools import MapsVerifyPlaceTool
from back_end.clients.openrouter import OpenRouterClient
from back_end.clients.settings import OpenRouterSettings
from back_end.domain.models import (
    CandidatePlace,
    LatLng,
    MapsOpeningHours,
    MapsPlace,
    MapsPlaceMatch,
)
from back_end.llm.models import AgentToolExecution, OpenRouterMessage
from back_end.rag.vector_store import ExactVectorStore


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


class QueryEmbeddingClient:
    async def embed_texts(self, texts: tuple[str, ...]) -> tuple[tuple[float, ...], ...]:
        return tuple((1.0, 0.0) for _ in texts)


class FakeMapsClient:
    def __init__(self) -> None:
        self.calls = 0

    async def resolve_place_match(self, candidate: CandidatePlace) -> MapsPlaceMatch:
        self.calls += 1
        return MapsPlaceMatch(
            candidate_place=candidate,
            google_place=MapsPlace(
                place_id="google-restaurant-1",
                resource_name="places/google-restaurant-1",
                display_name="Romantic Restaurant",
                location=LatLng(latitude=-33.8601, longitude=151.2001),
                formatted_address="1 Date St, Sydney NSW 2000",
                google_maps_uri="https://maps.google.com/?cid=restaurant-1",
                business_status="OPERATIONAL",
                rating=4.7,
                user_rating_count=300,
                regular_opening_hours=MapsOpeningHours(
                    open_now=None,
                    weekday_descriptions=("Saturday: 5:00 – 11:00 PM",),
                ),
            ),
            straight_line_distance_meters=12.0,
            name_similarity=1.0,
        )


class FakePlaceResolver:
    def __init__(self, failed_ids: set[str] | None = None) -> None:
        self.failed_ids = failed_ids or set()

    async def resolve_place_match(self, fsq_place_id: str) -> object:
        if fsq_place_id in self.failed_ids:
            return type("Failure", (), {"reason": "cached failure"})()
        return MapsPlaceMatch(
            candidate_place=CandidatePlace(
                fsq_place_id=fsq_place_id,
                name=fsq_place_id,
                latitude=-33.86,
                longitude=151.20,
                address="1 Date St",
                locality="Sydney",
                region="NSW",
                postcode="2000",
            ),
            google_place=MapsPlace(
                place_id=f"google-{fsq_place_id}",
                resource_name=f"places/google-{fsq_place_id}",
                display_name=f"Google {fsq_place_id}",
                location=LatLng(latitude=-33.86, longitude=151.20),
                formatted_address="1 Date St, Sydney NSW 2000",
                business_status="OPERATIONAL",
            ),
            straight_line_distance_meters=10.0,
            name_similarity=1.0,
            match_kind="address_match",
        )


class RagPlaceSearchToolTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tool = RagPlaceSearchTool(
            vector_store=ExactVectorStore(_documents_df(), _embeddings_df()),
            embedding_client=QueryEmbeddingClient(),
            default_top_k=3,
            max_top_k=5,
        )

    async def test_search_accepts_natural_language_without_stop_type(self) -> None:
        result = await self.tool.search(
            {
                "query_text": "romantic dinner with wine, then something sweet",
                "top_k": 2,
            }
        )

        self.assertIsNone(result["empty_reason"])
        self.assertEqual(
            ["restaurant-1", "dessert-1"],
            [item["fsq_place_id"] for item in result["results"]],
        )
        self.assertEqual("Romantic Restaurant", result["results"][0]["name"])

    async def test_search_rejects_invalid_top_k_instead_of_falling_back(self) -> None:
        with self.assertRaises(DateIdeaAgentToolError):
            await self.tool.search(
                {
                    "query_text": "romantic dinner",
                    "top_k": 100,
                }
            )

    async def test_search_rejects_missing_query_text(self) -> None:
        with self.assertRaises(DateIdeaAgentToolError):
            await self.tool.search({"top_k": 2})

    async def test_search_respects_location_scope_candidate_ids(self) -> None:
        scoped_tool = RagPlaceSearchTool(
            vector_store=ExactVectorStore(_documents_df(), _embeddings_df()),
            embedding_client=QueryEmbeddingClient(),
            default_top_k=3,
            max_top_k=5,
            candidate_place_ids=("dessert-1",),
            scope_label="dessert-only",
        )

        result = await scoped_tool.search(
            {
                "query_text": "romantic dinner with wine",
                "top_k": 3,
            }
        )

        self.assertEqual("dessert-only", result["scope_label"])
        self.assertEqual(1, result["scope_place_count"])
        self.assertEqual(["dessert-1"], [item["fsq_place_id"] for item in result["results"]])

    async def test_search_near_anchor_limits_results_by_distance(self) -> None:
        result = await self.tool.search_near_anchor(
            {
                "query_text": "sweet dessert after dinner",
                "anchor_fsq_place_id": "restaurant-1",
                "max_km": 0.2,
                "top_k": 5,
            }
        )

        self.assertIsNone(result["empty_reason"])
        self.assertEqual(["dessert-1"], [item["fsq_place_id"] for item in result["results"]])
        self.assertLess(result["results"][0]["distance_km"], 0.2)
        self.assertEqual(
            {
                "query_text",
                "stop_type",
                "scope_label",
                "scope_place_count",
                "empty_reason",
                "results",
                "anchor_fsq_place_id",
                "max_km",
            },
            set(result),
        )

    async def test_search_near_latlng_limits_results_by_distance(self) -> None:
        result = await self.tool.search_near_latlng(
            {
                "query_text": "restaurant near landmark",
                "latitude": -33.86,
                "longitude": 151.20,
                "max_km": 0.05,
                "top_k": 5,
            }
        )

        self.assertEqual(["restaurant-1"], [item["fsq_place_id"] for item in result["results"]])
        self.assertLess(result["results"][0]["distance_from_seed_km"], 0.05)

    async def test_search_near_latlng_respects_location_scope(self) -> None:
        scoped_tool = RagPlaceSearchTool(
            vector_store=ExactVectorStore(_documents_df(), _embeddings_df()),
            embedding_client=QueryEmbeddingClient(),
            default_top_k=3,
            max_top_k=5,
            candidate_place_ids=("dessert-1",),
            scope_label="dessert-only",
        )

        result = await scoped_tool.search_near_latlng(
            {
                "query_text": "restaurant near landmark",
                "latitude": -33.86,
                "longitude": 151.20,
                "max_km": 0.2,
                "top_k": 5,
            }
        )

        self.assertEqual("dessert-only", result["scope_label"])
        self.assertEqual(["dessert-1"], [item["fsq_place_id"] for item in result["results"]])

    async def test_validated_search_excludes_failed_fsq_id_on_next_search(self) -> None:
        scoped_tool = RagPlaceSearchTool(
            vector_store=ExactVectorStore(_documents_df(), _embeddings_df()),
            embedding_client=QueryEmbeddingClient(),
            default_top_k=2,
            max_top_k=5,
            candidate_place_ids=("restaurant-1", "dessert-1"),
            validated_only=True,
            place_resolver=FakePlaceResolver(failed_ids={"dessert-1"}),
        )

        result = await scoped_tool.search_near_latlng(
            {
                "query_text": "dessert near landmark",
                "latitude": -33.86,
                "longitude": 151.20,
                "max_km": 0.2,
                "top_k": 2,
                "exclude_place_ids": ["restaurant-1"],
            }
        )

        self.assertEqual([], result["results"])
        self.assertEqual(
            "No Maps-validated candidate places survived filtering.",
            result["empty_reason"],
        )

    async def test_search_near_latlng_returns_empty_reason_when_no_places_are_nearby(self) -> None:
        result = await self.tool.search_near_latlng(
            {
                "query_text": "restaurant near remote landmark",
                "latitude": 0.0,
                "longitude": 0.0,
                "max_km": 0.1,
                "top_k": 5,
            }
        )

        self.assertEqual([], result["results"])
        self.assertEqual(
            "No candidate places remain after scope, distance, and exclusion filters.",
            result["empty_reason"],
        )

    async def test_search_near_latlng_rejects_invalid_coordinates_and_radius(self) -> None:
        invalid_inputs = (
            {"latitude": 91, "longitude": 151.20, "max_km": 0.2},
            {"latitude": -33.86, "longitude": -181, "max_km": 0.2},
            {"latitude": -33.86, "longitude": 151.20, "max_km": 0},
            {"latitude": True, "longitude": 151.20, "max_km": 0.2},
        )
        for payload in invalid_inputs:
            with self.subTest(payload=payload):
                with self.assertRaises(DateIdeaAgentToolError):
                    await self.tool.search_near_latlng(
                        {
                            "query_text": "restaurant near landmark",
                            **payload,
                        }
                    )

    def test_agent_tools_include_near_latlng_schema(self) -> None:
        tools = self.tool.as_agent_tools()
        by_name = {tool.definition.name: tool.definition for tool in tools}

        self.assertIn("search_rag_places_near_latlng", by_name)
        schema = by_name["search_rag_places_near_latlng"].parameters_json_schema
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(
            ["query_text", "latitude", "longitude", "max_km"],
            schema["required"],
        )
        self.assertEqual(
            {"type": "number", "exclusiveMinimum": 0},
            schema["properties"]["max_km"],
        )

    def test_agent_tools_include_near_anchor_schema(self) -> None:
        tools = self.tool.as_agent_tools()
        by_name = {tool.definition.name: tool.definition for tool in tools}

        self.assertIn("search_rag_places_near_anchor", by_name)
        schema = by_name["search_rag_places_near_anchor"].parameters_json_schema
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(
            ["query_text", "anchor_fsq_place_id", "max_km"],
            schema["required"],
        )
        self.assertEqual(
            {"type": "number", "exclusiveMinimum": 0},
            schema["properties"]["max_km"],
        )

    async def test_search_near_anchor_rejects_anchor_outside_scope(self) -> None:
        scoped_tool = RagPlaceSearchTool(
            vector_store=ExactVectorStore(_documents_df(), _embeddings_df()),
            embedding_client=QueryEmbeddingClient(),
            default_top_k=3,
            max_top_k=5,
            candidate_place_ids=("dessert-1",),
            scope_label="dessert-only",
        )

        with self.assertRaises(DateIdeaAgentToolError):
            await scoped_tool.search_near_anchor(
                {
                    "query_text": "dessert",
                    "anchor_fsq_place_id": "restaurant-1",
                    "max_km": 1.0,
                }
            )

    async def test_search_near_anchor_rejects_unknown_anchor(self) -> None:
        with self.assertRaises(DateIdeaAgentToolError):
            await self.tool.search_near_anchor(
                {
                    "query_text": "dessert",
                    "anchor_fsq_place_id": "unknown-place",
                    "max_km": 1.0,
                }
            )

    async def test_search_near_anchor_does_not_fallback_when_scope_is_exhausted(self) -> None:
        scoped_tool = RagPlaceSearchTool(
            vector_store=ExactVectorStore(_documents_df(), _embeddings_df()),
            embedding_client=QueryEmbeddingClient(),
            default_top_k=3,
            max_top_k=5,
            candidate_place_ids=("restaurant-1",),
            scope_label="anchor-only",
        )

        result = await scoped_tool.search_near_anchor(
            {
                "query_text": "nearby dessert",
                "anchor_fsq_place_id": "restaurant-1",
                "max_km": 1.0,
                "top_k": 5,
            }
        )

        self.assertEqual([], result["results"])
        self.assertEqual(
            "No candidate places remain after scope, distance, and exclusion filters.",
            result["empty_reason"],
        )

    async def test_search_near_anchor_logs_and_skips_bad_candidate_coordinates(self) -> None:
        scoped_tool = RagPlaceSearchTool(
            vector_store=ExactVectorStore(
                _documents_with_bad_coordinates_df(),
                _embeddings_with_bad_coordinates_df(),
            ),
            embedding_client=QueryEmbeddingClient(),
            default_top_k=3,
            max_top_k=5,
            candidate_place_ids=("restaurant-1", "bad-coords"),
            scope_label="bad-coords-scope",
        )

        with self.assertLogs("back_end.agents.date_idea_agent", level="ERROR") as logs:
            result = await scoped_tool.search_near_anchor(
                {
                    "query_text": "nearby dessert",
                    "anchor_fsq_place_id": "restaurant-1",
                    "max_km": 1.0,
                    "top_k": 5,
                }
            )

        self.assertEqual([], result["results"])
        self.assertTrue(
            any("Skipping fsq_place_id=bad-coords during radius filtering" in message for message in logs.output)
        )

    async def test_get_place_profile_returns_full_rag_document(self) -> None:
        profile = self.tool.get_place_profile({"fsq_place_id": "restaurant-1"})

        self.assertEqual("restaurant-1", profile["fsq_place_id"])
        self.assertEqual("Romantic Restaurant", profile["name"])
        self.assertEqual(["restaurant"], profile["template_stop_tags"])
        self.assertIn("Romantic dining room", profile["document_text"])

    async def test_get_place_profile_rejects_unknown_place_id(self) -> None:
        with self.assertRaises(DateIdeaAgentToolError):
            self.tool.get_place_profile({"fsq_place_id": "invented-venue"})

    async def test_get_place_profile_rejects_place_outside_scope(self) -> None:
        scoped_tool = RagPlaceSearchTool(
            vector_store=ExactVectorStore(_documents_df(), _embeddings_df()),
            embedding_client=QueryEmbeddingClient(),
            default_top_k=3,
            max_top_k=5,
            candidate_place_ids=("dessert-1",),
            scope_label="dessert-only",
        )

        with self.assertRaises(DateIdeaAgentToolError):
            scoped_tool.get_place_profile({"fsq_place_id": "restaurant-1"})


class TemplateShapeValidationTests(unittest.TestCase):
    def test_parse_without_template_keeps_freeform_shape_allowed(self) -> None:
        ideas = _parse_and_validate_ideas(
            _valid_agent_output(),
            retrieved_places=_retrieved_places_for_validation(),
        )

        self.assertEqual(1, len(ideas))
        self.assertEqual(
            ("restaurant", "harbour_or_pier"),
            tuple(stop.stop_type for stop in ideas[0].stops),
        )

    def test_template_validator_accepts_exact_shape(self) -> None:
        ideas = _parse_and_validate_ideas(
            _valid_agent_output(),
            retrieved_places=_retrieved_places_for_validation(),
            template={
                "id": "dinner_and_walk",
                "stops": [
                    {"type": "restaurant"},
                    {"type": "harbour_or_pier", "kind": "connective"},
                ],
            },
        )

        self.assertEqual("Dinner With A Sweet Finish", ideas[0].title)

    def test_missing_title_falls_back_to_stop_names(self) -> None:
        output = _valid_agent_output()
        del output["date_ideas"][0]["title"]
        output["date_ideas"][0]["stops"].append(
            {
                "kind": "venue",
                "stop_type": "dessert_shop",
                "fsq_place_id": "dessert-1",
                "name": "Gelato Corner",
                "description": "Finish with dessert.",
                "why_it_fits": "A sweet ending.",
            }
        )

        ideas = _parse_and_validate_ideas(
            output,
            retrieved_places=_retrieved_places_for_validation(),
        )

        self.assertEqual(
            "Romantic Restaurant & Gelato Corner",
            ideas[0].title,
        )

    def test_template_validator_rejects_stop_count_mismatch(self) -> None:
        with self.assertRaisesRegex(DateIdeaAgentOutputError, "requires 3 stops"):
            _parse_and_validate_ideas(
                _valid_agent_output(),
                retrieved_places=_retrieved_places_for_validation(),
                template={
                    "id": "dinner_walk_dessert",
                    "stops": [
                        {"type": "restaurant"},
                        {"type": "harbour_or_pier", "kind": "connective"},
                        {"type": "dessert_shop"},
                    ],
                },
            )

    def test_template_validator_rejects_wrong_kind_in_slot(self) -> None:
        output = _valid_agent_output()
        output["date_ideas"][0]["stops"] = [
            {
                "kind": "connective",
                "stop_type": "harbour_or_pier",
                "fsq_place_id": None,
                "name": "Harbour stroll",
                "description": "Walk before dinner.",
                "why_it_fits": "A connective opener.",
            },
            {
                "kind": "venue",
                "stop_type": "restaurant",
                "fsq_place_id": "restaurant-1",
                "name": "Romantic Restaurant",
                "description": "Dinner after the walk.",
                "why_it_fits": "The retrieved profile mentions dinner.",
            },
        ]

        with self.assertRaisesRegex(
            DateIdeaAgentOutputError,
            "requires kind 'venue'",
        ):
            _parse_and_validate_ideas(
                output,
                retrieved_places=_retrieved_places_for_validation(),
                template={
                    "id": "dinner_then_walk",
                    "stops": [
                        {"type": "restaurant"},
                        {"type": "harbour_or_pier", "kind": "connective"},
                    ],
                },
            )

    def test_template_validator_rejects_wrong_stop_type_in_slot(self) -> None:
        with self.assertRaisesRegex(DateIdeaAgentOutputError, "requires 'cafe'"):
            _parse_and_validate_ideas(
                _valid_agent_output(),
                retrieved_places=_retrieved_places_for_validation(),
                template={
                    "id": "coffee_then_walk",
                    "stops": [
                        {"type": "cafe"},
                        {"type": "harbour_or_pier", "kind": "connective"},
                    ],
                },
            )

    def test_template_validator_accepts_compound_stop_type_aliases(self) -> None:
        output = {
            "date_ideas": [
                {
                    "title": "Bakery Then Bar",
                    "hook": "A simple pickup then drinks plan.",
                    "template_hint": "picnic_then_drink",
                    "maps_verification_needed": True,
                    "constraints_considered": [],
                    "stops": [
                        {
                            "kind": "venue",
                            "stop_type": "bakery",
                            "fsq_place_id": "bakery-1",
                            "name": "Local Bakery",
                            "description": "Pick up pastries.",
                            "why_it_fits": "The retrieved profile is a bakery.",
                        },
                        {
                            "kind": "venue",
                            "stop_type": "bar",
                            "fsq_place_id": "bar-1",
                            "name": "Neighbourhood Bar",
                            "description": "Finish with drinks.",
                            "why_it_fits": "The retrieved profile is a bar.",
                        },
                    ],
                }
            ],
            "rejected_ideas": [],
        }

        ideas = _parse_and_validate_ideas(
            output,
            retrieved_places=_retrieved_places_for_validation(),
            template={
                "id": "compound_aliases",
                "stops": [
                    {"type": "bakery_or_market"},
                    {"type": "brewery_or_bar"},
                ],
            },
        )

        self.assertEqual(
            ("bakery", "bar"),
            tuple(stop.stop_type for stop in ideas[0].stops),
        )


class DateIdeaAgentTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.rag_tool = RagPlaceSearchTool(
            vector_store=ExactVectorStore(_documents_df(), _embeddings_df()),
            embedding_client=QueryEmbeddingClient(),
            default_top_k=3,
            max_top_k=5,
        )
        self.settings = OpenRouterSettings(
            api_key="test-key",
            default_model="unused",
            max_tool_round_trips=2,
        )

    async def test_agent_queries_rag_and_returns_grounded_idea(self) -> None:
        call_count = {"value": 0}
        observed_bodies: list[dict] = []

        def handler(request: httpx.Request) -> httpx.Response:
            call_count["value"] += 1
            body = json.loads(request.content.decode("utf-8"))
            observed_bodies.append(body)
            if call_count["value"] == 1:
                return _make_response(
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
                                                        "query_text": "romantic restaurant with wine",
                                                        "stop_type": "restaurant",
                                                        "top_k": 2,
                                                        "exclude_place_ids": [],
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
            return _make_response(
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
                                "content": json.dumps(_valid_agent_output()),
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
        agent = DateIdeaAgent(
            llm_client=client,
            rag_search_tool=self.rag_tool,
            template_context="- drinks_dinner_dessert: cocktail_bar, restaurant, dessert_shop",
        )

        result = await agent.generate(DateIdeaRequest(prompt="romantic dinner date"))

        self.assertEqual(2, call_count["value"])
        self.assertEqual("anthropic/claude-sonnet-4.6", observed_bodies[0]["model"])
        self.assertEqual({"effort": "medium", "exclude": True}, observed_bodies[0]["reasoning"])
        self.assertNotIn("response_format", observed_bodies[0])
        self.assertEqual(1, len(result.ideas))
        self.assertEqual("Dinner With A Sweet Finish", result.ideas[0].title)
        self.assertEqual("restaurant-1", result.ideas[0].stops[0].fsq_place_id)
        self.assertEqual(1, len(result.tool_executions))

    async def test_agent_rejects_venue_not_returned_by_rag_tool(self) -> None:
        call_count = {"value": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            call_count["value"] += 1
            if call_count["value"] == 1:
                return _make_response(
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
                                                        "query_text": "romantic restaurant",
                                                        "stop_type": "restaurant",
                                                        "top_k": 1,
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
            bad_output = _valid_agent_output()
            bad_output["date_ideas"][0]["stops"][0]["fsq_place_id"] = "invented-venue"
            bad_output["date_ideas"][0]["stops"][0]["name"] = "Imaginary Room"
            return _make_response(
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
                                "content": json.dumps(bad_output),
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
        agent = DateIdeaAgent(
            llm_client=client,
            rag_search_tool=self.rag_tool,
            template_context="- dinner: restaurant",
        )

        with self.assertRaises(DateIdeaAgentOutputError):
            await agent.generate(DateIdeaRequest(prompt="romantic dinner date"))

    async def test_agent_accepts_venue_returned_by_get_place_profile(self) -> None:
        call_count = {"value": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            call_count["value"] += 1
            if call_count["value"] == 1:
                return _make_response(
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
                                                "name": "get_place_profile",
                                                "arguments": json.dumps(
                                                    {"fsq_place_id": "restaurant-1"}
                                                ),
                                            },
                                        }
                                    ],
                                },
                            }
                        ],
                    },
                )
            return _make_response(
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
                                "content": json.dumps(_valid_agent_output()),
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
        agent = DateIdeaAgent(
            llm_client=client,
            rag_search_tool=self.rag_tool,
            template_context="- dinner: restaurant",
        )

        result = await agent.generate(DateIdeaRequest(prompt="romantic dinner date"))

        self.assertEqual(2, call_count["value"])
        self.assertEqual("restaurant-1", result.ideas[0].stops[0].fsq_place_id)

    def test_retrieved_places_rejects_malformed_profile_tool_output(self) -> None:
        execution = AgentToolExecution(
            call_id="call_1",
            tool_name="get_place_profile",
            arguments={"fsq_place_id": "restaurant-1"},
            output_text=json.dumps({"name": "Missing FSQ ID"}),
            tool_message=OpenRouterMessage(
                role="tool",
                tool_call_id="call_1",
                content=json.dumps({"name": "Missing FSQ ID"}),
            ),
        )

        with self.assertRaises(DateIdeaAgentOutputError):
            _retrieved_places_by_id((execution,))

    def test_retrieved_places_rejects_profile_tool_output_without_name(self) -> None:
        execution = AgentToolExecution(
            call_id="call_1",
            tool_name="get_place_profile",
            arguments={"fsq_place_id": "restaurant-1"},
            output_text=json.dumps({"fsq_place_id": "restaurant-1"}),
            tool_message=OpenRouterMessage(
                role="tool",
                tool_call_id="call_1",
                content=json.dumps({"fsq_place_id": "restaurant-1"}),
            ),
        )

        with self.assertRaises(DateIdeaAgentOutputError):
            _retrieved_places_by_id((execution,))

    async def test_agent_accepts_json_fence_after_intro_text(self) -> None:
        call_count = {"value": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            call_count["value"] += 1
            if call_count["value"] == 1:
                return _make_response(
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
                                    "content": "I'll search.",
                                    "tool_calls": [
                                        {
                                            "id": "call_1",
                                            "type": "function",
                                            "function": {
                                                "name": "search_rag_places",
                                                "arguments": json.dumps(
                                                    {
                                                        "query_text": "romantic restaurant",
                                                        "stop_type": "restaurant",
                                                        "top_k": 1,
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
            return _make_response(
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
                                "content": (
                                    "Here is the final JSON:\n\n```json\n"
                                    + json.dumps(_valid_agent_output())
                                    + "\n```"
                                ),
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
        agent = DateIdeaAgent(
            llm_client=client,
            rag_search_tool=self.rag_tool,
            template_context="- dinner: restaurant",
        )

        result = await agent.generate(DateIdeaRequest(prompt="romantic dinner date"))

        self.assertEqual("Dinner With A Sweet Finish", result.ideas[0].title)

    async def test_agent_repairs_missing_stop_type_once(self) -> None:
        call_count = {"value": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            call_count["value"] += 1
            body = json.loads(request.content.decode("utf-8"))
            if call_count["value"] == 1:
                return _make_response(
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
                                    "content": "I'll search.",
                                    "tool_calls": [
                                        {
                                            "id": "call_1",
                                            "type": "function",
                                            "function": {
                                                "name": "search_rag_places",
                                                "arguments": json.dumps(
                                                    {
                                                        "query_text": "romantic restaurant",
                                                        "stop_type": "restaurant",
                                                        "top_k": 1,
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
            if call_count["value"] == 2:
                bad_output = _valid_agent_output()
                del bad_output["date_ideas"][0]["stops"][0]["stop_type"]
                return _make_response(
                    request,
                    200,
                    {
                        "id": "resp_final_bad",
                        "model": "anthropic/claude-sonnet-4.6",
                        "choices": [
                            {
                                "finish_reason": "stop",
                                "message": {
                                    "role": "assistant",
                                    "content": json.dumps(bad_output),
                                },
                            }
                        ],
                    },
                )

            self.assertEqual(
                {"effort": "medium", "exclude": True},
                body["reasoning"],
            )
            self.assertEqual(
                "date_idea_agent_response_repair",
                body["response_format"]["json_schema"]["name"],
            )
            return _make_response(
                request,
                200,
                {
                    "id": "resp_repair",
                    "model": "anthropic/claude-sonnet-4.6",
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {
                                "role": "assistant",
                                "content": json.dumps(_valid_agent_output()),
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
        agent = DateIdeaAgent(
            llm_client=client,
            rag_search_tool=self.rag_tool,
            template_context="- dinner: restaurant",
        )

        result = await agent.generate(DateIdeaRequest(prompt="romantic dinner date"))

        self.assertEqual(3, call_count["value"])
        self.assertEqual("restaurant", result.ideas[0].stops[0].stop_type)

    async def test_agent_can_execute_verify_place_tool_and_return_execution(self) -> None:
        call_count = {"value": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            call_count["value"] += 1
            if call_count["value"] == 1:
                return _make_response(
                    request,
                    200,
                    {
                        "id": "resp_search_tool",
                        "model": "anthropic/claude-sonnet-4.6",
                        "choices": [
                            {
                                "finish_reason": "tool_calls",
                                "message": {
                                    "role": "assistant",
                                    "content": None,
                                    "tool_calls": [
                                        {
                                            "id": "call_search",
                                            "type": "function",
                                            "function": {
                                                "name": "search_rag_places",
                                                "arguments": json.dumps(
                                                    {
                                                        "query_text": "romantic restaurant",
                                                        "stop_type": "restaurant",
                                                        "top_k": 1,
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
            if call_count["value"] == 2:
                return _make_response(
                    request,
                    200,
                    {
                        "id": "resp_verify_tool",
                        "model": "anthropic/claude-sonnet-4.6",
                        "choices": [
                            {
                                "finish_reason": "tool_calls",
                                "message": {
                                    "role": "assistant",
                                    "content": None,
                                    "tool_calls": [
                                        {
                                            "id": "call_verify",
                                            "type": "function",
                                            "function": {
                                                "name": "verify_place",
                                                "arguments": json.dumps(
                                                    {
                                                        "fsq_place_id": "restaurant-1",
                                                        "plan_time_iso": "2026-04-18T20:00:00",
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
            return _make_response(
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
                                "content": json.dumps(_valid_agent_output()),
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
        maps_client = FakeMapsClient()
        maps_tool = MapsVerifyPlaceTool(
            maps_client=maps_client,
            rag_documents=_documents_df(),
        )
        agent = DateIdeaAgent(
            llm_client=client,
            rag_search_tool=self.rag_tool,
            extra_tools=(maps_tool.as_agent_tool(),),
            template_context="- dinner: restaurant",
        )

        result = await agent.generate(DateIdeaRequest(prompt="romantic dinner date"))

        self.assertEqual(3, call_count["value"])
        self.assertEqual(
            ["search_rag_places", "verify_place"],
            [execution.tool_name for execution in result.tool_executions],
        )
        verify_output = json.loads(result.tool_executions[1].output_text)
        self.assertTrue(verify_output["matched"])
        self.assertEqual("google-restaurant-1", verify_output["google_place_id"])
        self.assertIs(verify_output["open_at_plan_time"], True)
        self.assertEqual(1, maps_client.calls)


def _valid_agent_output() -> dict:
    return {
        "date_ideas": [
            {
                "title": "Dinner With A Sweet Finish",
                "hook": "A warm dinner-first plan with a gentle wander after.",
                "template_hint": "dinner_and_dessert_walk",
                "maps_verification_needed": True,
                "constraints_considered": ["romantic"],
                "stops": [
                    {
                        "kind": "venue",
                        "stop_type": "restaurant",
                        "fsq_place_id": "restaurant-1",
                        "name": "Romantic Restaurant",
                        "description": "Start with dinner in an intimate room.",
                        "why_it_fits": "The retrieved profile mentions romantic dining and wine.",
                    },
                    {
                        "kind": "connective",
                        "stop_type": "harbour_or_pier",
                        "fsq_place_id": None,
                        "name": "Harbour stroll",
                        "description": "Walk somewhere scenic before the next stop.",
                        "why_it_fits": "A connective walk gives the date breathing room.",
                    },
                ],
            }
        ],
        "rejected_ideas": [],
    }


def _retrieved_places_for_validation() -> dict[str, dict]:
    return {
        "restaurant-1": {"name": "Romantic Restaurant"},
        "dessert-1": {"name": "Gelato Corner"},
        "bakery-1": {"name": "Local Bakery"},
        "bar-1": {"name": "Neighbourhood Bar"},
    }


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
            {
                "fsq_place_id": "gallery-1",
                "name": "Quiet Gallery",
                "document_hash": "hash-gallery",
                "document_text": "Independent gallery for slow browsing.",
                "crawl4ai_evidence_snippets": ["Independent gallery for slow browsing."],
                "crawl4ai_template_stop_tags": ["art_gallery"],
                "fsq_category_labels": ["Arts and Entertainment > Art Gallery"],
                "crawl4ai_ambience_tags": ["quiet"],
                "crawl4ai_setting_tags": [],
                "crawl4ai_activity_tags": ["art"],
                "crawl4ai_drink_tags": [],
                "crawl4ai_quality_score": 7,
                "latitude": -33.862,
                "longitude": 151.202,
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
                "embedding": [0.8, 0.2],
            },
            {
                "fsq_place_id": "gallery-1",
                "document_hash": "hash-gallery",
                "embedding_dimension": 2,
                "embedding": [0.0, 1.0],
            },
        ]
    )


def _documents_with_bad_coordinates_df() -> pd.DataFrame:
    rows = _documents_df().to_dict("records")
    rows.append(
        {
            "fsq_place_id": "bad-coords",
            "name": "Bad Coordinates",
            "document_hash": "hash-bad-coords",
            "document_text": "Candidate with malformed coordinates.",
            "crawl4ai_evidence_snippets": ["Candidate with malformed coordinates."],
            "crawl4ai_template_stop_tags": ["dessert_shop"],
            "fsq_category_labels": ["Dining and Drinking > Dessert Shop"],
            "crawl4ai_ambience_tags": [],
            "crawl4ai_setting_tags": [],
            "crawl4ai_activity_tags": [],
            "crawl4ai_drink_tags": [],
            "crawl4ai_quality_score": 7,
            "latitude": "not-a-number",
            "longitude": 151.201,
            "locality": "Sydney",
        }
    )
    return pd.DataFrame(rows)


def _embeddings_with_bad_coordinates_df() -> pd.DataFrame:
    rows = _embeddings_df().to_dict("records")
    rows.append(
        {
            "fsq_place_id": "bad-coords",
            "document_hash": "hash-bad-coords",
            "embedding_dimension": 2,
            "embedding": [0.8, 0.2],
        }
    )
    return pd.DataFrame(rows)


if __name__ == "__main__":
    unittest.main()
