"""Tool-using date-idea agent backed by the local RAG index."""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from back_end.clients.maps_hours import is_open_at_plan_time
from back_end.clients.openrouter import OpenRouterClient
from back_end.llm.models import (
    AgentTool,
    AgentToolExecution,
    OpenRouterFunctionTool,
    OpenRouterMessage,
    make_json_schema_response_format,
)
from back_end.precache.candidate_pools import _haversine_km
from back_end.rag.embeddings import EmbeddingClient
from back_end.rag.models import RagSearchHit, StopRetrievalRequest
from back_end.rag.retriever import CONNECTIVE_STOP_TYPES, RagRetriever, load_date_templates
from back_end.rag.vector_store import ExactVectorStore

logger = logging.getLogger(__name__)

DEFAULT_DATE_IDEA_AGENT_MODEL = "anthropic/claude-sonnet-4.6"
DEFAULT_REASONING_EFFORT = "medium"

TEMPLATE_STOP_TYPE_ALIASES: dict[str, frozenset[str]] = {
    "bakery_or_market": frozenset(
        {
            "bakery",
            "bakery_or_market",
            "farmers_market",
            "market",
            "produce_market",
        }
    ),
    "brewery_or_bar": frozenset(
        {
            "bar",
            "brewery",
            "brewery_or_bar",
            "pub",
            "taproom",
        }
    ),
}


class DateIdeaAgentError(RuntimeError):
    """Base class for date-idea agent failures."""


class DateIdeaAgentToolError(DateIdeaAgentError):
    """Raised when an agent tool receives unsafe or invalid arguments."""


class DateIdeaAgentOutputError(DateIdeaAgentError):
    """Raised when the model emits malformed or ungrounded final output."""


@dataclass(frozen=True)
class DateIdeaRequest:
    """User request normalized for the date-idea agent."""

    prompt: str
    location: str | None = None
    time_window: str | None = None
    vibe: str | None = None
    budget: str | None = None
    transport_mode: str | None = None
    party_size: int = 2
    constraints: str | None = None
    desired_idea_count: int = 3


@dataclass(frozen=True)
class DateIdeaStop:
    """One venue or connective stop in an agent-produced idea."""

    kind: str
    stop_type: str
    name: str
    description: str
    why_it_fits: str
    fsq_place_id: str | None = None


@dataclass(frozen=True)
class DateIdea:
    """A candidate date idea grounded in retrieved RAG venues."""

    title: str
    hook: str
    template_hint: str | None
    stops: tuple[DateIdeaStop, ...]
    maps_verification_needed: bool
    constraints_considered: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class _TemplateStopRequirement:
    """The exact stop slot required by a date template."""

    kind: str
    stop_type: str
    accepted_stop_types: frozenset[str]


@dataclass(frozen=True)
class DateIdeaAgentResult:
    """Terminal result from the date-idea agent."""

    ideas: tuple[DateIdea, ...]
    raw_output: dict[str, Any]
    tool_executions: tuple[AgentToolExecution, ...]
    model: str


DATE_IDEA_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "date_ideas": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "title": {"type": "string"},
                    "hook": {"type": "string"},
                    "template_hint": {"type": ["string", "null"]},
                    "maps_verification_needed": {"type": "boolean"},
                    "constraints_considered": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "stops": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                "kind": {"type": "string", "enum": ["venue", "connective"]},
                                "stop_type": {"type": "string"},
                                "fsq_place_id": {"type": ["string", "null"]},
                                "name": {"type": "string"},
                                "description": {"type": "string"},
                                "why_it_fits": {"type": "string"},
                            },
                            "required": [
                                "kind",
                                "stop_type",
                                "fsq_place_id",
                                "name",
                                "description",
                                "why_it_fits",
                            ],
                        },
                    },
                },
                "required": [
                    "title",
                    "hook",
                    "template_hint",
                    "maps_verification_needed",
                    "constraints_considered",
                    "stops",
                ],
            },
        },
        "rejected_ideas": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "required": ["date_ideas", "rejected_ideas"],
}


class RagPlaceSearchTool:
    """Natural-language RAG search tool exposed to the planning LLM."""

    def __init__(
        self,
        *,
        vector_store: ExactVectorStore,
        embedding_client: EmbeddingClient,
        default_top_k: int = 8,
        max_top_k: int = 15,
        candidate_place_ids: tuple[str, ...] | None = None,
        scope_label: str | None = None,
        include_place_profile_tool: bool = True,
        validated_only: bool = False,
        place_resolver: Any | None = None,
        plan_time_iso: str | None = None,
    ) -> None:
        if default_top_k <= 0:
            raise ValueError("default_top_k must be positive.")
        if max_top_k < default_top_k:
            raise ValueError("max_top_k must be >= default_top_k.")
        self._vector_store = vector_store
        self._embedding_client = embedding_client
        self._retriever = RagRetriever(
            vector_store=vector_store,
            embedding_client=embedding_client,
        )
        self._documents = vector_store.documents
        self._documents_by_id = {
            str(row["fsq_place_id"]): row.to_dict()
            for _, row in self._documents.iterrows()
        }
        self._default_top_k = default_top_k
        self._max_top_k = max_top_k
        self._candidate_place_ids = (
            frozenset(str(place_id) for place_id in candidate_place_ids)
            if candidate_place_ids is not None
            else None
        )
        if self._candidate_place_ids is not None and not self._candidate_place_ids:
            raise ValueError("candidate_place_ids must not be empty when supplied.")
        self._scope_label = scope_label
        self._include_place_profile_tool = include_place_profile_tool
        self._validated_only = validated_only
        self._place_resolver = place_resolver
        self._plan_time_iso = plan_time_iso
        if self._validated_only and self._place_resolver is None:
            raise ValueError("place_resolver is required when validated_only=True.")

    def as_agent_tool(self) -> AgentTool:
        """Return this searcher as an OpenRouter function tool."""

        return AgentTool(
            definition=OpenRouterFunctionTool(
                name="search_rag_places",
                description=(
                    "Search the local date-night RAG database with natural language. "
                    "Use this before proposing any concrete venue. Optional stop_type "
                    "narrows the search to an explicit date-template stop type. "
                    "When this tool is location-scoped, results can only come from "
                    "the configured candidate pool."
                ),
                parameters_json_schema={
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "query_text": {
                            "type": "string",
                            "description": "Natural-language description of the desired venue.",
                        },
                        "stop_type": {
                            "type": ["string", "null"],
                            "description": (
                                "Optional template stop type such as restaurant, cafe, "
                                "wine_bar, art_gallery, museum, bookstore, or dessert_shop."
                            ),
                        },
                        "top_k": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": self._max_top_k,
                            "description": "Maximum number of places to return.",
                        },
                        "exclude_place_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "FSQ place ids that must not be returned.",
                        },
                    },
                    "required": ["query_text"],
                },
                strict=True,
            ),
            handler=self.search,
        )

    def as_agent_tools(self) -> tuple[AgentTool, ...]:
        """Return the full RAG toolset exposed to the planning agent."""

        tools = [
            self.as_agent_tool(),
            self._near_anchor_agent_tool(),
            self._near_latlng_agent_tool(),
        ]
        if self._include_place_profile_tool:
            tools.append(self._place_profile_agent_tool())
        return tuple(tools)

    def _near_anchor_agent_tool(self) -> AgentTool:
        return AgentTool(
            definition=OpenRouterFunctionTool(
                name="search_rag_places_near_anchor",
                description=(
                    "Search the location-scoped RAG database near an already chosen "
                    "venue. Use this after locking stop 1 so later stops stay close "
                    "enough to walk or route feasibly."
                ),
                parameters_json_schema={
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "query_text": {"type": "string"},
                        "anchor_fsq_place_id": {"type": "string"},
                        "max_km": {"type": "number", "exclusiveMinimum": 0},
                        "stop_type": {"type": ["string", "null"]},
                        "top_k": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": self._max_top_k,
                        },
                        "exclude_place_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                    },
                    "required": ["query_text", "anchor_fsq_place_id", "max_km"],
                },
                strict=True,
            ),
            handler=self.search_near_anchor,
        )

    def _near_latlng_agent_tool(self) -> AgentTool:
        return AgentTool(
            definition=OpenRouterFunctionTool(
                name="search_rag_places_near_latlng",
                description=(
                    "Search the location-scoped RAG database near raw coordinates. "
                    "Use this for landmark or bucket anchors that are not venues."
                ),
                parameters_json_schema={
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "query_text": {"type": "string"},
                        "latitude": {"type": "number"},
                        "longitude": {"type": "number"},
                        "max_km": {"type": "number", "exclusiveMinimum": 0},
                        "stop_type": {"type": ["string", "null"]},
                        "top_k": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": self._max_top_k,
                        },
                        "exclude_place_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                    },
                    "required": ["query_text", "latitude", "longitude", "max_km"],
                },
                strict=True,
            ),
            handler=self.search_near_latlng,
        )

    def _place_profile_agent_tool(self) -> AgentTool:
        return AgentTool(
            definition=OpenRouterFunctionTool(
                name="get_place_profile",
                description=(
                    "Return the full RAG profile for one FSQ place id, including "
                    "document text, tags, evidence snippets, quality score, and "
                    "coordinates. Use this to drill into shortlisted places."
                ),
                parameters_json_schema={
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "fsq_place_id": {"type": "string"},
                    },
                    "required": ["fsq_place_id"],
                },
                strict=True,
            ),
            handler=self.get_place_profile,
        )

    async def search(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Execute one natural-language RAG search."""

        query_text = _required_tool_string(arguments, "query_text")
        stop_type = _optional_non_empty_string(arguments.get("stop_type"), "stop_type")
        top_k = _bounded_int(
            arguments.get("top_k", self._default_top_k),
            field_name="top_k",
            minimum=1,
            maximum=self._max_top_k,
        )
        exclude_place_ids = _string_set_argument(
            arguments.get("exclude_place_ids", ()),
            field_name="exclude_place_ids",
        )

        hits, empty_reason = await self._search_hits(
            query_text=query_text,
            stop_type=stop_type,
            top_k=self._search_fetch_limit(top_k),
            exclude_place_ids=exclude_place_ids,
            candidate_place_ids=None,
        )
        results = await self._tool_payloads(
            hits=hits,
            top_k=top_k,
            stop_type=stop_type,
            distances_by_id=None,
            distance_key=None,
        )
        if self._validated_only and not results and empty_reason is None:
            empty_reason = "No Maps-validated candidate places survived filtering."

        if empty_reason is not None:
            logger.warning(
                "RAG search returned no hits for stop_type=%r query=%r: %s",
                stop_type,
                query_text,
                empty_reason,
            )

        return {
            "query_text": query_text,
            "stop_type": stop_type,
            "scope_label": self._scope_label,
            "scope_place_count": (
                len(self._candidate_place_ids)
                if self._candidate_place_ids is not None
                else None
            ),
            "empty_reason": empty_reason,
            "results": results,
        }

    async def search_near_anchor(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Execute a RAG search near an already selected FSQ place."""

        query_text = _required_tool_string(arguments, "query_text")
        anchor_id = _required_tool_string(arguments, "anchor_fsq_place_id")
        max_km = _bounded_float(arguments.get("max_km"), field_name="max_km")
        stop_type = _optional_non_empty_string(arguments.get("stop_type"), "stop_type")
        top_k = _bounded_int(
            arguments.get("top_k", self._default_top_k),
            field_name="top_k",
            minimum=1,
            maximum=self._max_top_k,
        )
        exclude_place_ids = _string_set_argument(
            arguments.get("exclude_place_ids", ()),
            field_name="exclude_place_ids",
        )
        anchor = self._metadata_for_place_id(anchor_id, require_in_scope=True)
        anchor_latitude, anchor_longitude = _coordinates_from_metadata(
            anchor,
            label=f"anchor_fsq_place_id={anchor_id!r}",
        )
        distances_by_id = self._place_distances_near(
            latitude=anchor_latitude,
            longitude=anchor_longitude,
            max_km=max_km,
            excluded=exclude_place_ids | {anchor_id},
        )
        hits, empty_reason = await self._search_hits(
            query_text=query_text,
            stop_type=stop_type,
            top_k=self._search_fetch_limit(top_k),
            exclude_place_ids=exclude_place_ids | {anchor_id},
            candidate_place_ids=set(distances_by_id),
        )
        results = await self._tool_payloads(
            hits=hits,
            top_k=top_k,
            stop_type=stop_type,
            distances_by_id=distances_by_id,
            distance_key="distance_km",
        )
        if self._validated_only and not results and empty_reason is None:
            empty_reason = "No Maps-validated candidate places survived filtering."
        return {
            "query_text": query_text,
            "stop_type": stop_type,
            "scope_label": self._scope_label,
            "scope_place_count": (
                len(self._candidate_place_ids)
                if self._candidate_place_ids is not None
                else None
            ),
            "empty_reason": empty_reason,
            "results": results,
            "anchor_fsq_place_id": anchor_id,
            "max_km": max_km,
        }

    async def search_near_latlng(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Execute a RAG search near raw coordinates."""

        query_text = _required_tool_string(arguments, "query_text")
        latitude = _bounded_float(arguments.get("latitude"), field_name="latitude")
        longitude = _bounded_float(arguments.get("longitude"), field_name="longitude")
        max_km = _bounded_float(arguments.get("max_km"), field_name="max_km")
        stop_type = _optional_non_empty_string(arguments.get("stop_type"), "stop_type")
        top_k = _bounded_int(
            arguments.get("top_k", self._default_top_k),
            field_name="top_k",
            minimum=1,
            maximum=self._max_top_k,
        )
        exclude_place_ids = _string_set_argument(
            arguments.get("exclude_place_ids", ()),
            field_name="exclude_place_ids",
        )
        distances_by_id = self._place_distances_near(
            latitude=latitude,
            longitude=longitude,
            max_km=max_km,
            excluded=exclude_place_ids,
        )
        hits, empty_reason = await self._search_hits(
            query_text=query_text,
            stop_type=stop_type,
            top_k=self._search_fetch_limit(top_k),
            exclude_place_ids=exclude_place_ids,
            candidate_place_ids=set(distances_by_id),
        )
        results = await self._tool_payloads(
            hits=hits,
            top_k=top_k,
            stop_type=stop_type,
            distances_by_id=distances_by_id,
            distance_key="distance_from_seed_km",
        )
        if self._validated_only and not results and empty_reason is None:
            empty_reason = "No Maps-validated candidate places survived filtering."
        return {
            "query_text": query_text,
            "stop_type": stop_type,
            "latitude": latitude,
            "longitude": longitude,
            "max_km": max_km,
            "scope_label": self._scope_label,
            "empty_reason": empty_reason,
            "results": results,
        }

    def get_place_profile(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Return full RAG profile data for one scoped FSQ place."""

        place_id = _required_tool_string(arguments, "fsq_place_id")
        metadata = self._metadata_for_place_id(place_id, require_in_scope=True)
        return _metadata_profile_payload(metadata, scope_label=self._scope_label)

    async def _search_hits(
        self,
        *,
        query_text: str,
        stop_type: str | None,
        top_k: int,
        exclude_place_ids: set[str],
        candidate_place_ids: set[str] | None,
    ) -> tuple[tuple[RagSearchHit, ...], str | None]:
        allowed_ids = self._allowed_place_ids(exclude_place_ids)
        if candidate_place_ids is not None:
            allowed_ids = candidate_place_ids if allowed_ids is None else allowed_ids & candidate_place_ids
        if allowed_ids is not None and not allowed_ids:
            return (), "No candidate places remain after scope, distance, and exclusion filters."

        if stop_type is not None:
            result = await self._retriever.retrieve_stop(
                StopRetrievalRequest(
                    stop_type=stop_type,
                    query_text=query_text,
                    top_k=min(self._max_top_k, top_k + len(exclude_place_ids)),
                    candidate_place_ids=tuple(sorted(allowed_ids)) if allowed_ids is not None else None,
                )
            )
            hits = tuple(
                hit for hit in result.hits if hit.fsq_place_id not in exclude_place_ids
            )[:top_k]
            return hits, result.empty_reason if not hits else None

        query_embedding = (await self._embedding_client.embed_texts((query_text,)))[0]
        hits = self._vector_store.search(
            query_embedding,
            top_k=top_k,
            candidate_place_ids=allowed_ids,
        )
        return hits, "Vector search returned no hits." if not hits else None

    def _search_fetch_limit(self, requested_top_k: int) -> int:
        if not self._validated_only:
            return requested_top_k
        return min(self._max_top_k, max(requested_top_k, requested_top_k * 3))

    async def _tool_payloads(
        self,
        *,
        hits: tuple[RagSearchHit, ...],
        top_k: int,
        stop_type: str | None,
        distances_by_id: dict[str, float] | None,
        distance_key: str | None,
    ) -> list[dict[str, Any]]:
        if not self._validated_only:
            return [
                _hit_to_tool_payload_with_distance(
                    hit,
                    distances_by_id=distances_by_id,
                    distance_key=distance_key,
                )
                for hit in hits[:top_k]
            ]

        assert self._place_resolver is not None
        results: list[dict[str, Any]] = []
        for hit in hits:
            resolved = await self._place_resolver.resolve_place_match(hit.fsq_place_id)
            if not hasattr(resolved, "google_place"):
                logger.warning(
                    "Dropping fsq_place_id=%s from validated RAG results: %s",
                    hit.fsq_place_id,
                    getattr(resolved, "reason", "Maps resolution failed."),
                )
                continue

            place = resolved.google_place
            if place.business_status not in {None, "OPERATIONAL"}:
                logger.warning(
                    "Dropping fsq_place_id=%s from validated RAG results: "
                    "business_status=%s.",
                    hit.fsq_place_id,
                    place.business_status,
                )
                continue

            if self._plan_time_iso is not None and place.regular_opening_hours is not None:
                open_at_plan_time = is_open_at_plan_time(
                    place.regular_opening_hours,
                    self._plan_time_iso,
                )
                if open_at_plan_time is False:
                    logger.warning(
                        "Dropping fsq_place_id=%s from validated RAG results: "
                        "closed at plan_time_iso=%s.",
                        hit.fsq_place_id,
                        self._plan_time_iso,
                    )
                    continue

            results.append(
                _validated_hit_to_tool_payload(
                    hit,
                    resolved=resolved,
                    stop_type=stop_type,
                    distances_by_id=distances_by_id,
                    distance_key=distance_key,
                )
            )
            if len(results) >= top_k:
                break
        return results

    def _metadata_for_place_id(
        self,
        place_id: str,
        *,
        require_in_scope: bool,
    ) -> dict[str, Any]:
        if require_in_scope and self._candidate_place_ids is not None and place_id not in self._candidate_place_ids:
            raise DateIdeaAgentToolError(
                f"Place {place_id!r} is outside the current RAG search scope."
            )
        metadata = self._documents_by_id.get(place_id)
        if metadata is None:
            raise DateIdeaAgentToolError(f"Unknown fsq_place_id {place_id!r}.")
        return metadata

    def _place_distances_near(
        self,
        *,
        latitude: float,
        longitude: float,
        max_km: float,
        excluded: set[str],
    ) -> dict[str, float]:
        allowed_ids = self._allowed_place_ids(excluded)
        if allowed_ids is None:
            allowed_ids = set(self._documents_by_id)
        if not allowed_ids:
            return {}
        nearby: dict[str, float] = {}
        for place_id in allowed_ids:
            metadata = self._documents_by_id.get(place_id)
            if metadata is None:
                logger.error(
                    "RAG search scope referenced unknown fsq_place_id=%s.",
                    place_id,
                )
                continue
            try:
                distance = _distance_from_metadata(
                    metadata,
                    latitude=latitude,
                    longitude=longitude,
                )
            except DateIdeaAgentToolError as exc:
                logger.error(
                    "Skipping fsq_place_id=%s during radius filtering because "
                    "coordinates are unusable: %s",
                    place_id,
                    exc,
                )
                continue
            if distance <= max_km:
                nearby[place_id] = distance
        return nearby

    def _allowed_place_ids(self, excluded: set[str]) -> set[str] | None:
        if self._candidate_place_ids is None and not excluded:
            return None
        if self._candidate_place_ids is not None:
            return set(self._candidate_place_ids) - excluded
        documents = self._vector_store.documents
        return {
            str(place_id)
            for place_id in documents["fsq_place_id"].astype(str)
            if str(place_id) not in excluded
        }


class DateIdeaAgent:
    """LLM agent that queries RAG and returns grounded date ideas."""

    def __init__(
        self,
        *,
        llm_client: OpenRouterClient,
        rag_search_tool: RagPlaceSearchTool,
        extra_tools: tuple[AgentTool, ...] = (),
        model: str = DEFAULT_DATE_IDEA_AGENT_MODEL,
        reasoning_effort: str = DEFAULT_REASONING_EFFORT,
        max_tokens: int = 3000,
        max_tool_round_trips: int = 8,
        template_context: str | None = None,
        system_prompt_override: str | None = None,
    ) -> None:
        if max_tokens <= 0:
            raise ValueError("max_tokens must be positive.")
        if max_tool_round_trips <= 0:
            raise ValueError("max_tool_round_trips must be positive.")
        if system_prompt_override is not None and not system_prompt_override.strip():
            raise ValueError("system_prompt_override must be a non-empty string.")
        self._llm_client = llm_client
        self._rag_search_tool = rag_search_tool
        self._extra_tools = tuple(extra_tools)
        _validate_unique_tool_names(
            self._rag_search_tool.as_agent_tools() + self._extra_tools
        )
        self._has_maps_verify_tool = any(
            tool.definition.name == "verify_place" for tool in self._extra_tools
        )
        self._has_maps_compute_leg_tool = any(
            tool.definition.name == "compute_leg" for tool in self._extra_tools
        )
        self._model = model
        self._reasoning_effort = reasoning_effort
        self._max_tokens = max_tokens
        self._max_tool_round_trips = max_tool_round_trips
        self._template_context = template_context or _default_template_context()
        self._system_prompt_override = (
            system_prompt_override.strip() if system_prompt_override is not None else None
        )

    async def generate(
        self,
        request: DateIdeaRequest,
        *,
        template: Mapping[str, Any] | None = None,
    ) -> DateIdeaAgentResult:
        """Generate grounded date ideas for a user request.

        When ``template`` is supplied, the returned ideas are additionally
        required to match that template's stop shape (count, kinds, and
        stop_types). This is what the pre-cache pipeline uses to force the
        agent to obey an exact template.
        """

        if not request.prompt.strip():
            raise ValueError("DateIdeaRequest.prompt must not be empty.")
        if request.desired_idea_count <= 0:
            raise ValueError("desired_idea_count must be positive.")
        if request.party_size <= 0:
            raise ValueError("party_size must be positive.")

        messages = (
            OpenRouterMessage(role="system", content=self._system_prompt()),
            OpenRouterMessage(role="user", content=self._user_prompt(request)),
        )
        agent_result = await self._llm_client.run_agent(
            messages=messages,
            tools=list(self._rag_search_tool.as_agent_tools() + self._extra_tools),
            model=self._model,
            temperature=0.2,
            response_format=None,
            parallel_tool_calls=True,
            max_tokens=self._max_tokens,
            max_round_trips=self._max_tool_round_trips,
            extra_body={
                "reasoning": {
                    "effort": self._reasoning_effort,
                    "exclude": True,
                }
            },
        )
        retrieved_places = _retrieved_places_by_id(agent_result.tool_executions)
        try:
            raw_output = _parse_final_json(agent_result.final_response.output_text)
            ideas = _parse_and_validate_ideas(
                raw_output,
                retrieved_places=retrieved_places,
                template=template,
            )
        except DateIdeaAgentOutputError as exc:
            logger.warning(
                "Date-idea agent final output failed validation; attempting one "
                "schema repair. error=%s",
                exc,
            )
            raw_output = await self._repair_final_output(
                bad_output=agent_result.final_response.output_text or "",
                validation_error=str(exc),
                retrieved_places=retrieved_places,
            )
            ideas = _parse_and_validate_ideas(
                raw_output,
                retrieved_places=retrieved_places,
                template=template,
            )
        if not ideas:
            logger.warning(
                "Date-idea agent returned no ideas. rejected=%s",
                raw_output.get("rejected_ideas"),
            )
        return DateIdeaAgentResult(
            ideas=ideas,
            raw_output=raw_output,
            tool_executions=agent_result.tool_executions,
            model=agent_result.final_response.model,
        )

    def _system_prompt(self) -> str:
        if self._system_prompt_override is not None:
            return self._system_prompt_override
        maps_rules: list[str] = []
        if self._has_maps_verify_tool:
            maps_rules.append(
                "- Before final JSON, call verify_place for every venue you intend to use. "
                "If the request includes a precise ISO date/time for a stop, pass it as "
                "plan_time_iso; otherwise pass null. Reject or replace venues that are "
                "unmatched, permanently closed, implausibly rated, or closed at the plan time."
            )
        if self._has_maps_compute_leg_tool:
            maps_rules.append(
                "- Call compute_leg for adjacent venue stops when travel plausibility "
                "matters. Use the user's requested transport mode when it maps to a "
                "supported travel_mode. Reject or revise plans with failed endpoint "
                "matches, missing route durations, or implausible travel times."
            )
        if not maps_rules:
            maps_rules.append(
                "- Google Maps feasibility checks happen after this stage, so set "
                "maps_verification_needed=true."
            )
        return "\n".join(
            [
                "You are a Date Night planning agent for Sydney.",
                "Your job is to build concrete date ideas by querying the local RAG database.",
                "Rules:",
                "- Call search_rag_places before naming any venue.",
                "- Use search_rag_places_near_anchor after choosing a venue when the next stop should be walkable.",
                "- Use search_rag_places_near_latlng when a bucket or landmark coordinate should anchor the plan.",
                "- Use get_place_profile to inspect full evidence for shortlisted venues.",
                "- Every venue stop must use an fsq_place_id returned by one of the RAG tools in this run.",
                "- Do not invent venues, ratings, opening hours, maps links, routes, or travel times.",
                *maps_rules,
                "- Connective stops such as a walk, ferry ride, lookout moment, or harbour stroll are allowed without an fsq_place_id.",
                "- Prefer two to four stops per idea. Keep ideas distinct from each other.",
                "- If the RAG results are weak or empty, say so in rejected_ideas instead of fabricating.",
                "- Your final answer must be only a JSON object. No markdown fences, no prose.",
                "- Output schema is enforced separately. Include date_ideas and rejected_ideas only.",
                "- Each idea must include title, hook, template_hint, maps_verification_needed, constraints_considered, and stops.",
                "- Each stop must include kind, stop_type, fsq_place_id, name, description, and why_it_fits.",
                "",
                "Useful date-template shapes:",
                self._template_context,
            ]
        )

    @staticmethod
    def _user_prompt(request: DateIdeaRequest) -> str:
        fields = [
            ("Prompt", request.prompt),
            ("Location", request.location),
            ("Time window", request.time_window),
            ("Vibe", request.vibe),
            ("Budget", request.budget),
            ("Transport mode", request.transport_mode),
            ("Party size", str(request.party_size)),
            ("Constraints", request.constraints),
            ("Desired idea count", str(request.desired_idea_count)),
        ]
        return "\n".join(f"{label}: {value}" for label, value in fields if value)

    async def _repair_final_output(
        self,
        *,
        bad_output: str,
        validation_error: str,
        retrieved_places: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """Ask the model once to repair malformed JSON, then parse it."""

        response = await self._llm_client.create_chat_completion(
            messages=(
                OpenRouterMessage(
                    role="system",
                    content=(
                        "Repair a malformed Date Night agent JSON response. "
                        "Return only valid JSON matching the schema. Do not add venues. "
                        "Every venue fsq_place_id must be in the allowed venue list. "
                        "Every stop must include kind, stop_type, fsq_place_id, name, "
                        "description, and why_it_fits. For missing venue stop_type, "
                        "use the most relevant allowed template_stop_tags value. For "
                        "connective stops, use a descriptive type like harbor_or_pier, "
                        "walk, ferry_ride, or scenic_lookout and fsq_place_id=null."
                    ),
                ),
                OpenRouterMessage(
                    role="user",
                    content=json.dumps(
                        {
                            "validation_error": validation_error,
                            "bad_output": bad_output,
                            "allowed_venues": _retrieved_place_repair_context(
                                retrieved_places
                            ),
                        },
                        ensure_ascii=True,
                    ),
                ),
            ),
            model=self._model,
            temperature=0.0,
            response_format=make_json_schema_response_format(
                "date_idea_agent_response_repair",
                DATE_IDEA_RESPONSE_SCHEMA,
            ),
            max_tokens=self._max_tokens,
            extra_body={
                "reasoning": {
                    "effort": self._reasoning_effort,
                    "exclude": True,
                }
            },
        )
        return _parse_final_json(response.output_text)


def _validate_unique_tool_names(tools: tuple[AgentTool, ...]) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for tool in tools:
        name = tool.definition.name
        if name in seen:
            duplicates.add(name)
        seen.add(name)
    if duplicates:
        raise ValueError(
            "Agent tool names must be unique; duplicates: "
            + ", ".join(sorted(duplicates))
        )


def _hit_to_tool_payload(hit: RagSearchHit) -> dict[str, Any]:
    metadata = hit.metadata
    return {
        "fsq_place_id": hit.fsq_place_id,
        "name": hit.name,
        "address": _optional_metadata_str(metadata.get("address")),
        "semantic_score": round(hit.semantic_score, 6),
        "final_score": round(hit.final_score, 6),
        "score_breakdown": {
            key: round(float(value), 6)
            for key, value in sorted(hit.score_breakdown.items())
        },
        "locality": _optional_metadata_str(metadata.get("locality")),
        "latitude": _optional_float(metadata.get("latitude")),
        "longitude": _optional_float(metadata.get("longitude")),
        "category_labels": _string_list(metadata.get("fsq_category_labels")),
        "template_stop_tags": _string_list(metadata.get("crawl4ai_template_stop_tags")),
        "ambience_tags": _string_list(metadata.get("crawl4ai_ambience_tags")),
        "setting_tags": _string_list(metadata.get("crawl4ai_setting_tags")),
        "activity_tags": _string_list(metadata.get("crawl4ai_activity_tags")),
        "drink_tags": _string_list(metadata.get("crawl4ai_drink_tags")),
        "evidence_snippets": tuple(hit.evidence_snippets[:3]),
        "document_hash": hit.document_hash,
    }


def _hit_to_tool_payload_with_distance(
    hit: RagSearchHit,
    *,
    distances_by_id: dict[str, float] | None,
    distance_key: str | None,
) -> dict[str, Any]:
    payload = _hit_to_tool_payload(hit)
    if distances_by_id is not None and distance_key is not None:
        payload[distance_key] = round(distances_by_id[hit.fsq_place_id], 3)
    return payload


def _validated_hit_to_tool_payload(
    hit: RagSearchHit,
    *,
    resolved: Any,
    stop_type: str | None,
    distances_by_id: dict[str, float] | None,
    distance_key: str | None,
) -> dict[str, Any]:
    place = resolved.google_place
    payload: dict[str, Any] = {
        "fsq_place_id": hit.fsq_place_id,
        "name": place.display_name or hit.name,
        "address": place.formatted_address or _optional_metadata_str(hit.metadata.get("address")),
        "stop_type": stop_type,
        "match_kind": getattr(resolved, "match_kind", "validated"),
        "reason": _validated_candidate_reason(stop_type=stop_type, hit=hit),
    }
    if distances_by_id is not None and distance_key is not None:
        payload[distance_key] = round(distances_by_id[hit.fsq_place_id], 3)
    return payload


def _validated_candidate_reason(*, stop_type: str | None, hit: RagSearchHit) -> str:
    label = stop_type.replace("_", " ") if stop_type else "venue"
    tags = _string_list(hit.metadata.get("crawl4ai_ambience_tags"))
    if tags:
        return f"Validated {label}; tags: {', '.join(tags[:3])}."
    return f"Validated {label} candidate."


def _parse_final_json(text: str | None) -> dict[str, Any]:
    if text is None or text.strip() == "":
        raise DateIdeaAgentOutputError("Date-idea agent returned an empty final response.")
    cleaned = _strip_json_fence(text.strip())
    try:
        parsed = json.loads(cleaned)
    except ValueError:
        extracted = _extract_first_json_object(cleaned)
        if extracted is None:
            logger.error(
                "Date-idea agent returned non-JSON final output: %r",
                cleaned[:1000],
            )
            raise DateIdeaAgentOutputError(
                "Date-idea agent returned non-JSON final output."
            ) from None
        try:
            parsed = json.loads(extracted)
        except ValueError as exc:
            logger.error(
                "Date-idea agent returned non-JSON final output after JSON "
                "object extraction: %r",
                extracted[:1000],
            )
            raise DateIdeaAgentOutputError(
                "Date-idea agent returned non-JSON final output."
            ) from exc
        logger.warning(
            "Date-idea agent wrapped its JSON output in surrounding text; "
            "extracted the first balanced JSON object and continuing."
        )
    if not isinstance(parsed, dict):
        raise DateIdeaAgentOutputError("Date-idea agent final output must be a JSON object.")
    return parsed


def _extract_first_json_object(text: str) -> str | None:
    """Return the first balanced top-level JSON object in ``text``, if any.

    The date-idea agent is sometimes a little chatty and emits text like
    ``"Here is the plan:\\n\\n{...}\\n\\nHope it helps!"`` despite the
    system-prompt instruction to emit only JSON. Rather than burn a
    schema-repair round trip on that, this helper walks the string and
    returns the substring spanning the first complete ``{...}`` object,
    respecting string boundaries and escapes. Returns ``None`` if no such
    object exists.
    """

    start_index = text.find("{")
    if start_index == -1:
        return None
    depth = 0
    in_string = False
    escaped = False
    for index in range(start_index, len(text)):
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
                continue
            if char == "\\":
                escaped = True
                continue
            if char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            continue
        if char == "{":
            depth += 1
            continue
        if char == "}":
            depth -= 1
            if depth == 0:
                return text[start_index : index + 1]
    return None


def _strip_json_fence(text: str) -> str:
    fence_start = text.find("```json")
    if fence_start == -1:
        fence_start = text.find("```JSON")
    if fence_start != -1:
        content_start = text.find("\n", fence_start)
        if content_start == -1:
            return text
        fence_end = text.find("```", content_start + 1)
        if fence_end != -1:
            return text[content_start + 1 : fence_end].strip()

    if not text.startswith("```"):
        return text
    lines = text.splitlines()
    if len(lines) >= 3 and lines[0].strip() in {"```", "```json", "```JSON"}:
        return "\n".join(lines[1:-1]).strip()
    return text


def _retrieved_places_by_id(
    tool_executions: tuple[AgentToolExecution, ...],
) -> dict[str, dict[str, Any]]:
    places: dict[str, dict[str, Any]] = {}
    for execution in tool_executions:
        if execution.tool_name in {
            "search_rag_places",
            "search_rag_places_near_anchor",
            "search_rag_places_near_latlng",
        }:
            _add_search_results_to_retrieved_places(places, execution)
        elif execution.tool_name == "get_place_profile":
            _add_profile_to_retrieved_places(places, execution)
    return places


def _add_search_results_to_retrieved_places(
    places: dict[str, dict[str, Any]],
    execution: AgentToolExecution,
) -> None:
    try:
        payload = json.loads(execution.output_text)
    except ValueError as exc:
        raise DateIdeaAgentOutputError(
            f"{execution.tool_name} returned non-JSON tool output."
        ) from exc
    results = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(results, list):
        raise DateIdeaAgentOutputError(
            f"{execution.tool_name} tool output did not contain a results list."
        )
    for item in results:
        if not isinstance(item, dict):
            raise DateIdeaAgentOutputError(
                f"{execution.tool_name} tool output contained a non-object result."
            )
        place_id = item.get("fsq_place_id")
        if not isinstance(place_id, str) or not place_id.strip():
            raise DateIdeaAgentOutputError(
                f"{execution.tool_name} tool output contained a result without fsq_place_id."
            )
        if not isinstance(item.get("name"), str) or not item["name"].strip():
            raise DateIdeaAgentOutputError(
                f"{execution.tool_name} tool output contained a result without name."
            )
        places[place_id.strip()] = item


def _add_profile_to_retrieved_places(
    places: dict[str, dict[str, Any]],
    execution: AgentToolExecution,
) -> None:
    try:
        payload = json.loads(execution.output_text)
    except ValueError as exc:
        raise DateIdeaAgentOutputError(
            "get_place_profile returned non-JSON tool output."
        ) from exc
    if not isinstance(payload, dict):
        raise DateIdeaAgentOutputError(
            "get_place_profile tool output must be a JSON object."
        )
    place_id = payload.get("fsq_place_id")
    if not isinstance(place_id, str) or not place_id.strip():
        raise DateIdeaAgentOutputError(
            "get_place_profile tool output did not contain fsq_place_id."
        )
    if not isinstance(payload.get("name"), str) or not payload["name"].strip():
        raise DateIdeaAgentOutputError(
            "get_place_profile tool output did not contain name."
        )
    places[place_id.strip()] = payload


def _retrieved_place_repair_context(
    retrieved_places: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    context: list[dict[str, Any]] = []
    for place_id, place in sorted(retrieved_places.items()):
        context.append(
            {
                "fsq_place_id": place_id,
                "name": place.get("name"),
                "template_stop_tags": _string_list(place.get("template_stop_tags")),
                "category_labels": _string_list(place.get("category_labels")),
            }
        )
    return context


def _parse_and_validate_ideas(
    raw_output: dict[str, Any],
    *,
    retrieved_places: dict[str, dict[str, Any]],
    template: dict[str, Any] | None = None,
) -> tuple[DateIdea, ...]:
    raw_ideas = raw_output.get("date_ideas")
    if not isinstance(raw_ideas, list):
        raise DateIdeaAgentOutputError("date_ideas must be a list.")
    if "rejected_ideas" in raw_output and not isinstance(raw_output["rejected_ideas"], list):
        raise DateIdeaAgentOutputError("rejected_ideas must be a list.")
    template_requirements = (
        _template_stop_requirements(template) if template is not None else None
    )

    ideas: list[DateIdea] = []
    used_plan_titles: set[str] = set()
    for idea_index, raw_idea in enumerate(raw_ideas):
        if not isinstance(raw_idea, dict):
            raise DateIdeaAgentOutputError(f"date_ideas[{idea_index}] must be an object.")
        title = _output_string_or_default(
            raw_idea,
            "title",
            default=f"Date Night Plan {idea_index + 1}",
        )
        normalized_title = title.casefold()
        if normalized_title in used_plan_titles:
            raise DateIdeaAgentOutputError(f"Duplicate date idea title {title!r}.")
        used_plan_titles.add(normalized_title)

        raw_stops = raw_idea.get("stops")
        if not isinstance(raw_stops, list) or not raw_stops:
            raise DateIdeaAgentOutputError(f"Date idea {title!r} must contain stops.")
        stops = tuple(
            _parse_and_validate_stop(
                raw_stop,
                idea_title=title,
                stop_index=stop_index,
                retrieved_places=retrieved_places,
            )
            for stop_index, raw_stop in enumerate(raw_stops)
        )
        if template_requirements is not None:
            stops = _coerce_stops_to_template_shape(
                idea_title=title,
                stops=stops,
                template=template or {},
                requirements=template_requirements,
            )
            _validate_idea_matches_template(
                idea_title=title,
                stops=stops,
                template=template,
                requirements=template_requirements,
            )
        venue_ids = [stop.fsq_place_id for stop in stops if stop.kind == "venue"]
        if not venue_ids:
            raise DateIdeaAgentOutputError(
                f"Date idea {title!r} contains no grounded venue stops."
            )
        if len(set(venue_ids)) != len(venue_ids):
            raise DateIdeaAgentOutputError(
                f"Date idea {title!r} reuses the same venue more than once."
            )

        ideas.append(
            DateIdea(
                title=title,
                hook=_output_string_or_default(
                    raw_idea,
                    "hook",
                    default="A Maps-verified date night plan.",
                ),
                template_hint=_nullable_string(raw_idea.get("template_hint"), "template_hint"),
                stops=stops,
                maps_verification_needed=_required_bool(
                    raw_idea,
                    "maps_verification_needed",
                ),
                constraints_considered=tuple(
                    _string_list(raw_idea.get("constraints_considered"))
                ),
            )
        )
    return tuple(ideas)


def _parse_and_validate_stop(
    raw_stop: Any,
    *,
    idea_title: str,
    stop_index: int,
    retrieved_places: dict[str, dict[str, Any]],
) -> DateIdeaStop:
    if not isinstance(raw_stop, dict):
        raise DateIdeaAgentOutputError(
            f"Stop {stop_index} in {idea_title!r} must be an object."
        )
    kind = _required_output_string(raw_stop, "kind")
    if kind not in {"venue", "connective"}:
        raise DateIdeaAgentOutputError(
            f"Stop {stop_index} in {idea_title!r} has invalid kind {kind!r}."
        )
    fsq_place_id = _nullable_string(raw_stop.get("fsq_place_id"), "fsq_place_id")
    if kind == "venue":
        if fsq_place_id is None:
            raise DateIdeaAgentOutputError(
                f"Venue stop {stop_index} in {idea_title!r} is missing fsq_place_id."
            )
        retrieved = retrieved_places.get(fsq_place_id)
        if retrieved is None:
            logger.error(
                "Date-idea agent hallucinated fsq_place_id=%s in idea=%r stop=%s.",
                fsq_place_id,
                idea_title,
                stop_index,
            )
            raise DateIdeaAgentOutputError(
                f"Venue stop {stop_index} in {idea_title!r} used fsq_place_id "
                f"{fsq_place_id!r}, which was not returned by a RAG tool."
            )
        raw_name = _required_output_string(raw_stop, "name")
        retrieved_name = str(retrieved.get("name") or "").strip()
        if retrieved_name and raw_name.casefold() != retrieved_name.casefold():
            raise DateIdeaAgentOutputError(
                f"Venue stop {stop_index} in {idea_title!r} named fsq_place_id "
                f"{fsq_place_id!r} as {raw_name!r}, expected {retrieved_name!r}."
            )
    elif fsq_place_id is not None:
        raise DateIdeaAgentOutputError(
            f"Connective stop {stop_index} in {idea_title!r} must not have fsq_place_id."
        )

    return DateIdeaStop(
        kind=kind,
        stop_type=_required_output_string(raw_stop, "stop_type"),
        fsq_place_id=fsq_place_id,
        name=_required_output_string(raw_stop, "name"),
        description=_output_string_or_default(
            raw_stop,
            "description",
            default=_required_output_string(raw_stop, "name"),
        ),
        why_it_fits=_output_string_or_default(
            raw_stop,
            "why_it_fits",
            default="Matches the requested template stop and was returned by the validated search tools.",
        ),
    )


def _template_stop_requirements(
    template: dict[str, Any],
) -> tuple[_TemplateStopRequirement, ...]:
    template_id = _template_id_for_error(template)
    raw_stops = template.get("stops")
    if not isinstance(raw_stops, list) or not raw_stops:
        _raise_template_shape_error(
            f"Template {template_id!r} must contain a non-empty stops list."
        )

    requirements: list[_TemplateStopRequirement] = []
    for stop_index, raw_stop in enumerate(raw_stops):
        if not isinstance(raw_stop, dict):
            _raise_template_shape_error(
                f"Template {template_id!r} stop {stop_index} must be an object."
            )
        raw_stop_type = raw_stop.get("type")
        if not isinstance(raw_stop_type, str) or not raw_stop_type.strip():
            _raise_template_shape_error(
                f"Template {template_id!r} stop {stop_index} must have a non-empty type."
            )
        stop_type = raw_stop_type.strip()
        kind = _template_stop_kind(
            raw_stop,
            stop_type=stop_type,
            template_id=template_id,
            stop_index=stop_index,
        )
        requirements.append(
            _TemplateStopRequirement(
                kind=kind,
                stop_type=stop_type,
                accepted_stop_types=_accepted_template_stop_types(stop_type),
            )
        )
    return tuple(requirements)


def _template_stop_kind(
    raw_stop: dict[str, Any],
    *,
    stop_type: str,
    template_id: str,
    stop_index: int,
) -> str:
    raw_kind = raw_stop.get("kind")
    if raw_kind is None:
        return "connective" if stop_type in CONNECTIVE_STOP_TYPES else "venue"
    if not isinstance(raw_kind, str) or not raw_kind.strip():
        _raise_template_shape_error(
            f"Template {template_id!r} stop {stop_index} kind must be a non-empty string."
        )
    kind = raw_kind.strip()
    if kind not in {"venue", "connective"}:
        _raise_template_shape_error(
            f"Template {template_id!r} stop {stop_index} has invalid kind {kind!r}."
        )
    return kind


def _accepted_template_stop_types(stop_type: str) -> frozenset[str]:
    normalized = _normalize_stop_type(stop_type)
    aliases = TEMPLATE_STOP_TYPE_ALIASES.get(normalized)
    if aliases is None:
        return frozenset({normalized})
    return aliases | frozenset({normalized})


def _validate_idea_matches_template(
    *,
    idea_title: str,
    stops: tuple[DateIdeaStop, ...],
    template: dict[str, Any],
    requirements: tuple[_TemplateStopRequirement, ...],
) -> None:
    template_id = _template_id_for_error(template)
    if len(stops) != len(requirements):
        _raise_template_shape_error(
            f"Date idea {idea_title!r} has {len(stops)} stops, but template "
            f"{template_id!r} requires {len(requirements)} stops."
        )

    for stop_index, (stop, requirement) in enumerate(zip(stops, requirements)):
        if stop.kind != requirement.kind:
            _raise_template_shape_error(
                f"Stop {stop_index} in {idea_title!r} has kind {stop.kind!r}, "
                f"but template {template_id!r} requires kind {requirement.kind!r}."
            )
        normalized_stop_type = _normalize_stop_type(stop.stop_type)
        if normalized_stop_type not in requirement.accepted_stop_types:
            expected = _format_expected_stop_types(requirement)
            _raise_template_shape_error(
                f"Stop {stop_index} in {idea_title!r} has stop_type "
                f"{stop.stop_type!r}, but template {template_id!r} requires "
                f"{expected}."
            )


def _coerce_stops_to_template_shape(
    *,
    idea_title: str,
    stops: tuple[DateIdeaStop, ...],
    template: dict[str, Any],
    requirements: tuple[_TemplateStopRequirement, ...],
) -> tuple[DateIdeaStop, ...]:
    if len(stops) == len(requirements):
        return stops

    selected: list[DateIdeaStop] = []
    cursor = 0
    for requirement in requirements:
        found: DateIdeaStop | None = None
        while cursor < len(stops):
            stop = stops[cursor]
            cursor += 1
            if stop.kind != requirement.kind:
                continue
            if _normalize_stop_type(stop.stop_type) not in requirement.accepted_stop_types:
                continue
            found = stop
            break
        if found is None:
            return stops
        selected.append(found)

    logger.warning(
        "Date-idea agent returned %d stops for template=%s; keeping the %d "
        "stops that match the required template sequence for idea=%r.",
        len(stops),
        _template_id_for_error(template),
        len(selected),
        idea_title,
    )
    return tuple(selected)


def _template_id_for_error(template: dict[str, Any]) -> str:
    template_id = template.get("id")
    if isinstance(template_id, str) and template_id.strip():
        return template_id.strip()
    title = template.get("title")
    if isinstance(title, str) and title.strip():
        return title.strip()
    return "<unknown>"


def _normalize_stop_type(stop_type: str) -> str:
    return stop_type.strip().casefold()


def _format_expected_stop_types(requirement: _TemplateStopRequirement) -> str:
    if requirement.accepted_stop_types == frozenset(
        {_normalize_stop_type(requirement.stop_type)}
    ):
        return repr(requirement.stop_type)
    return "one of " + ", ".join(repr(item) for item in sorted(requirement.accepted_stop_types))


def _raise_template_shape_error(message: str) -> None:
    logger.error("Date-idea agent output failed template-shape validation: %s", message)
    raise DateIdeaAgentOutputError(message)


def _required_tool_string(payload: dict[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise DateIdeaAgentToolError(f"{field_name} must be a non-empty string.")
    return value.strip()


def _required_output_string(payload: dict[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise DateIdeaAgentOutputError(f"{field_name} must be a non-empty string.")
    return value.strip()


def _output_string_or_default(
    payload: dict[str, Any],
    field_name: str,
    *,
    default: str,
) -> str:
    value = payload.get(field_name)
    if isinstance(value, str) and value.strip():
        return value.strip()
    logger.warning(
        "Date-idea agent output field %s was missing or empty; using fallback text.",
        field_name,
    )
    return default


def _optional_non_empty_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise DateIdeaAgentToolError(f"{field_name} must be null or a non-empty string.")
    return value.strip()


def _nullable_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise DateIdeaAgentOutputError(f"{field_name} must be null or a non-empty string.")
    return value.strip()


def _required_bool(payload: dict[str, Any], field_name: str) -> bool:
    value = payload.get(field_name)
    if not isinstance(value, bool):
        raise DateIdeaAgentOutputError(f"{field_name} must be a boolean.")
    return value


def _bounded_int(value: Any, *, field_name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DateIdeaAgentToolError(f"{field_name} must be an integer.")
    if value < minimum or value > maximum:
        raise DateIdeaAgentToolError(
            f"{field_name} must be between {minimum} and {maximum}, got {value}."
        )
    return value


def _bounded_float(value: Any, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise DateIdeaAgentToolError(f"{field_name} must be numeric.")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise DateIdeaAgentToolError(f"{field_name} must be numeric.") from exc
    if not math.isfinite(parsed):
        raise DateIdeaAgentToolError(f"{field_name} must be finite.")
    if field_name == "latitude" and not -90.0 <= parsed <= 90.0:
        raise DateIdeaAgentToolError("latitude must be between -90 and 90.")
    if field_name == "longitude" and not -180.0 <= parsed <= 180.0:
        raise DateIdeaAgentToolError("longitude must be between -180 and 180.")
    if field_name == "max_km" and parsed <= 0:
        raise DateIdeaAgentToolError("max_km must be positive.")
    return parsed


def _string_set_argument(value: Any, *, field_name: str) -> set[str]:
    if value is None:
        return set()
    if not isinstance(value, list | tuple | set):
        raise DateIdeaAgentToolError(f"{field_name} must be a list of strings.")
    result: set[str] = set()
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise DateIdeaAgentToolError(f"{field_name} must contain only strings.")
        result.add(item.strip())
    return result


def _metadata_profile_payload(
    metadata: dict[str, Any],
    *,
    scope_label: str | None,
) -> dict[str, Any]:
    return {
        "fsq_place_id": str(metadata.get("fsq_place_id")),
        "name": str(metadata.get("name")),
        "scope_label": scope_label,
        "latitude": _optional_float(metadata.get("latitude")),
        "longitude": _optional_float(metadata.get("longitude")),
        "locality": _optional_metadata_str(metadata.get("locality")),
        "postcode": _optional_metadata_str(metadata.get("postcode")),
        "category_labels": _string_list(metadata.get("fsq_category_labels")),
        "template_stop_tags": _string_list(metadata.get("crawl4ai_template_stop_tags")),
        "ambience_tags": _string_list(metadata.get("crawl4ai_ambience_tags")),
        "setting_tags": _string_list(metadata.get("crawl4ai_setting_tags")),
        "activity_tags": _string_list(metadata.get("crawl4ai_activity_tags")),
        "drink_tags": _string_list(metadata.get("crawl4ai_drink_tags")),
        "booking_signals": _string_list(metadata.get("crawl4ai_booking_signals")),
        "evidence_snippets": _string_list(metadata.get("crawl4ai_evidence_snippets")),
        "quality_score": _safe_int(metadata.get("crawl4ai_quality_score")),
        "document_hash": _optional_metadata_str(metadata.get("document_hash")),
        "document_text": _optional_metadata_str(metadata.get("document_text")),
    }


def _coordinates_from_metadata(
    metadata: dict[str, Any],
    *,
    label: str,
) -> tuple[float, float]:
    latitude = _optional_float(metadata.get("latitude"))
    longitude = _optional_float(metadata.get("longitude"))
    if latitude is None or longitude is None:
        raise DateIdeaAgentToolError(f"{label} has no usable coordinates.")
    return latitude, longitude


def _distance_from_metadata(
    metadata: dict[str, Any],
    *,
    latitude: float,
    longitude: float,
) -> float:
    place_latitude, place_longitude = _coordinates_from_metadata(
        metadata,
        label=f"fsq_place_id={metadata.get('fsq_place_id')!r}",
    )
    return _haversine_km(latitude, longitude, place_latitude, place_longitude)


def _string_list(value: Any) -> list[str]:
    if _is_missing_value(value):
        return []
    if isinstance(value, str):
        values = [value]
    else:
        try:
            values = list(value)
        except TypeError:
            values = [value]
    return [
        str(item).strip()
        for item in values
        if not _is_missing_value(item) and str(item).strip()
    ]


def _optional_metadata_str(value: Any) -> str | None:
    if _is_missing_value(value):
        return None
    text = str(value).strip()
    return text or None


def _optional_float(value: Any) -> float | None:
    if _is_missing_value(value):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        logger.error("RAG metadata contained a non-numeric coordinate value: %r", value)
        raise DateIdeaAgentToolError(
            f"RAG metadata contained a non-numeric coordinate value: {value!r}."
        )
    if not math.isfinite(parsed):
        return None
    return parsed


def _safe_int(value: Any) -> int | None:
    if _is_missing_value(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_missing_value(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(value != value)
    except Exception:
        return False


def _default_template_context() -> str:
    try:
        templates = load_date_templates()
    except Exception as exc:  # pragma: no cover - startup diagnostic path
        logger.error("Failed to load date templates for agent prompt: %s", exc)
        return "No date-template context loaded."

    lines: list[str] = []
    for template in templates[:24]:
        if not isinstance(template, dict):
            continue
        stops = template.get("stops")
        stop_types: list[str] = []
        if isinstance(stops, list):
            for stop in stops:
                if isinstance(stop, dict) and stop.get("type"):
                    suffix = " (connective)" if stop.get("kind") == "connective" else ""
                    stop_types.append(f"{stop['type']}{suffix}")
        vibe = template.get("vibe")
        vibe_text = ", ".join(str(item) for item in vibe) if isinstance(vibe, list) else str(vibe or "")
        lines.append(
            "- "
            f"{template.get('id')}: {template.get('title')} | "
            f"vibe={vibe_text} | stops={', '.join(stop_types)}"
        )
    return "\n".join(lines)
