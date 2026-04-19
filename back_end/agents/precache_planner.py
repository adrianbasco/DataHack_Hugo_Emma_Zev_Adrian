"""Pre-cache planner agent wrapper.

This module stitches together the RAG and Google Maps tools into a single
cell-level planning call. Each call generates **one** grounded, template-
shaped, Maps-verified date plan for a particular
(location bucket x date template x plan time) cell.

The wrapper intentionally does **not** implement retry logic, plan
diversity tracking, or multi-cell orchestration. Those concerns belong to
the surrounding driver loop. The wrapper's sole job is:

  1. Build a bucket-scoped toolkit (RAG search tools + Maps verify tools)
     for one cell.
  2. Drive the ``DateIdeaAgent`` to produce exactly one grounded plan
     matching the supplied template shape.
  3. Re-run ``verify_plan`` against Google Maps to snapshot authoritative
     feasibility (the tool cache is reused from the agent call, so only
     route legs are re-computed).
  4. Return either a :class:`PrecachePlannerSuccess` (with a ready-to-
     persist :class:`PrecachePlanOutput`) or a
     :class:`PrecachePlannerFailure` with a clear reason the driver can
     act on.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from back_end.agents.date_idea_agent import (
    DATE_IDEA_RESPONSE_SCHEMA,
    DEFAULT_DATE_IDEA_AGENT_MODEL,
    DEFAULT_REASONING_EFFORT,
    DateIdea,
    DateIdeaAgent,
    DateIdeaAgentError,
    DateIdeaAgentOutputError,
    DateIdeaRequest,
    RagPlaceSearchTool,
)
from back_end.agents.maps_tools import (
    MapsPlaceResolver,
    MapsVerificationCache,
    MapsVerifyPlanTool,
)
from back_end.clients.maps import GoogleMapsClient
from back_end.clients.openrouter import (
    OpenRouterAgentLoopError,
    OpenRouterClient,
    OpenRouterClientError,
)
from back_end.domain.models import LatLng, TravelMode
from back_end.llm.models import AgentToolExecution
from back_end.precache.models import LocationBucket, LocationCandidatePool
from back_end.precache.output import (
    PrecachePlanOutput,
    fsq_place_ids_sorted_signature,
)
from back_end.rag.embeddings import EmbeddingClient
from back_end.rag.retriever import CONNECTIVE_STOP_TYPES
from back_end.rag.vector_store import ExactVectorStore

logger = logging.getLogger(__name__)

DEFAULT_PRECACHE_MAX_TOOL_ROUND_TRIPS = 8
DEFAULT_PRECACHE_TEMPERATURE = 0.3
DEFAULT_PRECACHE_MAX_TOKENS = 3500


class PrecachePlannerConfigurationError(ValueError):
    """Raised when the planner is constructed with invalid dependencies."""


@dataclass(frozen=True)
class PrecachePlannerRequest:
    """Inputs for one planner call: a single (bucket, template, time) cell."""

    bucket: LocationBucket
    pool: LocationCandidatePool
    template: Mapping[str, Any]
    plan_time_iso: str
    transport_mode: TravelMode
    max_leg_seconds: int
    existing_plan_signatures: tuple[str, ...] = ()
    excluded_place_ids: tuple[str, ...] = ()
    maps_cache_path: Path | None = None
    connective_anchors_by_stop_type: Mapping[str, LatLng] | None = None
    default_connective_anchor: LatLng | None = None
    desired_reasoning_effort: str | None = None

    def __post_init__(self) -> None:
        if self.pool.bucket.bucket_id != self.bucket.bucket_id:
            raise PrecachePlannerConfigurationError(
                "PrecachePlannerRequest.pool.bucket.bucket_id must match "
                f"bucket.bucket_id; got pool={self.pool.bucket.bucket_id!r} "
                f"vs bucket={self.bucket.bucket_id!r}."
            )
        if not isinstance(self.template, Mapping):
            raise PrecachePlannerConfigurationError(
                "PrecachePlannerRequest.template must be a Mapping."
            )
        if not _nonempty_string(self.template.get("id")):
            raise PrecachePlannerConfigurationError(
                "PrecachePlannerRequest.template must have a non-empty id."
            )
        if not _nonempty_string(self.plan_time_iso):
            raise PrecachePlannerConfigurationError(
                "PrecachePlannerRequest.plan_time_iso must be a non-empty ISO string."
            )
        try:
            parsed_time = datetime.fromisoformat(
                self.plan_time_iso[:-1] + "+00:00"
                if self.plan_time_iso.endswith("Z")
                else self.plan_time_iso
            )
        except ValueError as exc:
            raise PrecachePlannerConfigurationError(
                f"plan_time_iso must parse as ISO datetime: {self.plan_time_iso!r}."
            ) from exc
        if parsed_time.tzinfo is None or parsed_time.utcoffset() is None:
            raise PrecachePlannerConfigurationError(
                "plan_time_iso must include an explicit timezone offset."
            )
        if self.max_leg_seconds <= 0:
            raise PrecachePlannerConfigurationError(
                "max_leg_seconds must be positive."
            )
        for signature in self.existing_plan_signatures:
            if not _nonempty_string(signature):
                raise PrecachePlannerConfigurationError(
                    "existing_plan_signatures must contain only non-empty strings."
                )


@dataclass(frozen=True)
class PrecachePlannerSuccess:
    """One validated, Maps-verified plan ready to persist."""

    plan: PrecachePlanOutput
    idea: DateIdea
    signature: str
    verification: Mapping[str, Any]
    tool_executions: tuple[AgentToolExecution, ...]
    raw_output: Mapping[str, Any]
    model: str

    @property
    def status(self) -> str:
        return "success"


@dataclass(frozen=True)
class PrecachePlannerFailure:
    """The planner could not produce a usable plan for this call."""

    reason: str
    detail: str
    rejected_ideas: tuple[str, ...] = field(default_factory=tuple)
    tool_executions: tuple[AgentToolExecution, ...] = field(default_factory=tuple)
    raw_output: Mapping[str, Any] | None = None
    verification: Mapping[str, Any] | None = None
    signature: str | None = None
    model: str | None = None

    @property
    def status(self) -> str:
        return "failure"


PrecachePlannerResult = PrecachePlannerSuccess | PrecachePlannerFailure


FAILURE_REASON_EMPTY_POOL = "empty_candidate_pool"
FAILURE_REASON_AGENT_LOOP = "agent_loop_error"
FAILURE_REASON_AGENT_EMPTY = "agent_returned_no_ideas"
FAILURE_REASON_AGENT_MULTIPLE = "agent_returned_multiple_ideas"
FAILURE_REASON_OUTPUT_INVALID = "agent_output_invalid"
FAILURE_REASON_DUPLICATE = "duplicate_signature"
FAILURE_REASON_VERIFICATION = "final_verification_failed"


class PrecachePlanner:
    """Drive ``DateIdeaAgent`` to produce one grounded plan per call."""

    def __init__(
        self,
        *,
        llm_client: OpenRouterClient,
        maps_client: GoogleMapsClient,
        vector_store: ExactVectorStore,
        embedding_client: EmbeddingClient,
        rag_documents: pd.DataFrame,
        model: str = DEFAULT_DATE_IDEA_AGENT_MODEL,
        reasoning_effort: str = DEFAULT_REASONING_EFFORT,
        max_tokens: int = DEFAULT_PRECACHE_MAX_TOKENS,
        max_tool_round_trips: int = DEFAULT_PRECACHE_MAX_TOOL_ROUND_TRIPS,
        rag_default_top_k: int = 4,
        rag_max_top_k: int = 10,
    ) -> None:
        if rag_documents is None or len(rag_documents) == 0:
            raise PrecachePlannerConfigurationError(
                "rag_documents must be a non-empty DataFrame."
            )
        if "fsq_place_id" not in rag_documents.columns:
            raise PrecachePlannerConfigurationError(
                "rag_documents must contain an fsq_place_id column."
            )
        if max_tokens <= 0:
            raise PrecachePlannerConfigurationError("max_tokens must be positive.")
        if max_tool_round_trips <= 0:
            raise PrecachePlannerConfigurationError(
                "max_tool_round_trips must be positive."
            )
        if rag_default_top_k <= 0 or rag_max_top_k < rag_default_top_k:
            raise PrecachePlannerConfigurationError(
                "rag_default_top_k and rag_max_top_k must satisfy "
                "0 < default <= max."
            )

        self._llm_client = llm_client
        self._maps_client = maps_client
        self._vector_store = vector_store
        self._embedding_client = embedding_client
        self._rag_documents = rag_documents
        self._model = model
        self._reasoning_effort = reasoning_effort
        self._max_tokens = max_tokens
        self._max_tool_round_trips = max_tool_round_trips
        self._rag_default_top_k = rag_default_top_k
        self._rag_max_top_k = rag_max_top_k

    async def plan(self, request: PrecachePlannerRequest) -> PrecachePlannerResult:
        """Produce one validated plan for a single (bucket x template x time) cell."""

        if not request.pool.allowed_place_ids:
            detail = (
                request.pool.empty_reason
                or "Candidate pool for this bucket contains no allowed places."
            )
            logger.error(
                "Precache planner aborting early: empty pool for bucket=%s "
                "template=%s: %s",
                request.bucket.bucket_id,
                request.template.get("id"),
                detail,
            )
            return PrecachePlannerFailure(
                reason=FAILURE_REASON_EMPTY_POOL,
                detail=detail,
            )

        bucket_label = _bucket_scope_label(request.bucket)
        excluded_place_ids = {str(place_id) for place_id in request.excluded_place_ids}
        scoped_place_ids = tuple(
            place_id
            for place_id in request.pool.allowed_place_ids
            if place_id not in excluded_place_ids
        )
        if not scoped_place_ids:
            detail = "All candidate pool places are excluded by previous failures."
            logger.error(
                "Precache planner aborting early for bucket=%s template=%s: %s",
                request.bucket.bucket_id,
                request.template.get("id"),
                detail,
            )
            return PrecachePlannerFailure(
                reason=FAILURE_REASON_EMPTY_POOL,
                detail=detail,
            )

        cache = MapsVerificationCache(cache_path=request.maps_cache_path)
        place_resolver = MapsPlaceResolver(
            maps_client=self._maps_client,
            rag_documents=self._rag_documents,
            cache=cache,
        )
        rag_search_tool = RagPlaceSearchTool(
            vector_store=self._vector_store,
            embedding_client=self._embedding_client,
            default_top_k=self._rag_default_top_k,
            max_top_k=self._rag_max_top_k,
            candidate_place_ids=scoped_place_ids,
            scope_label=bucket_label,
            include_place_profile_tool=False,
            validated_only=True,
            place_resolver=place_resolver,
            plan_time_iso=request.plan_time_iso,
        )
        verify_plan_tool = MapsVerifyPlanTool(
            maps_client=self._maps_client,
            rag_documents=self._rag_documents,
            cache=cache,
            connective_anchors_by_stop_type=(
                request.connective_anchors_by_stop_type
            ),
            default_connective_anchor=(
                request.default_connective_anchor
                or LatLng(
                    latitude=request.bucket.latitude,
                    longitude=request.bucket.longitude,
                )
            ),
        )

        extra_tools = (
            verify_plan_tool.as_agent_tool(),
        )
        system_prompt = _build_precache_system_prompt(request=request)
        reasoning_effort = (
            request.desired_reasoning_effort
            or self._reasoning_effort
        )

        agent = DateIdeaAgent(
            llm_client=self._llm_client,
            rag_search_tool=rag_search_tool,
            extra_tools=extra_tools,
            model=self._model,
            reasoning_effort=reasoning_effort,
            max_tokens=self._max_tokens,
            max_tool_round_trips=self._max_tool_round_trips,
            system_prompt_override=system_prompt,
        )
        date_idea_request = _build_date_idea_request(request=request)

        try:
            result = await agent.generate(
                date_idea_request,
                template=request.template,
            )
        except OpenRouterAgentLoopError as exc:
            logger.error(
                "Precache planner exhausted agent round-trip budget for "
                "bucket=%s template=%s: %s",
                request.bucket.bucket_id,
                request.template.get("id"),
                exc,
            )
            return PrecachePlannerFailure(
                reason=FAILURE_REASON_AGENT_LOOP,
                detail=str(exc),
            )
        except OpenRouterClientError as exc:
            logger.error(
                "Precache planner hit an OpenRouter client error for "
                "bucket=%s template=%s: %s",
                request.bucket.bucket_id,
                request.template.get("id"),
                exc,
            )
            return PrecachePlannerFailure(
                reason=FAILURE_REASON_OUTPUT_INVALID,
                detail=str(exc),
            )
        except DateIdeaAgentOutputError as exc:
            logger.error(
                "Precache planner could not parse agent output for "
                "bucket=%s template=%s: %s",
                request.bucket.bucket_id,
                request.template.get("id"),
                exc,
            )
            return PrecachePlannerFailure(
                reason=FAILURE_REASON_OUTPUT_INVALID,
                detail=str(exc),
            )
        except DateIdeaAgentError as exc:
            logger.error(
                "Precache planner hit a date-idea agent error for "
                "bucket=%s template=%s: %s",
                request.bucket.bucket_id,
                request.template.get("id"),
                exc,
            )
            return PrecachePlannerFailure(
                reason=FAILURE_REASON_OUTPUT_INVALID,
                detail=str(exc),
            )

        rejected_ideas = _extract_rejected_ideas(result.raw_output)
        if not result.ideas:
            logger.warning(
                "Precache planner received zero ideas from agent for "
                "bucket=%s template=%s rejected=%s",
                request.bucket.bucket_id,
                request.template.get("id"),
                list(rejected_ideas),
            )
            return PrecachePlannerFailure(
                reason=FAILURE_REASON_AGENT_EMPTY,
                detail=(
                    "Agent returned no ideas; see rejected_ideas for the "
                    "agent's own explanation."
                ),
                rejected_ideas=rejected_ideas,
                tool_executions=result.tool_executions,
                raw_output=result.raw_output,
                model=result.model,
            )
        if len(result.ideas) > 1:
            logger.error(
                "Precache planner received %d ideas from agent for "
                "bucket=%s template=%s; expected exactly 1.",
                len(result.ideas),
                request.bucket.bucket_id,
                request.template.get("id"),
            )
            return PrecachePlannerFailure(
                reason=FAILURE_REASON_AGENT_MULTIPLE,
                detail=(
                    f"Agent returned {len(result.ideas)} ideas; this wrapper "
                    "requires exactly 1 per call."
                ),
                rejected_ideas=rejected_ideas,
                tool_executions=result.tool_executions,
                raw_output=result.raw_output,
                model=result.model,
            )

        idea = result.ideas[0]
        venue_ids = tuple(
            stop.fsq_place_id for stop in idea.stops
            if stop.kind == "venue" and stop.fsq_place_id
        )
        if not venue_ids:
            logger.error(
                "Precache planner got an idea with no venue stops for "
                "bucket=%s template=%s",
                request.bucket.bucket_id,
                request.template.get("id"),
            )
            return PrecachePlannerFailure(
                reason=FAILURE_REASON_OUTPUT_INVALID,
                detail="Idea contains no grounded venue stops.",
                rejected_ideas=rejected_ideas,
                tool_executions=result.tool_executions,
                raw_output=result.raw_output,
                model=result.model,
            )
        signature = fsq_place_ids_sorted_signature(venue_ids)
        if signature in set(request.existing_plan_signatures):
            logger.warning(
                "Precache planner rejected duplicate signature for "
                "bucket=%s template=%s signature=%s",
                request.bucket.bucket_id,
                request.template.get("id"),
                signature,
            )
            return PrecachePlannerFailure(
                reason=FAILURE_REASON_DUPLICATE,
                detail="Generated plan matches an existing signature.",
                rejected_ideas=rejected_ideas,
                tool_executions=result.tool_executions,
                raw_output=result.raw_output,
                signature=signature,
                model=result.model,
            )

        verification_payload = await _run_final_verification(
            verify_plan_tool=verify_plan_tool,
            idea=idea,
            plan_time_iso=request.plan_time_iso,
            transport_mode=request.transport_mode,
            max_leg_seconds=request.max_leg_seconds,
        )
        feasibility = verification_payload.get("feasibility", {})
        if not _is_feasible(feasibility):
            logger.warning(
                "Precache planner final verification failed for "
                "bucket=%s template=%s signature=%s reasons=%s",
                request.bucket.bucket_id,
                request.template.get("id"),
                signature,
                feasibility.get("summary_reasons"),
            )
            return PrecachePlannerFailure(
                reason=FAILURE_REASON_VERIFICATION,
                detail=_feasibility_detail(feasibility),
                rejected_ideas=rejected_ideas,
                tool_executions=result.tool_executions,
                raw_output=result.raw_output,
                verification=verification_payload,
                signature=signature,
                model=result.model,
            )

        plan_output = _build_plan_output(
            bucket=request.bucket,
            template=request.template,
            transport_mode=request.transport_mode,
            idea=idea,
            verification=verification_payload,
            model=result.model,
        )
        logger.info(
            "Precache planner produced plan bucket=%s template=%s signature=%s model=%s",
            request.bucket.bucket_id,
            request.template.get("id"),
            signature,
            result.model,
        )
        return PrecachePlannerSuccess(
            plan=plan_output,
            idea=idea,
            signature=signature,
            verification=verification_payload,
            tool_executions=result.tool_executions,
            raw_output=result.raw_output,
            model=result.model,
        )


def _bucket_scope_label(bucket: LocationBucket) -> str:
    return (
        f"{bucket.label} [bucket_id={bucket.bucket_id}, "
        f"center=({bucket.latitude:.5f},{bucket.longitude:.5f}), "
        f"radius_km={bucket.radius_km}, transport={bucket.transport_mode}]"
    )


def _build_date_idea_request(
    *,
    request: PrecachePlannerRequest,
) -> DateIdeaRequest:
    template = request.template
    vibe_values = template.get("vibe") or ()
    if isinstance(vibe_values, str):
        vibe_text = vibe_values
    else:
        vibe_text = ", ".join(str(v) for v in vibe_values if str(v).strip())
    constraints_parts = [
        f"max_leg_seconds={request.max_leg_seconds}",
        f"transport_mode={request.transport_mode.value}",
    ]
    if request.existing_plan_signatures:
        constraints_parts.append(
            "avoid_signatures_count="
            f"{len(request.existing_plan_signatures)} (see system prompt for list)"
        )
    prompt_text = (
        "Build exactly one grounded Date Night plan for the precache template "
        "described in the system prompt. Use the supplied RAG tools to ground "
        "every venue, then call verify_plan before emitting the final JSON."
    )
    return DateIdeaRequest(
        prompt=prompt_text,
        location=_bucket_scope_label(request.bucket),
        time_window=request.plan_time_iso,
        vibe=vibe_text or None,
        budget=None,
        transport_mode=request.transport_mode.value,
        party_size=2,
        constraints="; ".join(constraints_parts),
        desired_idea_count=1,
    )


def _build_precache_system_prompt(
    *,
    request: PrecachePlannerRequest,
) -> str:
    template = request.template
    template_id = str(template.get("id") or "").strip() or "unknown"
    template_title = str(template.get("title") or template_id).strip()
    vibe_values = template.get("vibe") or ()
    if isinstance(vibe_values, str):
        vibe_list: list[str] = [vibe_values]
    else:
        vibe_list = [str(v).strip() for v in vibe_values if str(v).strip()]
    time_of_day = str(template.get("time_of_day") or "").strip() or "flexible"
    duration_hours = template.get("duration_hours")
    weather_sensitive = bool(template.get("weather_sensitive"))
    description = str(template.get("description") or "").strip()
    stops = _normalized_template_stops(template)

    bucket = request.bucket
    signatures = request.existing_plan_signatures
    if signatures:
        signatures_block = "\n".join(f"- {sig}" for sig in signatures)
    else:
        signatures_block = "- (none yet)"

    stops_block_lines: list[str] = []
    for index, stop in enumerate(stops, start=1):
        kind = stop["kind"]
        stop_type = stop["type"]
        if kind == "connective":
            stops_block_lines.append(
                f"  {index}. kind=connective, stop_type={stop_type!r} "
                "(narrate from local knowledge; fsq_place_id=null)"
            )
        else:
            stops_block_lines.append(
                f"  {index}. kind=venue, stop_type={stop_type!r} "
                "(must be grounded in a RAG tool result from this bucket's pool)"
            )
    stops_block = "\n".join(stops_block_lines)

    lines = [
        "You are the Date Night PRE-CACHE planner agent for Sydney.",
        "Your job is to produce exactly ONE grounded, feasible date plan for a "
        "specific (location bucket, date template, plan time) cell. The plan "
        "will be persisted offline and served later, so it must be fully "
        "grounded and Maps-verified by the end of this run.",
        "",
        "=== BUCKET (the only geography you may plan inside) ===",
        f"bucket_id: {bucket.bucket_id}",
        f"label: {bucket.label}",
        f"center_lat: {bucket.latitude}",
        f"center_lng: {bucket.longitude}",
        f"radius_km: {bucket.radius_km}",
        f"bucket_transport_hint: {bucket.transport_mode}",
        (
            f"candidate_pool_size: {len(request.pool.allowed_place_ids)} "
            "(the RAG tools are scoped to exactly this pool; results outside it "
            "are not available)"
        ),
        "",
        "=== TEMPLATE (match its shape EXACTLY, stop-by-stop) ===",
        f"template_id: {template_id}",
        f"template_title: {template_title}",
        f"vibe: {', '.join(vibe_list) if vibe_list else '(unspecified)'}",
        f"time_of_day: {time_of_day}",
        f"duration_hours: {duration_hours if duration_hours is not None else '(unspecified)'}",
        f"weather_sensitive: {weather_sensitive}",
        f"description: {description or '(none)'}",
        "stops_required (in this order, kind and stop_type are both binding):",
        stops_block,
        "",
        "=== PLAN TIME & TRAVEL ===",
        f"plan_time_iso: {request.plan_time_iso}",
        f"transport_mode: {request.transport_mode.value}",
        f"max_leg_seconds: {request.max_leg_seconds}",
        "",
        "=== AVOID LIST ===",
        (
            "Do NOT emit a plan whose sorted set of venue fsq_place_ids "
            "matches any of the following already-cached signatures:"
        ),
        signatures_block,
        "",
        "=== TOOL DISCIPLINE (mandatory) ===",
        (
            "- Keep tool use lean. Prefer top_k <= 4 unless the first search is weak."
        ),
        (
            "- The RAG search tools are Maps-validated in this pre-cache run: treat "
            "returned venues as the only valid venue choices and do not invent or "
            "reuse venues that are absent from the returned results."
        ),
        (
            "- For stop 1, use search_rag_places_near_latlng anchored on the bucket "
            f"center ({bucket.latitude}, {bucket.longitude}) with max_km<={bucket.radius_km}."
        ),
        (
            "- For later venue stops, use search_rag_places_near_anchor anchored on "
            "the previous chosen venue so the route stays walkable."
        ),
        (
            "- Every venue you commit MUST use an fsq_place_id returned by one of "
            "the RAG tools in this run. No inventing venues."
        ),
        (
            "- When you have one candidate plan, call verify_plan for the full ordered stop list "
            f"(kind/stop_type/fsq_place_id per stop) with plan_time_iso="
            f"{request.plan_time_iso!r}, transport_mode="
            f"{request.transport_mode.value!r}, max_leg_seconds="
            f"{request.max_leg_seconds}. If feasibility fails, revise once and try "
            "verify_plan again. Do not individually verify venues or route legs."
        ),
        (
            "- Connective stops must have kind=connective, a descriptive "
            "stop_type matching the template, and fsq_place_id=null. "
            f"Connective types include {sorted(CONNECTIVE_STOP_TYPES)}."
        ),
        "",
        "=== OUTPUT RULES ===",
        (
            "- Emit exactly one idea in date_ideas (length must be 1). "
            "Set maps_verification_needed=false (you have already verified)."
        ),
        (
            "- Stops must match the template's shape above: same count, same "
            "order, same kind, same stop_type per stop."
        ),
        (
            "- Every venue stop's name MUST be the exact 'name' field as "
            "returned by the RAG tools for that fsq_place_id. Do not rename "
            "venues."
        ),
        (
            "- Your final answer must be ONLY a JSON object. No markdown "
            "fences, no prose, no commentary, no checklists, no emoji."
        ),
        (
            "- The FIRST character of your final message must be '{' and the "
            "LAST character must be '}'. Anything else will fail validation "
            "and waste a repair turn."
        ),
        (
            "- Output schema is enforced separately. Return a JSON object with "
            "date_ideas and rejected_ideas only."
        ),
        (
            "- If the RAG pool genuinely cannot support this template, do "
            "not fabricate. Return {\"date_ideas\":[],\"rejected_ideas\":"
            "[\"...reason...\"]}."
        ),
    ]
    return "\n".join(lines)


def _normalized_template_stops(
    template: Mapping[str, Any],
) -> list[dict[str, str]]:
    raw_stops = template.get("stops")
    if not isinstance(raw_stops, list) or not raw_stops:
        raise PrecachePlannerConfigurationError(
            f"Template {template.get('id')!r} must contain a non-empty stops list."
        )
    normalized: list[dict[str, str]] = []
    for index, stop in enumerate(raw_stops):
        if not isinstance(stop, Mapping):
            raise PrecachePlannerConfigurationError(
                f"Template {template.get('id')!r} stop {index} must be a mapping."
            )
        raw_type = stop.get("type")
        if not _nonempty_string(raw_type):
            raise PrecachePlannerConfigurationError(
                f"Template {template.get('id')!r} stop {index} is missing a type."
            )
        stop_type = str(raw_type).strip()
        raw_kind = stop.get("kind")
        if raw_kind is None:
            kind = (
                "connective"
                if stop_type in CONNECTIVE_STOP_TYPES
                else "venue"
            )
        elif isinstance(raw_kind, str) and raw_kind.strip() in {"venue", "connective"}:
            kind = raw_kind.strip()
        else:
            raise PrecachePlannerConfigurationError(
                f"Template {template.get('id')!r} stop {index} has invalid kind "
                f"{raw_kind!r}."
            )
        normalized.append({"kind": kind, "type": stop_type})
    return normalized


async def _run_final_verification(
    *,
    verify_plan_tool: MapsVerifyPlanTool,
    idea: DateIdea,
    plan_time_iso: str,
    transport_mode: TravelMode,
    max_leg_seconds: int,
) -> dict[str, Any]:
    stops_payload = [
        {
            "kind": stop.kind,
            "stop_type": stop.stop_type,
            "fsq_place_id": stop.fsq_place_id,
        }
        for stop in idea.stops
    ]
    return await verify_plan_tool.verify_plan(
        {
            "plan_time_iso": plan_time_iso,
            "transport_mode": transport_mode.name,
            "stops": stops_payload,
            "max_leg_seconds": max_leg_seconds,
        }
    )


def _is_feasible(feasibility: Mapping[str, Any] | Any) -> bool:
    if not isinstance(feasibility, Mapping):
        return False
    return (
        feasibility.get("all_venues_matched") is True
        and feasibility.get("all_open_at_plan_time") is True
        and feasibility.get("all_legs_under_threshold") is True
    )


def _feasibility_detail(feasibility: Mapping[str, Any] | Any) -> str:
    if not isinstance(feasibility, Mapping):
        return "Final verification returned an invalid feasibility payload."
    reasons = feasibility.get("summary_reasons")
    if isinstance(reasons, list) and reasons:
        return "; ".join(str(r) for r in reasons)
    missing: list[str] = []
    if feasibility.get("all_venues_matched") is not True:
        missing.append("venues did not all match")
    if feasibility.get("all_open_at_plan_time") is not True:
        missing.append("a venue was not open at plan_time")
    if feasibility.get("all_legs_under_threshold") is not True:
        missing.append("a leg exceeded max_leg_seconds")
    if not missing:
        return "Final verification reported failure without a specific reason."
    return "; ".join(missing)


def _extract_rejected_ideas(raw_output: Mapping[str, Any]) -> tuple[str, ...]:
    rejected = raw_output.get("rejected_ideas")
    if not isinstance(rejected, list):
        return ()
    out: list[str] = []
    for item in rejected:
        if isinstance(item, str) and item.strip():
            out.append(item.strip())
    return tuple(out)


def _build_plan_output(
    *,
    bucket: LocationBucket,
    template: Mapping[str, Any],
    transport_mode: TravelMode,
    idea: DateIdea,
    verification: Mapping[str, Any],
    model: str,
) -> PrecachePlanOutput:
    bucket_metadata: dict[str, Any] = {
        "bucket_id": bucket.bucket_id,
        "label": bucket.label,
        "latitude": bucket.latitude,
        "longitude": bucket.longitude,
        "radius_km": bucket.radius_km,
        "transport_mode": transport_mode.value,
        "original_transport_mode": bucket.transport_mode,
        "minimum_plan_count": bucket.minimum_plan_count,
        "maximum_plan_count": bucket.maximum_plan_count,
        "strategic_boost": bucket.strategic_boost,
        "tags": list(bucket.tags),
    }
    template_metadata: dict[str, Any] = {
        "id": str(template.get("id") or ""),
        "title": str(template.get("title") or ""),
        "vibe": _template_vibe_list(template.get("vibe")),
        "time_of_day": str(template.get("time_of_day") or "flexible"),
        "weather_sensitive": bool(template.get("weather_sensitive")),
        "duration_hours": _optional_number(template.get("duration_hours")),
        "description": _optional_str(template.get("description")),
        "stops_shape": _normalized_template_stops(template),
    }
    stops_payload = [
        {
            "kind": stop.kind,
            "stop_type": stop.stop_type,
            "fsq_place_id": stop.fsq_place_id,
            "name": stop.name,
            "description": stop.description,
            "why_it_fits": stop.why_it_fits,
        }
        for stop in idea.stops
    ]
    return PrecachePlanOutput(
        bucket_id=bucket.bucket_id,
        template_id=str(template.get("id") or ""),
        bucket_metadata=bucket_metadata,
        template_metadata=template_metadata,
        stops=stops_payload,
        verification=verification,
        generated_at_utc=datetime.now(UTC),
        model=model,
    )


def _template_vibe_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    try:
        items = list(value)
    except TypeError:
        return []
    return [str(v).strip() for v in items if str(v).strip()]


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _nonempty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


__all__ = [
    "DEFAULT_PRECACHE_MAX_TOKENS",
    "DEFAULT_PRECACHE_MAX_TOOL_ROUND_TRIPS",
    "FAILURE_REASON_AGENT_EMPTY",
    "FAILURE_REASON_AGENT_LOOP",
    "FAILURE_REASON_AGENT_MULTIPLE",
    "FAILURE_REASON_DUPLICATE",
    "FAILURE_REASON_EMPTY_POOL",
    "FAILURE_REASON_OUTPUT_INVALID",
    "FAILURE_REASON_VERIFICATION",
    "PrecachePlanner",
    "PrecachePlannerConfigurationError",
    "PrecachePlannerFailure",
    "PrecachePlannerRequest",
    "PrecachePlannerResult",
    "PrecachePlannerSuccess",
]
