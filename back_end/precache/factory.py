"""Factory helpers for building the precache planner runtime and inputs."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from back_end.agents.precache_planner import PrecachePlanner
from back_end.clients.maps import GoogleMapsClient
from back_end.clients.openrouter import OpenRouterClient
from back_end.clients.settings import MapsSettings, OpenRouterSettings
from back_end.domain.models import TravelMode
from back_end.precache.candidate_pools import (
    REQUIRED_RAG_DOCUMENT_COLUMNS,
    build_location_candidate_pool_from_documents,
    load_location_buckets,
    plan_budget_for_pair,
)
from back_end.precache.models import LocationBucket, LocationCandidatePool
from back_end.precache.plan_time import PlanTimeCandidate, bucket_timezone, resolve_plan_time
from back_end.precache.settings import (
    PrecacheSettings,
    resolve_bucket_transport_mode,
)
from back_end.rag.embeddings import (
    LOCAL_HASHING_EMBEDDING_MODEL_PREFIX,
    LocalHashingEmbeddingClient,
    default_local_embedding_client,
)
from back_end.rag.retriever import load_date_templates
from back_end.rag.vector_store import ExactVectorStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PrecachePlanCell:
    """One resolved matrix cell for the precache planner."""

    bucket: LocationBucket
    template: Mapping[str, Any]
    pool: LocationCandidatePool
    budget: int
    plan_time: PlanTimeCandidate
    transport_mode: TravelMode
    max_leg_seconds: int

    @property
    def bucket_id(self) -> str:
        return self.bucket.bucket_id

    @property
    def template_id(self) -> str:
        return str(self.template["id"])

    @property
    def pool_size(self) -> int:
        return len(self.pool.allowed_place_ids)


@dataclass(frozen=True)
class PrecacheBuildInputs:
    """Resolved non-network inputs for one precache invocation."""

    cells: tuple[PrecachePlanCell, ...]
    buckets: tuple[LocationBucket, ...]
    templates: tuple[Mapping[str, Any], ...]
    reference_now: datetime
    plan_time_seed: str | None


@dataclass
class PrecacheRuntime:
    """Resolved runtime bundle for a precache command invocation."""

    settings: PrecacheSettings
    inputs: PrecacheBuildInputs
    planner: PrecachePlanner | None = None
    _llm_client: OpenRouterClient | None = field(default=None, repr=False)
    _maps_client: GoogleMapsClient | None = field(default=None, repr=False)

    async def __aenter__(self) -> "PrecacheRuntime":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        close_errors: list[BaseException] = []
        if self._maps_client is not None:
            try:
                await self._maps_client.aclose()
            except BaseException as exc:  # pragma: no cover - defensive close path
                close_errors.append(exc)
        if self._llm_client is not None:
            try:
                await self._llm_client.aclose()
            except BaseException as exc:  # pragma: no cover - defensive close path
                close_errors.append(exc)
        if close_errors:
            raise RuntimeError(
                "Failed to close one or more precache runtime clients cleanly."
            ) from close_errors[0]


def build_precache_runtime(
    *,
    settings: PrecacheSettings,
    bucket_ids: Sequence[str] | None = None,
    template_ids: Sequence[str] | None = None,
    plan_time_seed: str | None = None,
    enable_live_clients: bool,
) -> PrecacheRuntime:
    """Build precache inputs and, optionally, the live planner runtime."""

    inputs = build_precache_inputs(
        settings=settings,
        bucket_ids=bucket_ids,
        template_ids=template_ids,
        plan_time_seed=plan_time_seed,
    )
    if not enable_live_clients:
        return PrecacheRuntime(settings=settings, inputs=inputs)

    vector_store = ExactVectorStore.from_parquet(
        documents_path=settings.rag_documents_path,
        embeddings_path=settings.rag_embeddings_path,
    )
    rag_documents = pd.read_parquet(settings.rag_documents_path)
    embedding_client = _embedding_client_for_existing_embeddings(
        settings.rag_embeddings_path
    )
    llm_client = OpenRouterClient(OpenRouterSettings.from_env())
    maps_client = GoogleMapsClient(MapsSettings.from_env())
    planner = PrecachePlanner(
        llm_client=llm_client,
        maps_client=maps_client,
        vector_store=vector_store,
        embedding_client=embedding_client,
        rag_documents=rag_documents,
        model=settings.planner_model,
        reasoning_effort=settings.planner_reasoning_effort,
        max_tokens=settings.planner_max_tokens,
        max_tool_round_trips=settings.planner_max_tool_round_trips,
        rag_default_top_k=settings.rag_default_top_k,
        rag_max_top_k=settings.rag_max_top_k,
    )
    return PrecacheRuntime(
        settings=settings,
        inputs=inputs,
        planner=planner,
        _llm_client=llm_client,
        _maps_client=maps_client,
    )


def build_precache_inputs(
    *,
    settings: PrecacheSettings,
    bucket_ids: Sequence[str] | None = None,
    template_ids: Sequence[str] | None = None,
    plan_time_seed: str | None = None,
) -> PrecacheBuildInputs:
    """Resolve the deterministic bucket/template matrix for a precache run."""

    buckets = _filter_buckets(
        load_location_buckets(settings.location_buckets_path),
        requested_ids=bucket_ids,
    )
    templates = _filter_templates(
        load_date_templates(settings.date_templates_path),
        requested_ids=template_ids,
    )
    reference_now = _resolve_reference_now(
        seed=plan_time_seed,
        sample_bucket=buckets[0],
    )
    rag_documents = pd.read_parquet(
        settings.rag_documents_path,
        columns=list(REQUIRED_RAG_DOCUMENT_COLUMNS),
    )
    pools_by_bucket_id = {
        bucket.bucket_id: build_location_candidate_pool_from_documents(
            rag_documents=rag_documents,
            bucket=bucket,
            max_candidates=settings.candidate_pool_max_candidates,
        )
        for bucket in buckets
    }
    cells = tuple(
        _build_cell(
            settings=settings,
            bucket=bucket,
            template=template,
            reference_now=reference_now,
            pool=pools_by_bucket_id[bucket.bucket_id],
        )
        for bucket in buckets
        for template in templates
    )
    logger.info(
        "Built precache matrix with %d bucket(s), %d template(s), and %d cell(s).",
        len(buckets),
        len(templates),
        len(cells),
    )
    return PrecacheBuildInputs(
        cells=cells,
        buckets=buckets,
        templates=templates,
        reference_now=reference_now,
        plan_time_seed=plan_time_seed,
    )


def _build_cell(
    *,
    settings: PrecacheSettings,
    bucket: LocationBucket,
    template: Mapping[str, Any],
    reference_now: datetime,
    pool: LocationCandidatePool,
) -> PrecachePlanCell:
    budget = plan_budget_for_pair(
        bucket=bucket,
        template=template,
        candidate_pool=pool,
    )
    transport_mode = resolve_bucket_transport_mode(bucket.transport_mode)
    plan_time = resolve_plan_time(template, bucket, reference_now)
    return PrecachePlanCell(
        bucket=bucket,
        template=template,
        pool=pool,
        budget=budget,
        plan_time=plan_time,
        transport_mode=transport_mode,
        max_leg_seconds=settings.max_leg_seconds_for(transport_mode),
    )


def _filter_buckets(
    buckets: Sequence[LocationBucket],
    *,
    requested_ids: Sequence[str] | None,
) -> tuple[LocationBucket, ...]:
    available = {bucket.bucket_id: bucket for bucket in buckets}
    if not requested_ids:
        return tuple(buckets)
    missing = sorted(bucket_id for bucket_id in requested_ids if bucket_id not in available)
    if missing:
        raise ValueError(
            f"Unknown --buckets value(s): {missing}. Available: {sorted(available)}."
        )
    return tuple(available[bucket_id] for bucket_id in requested_ids)


def _filter_templates(
    templates: Sequence[Mapping[str, Any]],
    *,
    requested_ids: Sequence[str] | None,
) -> tuple[Mapping[str, Any], ...]:
    available = {
        str(template.get("id")): template
        for template in templates
        if str(template.get("id") or "").strip()
    }
    if not requested_ids:
        return tuple(templates)
    missing = sorted(
        template_id for template_id in requested_ids if template_id not in available
    )
    if missing:
        raise ValueError(
            "Unknown --templates value(s): "
            f"{missing}. Available: {sorted(available)}."
        )
    return tuple(available[template_id] for template_id in requested_ids)


def _resolve_reference_now(*, seed: str | None, sample_bucket: LocationBucket) -> datetime:
    timezone = bucket_timezone(sample_bucket)
    if seed is None:
        return datetime.now(UTC).astimezone(timezone)

    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    day_offset = int.from_bytes(digest[:2], "big") % 28
    hour = 8 + (digest[2] % 12)
    minute = (digest[3] % 4) * 15
    base = datetime(2026, 1, 1, hour, minute, tzinfo=timezone)
    resolved = base + timedelta(days=day_offset)
    logger.info("Using deterministic precache reference_now=%s for seed=%r.", resolved, seed)
    return resolved


def _embedding_client_for_existing_embeddings(embeddings_path: Path):
    metadata = pd.read_parquet(embeddings_path, columns=["embedding_model"])
    if metadata.empty:
        raise ValueError(f"Embedding parquet {embeddings_path} is empty.")
    models = set(metadata["embedding_model"].astype(str))
    if len(models) != 1:
        raise ValueError(
            f"Embedding parquet {embeddings_path} contains mixed models: {sorted(models)}."
        )
    model = next(iter(models))
    if model.startswith(f"{LOCAL_HASHING_EMBEDDING_MODEL_PREFIX}:"):
        raw_dimension = model.rsplit(":", maxsplit=1)[-1]
        try:
            dimension = int(raw_dimension)
        except ValueError as exc:
            raise ValueError(
                f"Could not parse hashing embedding dimension from model {model!r}."
            ) from exc
        return LocalHashingEmbeddingClient(dimension=dimension)
    return default_local_embedding_client()
