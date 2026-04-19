"""Typed models for the local RAG pipeline."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RagPlaceDocument:
    """One searchable, evidence-backed venue document."""

    fsq_place_id: str
    name: str
    latitude: float
    longitude: float
    locality: str | None
    region: str | None
    postcode: str | None
    fsq_category_labels: tuple[str, ...]
    crawl4ai_quality_score: int
    crawl4ai_template_stop_tags: tuple[str, ...]
    crawl4ai_ambience_tags: tuple[str, ...]
    crawl4ai_setting_tags: tuple[str, ...]
    crawl4ai_activity_tags: tuple[str, ...]
    crawl4ai_drink_tags: tuple[str, ...]
    crawl4ai_booking_signals: tuple[str, ...]
    crawl4ai_evidence_snippets: tuple[str, ...]
    source_chunk_path: str
    source_run_id: str
    document_text: str
    document_hash: str

    def to_dict(self) -> dict[str, Any]:
        """Return a parquet/JSON-friendly mapping."""

        return asdict(self)


@dataclass(frozen=True)
class RagCorpusBuildResult:
    """Paths and counts produced by a corpus build."""

    output_dir: Path
    documents_path: Path
    manifest_path: Path
    document_count: int
    excluded_count: int
    source_chunk_count: int


@dataclass(frozen=True)
class EmbeddingBuildResult:
    """Paths and counts produced by an embedding build."""

    output_path: Path
    embedding_count: int
    new_embedding_count: int
    reused_embedding_count: int
    embedding_dimension: int


@dataclass(frozen=True)
class RagSearchHit:
    """A semantic retrieval hit with traceable score metadata."""

    fsq_place_id: str
    name: str
    semantic_score: float
    final_score: float
    score_breakdown: dict[str, float]
    document_hash: str
    evidence_snippets: tuple[str, ...]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StopRetrievalRequest:
    """Semantic retrieval request for one template stop."""

    stop_type: str
    query_text: str
    top_k: int = 10
    candidate_place_ids: tuple[str, ...] | None = None


@dataclass(frozen=True)
class StopRetrievalResult:
    """Retrieval output for one template stop."""

    stop_type: str
    query_text: str
    hits: tuple[RagSearchHit, ...]
    empty_reason: str | None = None
    is_connective: bool = False


@dataclass(frozen=True)
class DatePlanCandidate:
    """A grounded candidate date plan assembled from retrieved stops."""

    template_id: str
    template_title: str
    stop_results: tuple[StopRetrievalResult, ...]
    empty_reason: str | None = None
