"""Settings for the local RAG pipeline over scraped place profiles."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from back_end.query.settings import REPO_ROOT


@dataclass(frozen=True)
class RagSettings:
    """Filesystem and model defaults for derived RAG artifacts."""

    candidate_parquet_path: Path = (
        REPO_ROOT / "data" / "au_places_date_worthy_sydney_100km.parquet"
    )
    profile_runs_root: Path = REPO_ROOT / "data" / "website_profile_runs"
    date_templates_path: Path = REPO_ROOT / "config" / "date_templates.yaml"
    rag_runs_root: Path = REPO_ROOT / "data" / "rag" / "runs"
    artifacts_root: Path = REPO_ROOT / "artifacts" / "rag"
    local_embedding_base_url: str = "http://localhost:1234/v1"
    embedding_model: str = "Qwen3-Embedding-0.6B"
    embedding_batch_size: int = 16
    embedding_timeout_seconds: float = 120.0
    min_profile_quality_score: int = 1


def load_rag_settings() -> RagSettings:
    """Return local RAG settings."""

    return RagSettings()


def make_run_id(prefix: str = "rag") -> str:
    """Return a stable filesystem-safe UTC run id."""

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{stamp}"

