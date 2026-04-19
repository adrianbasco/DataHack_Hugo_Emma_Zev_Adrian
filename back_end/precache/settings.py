"""Runtime settings for the pre-cache generation pipeline."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from back_end.agents.date_idea_agent import (
    DEFAULT_DATE_IDEA_AGENT_MODEL,
    DEFAULT_REASONING_EFFORT,
)
from back_end.agents.precache_planner import (
    DEFAULT_PRECACHE_MAX_TOKENS,
    DEFAULT_PRECACHE_MAX_TOOL_ROUND_TRIPS,
)
from back_end.domain.models import TravelMode
from back_end.query.settings import REPO_ROOT
from back_end.rag.settings import load_rag_settings

logger = logging.getLogger(__name__)

DEFAULT_PRECACHE_OUTPUT_PATH = REPO_ROOT / "data" / "precache" / "plans.parquet"
DEFAULT_PRECACHE_RUNS_ROOT = REPO_ROOT / "data" / "precache" / "runs"
DEFAULT_LOCATION_BUCKETS_PATH = REPO_ROOT / "config" / "location_buckets.yaml"
DEFAULT_WALK_MAX_LEG_SECONDS = 25 * 60
DEFAULT_TRANSIT_MAX_LEG_SECONDS = 35 * 60
DEFAULT_DRIVE_MAX_LEG_SECONDS = 30 * 60
DEFAULT_BICYCLE_MAX_LEG_SECONDS = 30 * 60


class PrecacheConfigurationError(RuntimeError):
    """Raised when the pre-cache runtime is configured unsafely."""


@dataclass(frozen=True)
class PrecacheSettings:
    """Filesystem and planner defaults for precache generation."""

    rag_documents_path: Path
    rag_embeddings_path: Path
    location_buckets_path: Path = DEFAULT_LOCATION_BUCKETS_PATH
    date_templates_path: Path = load_rag_settings().date_templates_path
    output_path: Path = DEFAULT_PRECACHE_OUTPUT_PATH
    runs_root: Path = DEFAULT_PRECACHE_RUNS_ROOT
    candidate_pool_max_candidates: int = 250
    planner_model: str = DEFAULT_DATE_IDEA_AGENT_MODEL
    planner_reasoning_effort: str = DEFAULT_REASONING_EFFORT
    planner_max_tokens: int = DEFAULT_PRECACHE_MAX_TOKENS
    planner_max_tool_round_trips: int = DEFAULT_PRECACHE_MAX_TOOL_ROUND_TRIPS
    rag_default_top_k: int = 8
    rag_max_top_k: int = 15
    walk_max_leg_seconds: int = DEFAULT_WALK_MAX_LEG_SECONDS
    transit_max_leg_seconds: int = DEFAULT_TRANSIT_MAX_LEG_SECONDS
    drive_max_leg_seconds: int = DEFAULT_DRIVE_MAX_LEG_SECONDS
    bicycle_max_leg_seconds: int = DEFAULT_BICYCLE_MAX_LEG_SECONDS

    @classmethod
    def from_env(cls) -> "PrecacheSettings":
        """Load explicit precache settings from environment variables.

        This loader is intentionally strict. It never guesses across multiple
        available RAG runs because that would make generation nondeterministic
        and hide configuration mistakes.
        """

        rag_settings = load_rag_settings()
        documents_env = _read_optional_path("PRECACHE_RAG_DOCUMENTS_PATH")
        embeddings_env = _read_optional_path("PRECACHE_RAG_EMBEDDINGS_PATH")
        rag_run_id = _read_optional_string("PRECACHE_RAG_RUN_ID")

        if (documents_env is None) != (embeddings_env is None):
            raise PrecacheConfigurationError(
                "Set both PRECACHE_RAG_DOCUMENTS_PATH and "
                "PRECACHE_RAG_EMBEDDINGS_PATH together, or neither."
            )

        if documents_env is not None and rag_run_id is not None:
            raise PrecacheConfigurationError(
                "Set PRECACHE_RAG_RUN_ID or explicit PRECACHE_RAG_*_PATH "
                "variables, not both."
            )

        if documents_env is not None and embeddings_env is not None:
            rag_documents_path = documents_env
            rag_embeddings_path = embeddings_env
        elif rag_run_id is not None:
            rag_documents_path = rag_settings.rag_runs_root / rag_run_id / "place_documents.parquet"
            rag_embeddings_path = rag_settings.rag_runs_root / rag_run_id / "place_embeddings.parquet"
        else:
            rag_documents_path, rag_embeddings_path = _resolve_single_available_rag_run(
                rag_settings.rag_runs_root
            )

        settings = cls(
            rag_documents_path=rag_documents_path,
            rag_embeddings_path=rag_embeddings_path,
            location_buckets_path=_read_optional_path(
                "PRECACHE_LOCATION_BUCKETS_PATH"
            )
            or DEFAULT_LOCATION_BUCKETS_PATH,
            date_templates_path=_read_optional_path("PRECACHE_DATE_TEMPLATES_PATH")
            or rag_settings.date_templates_path,
            output_path=_read_optional_path("PRECACHE_OUTPUT_PATH")
            or DEFAULT_PRECACHE_OUTPUT_PATH,
            runs_root=_read_optional_path("PRECACHE_RUNS_ROOT")
            or DEFAULT_PRECACHE_RUNS_ROOT,
            candidate_pool_max_candidates=_read_int(
                "PRECACHE_CANDIDATE_POOL_MAX_CANDIDATES",
                default=250,
                minimum=1,
            ),
            planner_model=_read_optional_string("PRECACHE_MODEL")
            or DEFAULT_DATE_IDEA_AGENT_MODEL,
            planner_reasoning_effort=_read_optional_string(
                "PRECACHE_REASONING_EFFORT"
            )
            or DEFAULT_REASONING_EFFORT,
            planner_max_tokens=_read_int(
                "PRECACHE_MAX_TOKENS",
                default=DEFAULT_PRECACHE_MAX_TOKENS,
                minimum=1,
            ),
            planner_max_tool_round_trips=_read_int(
                "PRECACHE_MAX_TOOL_ROUND_TRIPS",
                default=DEFAULT_PRECACHE_MAX_TOOL_ROUND_TRIPS,
                minimum=1,
            ),
            rag_default_top_k=_read_int(
                "PRECACHE_RAG_DEFAULT_TOP_K",
                default=8,
                minimum=1,
            ),
            rag_max_top_k=_read_int(
                "PRECACHE_RAG_MAX_TOP_K",
                default=15,
                minimum=1,
            ),
            walk_max_leg_seconds=_read_int(
                "PRECACHE_WALK_MAX_LEG_SECONDS",
                default=DEFAULT_WALK_MAX_LEG_SECONDS,
                minimum=1,
            ),
            transit_max_leg_seconds=_read_int(
                "PRECACHE_TRANSIT_MAX_LEG_SECONDS",
                default=DEFAULT_TRANSIT_MAX_LEG_SECONDS,
                minimum=1,
            ),
            drive_max_leg_seconds=_read_int(
                "PRECACHE_DRIVE_MAX_LEG_SECONDS",
                default=DEFAULT_DRIVE_MAX_LEG_SECONDS,
                minimum=1,
            ),
            bicycle_max_leg_seconds=_read_int(
                "PRECACHE_BICYCLE_MAX_LEG_SECONDS",
                default=DEFAULT_BICYCLE_MAX_LEG_SECONDS,
                minimum=1,
            ),
        )
        settings._validate()
        return settings

    def max_leg_seconds_for(self, transport_mode: TravelMode) -> int:
        """Return the configured max route leg duration for a travel mode."""

        if transport_mode is TravelMode.WALK:
            return self.walk_max_leg_seconds
        if transport_mode is TravelMode.TRANSIT:
            return self.transit_max_leg_seconds
        if transport_mode is TravelMode.DRIVE:
            return self.drive_max_leg_seconds
        if transport_mode is TravelMode.BICYCLE:
            return self.bicycle_max_leg_seconds
        raise PrecacheConfigurationError(
            f"Unsupported transport_mode={transport_mode!r}."
        )

    def _validate(self) -> None:
        missing_paths = [
            path
            for path in (
                self.rag_documents_path,
                self.rag_embeddings_path,
                self.location_buckets_path,
                self.date_templates_path,
            )
            if not path.exists()
        ]
        if missing_paths:
            raise PrecacheConfigurationError(
                "Missing precache input path(s): "
                + ", ".join(str(path) for path in missing_paths)
                + "."
            )
        if self.rag_documents_path.suffix != ".parquet":
            raise PrecacheConfigurationError(
                f"rag_documents_path must be a parquet file, got {self.rag_documents_path}."
            )
        if self.rag_embeddings_path.suffix != ".parquet":
            raise PrecacheConfigurationError(
                f"rag_embeddings_path must be a parquet file, got {self.rag_embeddings_path}."
            )
        if self.output_path.suffix != ".parquet":
            raise PrecacheConfigurationError(
                f"output_path must be a parquet file, got {self.output_path}."
            )
        if self.rag_max_top_k < self.rag_default_top_k:
            raise PrecacheConfigurationError(
                "PRECACHE_RAG_MAX_TOP_K must be >= PRECACHE_RAG_DEFAULT_TOP_K."
            )


def resolve_bucket_transport_mode(raw_value: str) -> TravelMode:
    """Normalize bucket transport strings into the strict TravelMode enum."""

    normalized = str(raw_value).strip().casefold()
    mapping = {
        "walk": TravelMode.WALK,
        "walking": TravelMode.WALK,
        "transit": TravelMode.TRANSIT,
        "public_transport": TravelMode.TRANSIT,
        "drive": TravelMode.DRIVE,
        "driving": TravelMode.DRIVE,
        "car": TravelMode.DRIVE,
        "bicycle": TravelMode.BICYCLE,
        "bike": TravelMode.BICYCLE,
        "cycling": TravelMode.BICYCLE,
    }
    try:
        return mapping[normalized]
    except KeyError as exc:
        raise PrecacheConfigurationError(
            f"Unsupported bucket transport_mode={raw_value!r}."
        ) from exc


def _resolve_single_available_rag_run(rag_runs_root: Path) -> tuple[Path, Path]:
    run_candidates: list[tuple[Path, Path]] = []
    if rag_runs_root.exists():
        for run_dir in sorted(path for path in rag_runs_root.iterdir() if path.is_dir()):
            documents = run_dir / "place_documents.parquet"
            embeddings = run_dir / "place_embeddings.parquet"
            if documents.exists() and embeddings.exists():
                run_candidates.append((documents, embeddings))

    if not run_candidates:
        raise PrecacheConfigurationError(
            "Could not find any usable RAG run under "
            f"{rag_runs_root}. Set PRECACHE_RAG_RUN_ID or explicit "
            "PRECACHE_RAG_DOCUMENTS_PATH/PRECACHE_RAG_EMBEDDINGS_PATH."
        )
    if len(run_candidates) > 1:
        available = sorted(path.parent.name for path, _ in run_candidates)
        raise PrecacheConfigurationError(
            "Multiple RAG runs are available and no explicit precache input was "
            f"chosen: {available}. Set PRECACHE_RAG_RUN_ID or explicit "
            "PRECACHE_RAG_DOCUMENTS_PATH/PRECACHE_RAG_EMBEDDINGS_PATH."
        )

    documents, embeddings = run_candidates[0]
    logger.info(
        "Using the only available RAG run for precache input: %s",
        documents.parent.name,
    )
    return documents, embeddings


def _read_optional_string(name: str) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def _read_optional_path(name: str) -> Path | None:
    value = _read_optional_string(name)
    if value is None:
        return None
    return Path(value)


def _read_int(name: str, *, default: int, minimum: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise PrecacheConfigurationError(
            f"{name} must be an integer, got {raw!r}."
        ) from exc
    if value < minimum:
        raise PrecacheConfigurationError(
            f"{name} must be >= {minimum}, got {value}."
        )
    return value
