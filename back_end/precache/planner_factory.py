"""Factory helpers for constructing a live pre-cache planner from config."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
from pydantic.v1 import BaseSettings, Field, ValidationError, validator

from back_end.clients.maps import GoogleMapsClient
from back_end.clients.openrouter import OpenRouterClient
from back_end.clients.settings import MapsSettings, OpenRouterSettings
from back_end.precache.candidate_pools import load_location_buckets
from back_end.precache.models import LocationBucket
from back_end.query.settings import REPO_ROOT
from back_end.rag.embeddings import (
    LOCAL_HASHING_EMBEDDING_MODEL_PREFIX,
    EmbeddingClient,
    LocalHashingEmbeddingClient,
    LocalOpenAICompatibleEmbeddingClient,
)
from back_end.rag.retriever import load_date_templates
from back_end.rag.settings import load_rag_settings
from back_end.rag.vector_store import ExactVectorStore

if TYPE_CHECKING:
    from back_end.agents.precache_planner import PrecachePlanner


class PrecachePlannerFactoryError(RuntimeError):
    """Raised when the pre-cache planner factory cannot build safely."""


@dataclass(frozen=True)
class PrecachePlannerFactoryOverrides:
    """Optional runtime overrides for planner behavior."""

    model: str | None = None
    reasoning_effort: str | None = None
    max_tokens: int | None = None
    rag_default_top_k: int | None = None

    def __post_init__(self) -> None:
        if self.max_tokens is not None and self.max_tokens <= 0:
            raise ValueError("max_tokens override must be positive.")
        if self.rag_default_top_k is not None and self.rag_default_top_k <= 0:
            raise ValueError("rag_default_top_k override must be positive.")


@dataclass(frozen=True)
class PrecachePlannerFactoryResult:
    """Fully constructed planner dependencies for the pre-cache driver."""

    planner: PrecachePlanner
    buckets: tuple[LocationBucket, ...]
    templates: tuple[dict[str, object], ...]
    rag_documents_df: pd.DataFrame


class PrecacheSettings(BaseSettings):
    """Environment-backed settings for constructing a real pre-cache planner."""

    openrouter_api_key: str = Field(..., env="OPENROUTER_API_KEY")
    openrouter_model: str = Field(..., env="OPENROUTER_MODEL")
    maps_api_key: str = Field(..., env="MAPS_API_KEY")
    rag_documents_path: Path = Field(..., env="PRECACHE_RAG_DOCUMENTS_PATH")
    rag_embeddings_path: Path = Field(..., env="PRECACHE_RAG_EMBEDDINGS_PATH")
    location_buckets_path: Path = Field(
        default=REPO_ROOT / "config" / "location_buckets.yaml",
        env="PRECACHE_LOCATION_BUCKETS_PATH",
    )
    date_templates_path: Path = Field(
        default=load_rag_settings().date_templates_path,
        env="PRECACHE_DATE_TEMPLATES_PATH",
    )
    openrouter_base_url: str = Field(
        default="https://openrouter.ai/api/v1",
        env="OPENROUTER_BASE_URL",
    )
    openrouter_timeout_seconds: float = Field(
        default=45.0,
        env="OPENROUTER_TIMEOUT_SECONDS",
    )
    openrouter_retry_count: int = Field(default=1, env="OPENROUTER_RETRY_COUNT")
    openrouter_max_tool_round_trips: int = Field(
        default=8,
        env="OPENROUTER_MAX_TOOL_ROUND_TRIPS",
    )
    openrouter_http_referer: str | None = Field(
        default=None,
        env="OPENROUTER_HTTP_REFERER",
    )
    openrouter_app_title: str | None = Field(
        default=None,
        env="OPENROUTER_APP_TITLE",
    )
    maps_places_base_url: str = Field(
        default="https://places.googleapis.com/v1",
        env="MAPS_PLACES_BASE_URL",
    )
    maps_routes_base_url: str = Field(
        default="https://routes.googleapis.com/directions/v2",
        env="MAPS_ROUTES_BASE_URL",
    )
    maps_timeout_seconds: float = Field(default=10.0, env="MAPS_TIMEOUT_SECONDS")
    maps_retry_count: int = Field(default=1, env="MAPS_RETRY_COUNT")
    maps_text_search_result_limit: int = Field(
        default=5,
        env="MAPS_TEXT_SEARCH_RESULT_LIMIT",
    )
    maps_text_search_bias_radius_meters: float = Field(
        default=500.0,
        env="MAPS_TEXT_SEARCH_BIAS_RADIUS_METERS",
    )
    maps_max_match_distance_meters: float = Field(
        default=250.0,
        env="MAPS_MAX_MATCH_DISTANCE_METERS",
    )
    maps_min_name_similarity: float = Field(
        default=0.92,
        env="MAPS_MIN_NAME_SIMILARITY",
    )
    maps_min_place_rating: float = Field(default=3.8, env="MAPS_MIN_PLACE_RATING")
    maps_min_user_rating_count: int = Field(
        default=0,
        env="MAPS_MIN_USER_RATING_COUNT",
    )
    maps_default_photo_max_width_px: int = Field(
        default=1200,
        env="MAPS_DEFAULT_PHOTO_MAX_WIDTH_PX",
    )
    maps_default_photo_max_height_px: int = Field(
        default=900,
        env="MAPS_DEFAULT_PHOTO_MAX_HEIGHT_PX",
    )

    class Config:
        case_sensitive = True

    @classmethod
    def from_env(cls) -> "PrecacheSettings":
        """Load factory settings from the current environment."""

        try:
            return cls()
        except ValidationError as exc:
            raise PrecachePlannerFactoryError(
                _format_settings_validation_error(exc, cls)
            ) from exc

    @validator(
        "openrouter_api_key",
        "openrouter_model",
        "maps_api_key",
        pre=True,
    )
    def _strip_required_string(cls, value: Any) -> Any:
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                raise ValueError("value must not be empty.")
            return stripped
        return value

    @validator(
        "rag_documents_path",
        "rag_embeddings_path",
        pre=True,
    )
    def _strip_required_path(cls, value: Any) -> Any:
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                raise ValueError("path must not be empty.")
            return stripped
        return value

    @validator("rag_documents_path", "rag_embeddings_path")
    def _validate_parquet_path(cls, value: Path) -> Path:
        if value.suffix != ".parquet":
            raise ValueError(f"expected a .parquet path, got {value}.")
        return value

    @validator("location_buckets_path", "date_templates_path")
    def _validate_yaml_path(cls, value: Path) -> Path:
        if value.suffix not in {".yaml", ".yml"}:
            raise ValueError(f"expected a YAML path, got {value}.")
        return value


def build_precache_planner(
    *,
    settings: PrecacheSettings | None = None,
    overrides: PrecachePlannerFactoryOverrides | None = None,
) -> PrecachePlannerFactoryResult:
    """Construct a live pre-cache planner and the inputs its driver needs."""

    resolved_settings = settings or PrecacheSettings.from_env()
    resolved_overrides = overrides or PrecachePlannerFactoryOverrides()

    _require_file(
        resolved_settings.location_buckets_path,
        label="Location buckets YAML",
    )
    _require_file(
        resolved_settings.date_templates_path,
        label="Date templates YAML",
    )
    _require_file(
        resolved_settings.rag_documents_path,
        label="RAG documents parquet",
    )
    _require_file(
        resolved_settings.rag_embeddings_path,
        label="RAG embeddings parquet",
    )

    buckets = _load_buckets(resolved_settings.location_buckets_path)
    templates = _load_templates(resolved_settings.date_templates_path)
    rag_documents_df = _read_parquet(
        resolved_settings.rag_documents_path,
        label="RAG documents parquet",
    )
    rag_embeddings_df = _read_parquet(
        resolved_settings.rag_embeddings_path,
        label="RAG embeddings parquet",
    )
    vector_store = ExactVectorStore(rag_documents_df, rag_embeddings_df)
    embedding_client = _embedding_client_for_embeddings(
        embeddings_df=rag_embeddings_df,
        embeddings_path=resolved_settings.rag_embeddings_path,
    )

    try:
        from back_end.agents.date_idea_agent import DEFAULT_REASONING_EFFORT
        from back_end.agents.precache_planner import (
            DEFAULT_PRECACHE_MAX_TOKENS,
            PrecachePlanner,
        )
    except ModuleNotFoundError as exc:
        raise PrecachePlannerFactoryError(
            "Could not import a required pre-cache planner dependency module: "
            f"{exc.name}. The current worktree is missing a module required to "
            "construct PrecachePlanner."
        ) from exc

    planner = PrecachePlanner(
        llm_client=OpenRouterClient(_build_openrouter_settings(resolved_settings)),
        maps_client=GoogleMapsClient(_build_maps_settings(resolved_settings)),
        vector_store=vector_store,
        embedding_client=embedding_client,
        rag_documents=rag_documents_df,
        model=resolved_overrides.model or resolved_settings.openrouter_model,
        reasoning_effort=resolved_overrides.reasoning_effort
        or DEFAULT_REASONING_EFFORT,
        max_tokens=resolved_overrides.max_tokens or DEFAULT_PRECACHE_MAX_TOKENS,
        rag_default_top_k=resolved_overrides.rag_default_top_k or 8,
    )
    return PrecachePlannerFactoryResult(
        planner=planner,
        buckets=buckets,
        templates=templates,
        rag_documents_df=rag_documents_df,
    )


def _build_openrouter_settings(settings: PrecacheSettings) -> OpenRouterSettings:
    return OpenRouterSettings(
        api_key=settings.openrouter_api_key,
        base_url=settings.openrouter_base_url,
        default_model=settings.openrouter_model,
        timeout_seconds=settings.openrouter_timeout_seconds,
        retry_count=settings.openrouter_retry_count,
        max_tool_round_trips=settings.openrouter_max_tool_round_trips,
        http_referer=settings.openrouter_http_referer,
        app_title=settings.openrouter_app_title,
    )


def _build_maps_settings(settings: PrecacheSettings) -> MapsSettings:
    return MapsSettings(
        api_key=settings.maps_api_key,
        places_base_url=settings.maps_places_base_url,
        routes_base_url=settings.maps_routes_base_url,
        timeout_seconds=settings.maps_timeout_seconds,
        retry_count=settings.maps_retry_count,
        text_search_result_limit=settings.maps_text_search_result_limit,
        text_search_location_bias_radius_meters=(
            settings.maps_text_search_bias_radius_meters
        ),
        max_match_distance_meters=settings.maps_max_match_distance_meters,
        min_name_similarity=settings.maps_min_name_similarity,
        min_place_rating=settings.maps_min_place_rating,
        min_user_rating_count=settings.maps_min_user_rating_count,
        default_photo_max_width_px=settings.maps_default_photo_max_width_px,
        default_photo_max_height_px=settings.maps_default_photo_max_height_px,
    )


def _embedding_client_for_embeddings(
    *,
    embeddings_df: pd.DataFrame,
    embeddings_path: Path,
) -> EmbeddingClient:
    if "embedding_model" not in embeddings_df.columns:
        raise PrecachePlannerFactoryError(
            "RAG embeddings parquet at "
            f"{embeddings_path} is missing required column 'embedding_model'."
        )
    if embeddings_df.empty:
        raise PrecachePlannerFactoryError(
            f"RAG embeddings parquet at {embeddings_path} is empty."
        )

    models = {str(model) for model in embeddings_df["embedding_model"].astype(str)}
    if len(models) != 1:
        raise PrecachePlannerFactoryError(
            f"RAG embeddings parquet at {embeddings_path} contains mixed embedding "
            f"models: {sorted(models)}."
        )

    model = next(iter(models))
    if model.startswith(f"{LOCAL_HASHING_EMBEDDING_MODEL_PREFIX}:"):
        raw_dimension = model.rsplit(":", maxsplit=1)[-1]
        try:
            dimension = int(raw_dimension)
        except ValueError as exc:
            raise PrecachePlannerFactoryError(
                "Could not parse hashing embedding dimension from model "
                f"{model!r} in {embeddings_path}."
            ) from exc
        return LocalHashingEmbeddingClient(dimension=dimension)

    rag_settings = load_rag_settings()
    return LocalOpenAICompatibleEmbeddingClient(
        base_url=rag_settings.local_embedding_base_url,
        model=model,
        timeout_seconds=rag_settings.embedding_timeout_seconds,
    )


def _load_buckets(path: Path) -> tuple[LocationBucket, ...]:
    try:
        return load_location_buckets(path)
    except Exception as exc:
        raise PrecachePlannerFactoryError(
            f"Failed to load location buckets from {path}: {exc}"
        ) from exc


def _load_templates(path: Path) -> tuple[dict[str, object], ...]:
    try:
        return load_date_templates(path)
    except Exception as exc:
        raise PrecachePlannerFactoryError(
            f"Failed to load date templates from {path}: {exc}"
        ) from exc


def _read_parquet(path: Path, *, label: str) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        raise PrecachePlannerFactoryError(
            f"Failed to read {label} at {path}: {exc}"
        ) from exc


def _require_file(path: Path, *, label: str) -> None:
    if not path.exists():
        raise PrecachePlannerFactoryError(f"{label} not found at {path}.")
    if not path.is_file():
        raise PrecachePlannerFactoryError(f"{label} at {path} is not a file.")


def _missing_env_names(
    exc: ValidationError,
    settings_cls: type[PrecacheSettings],
) -> list[str]:
    missing_field_names = {
        error["loc"][0]
        for error in exc.errors()
        if error.get("type") == "value_error.missing" and error.get("loc")
    }
    env_names: list[str] = []
    for field_name in missing_field_names:
        field = settings_cls.__fields__.get(field_name)
        if field is None:
            continue
        extra = field.field_info.extra
        env = extra.get("env")
        if isinstance(env, str):
            env_names.append(env)
            continue
        if isinstance(env, (tuple, list)) and env:
            env_names.append(str(env[0]))
            continue
        env_names.append(field_name)
    return sorted(env_names)


def _format_settings_validation_error(
    exc: ValidationError,
    settings_cls: type[PrecacheSettings],
) -> str:
    missing_envs = _missing_env_names(exc, settings_cls)
    if missing_envs:
        return (
            "Missing required pre-cache environment variable(s): "
            + ", ".join(missing_envs)
            + "."
        )

    problems: list[str] = []
    for error in exc.errors():
        loc = error.get("loc") or ()
        field_name = str(loc[0]) if loc else "unknown"
        field = settings_cls.__fields__.get(field_name)
        env_name = field_name
        if field is not None:
            env = field.field_info.extra.get("env")
            if isinstance(env, str):
                env_name = env
            elif isinstance(env, (tuple, list)) and env:
                env_name = str(env[0])
        problems.append(f"{env_name}: {error.get('msg', 'invalid value')}")
    return "Invalid pre-cache environment setting(s): " + "; ".join(problems) + "."


__all__ = [
    "PrecachePlannerFactoryError",
    "PrecachePlannerFactoryOverrides",
    "PrecachePlannerFactoryResult",
    "PrecacheSettings",
    "build_precache_planner",
]
