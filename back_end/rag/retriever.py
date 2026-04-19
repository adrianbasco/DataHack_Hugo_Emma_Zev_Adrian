"""Template-aware semantic retrieval over valid scraped place documents."""

from __future__ import annotations

import logging
from pathlib import Path

import yaml

from back_end.rag.embeddings import EmbeddingClient
from back_end.rag.models import StopRetrievalRequest, StopRetrievalResult
from back_end.rag.reranker import rerank_hits
from back_end.rag.settings import RagSettings, load_rag_settings
from back_end.rag.vector_store import ExactVectorStore

logger = logging.getLogger(__name__)

CONNECTIVE_STOP_TYPES: frozenset[str] = frozenset(
    {
        "boardwalk_or_lookout",
        "botanical_garden",
        "ferry_ride",
        "harbor_or_pier",
        "park_or_garden",
        "scenic_lookout",
    }
)

STOP_TYPE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "aquarium": ("aquarium",),
    "arcade": ("arcade", "amusement"),
    "art_gallery": ("gallery", "art"),
    "bakery": ("bakery", "pastry", "patisserie"),
    "bakery_or_market": ("bakery", "market", "pastry", "farmers"),
    "bar": ("bar",),
    "beach": ("beach",),
    "bookstore": ("bookstore", "book"),
    "bowling_alley": ("bowling",),
    "brewery_or_bar": ("brewery", "bar", "beer"),
    "brunch_restaurant": ("brunch", "breakfast", "restaurant", "cafe"),
    "cafe": ("cafe", "coffee"),
    "casual_restaurant": ("restaurant", "dining"),
    "cocktail_bar": ("cocktail", "bar"),
    "comedy_club": ("comedy",),
    "dance_hall_or_club": ("dance", "club", "nightclub"),
    "dessert_shop": ("dessert", "ice cream", "gelato", "cake"),
    "escape_room": ("escape",),
    "fish_market": ("fish market", "seafood market", "fish"),
    "amusement_park": ("amusement", "theme park", "rides", "funfair"),
    "live_music_venue": ("live music", "music"),
    "mini_golf": ("mini golf", "putt"),
    "movie_theater": ("cinema", "movie", "theater"),
    "museum": ("museum",),
    "performing_arts_venue": ("theatre", "theater", "performing", "arts"),
    "restaurant": ("restaurant", "dining"),
    "rooftop_bar": ("rooftop", "bar"),
    "seafood_restaurant": ("seafood", "restaurant"),
    "wine_bar": ("wine", "bar"),
    "zoo": ("zoo", "wildlife"),
}


class RagRetrieverError(RuntimeError):
    """Raised when retrieval cannot run safely."""


class RagRetriever:
    """Retrieve valid scraped places for template stops."""

    def __init__(
        self,
        *,
        vector_store: ExactVectorStore,
        embedding_client: EmbeddingClient,
    ) -> None:
        self._vector_store = vector_store
        self._embedding_client = embedding_client

    async def retrieve_stop(
        self,
        request: StopRetrievalRequest,
    ) -> StopRetrievalResult:
        """Retrieve and rerank candidates for a single date-template stop."""

        stop_type = request.stop_type.strip()
        if not stop_type:
            raise ValueError("stop_type must not be empty.")
        if stop_type in CONNECTIVE_STOP_TYPES:
            return StopRetrievalResult(
                stop_type=stop_type,
                query_text=request.query_text,
                hits=(),
                empty_reason="Connective stop; no semantic venue retrieval required.",
                is_connective=True,
            )

        compatible_ids = self._compatible_place_ids(stop_type)
        if request.candidate_place_ids is not None:
            requested = {str(place_id) for place_id in request.candidate_place_ids}
            compatible_ids &= requested
        if not compatible_ids:
            return StopRetrievalResult(
                stop_type=stop_type,
                query_text=request.query_text,
                hits=(),
                empty_reason=f"No valid scraped documents are compatible with stop_type={stop_type!r}.",
            )

        query_embedding = (await self._embedding_client.embed_texts((request.query_text,)))[0]
        semantic_hits = self._vector_store.search(
            query_embedding,
            top_k=max(request.top_k * 5, request.top_k),
            candidate_place_ids=compatible_ids,
        )
        if not semantic_hits:
            return StopRetrievalResult(
                stop_type=stop_type,
                query_text=request.query_text,
                hits=(),
                empty_reason=f"Vector search returned no hits for stop_type={stop_type!r}.",
            )
        reranked = rerank_hits(
            semantic_hits,
            stop_type=stop_type,
            query_text=request.query_text,
        )[: request.top_k]
        return StopRetrievalResult(
            stop_type=stop_type,
            query_text=request.query_text,
            hits=reranked,
            empty_reason=None,
        )

    def _compatible_place_ids(self, stop_type: str) -> set[str]:
        documents = self._vector_store.documents
        compatible: set[str] = set()
        keywords = STOP_TYPE_KEYWORDS.get(stop_type)
        if keywords is None:
            raise RagRetrieverError(
                f"No explicit retrieval keyword mapping for stop_type={stop_type!r}."
            )
        for row in documents.itertuples(index=False):
            stop_tags = _string_set(getattr(row, "crawl4ai_template_stop_tags", ()))
            if stop_type in stop_tags:
                compatible.add(str(row.fsq_place_id))
                continue
            searchable = " ".join(
                [
                    " ".join(_string_set(getattr(row, "fsq_category_labels", ()))),
                    " ".join(_string_set(getattr(row, "crawl4ai_evidence_snippets", ()))),
                ]
            )
            if any(keyword in searchable for keyword in keywords):
                compatible.add(str(row.fsq_place_id))
        return compatible


def load_date_templates(path: Path | str | None = None) -> tuple[dict[str, object], ...]:
    """Load date templates from YAML."""

    settings = load_rag_settings()
    template_path = Path(path) if path is not None else settings.date_templates_path
    with template_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    templates = raw.get("templates") if isinstance(raw, dict) else None
    if not isinstance(templates, list) or not templates:
        raise RagRetrieverError(f"Date templates at {template_path} are missing templates.")
    return tuple(templates)


def build_stop_query(
    *,
    user_query: str,
    template: dict[str, object],
    stop_type: str,
    city: str = "Sydney",
) -> str:
    """Build an instruction-style semantic query for one template stop.

    ``city`` defaults to Sydney to match the current launch dataset. Callers
    serving other cities should pass the target city explicitly — the date
    templates themselves are location-agnostic; only the query wrapper
    localises the search.
    """

    vibe = template.get("vibe", ())
    if isinstance(vibe, str):
        vibe_text = vibe
    else:
        vibe_text = ", ".join(str(item) for item in vibe)
    city_text = city.strip()
    location_clause = f" in {city_text}" if city_text else ""
    parts = [
        f"Find a {stop_type.replace('_', ' ')} for a date{location_clause}.",
        f"Template: {template.get('title', template.get('id', 'unknown'))}.",
        f"Vibe: {vibe_text}.",
        f"Time of day: {template.get('time_of_day', 'flexible')}.",
        f"User preference: {user_query.strip()}",
    ]
    return "\n".join(part for part in parts if part.strip())


def _string_set(value: object) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        values = [value]
    else:
        try:
            values = list(value)  # type: ignore[arg-type]
        except TypeError:
            values = [value]
    return {str(item).strip().casefold() for item in values if str(item).strip()}

