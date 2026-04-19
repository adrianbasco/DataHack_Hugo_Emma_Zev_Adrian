"""Async Brave Search API client for web and local place evidence."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

import httpx

from back_end.clients.settings import BraveSettings

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})


class BraveClientError(RuntimeError):
    """Base class for Brave Search client failures."""


class BraveUpstreamError(BraveClientError):
    """Raised when Brave rejects a request or returns invalid HTTP."""


class BraveResponseSchemaError(BraveClientError):
    """Raised when Brave returns an unexpected payload shape."""


@dataclass(frozen=True)
class BraveWebResult:
    title: str | None
    url: str | None
    description: str | None
    extra_snippets: tuple[str, ...]


@dataclass(frozen=True)
class BraveLocalRating:
    rating_value: float | None
    best_rating: float | None
    review_count: int | None
    is_tripadvisor: bool | None


@dataclass(frozen=True)
class BraveLocalResult:
    brave_id: str | None
    title: str | None
    url: str | None
    provider_url: str | None
    latitude: float | None
    longitude: float | None
    rating: BraveLocalRating | None
    categories: tuple[str, ...]
    price_range: str | None


@dataclass(frozen=True)
class BraveLocalDescription:
    brave_id: str | None
    description: str


class BraveSearchClient:
    """Thin async client around Brave Search endpoints used for enrichment."""

    def __init__(
        self,
        settings: BraveSettings,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = settings
        self._http_client = http_client or httpx.AsyncClient()
        self._owns_http_client = http_client is None

    async def __aenter__(self) -> "BraveSearchClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_http_client:
            await self._http_client.aclose()

    def close(self) -> None:
        """Compatibility shim for non-async callers."""

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self.aclose())
            return
        raise RuntimeError(
            "BraveSearchClient.close() was called inside a running event loop. "
            "Use 'await client.aclose()' instead."
        )

    async def search_web(
        self,
        query: str,
        *,
        count: int = 5,
    ) -> tuple[BraveWebResult, ...]:
        if not query.strip():
            raise ValueError("query must not be empty.")
        payload = await self._request_json(
            "GET",
            f"{self._settings.base_url.rstrip('/')}/web/search",
            params={
                "q": query,
                "count": count,
                "country": self._settings.country,
                "search_lang": self._settings.search_lang,
                "ui_lang": self._settings.ui_lang,
                "spellcheck": 1,
                "extra_snippets": 1,
            },
        )
        web = payload.get("web")
        if web is None:
            return ()
        if not isinstance(web, dict):
            raise BraveResponseSchemaError("Brave web payload was not an object.")
        results = web.get("results") or []
        if not isinstance(results, list):
            raise BraveResponseSchemaError("Brave web results was not a list.")
        return tuple(_parse_web_result(item) for item in results)

    async def search_local(
        self,
        query: str,
        *,
        latitude: float,
        longitude: float,
        radius_meters: int = 1000,
        count: int = 3,
    ) -> tuple[BraveLocalResult, ...]:
        if not query.strip():
            raise ValueError("query must not be empty.")
        payload = await self._request_json(
            "GET",
            f"{self._settings.base_url.rstrip('/')}/local/place_search",
            params={
                "q": query,
                "latitude": latitude,
                "longitude": longitude,
                "radius": radius_meters,
                "count": count,
                "country": self._settings.country,
                "search_lang": self._settings.search_lang,
                "ui_lang": self._settings.ui_lang,
                "units": "metric",
            },
        )
        results = payload.get("results") or []
        if not isinstance(results, list):
            raise BraveResponseSchemaError("Brave local results was not a list.")
        return tuple(_parse_local_result(item) for item in results)

    async def get_local_descriptions(
        self,
        brave_ids: tuple[str, ...] | list[str],
    ) -> dict[str, BraveLocalDescription]:
        ids = [brave_id for brave_id in brave_ids if brave_id]
        if not ids:
            return {}
        payload = await self._request_json(
            "GET",
            f"{self._settings.base_url.rstrip('/')}/local/descriptions",
            params=[("ids", brave_id) for brave_id in ids],
        )
        results = payload.get("results") or []
        if not isinstance(results, list):
            raise BraveResponseSchemaError("Brave local descriptions was not a list.")
        descriptions: dict[str, BraveLocalDescription] = {}
        for item in results:
            if not isinstance(item, dict):
                continue
            brave_id = item.get("id")
            description = item.get("description")
            if isinstance(brave_id, str) and isinstance(description, str) and description:
                descriptions[brave_id] = BraveLocalDescription(
                    brave_id=brave_id,
                    description=description,
                )
        return descriptions

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, object] | list[tuple[str, object]],
    ) -> dict[str, Any]:
        attempts = self._settings.retry_count + 1
        last_response: httpx.Response | None = None
        for attempt_index in range(attempts):
            response = await self._http_client.request(
                method,
                url,
                params=params,
                headers={
                    "Accept": "application/json",
                    "X-Subscription-Token": self._settings.api_key,
                },
                timeout=self._settings.timeout_seconds,
            )
            last_response = response
            if response.status_code < 400:
                break
            if (
                response.status_code in RETRYABLE_STATUS_CODES
                and attempt_index < self._settings.retry_count
            ):
                logger.warning(
                    "Brave request failed with status=%s on attempt %s/%s; retrying.",
                    response.status_code,
                    attempt_index + 1,
                    attempts,
                )
                continue
            logger.error(
                "Brave request failed with status=%s body=%r",
                response.status_code,
                response.text[:1000],
            )
            raise BraveUpstreamError(
                f"Brave Search request failed with status {response.status_code}."
            )

        if last_response is None:
            raise BraveUpstreamError("Brave Search request did not produce a response.")

        try:
            payload = last_response.json()
        except ValueError as exc:
            logger.error("Brave returned non-JSON response: %r", last_response.text[:1000])
            raise BraveResponseSchemaError("Brave returned a non-JSON response.") from exc

        if not isinstance(payload, dict):
            raise BraveResponseSchemaError(
                "Brave returned a top-level payload that was not an object."
            )
        return payload


def _parse_web_result(item: Any) -> BraveWebResult:
    if not isinstance(item, dict):
        raise BraveResponseSchemaError("Brave web result entry was not an object.")
    extra_snippets = item.get("extra_snippets") or []
    if not isinstance(extra_snippets, list):
        extra_snippets = []
    return BraveWebResult(
        title=_optional_string(item.get("title")),
        url=_optional_string(item.get("url")),
        description=_optional_string(item.get("description")),
        extra_snippets=tuple(
            snippet for snippet in extra_snippets if isinstance(snippet, str) and snippet
        ),
    )


def _parse_local_result(item: Any) -> BraveLocalResult:
    if not isinstance(item, dict):
        raise BraveResponseSchemaError("Brave local result entry was not an object.")
    coordinates = item.get("coordinates")
    latitude = None
    longitude = None
    if isinstance(coordinates, (list, tuple)) and len(coordinates) >= 2:
        latitude = _optional_float(coordinates[0])
        longitude = _optional_float(coordinates[1])
    categories = item.get("categories") or []
    if not isinstance(categories, list):
        categories = []
    return BraveLocalResult(
        brave_id=_optional_string(item.get("id")),
        title=_optional_string(item.get("title")),
        url=_optional_string(item.get("url")),
        provider_url=_optional_string(item.get("provider_url")),
        latitude=latitude,
        longitude=longitude,
        rating=_parse_rating(item.get("rating")),
        categories=tuple(
            category for category in categories if isinstance(category, str) and category
        ),
        price_range=_optional_string(item.get("price_range")),
    )


def _parse_rating(item: Any) -> BraveLocalRating | None:
    if not isinstance(item, dict):
        return None
    return BraveLocalRating(
        rating_value=_optional_float(item.get("ratingValue")),
        best_rating=_optional_float(item.get("bestRating")),
        review_count=_optional_int(item.get("reviewCount")),
        is_tripadvisor=item.get("is_tripadvisor")
        if isinstance(item.get("is_tripadvisor"), bool)
        else None,
    )


def _optional_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _optional_float(value: Any) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return None


def _optional_int(value: Any) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None
