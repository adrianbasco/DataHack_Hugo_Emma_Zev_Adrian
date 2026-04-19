"""Feature-profile enrichment for curated places that do not expose websites."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
from collections.abc import Iterable
from dataclasses import dataclass
from difflib import SequenceMatcher
from html import unescape
from typing import Any, Protocol
from urllib.parse import urlparse

import httpx
import pandas as pd

from back_end.clients.brave import (
    BraveLocalDescription,
    BraveLocalResult,
    BraveSearchClient,
    BraveWebResult,
)
from back_end.clients.openrouter import OpenRouterClient
from back_end.clients.settings import BraveSettings, MapsSettings, OpenRouterSettings
from back_end.llm.models import OpenRouterMessage
from back_end.services.website_profiles import _normalize_space

logger = logging.getLogger(__name__)

MAPS_PROFILE_FIELD_MASK = ",".join(
    (
        "places.id",
        "places.displayName",
        "places.formattedAddress",
        "places.location",
        "places.types",
        "places.primaryType",
        "places.primaryTypeDisplayName",
        "places.rating",
        "places.userRatingCount",
        "places.websiteUri",
        "places.googleMapsUri",
        "places.businessStatus",
        "places.regularOpeningHours",
        "places.editorialSummary",
        "places.reviews",
        "places.outdoorSeating",
        "places.goodForChildren",
        "places.goodForGroups",
        "places.allowsDogs",
    )
)

PUBLIC_AUTHORITY_DOMAINS = (
    ".nsw.gov.au",
    ".gov.au",
    "nationalparks.nsw.gov.au",
    "sydney.com",
    "australia.com",
)
OFFICIAL_SOCIAL_DOMAINS = (
    "facebook.com",
    "instagram.com",
)
DIRECTORY_DOMAINS = (
    "tripadvisor.",
    "whitepages.",
    "ubereats.",
    "doordash.",
    "menulog.",
    "yellowpages.",
)
RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})
JSON_BLOCK_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)
TAG_TOKEN_RE = re.compile(r"[^a-z0-9]+")


class _BraveClient(Protocol):
    async def search_web(self, query: str, *, count: int = 5) -> tuple[BraveWebResult, ...]: ...

    async def search_local(
        self,
        query: str,
        *,
        latitude: float,
        longitude: float,
        radius_meters: int = 1000,
        count: int = 3,
    ) -> tuple[BraveLocalResult, ...]: ...

    async def get_local_descriptions(
        self,
        brave_ids: tuple[str, ...] | list[str],
    ) -> dict[str, BraveLocalDescription]: ...


@dataclass(frozen=True)
class NoWebsiteProfileSettings:
    """Settings for no-website feature-vector enrichment."""

    use_maps: bool = True
    use_brave_web: bool = True
    use_brave_local: bool = True
    use_llm: bool = True
    global_concurrency: int = 6
    maps_result_limit: int = 3
    maps_location_bias_radius_meters: float = 900.0
    maps_timeout_seconds: float = 15.0
    maps_retry_count: int = 1
    maps_min_name_similarity: float = 0.74
    maps_max_match_distance_meters: float = 900.0
    brave_web_result_count: int = 5
    brave_local_result_count: int = 3
    brave_local_radius_meters: int = 1000
    accepted_web_result_limit: int = 4
    openrouter_model: str = "google/gemini-2.5-flash-lite"
    llm_max_tokens: int = 900


@dataclass(frozen=True)
class PlaceEvidence:
    """One accepted evidence item used to build a no-website profile."""

    source: str
    title: str | None
    url: str | None
    text: str
    confidence: float

    def to_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "title": self.title,
            "url": self.url,
            "text": self.text,
            "confidence": round(self.confidence, 3),
        }


@dataclass(frozen=True)
class MapsPlaceEvidence:
    """Accepted Google Maps match evidence for a no-website row."""

    place_id: str
    display_name: str
    name_similarity: float
    distance_meters: float | None
    primary_type: str | None
    types: tuple[str, ...]
    rating: float | None
    user_rating_count: int | None
    website_uri: str | None
    google_maps_uri: str | None
    summary: str | None
    review_snippets: tuple[str, ...]
    attributes: dict[str, bool]


@dataclass(frozen=True)
class FeatureProfile:
    """Normalized feature payload that can be converted into vector text."""

    venue_type: str | None
    setting_tags: tuple[str, ...]
    ambience_tags: tuple[str, ...]
    activity_tags: tuple[str, ...]
    date_strengths: tuple[str, ...]
    date_risks: tuple[str, ...]
    best_for: tuple[str, ...]
    avoid_for: tuple[str, ...]
    weather_exposure: str | None
    confidence: float
    missing_data: tuple[str, ...]
    feature_text: str


class NoWebsiteProfileClient:
    """Build grounded feature text for website-exempt places."""

    def __init__(
        self,
        settings: NoWebsiteProfileSettings | None = None,
        *,
        maps_settings: MapsSettings | None = None,
        brave_client: _BraveClient | None = None,
        llm_client: OpenRouterClient | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = settings or NoWebsiteProfileSettings()
        self._maps_settings = maps_settings
        self._http_client = http_client or httpx.AsyncClient()
        self._owns_http_client = http_client is None
        self._brave_client = brave_client
        self._owns_brave_client = False
        self._llm_client = llm_client
        self._owns_llm_client = False
        self._semaphore = asyncio.Semaphore(self._settings.global_concurrency)

    async def __aenter__(self) -> "NoWebsiteProfileClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_http_client:
            await self._http_client.aclose()
        if self._owns_brave_client and self._brave_client is not None:
            aclose = getattr(self._brave_client, "aclose", None)
            if callable(aclose):
                await aclose()
        if self._owns_llm_client and self._llm_client is not None:
            await self._llm_client.aclose()

    async def enrich_dataframe(self, places: pd.DataFrame) -> pd.DataFrame:
        required_columns = {
            "fsq_place_id",
            "name",
            "latitude",
            "longitude",
            "fsq_category_labels",
        }
        missing = sorted(required_columns - set(places.columns))
        if missing:
            raise ValueError(f"places DataFrame is missing required columns: {missing}")
        if places.empty:
            raise ValueError("Refusing to enrich 0 no-website places.")

        self._ensure_optional_clients()
        tasks = [self._enrich_row(row.to_dict()) for _, row in places.iterrows()]
        results = await asyncio.gather(*tasks)
        enrichment = pd.DataFrame(results)
        return places.merge(enrichment, on="fsq_place_id", how="left", validate="one_to_one")

    def _ensure_optional_clients(self) -> None:
        if self._settings.use_brave_web or self._settings.use_brave_local:
            if self._brave_client is None:
                self._brave_client = BraveSearchClient(BraveSettings.from_env())
                self._owns_brave_client = True
        if self._settings.use_llm and self._llm_client is None:
            self._llm_client = OpenRouterClient(OpenRouterSettings.from_env())
            self._owns_llm_client = True
        if self._settings.use_maps and self._maps_settings is None:
            self._maps_settings = MapsSettings.from_env()

    async def _enrich_row(self, row: dict[str, Any]) -> dict[str, Any]:
        place_id = str(row["fsq_place_id"])
        async with self._semaphore:
            try:
                return await self._enrich_row_inner(row)
            except Exception as exc:  # pragma: no cover - loud catch for batch safety
                logger.exception("No-website profile enrichment failed for %s.", place_id)
                fallback = _heuristic_profile(row, evidence=())
                return _result_payload(
                    row=row,
                    status="failed",
                    error=f"{exc.__class__.__name__}: {exc}",
                    maps_match_status="not_run",
                    maps_evidence=None,
                    evidence=(),
                    profile=fallback,
                    source_statuses=("failed",),
                    llm_status="not_run",
                )

    async def _enrich_row_inner(self, row: dict[str, Any]) -> dict[str, Any]:
        evidence: list[PlaceEvidence] = [_baseline_evidence(row)]
        source_statuses: list[str] = ["fsq_baseline"]
        maps_evidence: MapsPlaceEvidence | None = None
        maps_match_status = "not_run"

        if self._settings.use_maps:
            try:
                maps_candidates = await self._search_maps(row)
                maps_evidence = _select_maps_match(row, maps_candidates, self._settings)
                if maps_evidence is None:
                    maps_match_status = "rejected_or_no_match"
                    source_statuses.append("maps_rejected_or_no_match")
                else:
                    maps_match_status = "accepted"
                    source_statuses.append("maps")
                    evidence.append(_maps_to_evidence(maps_evidence))
            except Exception as exc:
                logger.error(
                    "Maps no-website evidence failed for %s: %s",
                    row.get("fsq_place_id"),
                    exc,
                )
                maps_match_status = "error"
                source_statuses.append("maps_error")

        if self._settings.use_brave_web:
            try:
                web_results = await self._brave_client.search_web(
                    _build_web_query(row),
                    count=self._settings.brave_web_result_count,
                )
                accepted = _accepted_web_evidence(
                    row,
                    web_results,
                    limit=self._settings.accepted_web_result_limit,
                )
                evidence.extend(accepted)
                source_statuses.append(
                    f"brave_web_{'accepted' if accepted else 'no_accepted_results'}"
                )
            except Exception as exc:
                logger.error(
                    "Brave web evidence failed for %s: %s",
                    row.get("fsq_place_id"),
                    exc,
                )
                source_statuses.append("brave_web_error")

        if self._settings.use_brave_local and _should_use_brave_local(row):
            try:
                local_results = await self._brave_client.search_local(
                    str(row["name"]),
                    latitude=float(row["latitude"]),
                    longitude=float(row["longitude"]),
                    radius_meters=self._settings.brave_local_radius_meters,
                    count=self._settings.brave_local_result_count,
                )
                accepted_local = _accepted_local_results(row, local_results)
                descriptions = await self._brave_client.get_local_descriptions(
                    [result.brave_id for result in accepted_local if result.brave_id]
                )
                local_evidence = _local_results_to_evidence(accepted_local, descriptions)
                evidence.extend(local_evidence)
                source_statuses.append(
                    f"brave_local_{'accepted' if local_evidence else 'no_accepted_results'}"
                )
            except Exception as exc:
                logger.error(
                    "Brave local evidence failed for %s: %s",
                    row.get("fsq_place_id"),
                    exc,
                )
                source_statuses.append("brave_local_error")

        llm_status = "not_run"
        profile = _heuristic_profile(row, evidence=tuple(evidence))
        has_external_evidence = any(item.source != "fsq_baseline" for item in evidence)
        if self._settings.use_llm and self._llm_client is not None and has_external_evidence:
            try:
                llm_profile = await self._llm_extract_profile(row, tuple(evidence), profile)
                profile = llm_profile
                llm_status = "ok"
            except Exception as exc:
                logger.error(
                    "LLM feature extraction failed for no-website place %s: %s",
                    row.get("fsq_place_id"),
                    exc,
                )
                source_statuses.append("llm_error")
                llm_status = "error"
        elif self._settings.use_llm and not has_external_evidence:
            llm_status = "skipped_no_external_evidence"

        status = "ok" if len(evidence) > 1 else "baseline_only"
        return _result_payload(
            row=row,
            status=status,
            error=None,
            maps_match_status=maps_match_status,
            maps_evidence=maps_evidence,
            evidence=tuple(evidence),
            profile=profile,
            source_statuses=tuple(source_statuses),
            llm_status=llm_status,
        )

    async def _search_maps(self, row: dict[str, Any]) -> tuple[dict[str, Any], ...]:
        if self._maps_settings is None:
            raise RuntimeError("Maps settings were not initialized.")
        query = _build_maps_query(row)
        payload = {
            "textQuery": query,
            "maxResultCount": self._settings.maps_result_limit,
            "locationBias": {
                "circle": {
                    "center": {
                        "latitude": float(row["latitude"]),
                        "longitude": float(row["longitude"]),
                    },
                    "radius": self._settings.maps_location_bias_radius_meters,
                }
            },
            "languageCode": "en",
            "regionCode": "AU",
        }
        response = await self._maps_request_json(
            "POST",
            f"{self._maps_settings.places_base_url.rstrip('/')}/places:searchText",
            body=payload,
        )
        places = response.get("places") or []
        if not isinstance(places, list):
            raise ValueError("Google Maps text-search payload did not include a places list.")
        return tuple(place for place in places if isinstance(place, dict))

    async def _maps_request_json(
        self,
        method: str,
        url: str,
        *,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        if self._maps_settings is None:
            raise RuntimeError("Maps settings were not initialized.")
        attempts = self._settings.maps_retry_count + 1
        last_response: httpx.Response | None = None
        for attempt_index in range(attempts):
            response = await self._http_client.request(
                method,
                url,
                json=body,
                headers={
                    "X-Goog-Api-Key": self._maps_settings.api_key,
                    "X-Goog-FieldMask": MAPS_PROFILE_FIELD_MASK,
                },
                timeout=self._settings.maps_timeout_seconds,
            )
            last_response = response
            if response.status_code < 400:
                break
            if (
                response.status_code in RETRYABLE_STATUS_CODES
                and attempt_index < self._settings.maps_retry_count
            ):
                logger.warning(
                    "Maps profile search failed with status=%s on attempt %s/%s; retrying.",
                    response.status_code,
                    attempt_index + 1,
                    attempts,
                )
                continue
            logger.error(
                "Maps profile search failed with status=%s body=%r",
                response.status_code,
                response.text[:1000],
            )
            raise RuntimeError(f"Maps profile search failed with status {response.status_code}.")
        if last_response is None:
            raise RuntimeError("Maps profile search did not produce a response.")
        payload = last_response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("Maps profile search returned a non-object payload.")
        return payload

    async def _llm_extract_profile(
        self,
        row: dict[str, Any],
        evidence: tuple[PlaceEvidence, ...],
        fallback: FeatureProfile,
    ) -> FeatureProfile:
        if self._llm_client is None:
            raise RuntimeError("LLM client was not initialized.")
        result = await self._llm_client.create_chat_completion(
            model=self._settings.openrouter_model,
            messages=(
                OpenRouterMessage(
                    role="system",
                    content=(
                        "You are a strict information extraction engine for a "
                        "date-planning retrieval index. Output valid JSON only. "
                        "Use only supplied evidence; do not invent details. "
                        "Keep every string compact. Do not include raw source snippets."
                    ),
                ),
                OpenRouterMessage(
                    role="user",
                    content=_build_llm_prompt(row, evidence, fallback),
                ),
            ),
            temperature=0,
            response_format={"type": "json_object"},
            max_tokens=self._settings.llm_max_tokens,
        )
        parsed = _parse_llm_json(result.output_text or "")
        return _feature_profile_from_llm(parsed, fallback=fallback)


def _select_maps_match(
    row: dict[str, Any],
    candidates: tuple[dict[str, Any], ...],
    settings: NoWebsiteProfileSettings,
) -> MapsPlaceEvidence | None:
    scored: list[tuple[float, MapsPlaceEvidence]] = []
    for place in candidates:
        evidence = _parse_maps_place(row, place)
        if evidence is None:
            continue
        distance_score = 0.0
        if evidence.distance_meters is not None:
            distance_score = max(
                0.0,
                1.0 - evidence.distance_meters / settings.maps_max_match_distance_meters,
            )
        category_score = 0.15 if _maps_type_matches_categories(evidence, row) else 0.0
        total_score = evidence.name_similarity + distance_score + category_score
        if evidence.name_similarity < settings.maps_min_name_similarity:
            if not (
                evidence.distance_meters is not None
                and evidence.distance_meters <= 150
                and _maps_type_matches_categories(evidence, row)
            ):
                continue
        if (
            evidence.distance_meters is not None
            and evidence.distance_meters > settings.maps_max_match_distance_meters
        ):
            continue
        scored.append((total_score, evidence))
    if not scored:
        return None
    scored.sort(key=lambda item: item[0], reverse=True)
    return scored[0][1]


def _parse_maps_place(row: dict[str, Any], place: dict[str, Any]) -> MapsPlaceEvidence | None:
    display_name = ((place.get("displayName") or {}).get("text") or "").strip()
    place_id = place.get("id")
    if not display_name or not isinstance(place_id, str):
        return None
    location = place.get("location") or {}
    distance_meters = None
    if isinstance(location, dict) and "latitude" in location and "longitude" in location:
        distance_meters = _distance_meters(
            float(row["latitude"]),
            float(row["longitude"]),
            float(location["latitude"]),
            float(location["longitude"]),
        )
    primary_type = ((place.get("primaryTypeDisplayName") or {}).get("text") or place.get("primaryType"))
    types = place.get("types") or []
    summary = ((place.get("editorialSummary") or {}).get("text") or _generative_summary_text(place))
    review_snippets = _maps_review_snippets(place.get("reviews") or [])
    attributes = {
        key: bool(place[key])
        for key in ("outdoorSeating", "goodForChildren", "goodForGroups", "allowsDogs")
        if isinstance(place.get(key), bool)
    }
    return MapsPlaceEvidence(
        place_id=place_id,
        display_name=display_name,
        name_similarity=_name_similarity(str(row["name"]), display_name),
        distance_meters=distance_meters,
        primary_type=primary_type if isinstance(primary_type, str) else None,
        types=tuple(item for item in types if isinstance(item, str)),
        rating=float(place["rating"]) if isinstance(place.get("rating"), (int, float)) else None,
        user_rating_count=place.get("userRatingCount")
        if isinstance(place.get("userRatingCount"), int)
        else None,
        website_uri=place.get("websiteUri") if isinstance(place.get("websiteUri"), str) else None,
        google_maps_uri=place.get("googleMapsUri")
        if isinstance(place.get("googleMapsUri"), str)
        else None,
        summary=summary if isinstance(summary, str) and summary else None,
        review_snippets=review_snippets,
        attributes=attributes,
    )


def _maps_to_evidence(maps: MapsPlaceEvidence) -> PlaceEvidence:
    parts = [
        f"Maps match: {maps.display_name}",
        f"type: {maps.primary_type or ', '.join(maps.types)}",
    ]
    if maps.rating is not None and maps.user_rating_count is not None:
        parts.append(f"rating: {maps.rating} from {maps.user_rating_count} reviews")
    if maps.summary:
        parts.append(f"summary: {maps.summary}")
    if maps.review_snippets:
        parts.append("review snippets: " + " | ".join(maps.review_snippets[:2]))
    if maps.attributes:
        attrs = ", ".join(f"{key}={value}" for key, value in sorted(maps.attributes.items()))
        parts.append(f"attributes: {attrs}")
    return PlaceEvidence(
        source="maps",
        title=maps.display_name,
        url=maps.google_maps_uri,
        text=_normalize_space("; ".join(parts))[:1200],
        confidence=0.9,
    )


def _accepted_web_evidence(
    row: dict[str, Any],
    results: tuple[BraveWebResult, ...],
    *,
    limit: int,
) -> tuple[PlaceEvidence, ...]:
    scored: list[tuple[float, PlaceEvidence]] = []
    for result in results:
        score = _web_result_score(row, result)
        if score < 0.45:
            continue
        text_parts = [result.description or "", *result.extra_snippets]
        text = _normalize_space(" ".join(text_parts))
        if not text:
            continue
        scored.append(
            (
                score,
                PlaceEvidence(
                    source="brave_web",
                    title=result.title,
                    url=result.url,
                    text=text[:1200],
                    confidence=min(score, 0.85),
                ),
            )
        )
    scored.sort(key=lambda item: item[0], reverse=True)
    return tuple(evidence for _, evidence in scored[:limit])


def _accepted_local_results(
    row: dict[str, Any],
    results: tuple[BraveLocalResult, ...],
) -> tuple[BraveLocalResult, ...]:
    accepted: list[BraveLocalResult] = []
    for result in results:
        if not result.title:
            continue
        similarity = _name_similarity(str(row["name"]), result.title)
        distance = None
        if result.latitude is not None and result.longitude is not None:
            distance = _distance_meters(
                float(row["latitude"]),
                float(row["longitude"]),
                result.latitude,
                result.longitude,
            )
        if similarity >= 0.80 and (distance is None or distance <= 350):
            accepted.append(result)
        elif similarity >= 0.68 and distance is not None and distance <= 100:
            accepted.append(result)
    return tuple(accepted)


def _local_results_to_evidence(
    results: tuple[BraveLocalResult, ...],
    descriptions: dict[str, BraveLocalDescription],
) -> tuple[PlaceEvidence, ...]:
    evidence: list[PlaceEvidence] = []
    for result in results:
        description = (
            descriptions.get(result.brave_id).description
            if result.brave_id and result.brave_id in descriptions
            else None
        )
        parts = [description or ""]
        if result.categories:
            parts.append("Brave local categories: " + ", ".join(result.categories))
        if result.rating and result.rating.rating_value is not None:
            parts.append(
                f"Brave local rating: {result.rating.rating_value}"
                + (
                    f" from {result.rating.review_count} reviews"
                    if result.rating.review_count is not None
                    else ""
                )
            )
        text = _normalize_space(" ".join(parts))
        if not text:
            continue
        evidence.append(
            PlaceEvidence(
                source="brave_local",
                title=result.title,
                url=result.url,
                text=text[:1200],
                confidence=0.72,
            )
        )
    return tuple(evidence)


def _heuristic_profile(
    row: dict[str, Any],
    *,
    evidence: tuple[PlaceEvidence, ...],
) -> FeatureProfile:
    category_text = _category_text(row)
    evidence_text = " ".join(item.text for item in evidence).casefold()
    combined = f"{category_text} {evidence_text}".casefold()
    venue_type = _venue_type_from_categories(category_text)
    setting_tags = _dedupe(
        [
            *_category_setting_tags(category_text),
            *_keyword_tags(
                combined,
                {
                    "coastal": ("beach", "coastal", "surf", "ocean"),
                    "waterfront": ("waterfront", "harbour", "harbor", "marina", "pier", "wharf"),
                    "garden": ("garden", "botanical"),
                    "views": ("view", "lookout", "scenic", "sunset"),
                    "outdoor": ("outdoor", "trail", "park", "beach"),
                    "heritage": ("heritage", "historic", "lighthouse"),
                },
            ),
        ]
    )
    ambience_tags = _dedupe(
        _keyword_tags(
            combined,
            {
                "scenic": ("scenic", "view", "picturesque", "beautiful"),
                "lively": ("lively", "buzzing", "market", "food truck", "crowd"),
                "casual": ("casual", "relaxed", "easy"),
                "quiet": ("quiet", "peaceful", "calm"),
                "active": ("hiking", "walking", "track", "trail", "bike"),
                "romantic": ("romantic", "sunset", "date"),
            },
        )
    )
    activity_tags = _dedupe(
        [
            *_category_activity_tags(category_text),
            *_keyword_tags(
                combined,
                {
                    "walking": ("walk", "walking", "stroll", "track", "trail"),
                    "swimming": ("swim", "surf", "beach"),
                    "food": ("food", "market", "produce", "restaurant"),
                    "picnic": ("picnic",),
                    "whale_watching": ("whale",),
                    "sunset": ("sunset",),
                },
            ),
        ]
    )
    date_strengths = _date_strengths(venue_type, setting_tags, activity_tags, combined)
    date_risks = _date_risks(venue_type, setting_tags, activity_tags, combined)
    best_for = _best_for(venue_type, setting_tags, activity_tags)
    avoid_for = _avoid_for(venue_type, setting_tags, activity_tags)
    weather_exposure = _weather_exposure(venue_type, setting_tags)
    confidence = min(0.95, 0.35 + (0.12 * max(0, len(evidence) - 1)))
    missing_data = []
    if len(evidence) <= 1:
        missing_data.append("external_evidence")
    if "maps" not in {item.source for item in evidence}:
        missing_data.append("maps_match")
    feature_text = _build_feature_text(
        row=row,
        venue_type=venue_type,
        setting_tags=tuple(setting_tags),
        ambience_tags=tuple(ambience_tags),
        activity_tags=tuple(activity_tags),
        date_strengths=tuple(date_strengths),
        date_risks=tuple(date_risks),
        best_for=tuple(best_for),
        avoid_for=tuple(avoid_for),
        evidence=evidence,
    )
    return FeatureProfile(
        venue_type=venue_type,
        setting_tags=tuple(setting_tags),
        ambience_tags=tuple(ambience_tags),
        activity_tags=tuple(activity_tags),
        date_strengths=tuple(date_strengths),
        date_risks=tuple(date_risks),
        best_for=tuple(best_for),
        avoid_for=tuple(avoid_for),
        weather_exposure=weather_exposure,
        confidence=confidence,
        missing_data=tuple(missing_data),
        feature_text=feature_text,
    )


def _result_payload(
    *,
    row: dict[str, Any],
    status: str,
    error: str | None,
    maps_match_status: str,
    maps_evidence: MapsPlaceEvidence | None,
    evidence: tuple[PlaceEvidence, ...],
    profile: FeatureProfile,
    source_statuses: tuple[str, ...],
    llm_status: str,
) -> dict[str, Any]:
    evidence_json = json.dumps(
        [item.to_dict() for item in evidence],
        ensure_ascii=False,
        sort_keys=True,
    )
    return {
        "fsq_place_id": str(row["fsq_place_id"]),
        "no_website_profile_status": status,
        "no_website_profile_error": error,
        "no_website_source_statuses": list(source_statuses),
        "no_website_llm_status": llm_status,
        "no_website_maps_match_status": maps_match_status,
        "no_website_maps_place_id": maps_evidence.place_id if maps_evidence else None,
        "no_website_maps_display_name": maps_evidence.display_name if maps_evidence else None,
        "no_website_maps_name_similarity": maps_evidence.name_similarity if maps_evidence else None,
        "no_website_maps_distance_meters": maps_evidence.distance_meters if maps_evidence else None,
        "no_website_maps_primary_type": maps_evidence.primary_type if maps_evidence else None,
        "no_website_maps_rating": maps_evidence.rating if maps_evidence else None,
        "no_website_maps_user_rating_count": (
            maps_evidence.user_rating_count if maps_evidence else None
        ),
        "no_website_maps_website_uri": maps_evidence.website_uri if maps_evidence else None,
        "no_website_maps_google_maps_uri": maps_evidence.google_maps_uri if maps_evidence else None,
        "no_website_evidence_count": len(evidence),
        "no_website_evidence_json": evidence_json,
        "no_website_venue_type": profile.venue_type,
        "no_website_setting_tags": list(profile.setting_tags),
        "no_website_ambience_tags": list(profile.ambience_tags),
        "no_website_activity_tags": list(profile.activity_tags),
        "no_website_date_strengths": list(profile.date_strengths),
        "no_website_date_risks": list(profile.date_risks),
        "no_website_best_for": list(profile.best_for),
        "no_website_avoid_for": list(profile.avoid_for),
        "no_website_weather_exposure": profile.weather_exposure,
        "no_website_confidence": profile.confidence,
        "no_website_missing_data": list(profile.missing_data),
        "no_website_feature_text": profile.feature_text,
    }


def _build_llm_prompt(
    row: dict[str, Any],
    evidence: tuple[PlaceEvidence, ...],
    fallback: FeatureProfile,
) -> str:
    evidence_lines = "\n".join(
        f"- source={item.source}; title={item.title}; url={item.url}; text={item.text[:650]}"
        for item in evidence
    )[:3200]
    return (
        "Extract a date-planning feature vector from evidence only. "
        "Use null or [] when evidence is weak. Return only JSON with keys: "
        "feature_text, venue_type, setting_tags, ambience_tags, activity_tags, "
        "date_strengths, date_risks, best_for, avoid_for, weather_exposure, "
        "confidence, missing_data.\n"
        "Rules: feature_text must be 80-600 characters, no newlines inside string values, "
        "arrays max 6 short items, confidence is 0-1.\n\n"
        f"Place: {row.get('name')}\n"
        f"Location: {_join_nonempty([row.get('locality'), row.get('region'), row.get('postcode')])}\n"
        f"FSQ categories: {_category_text(row)}\n"
        f"Heuristic fallback venue_type: {fallback.venue_type}\n"
        f"Evidence:\n{evidence_lines}"
    )


def _parse_llm_json(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    match = JSON_BLOCK_RE.search(cleaned)
    if match:
        cleaned = match.group(1).strip()
    payload = json.loads(cleaned)
    if not isinstance(payload, dict):
        raise ValueError("LLM returned JSON but not an object.")
    return payload


def _feature_profile_from_llm(
    payload: dict[str, Any],
    *,
    fallback: FeatureProfile,
) -> FeatureProfile:
    feature_text = _optional_text(payload.get("feature_text"))
    if feature_text is None or not _is_useful_llm_feature_text(feature_text, fallback):
        feature_text = fallback.feature_text
    confidence = payload.get("confidence")
    if not isinstance(confidence, (int, float)) or isinstance(confidence, bool):
        confidence = fallback.confidence
    confidence = max(0.0, min(1.0, float(confidence)))
    return FeatureProfile(
        venue_type=_optional_text(payload.get("venue_type")) or fallback.venue_type,
        setting_tags=_tuple_strings(payload.get("setting_tags")) or fallback.setting_tags,
        ambience_tags=_tuple_strings(payload.get("ambience_tags")) or fallback.ambience_tags,
        activity_tags=_tuple_strings(payload.get("activity_tags")) or fallback.activity_tags,
        date_strengths=_tuple_strings(payload.get("date_strengths")) or fallback.date_strengths,
        date_risks=_tuple_strings(payload.get("date_risks")) or fallback.date_risks,
        best_for=_tuple_strings(payload.get("best_for")) or fallback.best_for,
        avoid_for=_tuple_strings(payload.get("avoid_for")) or fallback.avoid_for,
        weather_exposure=_optional_text(payload.get("weather_exposure"))
        or fallback.weather_exposure,
        confidence=confidence,
        missing_data=_tuple_strings(payload.get("missing_data")) or fallback.missing_data,
        feature_text=feature_text[:2400],
    )


def _is_useful_llm_feature_text(text: str, fallback: FeatureProfile) -> bool:
    normalized = _normalize_space(text)
    if len(normalized) < 80:
        return False
    if fallback.venue_type and normalized.casefold() == fallback.venue_type.casefold():
        return False
    return True


def _baseline_evidence(row: dict[str, Any]) -> PlaceEvidence:
    text = (
        f"FSQ baseline: {row.get('name')}; categories: {_category_text(row)}; "
        f"location: {_join_nonempty([row.get('address'), row.get('locality'), row.get('region'), row.get('postcode')])}"
    )
    return PlaceEvidence(
        source="fsq_baseline",
        title=str(row.get("name") or ""),
        url=None,
        text=_normalize_space(text),
        confidence=0.35,
    )


def _build_maps_query(row: dict[str, Any]) -> str:
    return _join_distinct_query_parts(
        (row.get("name"), row.get("locality"), row.get("region"), "Australia")
    )


def _build_web_query(row: dict[str, Any]) -> str:
    category_hint = _web_category_hint(_category_text(row))
    return _join_distinct_query_parts(
        (
            f'"{row.get("name")}"',
            row.get("locality"),
            row.get("region"),
            "Sydney",
            category_hint,
        )
    )


def _web_result_score(row: dict[str, Any], result: BraveWebResult) -> float:
    haystack = " ".join(
        part
        for part in [result.title or "", result.description or "", *result.extra_snippets]
        if part
    )
    if not haystack:
        return 0.0
    normalized_haystack = _token_text(haystack)
    normalized_name = _token_text(str(row.get("name") or ""))
    name_overlap = _token_overlap(normalized_name, normalized_haystack)
    locality_bonus = 0.0
    if _present(row.get("locality")) and _token_text(str(row["locality"])) in normalized_haystack:
        locality_bonus = 0.1
    category_bonus = 0.08 if _category_hint_present(row, normalized_haystack) else 0.0
    domain_bonus = _domain_bonus(result.url)
    directory_penalty = 0.15 if _is_directory_url(result.url) else 0.0
    return max(0.0, min(1.0, name_overlap + locality_bonus + category_bonus + domain_bonus - directory_penalty))


def _domain_bonus(url: str | None) -> float:
    if not url:
        return 0.0
    domain = urlparse(url).netloc.casefold().lstrip("www.")
    if any(domain.endswith(public_domain) or public_domain in domain for public_domain in PUBLIC_AUTHORITY_DOMAINS):
        return 0.35
    if any(social in domain for social in OFFICIAL_SOCIAL_DOMAINS):
        return 0.2
    if _is_directory_url(url):
        return 0.05
    return 0.12


def _is_directory_url(url: str | None) -> bool:
    if not url:
        return False
    domain = urlparse(url).netloc.casefold()
    return any(marker in domain for marker in DIRECTORY_DOMAINS)


def _should_use_brave_local(row: dict[str, Any]) -> bool:
    category_text = _category_text(row).casefold()
    return any(
        marker in category_text
        for marker in ("food truck", "night market", "farmers market", "food and beverage retail")
    )


def _maps_type_matches_categories(evidence: MapsPlaceEvidence, row: dict[str, Any]) -> bool:
    category_text = _category_text(row).casefold()
    maps_text = " ".join([evidence.primary_type or "", *evidence.types]).casefold()
    pairs = (
        ("beach", ("beach", "natural_feature")),
        ("hiking trail", ("route", "park", "tourist_attraction")),
        ("scenic lookout", ("tourist_attraction", "park", "natural_feature")),
        ("harbor", ("marina", "pier", "point_of_interest")),
        ("marina", ("marina", "point_of_interest")),
        ("pier", ("pier", "transit_station", "point_of_interest")),
        ("food truck", ("restaurant", "food")),
        ("market", ("market", "food_store", "store")),
    )
    return any(category in category_text and any(marker in maps_text for marker in markers) for category, markers in pairs)


def _category_text(row: dict[str, Any]) -> str:
    labels = row.get("fsq_category_labels")
    if isinstance(labels, str):
        return labels
    if isinstance(labels, Iterable) and not isinstance(labels, (bytes, dict)):
        return ", ".join(str(label) for label in labels if str(label).strip())
    return ""


def _venue_type_from_categories(category_text: str) -> str | None:
    lowered = category_text.casefold()
    mapping = (
        ("beach", "beach"),
        ("hiking trail", "hiking_trail"),
        ("bike trail", "bike_trail"),
        ("scenic lookout", "scenic_lookout"),
        ("botanical garden", "botanical_garden"),
        ("harbor", "harbor_or_marina"),
        ("marina", "harbor_or_marina"),
        ("pier", "pier"),
        ("night market", "night_market"),
        ("farmers market", "farmers_market"),
        ("food truck", "food_truck"),
    )
    for marker, venue_type in mapping:
        if marker in lowered:
            return venue_type
    return None


def _category_setting_tags(category_text: str) -> list[str]:
    lowered = category_text.casefold()
    tags: list[str] = []
    if any(marker in lowered for marker in ("beach", "harbor", "marina", "pier")):
        tags.extend(["outdoor", "waterfront"])
    if "beach" in lowered:
        tags.append("coastal")
    if "scenic lookout" in lowered:
        tags.extend(["outdoor", "views"])
    if "botanical garden" in lowered:
        tags.extend(["outdoor", "garden"])
    if "hiking trail" in lowered or "bike trail" in lowered:
        tags.append("outdoor")
    return tags


def _category_activity_tags(category_text: str) -> list[str]:
    lowered = category_text.casefold()
    tags: list[str] = []
    if any(marker in lowered for marker in ("hiking trail", "scenic lookout", "pier", "harbor", "marina", "botanical garden")):
        tags.append("walking")
    if "bike trail" in lowered:
        tags.append("cycling")
    if "beach" in lowered:
        tags.extend(["walking", "swimming"])
    if any(marker in lowered for marker in ("market", "food truck")):
        tags.append("food")
    return tags


def _date_strengths(
    venue_type: str | None,
    setting_tags: list[str],
    activity_tags: list[str],
    text: str,
) -> list[str]:
    strengths: list[str] = []
    if "views" in setting_tags or "scenic" in text:
        strengths.append("scenic views")
    if "waterfront" in setting_tags or "coastal" in setting_tags:
        strengths.append("waterfront setting")
    if "walking" in activity_tags:
        strengths.append("walkable low-pressure date")
    if "food" in activity_tags:
        strengths.append("casual food exploration")
    if venue_type in {"beach", "hiking_trail", "scenic_lookout", "botanical_garden"}:
        strengths.append("low-cost outdoor option")
    return _dedupe(strengths)


def _date_risks(
    venue_type: str | None,
    setting_tags: list[str],
    activity_tags: list[str],
    text: str,
) -> list[str]:
    risks: list[str] = []
    if "outdoor" in setting_tags:
        risks.append("weather dependent")
    if venue_type in {"hiking_trail", "bike_trail"} or "steep" in text:
        risks.append("mobility or fitness constraints")
    if "crowd" in text or "popular" in text:
        risks.append("can be crowded")
    if venue_type in {"beach", "scenic_lookout", "hiking_trail"}:
        risks.append("weaker fit for late-night dates")
    return _dedupe(risks)


def _best_for(
    venue_type: str | None,
    setting_tags: list[str],
    activity_tags: list[str],
) -> list[str]:
    best: list[str] = []
    if "walking" in activity_tags:
        best.append("active couples")
    if "food" in activity_tags:
        best.append("casual foodie dates")
    if "views" in setting_tags or "coastal" in setting_tags:
        best.append("scenic daytime dates")
    if venue_type in {"farmers_market", "night_market", "food_truck"}:
        best.append("low-commitment casual dates")
    return _dedupe(best)


def _avoid_for(
    venue_type: str | None,
    setting_tags: list[str],
    activity_tags: list[str],
) -> list[str]:
    avoid: list[str] = []
    if "outdoor" in setting_tags:
        avoid.append("rainy weather")
    if venue_type in {"hiking_trail", "bike_trail"}:
        avoid.append("mobility-limited plans")
    if venue_type in {"beach", "hiking_trail", "scenic_lookout"}:
        avoid.append("formal dinner-only dates")
    return _dedupe(avoid)


def _weather_exposure(venue_type: str | None, setting_tags: list[str]) -> str | None:
    if venue_type in {"hiking_trail", "bike_trail", "beach"}:
        return "active_outdoor"
    if "outdoor" in setting_tags:
        return "outdoor"
    return None


def _build_feature_text(
    *,
    row: dict[str, Any],
    venue_type: str | None,
    setting_tags: tuple[str, ...],
    ambience_tags: tuple[str, ...],
    activity_tags: tuple[str, ...],
    date_strengths: tuple[str, ...],
    date_risks: tuple[str, ...],
    best_for: tuple[str, ...],
    avoid_for: tuple[str, ...],
    evidence: tuple[PlaceEvidence, ...],
) -> str:
    evidence_summary = " ".join(
        item.text for item in evidence if item.source != "fsq_baseline"
    )[:900]
    location = _join_nonempty([row.get("locality"), row.get("region")])
    lines = [f"{row.get('name')} is a {venue_type or 'place'}"]
    if location:
        lines[0] += f" in {location}"
    lines[0] += "."
    optional_lines = [
        ("Dataset categories", _category_text(row)),
        ("Setting tags", ", ".join(setting_tags)),
        ("Ambience tags", ", ".join(ambience_tags)),
        ("Activity tags", ", ".join(activity_tags)),
        ("Date strengths", ", ".join(date_strengths)),
        ("Date risks", ", ".join(date_risks)),
        ("Best for", ", ".join(best_for)),
        ("Avoid for", ", ".join(avoid_for)),
    ]
    lines.extend(f"{label}: {value}." for label, value in optional_lines if value)
    if evidence_summary:
        lines.append(f"Grounded evidence: {evidence_summary}")
    return _normalize_space(" ".join(lines))[:2400]


def _keyword_tags(text: str, mapping: dict[str, tuple[str, ...]]) -> list[str]:
    return [tag for tag, keywords in mapping.items() if any(keyword in text for keyword in keywords)]


def _web_category_hint(category_text: str) -> str:
    lowered = category_text.casefold()
    if "hiking trail" in lowered:
        return "walk track"
    if "scenic lookout" in lowered:
        return "lookout views"
    if "beach" in lowered:
        return "beach"
    if "market" in lowered:
        return "market"
    if "food truck" in lowered:
        return "food truck"
    return ""


def _category_hint_present(row: dict[str, Any], normalized_text: str) -> bool:
    hint = _web_category_hint(_category_text(row))
    return bool(hint and _token_text(hint) in normalized_text)


def _generative_summary_text(place: dict[str, Any]) -> str | None:
    summary = place.get("generativeSummary")
    if not isinstance(summary, dict):
        return None
    overview = summary.get("overview")
    if isinstance(overview, dict) and isinstance(overview.get("text"), str):
        return overview["text"]
    return None


def _maps_review_snippets(reviews: object) -> tuple[str, ...]:
    if not isinstance(reviews, list):
        return ()
    snippets: list[str] = []
    for review in reviews:
        if not isinstance(review, dict):
            continue
        text_payload = review.get("text")
        if isinstance(text_payload, dict) and isinstance(text_payload.get("text"), str):
            snippets.append(_normalize_space(text_payload["text"])[:700])
        elif isinstance(text_payload, str):
            snippets.append(_normalize_space(text_payload)[:700])
        if len(snippets) >= 3:
            break
    return tuple(snippet for snippet in snippets if snippet)


def _distance_meters(
    a_latitude: float,
    a_longitude: float,
    b_latitude: float,
    b_longitude: float,
) -> float:
    radius_meters = 6_371_000.0
    a_phi = math.radians(a_latitude)
    b_phi = math.radians(b_latitude)
    delta_phi = math.radians(b_latitude - a_latitude)
    delta_lambda = math.radians(b_longitude - a_longitude)
    haversine = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(a_phi) * math.cos(b_phi) * math.sin(delta_lambda / 2) ** 2
    )
    return 2 * radius_meters * math.asin(math.sqrt(haversine))


def _name_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, _token_text(a), _token_text(b)).ratio()


def _token_overlap(a: str, b: str) -> float:
    a_tokens = {token for token in a.split() if len(token) > 1}
    b_tokens = {token for token in b.split() if len(token) > 1}
    if not a_tokens:
        return 0.0
    return len(a_tokens.intersection(b_tokens)) / len(a_tokens)


def _token_text(text: str) -> str:
    return TAG_TOKEN_RE.sub(" ", unescape(text.casefold())).strip()


def _join_nonempty(values: list[object]) -> str:
    return ", ".join(str(value) for value in values if _present(value))


def _join_distinct_query_parts(values: tuple[object, ...]) -> str:
    parts: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not _present(value):
            continue
        text = str(value).strip()
        key = _token_text(text).strip('" ')
        if not key or key in seen:
            continue
        seen.add(key)
        parts.append(text)
    return _normalize_space(" ".join(parts))


def _present(value: object) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    return bool(text and text.casefold() not in {"nan", "none"})


def _dedupe(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = value.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _optional_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _tuple_strings(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item.strip() for item in value if isinstance(item, str) and item.strip())
