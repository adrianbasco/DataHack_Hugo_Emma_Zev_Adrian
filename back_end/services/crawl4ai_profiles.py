"""Crawl4AI-backed website enrichment for curated date-place datasets."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

import pandas as pd
from crawl4ai import AsyncWebCrawler, CacheMode
from crawl4ai.async_dispatcher import RateLimiter, SemaphoreDispatcher
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig
from crawl4ai.content_filter_strategy import PruningContentFilter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

from back_end.services.website_profiles import (
    ACTIVITY_KEYWORDS,
    AMBIENCE_KEYWORDS,
    BOOKING_HOST_HINTS,
    CUISINE_KEYWORDS,
    DRINK_KEYWORDS,
    MAX_EVIDENCE_SNIPPETS,
    MAX_RICH_TEXT_LENGTH,
    SETTING_KEYWORDS,
    STRUCTURED_PAGE_HINTS,
    WebsiteContentError,
    _discover_same_domain_urls,
    _infer_template_stop_tags,
    _join_nonempty,
    _join_sequence,
    _keyword_tags,
    _normalize_space,
    _normalize_website_url,
    _profile_quality_score,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Crawl4AIProfileSettings:
    """Settings for Crawl4AI-backed website enrichment."""

    max_pages_per_site: int = 5
    semaphore_count: int = 12
    max_session_permit: int = 24
    pruning_threshold: float = 0.4
    homepage_timeout_ms: int = 12000
    detail_timeout_ms: int = 9000
    rate_limit_base_delay_min_seconds: float = 0.0
    rate_limit_base_delay_max_seconds: float = 0.15
    rate_limit_max_delay_seconds: float = 5.0
    rate_limit_max_retries: int = 1
    retry_failed_with_full_browser: bool = True
    retry_timeout_ms: int = 18000


@dataclass(frozen=True)
class _Crawl4AIPage:
    url: str
    text: str
    links: list[tuple[str, str]]


@dataclass(frozen=True)
class _Crawl4AIRequestResult:
    requested_url: str
    page: _Crawl4AIPage | None
    error_type: str | None = None
    error_message: str | None = None


class Crawl4AIWebsiteProfileClient:
    """Fetch and profile place websites with Crawl4AI."""

    def __init__(self, settings: Crawl4AIProfileSettings | None = None) -> None:
        self._settings = settings or Crawl4AIProfileSettings()
        self._fast_browser_config = BrowserConfig(
            headless=True,
            verbose=False,
            text_mode=True,
            light_mode=True,
            memory_saving_mode=True,
        )
        self._retry_browser_config = BrowserConfig(
            headless=True,
            verbose=False,
            text_mode=False,
            light_mode=False,
            memory_saving_mode=False,
        )
        self._homepage_run_config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            wait_until="domcontentloaded",
            page_timeout=self._settings.homepage_timeout_ms,
            excluded_tags=["nav", "footer", "script", "style", "form"],
            exclude_external_links=True,
            remove_overlay_elements=True,
            remove_consent_popups=True,
            stream=False,
            verbose=False,
            markdown_generator=DefaultMarkdownGenerator(
                content_filter=PruningContentFilter(
                    threshold=self._settings.pruning_threshold,
                    threshold_type="fixed",
                )
            ),
        )
        self._detail_run_config = self._homepage_run_config.clone(
            page_timeout=self._settings.detail_timeout_ms,
        )
        self._retry_run_config = self._homepage_run_config.clone(
            wait_until="load",
            page_timeout=self._settings.retry_timeout_ms,
            delay_before_return_html=0.35,
        )

    async def enrich_dataframe(self, places: pd.DataFrame) -> pd.DataFrame:
        if "website" not in places.columns:
            raise ValueError("places DataFrame must contain a website column.")

        normalized_places = places.copy()
        normalized_places["crawl4ai_requested_url"] = [
            _safe_normalize_website_url(website)
            for website in normalized_places["website"]
        ]
        async with AsyncWebCrawler(config=self._fast_browser_config) as crawler:
            requested_urls = [
                url
                for url in normalized_places["crawl4ai_requested_url"].dropna().drop_duplicates().tolist()
            ]
            homepage_results = await self._crawl_batch(
                crawler,
                requested_urls,
                self._homepage_run_config,
            )
            homepage_results = await self._retry_failed_batch(
                homepage_results,
                run_config=self._retry_run_config,
            )
            extra_urls_by_site: dict[str, list[str]] = {}
            unique_extra_urls: list[str] = []
            seen_extra_urls: set[str] = set()
            for requested_url, crawl_result in homepage_results.items():
                if crawl_result.page is None:
                    continue
                extra_urls = _discover_same_domain_urls(
                    crawl_result.page.url,
                    crawl_result.page.links,
                    max_urls=self._settings.max_pages_per_site - 1,
                )
                extra_urls_by_site[requested_url] = extra_urls
                for extra_url in extra_urls:
                    if extra_url in seen_extra_urls:
                        continue
                    seen_extra_urls.add(extra_url)
                    unique_extra_urls.append(extra_url)

            extra_results = await self._crawl_batch(
                crawler,
                unique_extra_urls,
                self._detail_run_config,
            )
            extra_results = await self._retry_failed_batch(
                extra_results,
                run_config=self._retry_run_config,
            )

        results = []
        for _, row in normalized_places.iterrows():
            requested_url = row["crawl4ai_requested_url"]
            place_id = str(row["fsq_place_id"])
            if requested_url is None:
                logger.error(
                    "Place %s is missing or has an invalid website URL during Crawl4AI enrichment.",
                    place_id,
                )
                results.append(
                    _error_result(
                        place_id,
                        "missing_website",
                        "No valid website URL was provided.",
                    )
                )
                continue

            homepage_result = homepage_results.get(requested_url)
            if homepage_result is None or homepage_result.page is None:
                error_type = (
                    homepage_result.error_type if homepage_result is not None else "WebsiteContentError"
                )
                error_message = (
                    homepage_result.error_message
                    if homepage_result is not None
                    else f"No homepage crawl result was recorded for {requested_url}."
                )
                logger.error(
                    "Crawl4AI website enrichment failed for %s (%s): %s",
                    place_id,
                    requested_url,
                    error_message,
                )
                results.append(_error_result(place_id, error_type, error_message))
                continue

            pages = [homepage_result.page]
            for extra_url in extra_urls_by_site.get(requested_url, []):
                extra_result = extra_results.get(extra_url)
                if extra_result is None or extra_result.page is None:
                    continue
                pages.append(extra_result.page)

            try:
                profile = _build_crawl4ai_profile(row.to_dict(), pages)
                profile["fsq_place_id"] = place_id
                results.append(profile)
            except Exception as exc:
                logger.error(
                    "Crawl4AI profile build failed for %s (%s): %s",
                    place_id,
                    requested_url,
                    exc,
                )
                results.append(_error_result(place_id, exc.__class__.__name__, str(exc)))

        enrichment = pd.DataFrame(results)
        return places.merge(enrichment, on="fsq_place_id", how="left", validate="one_to_one")

    async def _crawl_batch(
        self,
        crawler: AsyncWebCrawler,
        urls: list[str],
        config: CrawlerRunConfig,
    ) -> dict[str, _Crawl4AIRequestResult]:
        if not urls:
            return {}

        results = await crawler.arun_many(
            urls=urls,
            config=config,
            dispatcher=self._make_dispatcher(),
        )
        by_requested_url: dict[str, _Crawl4AIRequestResult] = {}
        for requested_url, result in zip(urls, results):
            if not result.success:
                by_requested_url[requested_url] = _Crawl4AIRequestResult(
                    requested_url=requested_url,
                    page=None,
                    error_type="WebsiteContentError",
                    error_message=(
                        f"Crawl4AI failed for {requested_url}: "
                        f"{getattr(result, 'error_message', '')}"
                    ),
                )
                continue

            text = _crawl_result_text(result)
            if not text:
                by_requested_url[requested_url] = _Crawl4AIRequestResult(
                    requested_url=requested_url,
                    page=None,
                    error_type="WebsiteContentError",
                    error_message=(
                        f"Crawl4AI produced no usable markdown for {requested_url}."
                    ),
                )
                continue

            by_requested_url[requested_url] = _Crawl4AIRequestResult(
                requested_url=requested_url,
                page=_Crawl4AIPage(
                    url=str(getattr(result, "url", requested_url)),
                    text=text,
                    links=_crawl_result_internal_links(result),
                ),
            )
        return by_requested_url

    async def _retry_failed_batch(
        self,
        results_by_url: dict[str, _Crawl4AIRequestResult],
        *,
        run_config: CrawlerRunConfig,
    ) -> dict[str, _Crawl4AIRequestResult]:
        if not self._settings.retry_failed_with_full_browser:
            return results_by_url

        failed_urls = [
            requested_url
            for requested_url, result in results_by_url.items()
            if result.page is None
        ]
        if not failed_urls:
            return results_by_url

        async with AsyncWebCrawler(config=self._retry_browser_config) as retry_crawler:
            retried_results = await self._crawl_batch(
                retry_crawler,
                failed_urls,
                run_config,
            )

        merged = dict(results_by_url)
        for requested_url, retried_result in retried_results.items():
            if retried_result.page is not None:
                merged[requested_url] = retried_result
        return merged

    def _make_dispatcher(self) -> SemaphoreDispatcher:
        return SemaphoreDispatcher(
            semaphore_count=self._settings.semaphore_count,
            max_session_permit=self._settings.max_session_permit,
            rate_limiter=RateLimiter(
                base_delay=(
                    self._settings.rate_limit_base_delay_min_seconds,
                    self._settings.rate_limit_base_delay_max_seconds,
                ),
                max_delay=self._settings.rate_limit_max_delay_seconds,
                max_retries=self._settings.rate_limit_max_retries,
                rate_limit_codes=[429, 503],
            ),
        )


def _crawl_result_text(result: object) -> str:
    markdown = getattr(result, "markdown", None)
    fit_markdown = getattr(markdown, "fit_markdown", None)
    raw_markdown = getattr(markdown, "raw_markdown", None)
    metadata = getattr(result, "metadata", None) or {}

    parts: list[str] = []
    if isinstance(metadata, dict):
        title = _normalize_space(str(metadata.get("title") or ""))
        description = _normalize_space(str(metadata.get("description") or ""))
        if title:
            parts.append(title)
        if description:
            parts.append(description)
    body = fit_markdown or raw_markdown or (str(markdown) if markdown is not None else "")
    if body:
        parts.append(str(body))
    return "\n".join(part for part in parts if part).strip()


def _safe_normalize_website_url(url: object) -> str | None:
    try:
        return _normalize_website_url(url)
    except Exception:
        return None


def _crawl_result_internal_links(result: object) -> list[tuple[str, str]]:
    links = getattr(result, "links", None)
    if not isinstance(links, dict):
        return []
    internal = links.get("internal")
    if not isinstance(internal, list):
        return []
    normalized: list[tuple[str, str]] = []
    for link in internal:
        if not isinstance(link, dict):
            continue
        href = link.get("href")
        text = link.get("text")
        if not isinstance(href, str) or not href.strip():
            continue
        normalized.append((href, str(text or "")))
    return normalized


def _build_crawl4ai_profile(
    row: dict[str, Any],
    pages: list[_Crawl4AIPage],
) -> dict[str, Any]:
    if not pages:
        raise WebsiteContentError("No Crawl4AI pages were fetched for profile building.")

    combined_text = "\n".join(page.text for page in pages)
    combined_lower = combined_text.casefold()
    cuisines = _keyword_tags(combined_lower, CUISINE_KEYWORDS)
    ambience_tags = _keyword_tags(combined_lower, AMBIENCE_KEYWORDS)
    setting_tags = _keyword_tags(combined_lower, SETTING_KEYWORDS)
    activity_tags = _keyword_tags(combined_lower, ACTIVITY_KEYWORDS)
    drink_tags = _keyword_tags(combined_lower, DRINK_KEYWORDS)
    template_stop_tags = _infer_template_stop_tags(
        text=combined_lower,
        category_labels=row.get("fsq_category_labels"),
    )
    discovered_page_types = _crawl4ai_page_types(pages)
    booking_signals = sorted(
        set(discovered_page_types)
        | _crawl4ai_booking_signals(pages)
    )
    evidence_snippets = _crawl4ai_evidence_snippets(
        pages,
        cuisines=cuisines,
        ambience_tags=ambience_tags,
        setting_tags=setting_tags,
        activity_tags=activity_tags,
    )
    rich_profile_text = _build_crawl4ai_rich_profile_text(
        row=row,
        pages=pages,
        cuisines=cuisines,
        ambience_tags=ambience_tags,
        setting_tags=setting_tags,
        activity_tags=activity_tags,
        drink_tags=drink_tags,
        template_stop_tags=template_stop_tags,
        booking_signals=booking_signals,
        evidence_snippets=evidence_snippets,
    )
    quality_score = _profile_quality_score(
        page_count=len(pages),
        rich_profile_text=rich_profile_text,
        feature_groups=(
            cuisines,
            ambience_tags,
            setting_tags,
            activity_tags,
            drink_tags,
            template_stop_tags,
            booking_signals,
        ),
        evidence_snippets=evidence_snippets,
    )

    return {
        "crawl4ai_enrichment_status": "ok",
        "crawl4ai_enrichment_error": None,
        "crawl4ai_canonical_url": pages[0].url,
        "crawl4ai_page_count": len(pages),
        "crawl4ai_discovered_page_types": discovered_page_types,
        "crawl4ai_cuisines": cuisines,
        "crawl4ai_ambience_tags": ambience_tags,
        "crawl4ai_setting_tags": setting_tags,
        "crawl4ai_activity_tags": activity_tags,
        "crawl4ai_drink_tags": drink_tags,
        "crawl4ai_template_stop_tags": template_stop_tags,
        "crawl4ai_booking_signals": booking_signals,
        "crawl4ai_evidence_snippets": evidence_snippets,
        "crawl4ai_quality_score": quality_score,
        "crawl4ai_rich_profile_text": rich_profile_text,
    }


def _crawl4ai_page_types(pages: list[_Crawl4AIPage]) -> list[str]:
    page_types: set[str] = set()
    for page in pages[1:]:
        lowered = page.url.casefold()
        for hint in STRUCTURED_PAGE_HINTS:
            if hint in lowered:
                page_types.add(hint)
    return sorted(page_types)


def _crawl4ai_booking_signals(pages: list[_Crawl4AIPage]) -> set[str]:
    signals: set[str] = set()
    for page in pages:
        lowered = page.url.casefold()
        if any(host_hint in lowered for host_hint in BOOKING_HOST_HINTS):
            signals.add("third_party_booking")
        for href, _ in page.links:
            lowered_href = href.casefold()
            if any(host_hint in lowered_href for host_hint in BOOKING_HOST_HINTS):
                signals.add("third_party_booking")
    return signals


def _crawl4ai_evidence_snippets(
    pages: list[_Crawl4AIPage],
    *,
    cuisines: list[str],
    ambience_tags: list[str],
    setting_tags: list[str],
    activity_tags: list[str],
) -> list[str]:
    terms = set(cuisines) | set(ambience_tags) | set(setting_tags) | set(activity_tags)
    if not terms:
        terms = {"menu", "reservation", "book", "event"}

    snippets: list[str] = []
    for page in pages:
        for line in page.text.splitlines():
            normalized = _normalize_space(line)
            if len(normalized) < 20:
                continue
            lowered = normalized.casefold()
            if any(term.replace("_", " ") in lowered for term in terms):
                snippets.append(normalized[:220])
                if len(snippets) >= MAX_EVIDENCE_SNIPPETS:
                    return snippets
    return snippets[:MAX_EVIDENCE_SNIPPETS]


def _build_crawl4ai_rich_profile_text(
    *,
    row: dict[str, Any],
    pages: list[_Crawl4AIPage],
    cuisines: list[str],
    ambience_tags: list[str],
    setting_tags: list[str],
    activity_tags: list[str],
    drink_tags: list[str],
    template_stop_tags: list[str],
    booking_signals: list[str],
    evidence_snippets: list[str],
) -> str:
    lines = [
        f"Place: {row.get('name')}",
        f"Location: {_join_nonempty([row.get('locality'), row.get('region'), row.get('postcode')])}",
        f"Dataset categories: {_join_sequence(row.get('fsq_category_labels'))}",
        f"Primary website: {pages[0].url}",
        f"Structured page types found: {_join_sequence(_crawl4ai_page_types(pages))}",
        f"Cuisines: {_join_sequence(cuisines)}",
        f"Ambience tags: {_join_sequence(ambience_tags)}",
        f"Setting tags: {_join_sequence(setting_tags)}",
        f"Activity tags: {_join_sequence(activity_tags)}",
        f"Drink tags: {_join_sequence(drink_tags)}",
        f"Template stop tags: {_join_sequence(template_stop_tags)}",
        f"Booking signals: {_join_sequence(booking_signals)}",
    ]
    if evidence_snippets:
        lines.append("Evidence snippets: " + " | ".join(evidence_snippets))
    text = "\n".join(line for line in lines if not line.endswith(": "))
    return text[:MAX_RICH_TEXT_LENGTH]


def _error_result(place_id: str, status: str, error: str) -> dict[str, Any]:
    return {
        "fsq_place_id": place_id,
        "crawl4ai_enrichment_status": status,
        "crawl4ai_enrichment_error": error,
        "crawl4ai_canonical_url": None,
        "crawl4ai_page_count": 0,
        "crawl4ai_discovered_page_types": [],
        "crawl4ai_cuisines": [],
        "crawl4ai_ambience_tags": [],
        "crawl4ai_setting_tags": [],
        "crawl4ai_activity_tags": [],
        "crawl4ai_drink_tags": [],
        "crawl4ai_template_stop_tags": [],
        "crawl4ai_booking_signals": [],
        "crawl4ai_evidence_snippets": [],
        "crawl4ai_quality_score": 0,
        "crawl4ai_rich_profile_text": None,
    }
