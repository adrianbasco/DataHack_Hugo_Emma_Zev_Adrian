"""Crawl4AI-backed website enrichment for curated date-place datasets."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
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

NON_RETRYABLE_CRAWL4AI_ERROR_TYPES = frozenset(
    {
        "dns_not_resolved",
        "anti_bot_blocked",
        "crawl_timeout",
        "crawl_watchdog_timeout",
        "unsupported_content",
    }
)


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
    crawl_watchdog_extra_seconds: float = 3.0
    crawl_watchdog_max_seconds: float | None = 15.0
    batch_progress_interval_seconds: float = 30.0


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
                batch_label="homepage",
            )
            homepage_results = await self._retry_failed_batch(
                homepage_results,
                run_config=self._retry_run_config,
                batch_label="homepage-retry",
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
                batch_label="detail",
            )
            extra_results = await self._retry_failed_batch(
                extra_results,
                run_config=self._retry_run_config,
                batch_label="detail-retry",
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
                logger.debug(
                    "Crawl4AI website enrichment failed for %s (%s): [%s] %s",
                    place_id,
                    requested_url,
                    error_type,
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
        *,
        batch_label: str,
    ) -> dict[str, _Crawl4AIRequestResult]:
        if not urls:
            return {}

        dispatcher = self._make_dispatcher()
        semaphore = asyncio.Semaphore(dispatcher.semaphore_count)
        started_at = time.perf_counter()
        logger.info(
            "Starting Crawl4AI %s batch for %d URLs with concurrency=%d, page_timeout_ms=%s, watchdog_seconds=%.1f.",
            batch_label,
            len(urls),
            dispatcher.semaphore_count,
            getattr(config, "page_timeout", None),
            self._crawl_watchdog_seconds(config),
        )
        tasks = {
            asyncio.create_task(
                self._crawl_one_url(
                    crawler,
                    requested_url=url,
                    config=config,
                    semaphore=semaphore,
                    rate_limiter=dispatcher.rate_limiter,
                    batch_label=batch_label,
                )
            )
            for url in urls
        }
        by_requested_url: dict[str, _Crawl4AIRequestResult] = {}
        last_progress_log_at = started_at
        while tasks:
            done, tasks = await asyncio.wait(
                tasks,
                timeout=self._settings.batch_progress_interval_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                logger.warning(
                    "Crawl4AI %s batch has not completed a URL for %.1fs (%d/%d done, %d pending).",
                    batch_label,
                    time.perf_counter() - last_progress_log_at,
                    len(by_requested_url),
                    len(urls),
                    len(tasks),
                )
                last_progress_log_at = time.perf_counter()
                continue

            for task in done:
                requested_url, request_result = task.result()
                by_requested_url[requested_url] = request_result

            now = time.perf_counter()
            if (
                not tasks
                or len(by_requested_url) % max(dispatcher.semaphore_count, 1) == 0
                or now - last_progress_log_at >= self._settings.batch_progress_interval_seconds
            ):
                logger.info(
                    "Crawl4AI %s batch progress: %d/%d URLs done in %.1fs. Statuses: %s",
                    batch_label,
                    len(by_requested_url),
                    len(urls),
                    now - started_at,
                    _crawl_request_status_summary(by_requested_url.values()),
                )
                last_progress_log_at = now
        logger.info(
            "Completed Crawl4AI %s batch for %d URLs in %.1fs. Statuses: %s",
            batch_label,
            len(urls),
            time.perf_counter() - started_at,
            _crawl_request_status_summary(by_requested_url.values()),
        )
        return by_requested_url

    async def _crawl_one_url(
        self,
        crawler: AsyncWebCrawler,
        *,
        requested_url: str,
        config: CrawlerRunConfig,
        semaphore: asyncio.Semaphore,
        rate_limiter: RateLimiter | None,
        batch_label: str,
    ) -> tuple[str, _Crawl4AIRequestResult]:
        try:
            if rate_limiter is not None:
                await rate_limiter.wait_if_needed(requested_url)
            async with semaphore:
                crawl_task = asyncio.create_task(
                    crawler.arun(
                        requested_url,
                        config=config,
                        session_id=f"crawl4ai-{uuid.uuid4()}",
                    )
                )
                done, _ = await asyncio.wait(
                    {crawl_task},
                    timeout=self._crawl_watchdog_seconds(config),
                )
                if not done:
                    crawl_task.cancel()
                    crawl_task.add_done_callback(
                        lambda abandoned: _consume_abandoned_crawl_task(
                            abandoned,
                            requested_url=requested_url,
                        )
                    )
                    error_message = (
                        f"Crawl4AI {batch_label} crawl exceeded watchdog of "
                        f"{self._crawl_watchdog_seconds(config):.1f}s for {requested_url}."
                    )
                    logger.error(error_message)
                    return requested_url, _Crawl4AIRequestResult(
                        requested_url=requested_url,
                        page=None,
                        error_type="crawl_watchdog_timeout",
                        error_message=error_message,
                    )
                result = crawl_task.result()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception(
                "Crawl4AI %s crawl raised unexpectedly for %s.",
                batch_label,
                requested_url,
            )
            return requested_url, _Crawl4AIRequestResult(
                requested_url=requested_url,
                page=None,
                error_type=exc.__class__.__name__,
                error_message=str(exc),
            )

        return requested_url, _crawl_request_result_from_crawl_result(
            requested_url,
            result,
        )

    async def _retry_failed_batch(
        self,
        results_by_url: dict[str, _Crawl4AIRequestResult],
        *,
        run_config: CrawlerRunConfig,
        batch_label: str,
    ) -> dict[str, _Crawl4AIRequestResult]:
        if not self._settings.retry_failed_with_full_browser:
            return results_by_url

        failed_urls = [
            requested_url
            for requested_url, result in results_by_url.items()
            if result.page is None
            and result.error_type not in NON_RETRYABLE_CRAWL4AI_ERROR_TYPES
        ]
        if not failed_urls:
            return results_by_url

        async with AsyncWebCrawler(config=self._retry_browser_config) as retry_crawler:
            retried_results = await self._crawl_batch(
                retry_crawler,
                failed_urls,
                run_config,
                batch_label=batch_label,
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

    def _crawl_watchdog_seconds(self, config: CrawlerRunConfig) -> float:
        page_timeout_ms = getattr(config, "page_timeout", None) or 0
        page_timeout_seconds = max(float(page_timeout_ms) / 1000.0, 0.0)
        watchdog_seconds = max(
            1.0,
            page_timeout_seconds + self._settings.crawl_watchdog_extra_seconds,
        )
        if self._settings.crawl_watchdog_max_seconds is not None:
            watchdog_seconds = min(
                watchdog_seconds,
                self._settings.crawl_watchdog_max_seconds,
            )
        return max(1.0, watchdog_seconds)


def _crawl_request_result_from_crawl_result(
    requested_url: str,
    result: object,
) -> _Crawl4AIRequestResult:
    if not getattr(result, "success", False):
        error_type, error_message = _classify_crawl4ai_failure(
            requested_url,
            result,
        )
        return _Crawl4AIRequestResult(
            requested_url=requested_url,
            page=None,
            error_type=error_type,
            error_message=error_message,
        )

    text = _crawl_result_text(result)
    if not text:
        return _Crawl4AIRequestResult(
            requested_url=requested_url,
            page=None,
            error_type="WebsiteContentError",
            error_message=f"Crawl4AI produced no usable markdown for {requested_url}.",
        )

    return _Crawl4AIRequestResult(
        requested_url=requested_url,
        page=_Crawl4AIPage(
            url=str(getattr(result, "url", requested_url)),
            text=text,
            links=_crawl_result_internal_links(result),
        ),
    )


def _crawl_request_status_summary(results: object) -> str:
    counts: dict[str, int] = {}
    for result in results:
        status = "ok" if result.page is not None else str(result.error_type or "unknown_error")
        counts[status] = counts.get(status, 0) + 1
    if not counts:
        return "none"
    return ", ".join(
        f"{status}={count}"
        for status, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    )


def _consume_abandoned_crawl_task(
    task: asyncio.Task[object],
    *,
    requested_url: str,
) -> None:
    if task.cancelled():
        return
    try:
        task.result()
    except Exception:
        logger.exception(
            "Abandoned Crawl4AI task for %s raised after watchdog cancellation.",
            requested_url,
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


def _classify_crawl4ai_failure(
    requested_url: str,
    result: object,
) -> tuple[str, str]:
    raw_error = str(getattr(result, "error_message", "") or "").strip()
    status_code = getattr(result, "status_code", None)
    lowered = raw_error.casefold()
    concise_error = _concise_crawl4ai_error(raw_error)

    if "err_name_not_resolved" in lowered:
        return "dns_not_resolved", f"DNS did not resolve for {requested_url}."
    if "blocked by anti-bot protection" in lowered:
        return "anti_bot_blocked", concise_error
    if "unsupported content-type" in lowered:
        return "unsupported_content", concise_error
    if status_code:
        return "http_error", f"HTTP {status_code} while crawling {requested_url}."
    if "timeout" in lowered:
        return "crawl_timeout", concise_error
    if "target page, context or browser has been closed" in lowered:
        return "browser_target_closed", concise_error
    if "failed on navigating acs-goto" in lowered or "page.goto" in lowered:
        return "navigation_failed", concise_error
    return "WebsiteContentError", concise_error or f"Crawl4AI failed for {requested_url}."


def _concise_crawl4ai_error(raw_error: str) -> str:
    if not raw_error:
        return "Crawl4AI failed without an error message."
    prefixes = (
        "\nCode context:",
        "\nCall log:",
    )
    concise = raw_error
    for prefix in prefixes:
        if prefix in concise:
            concise = concise.split(prefix, 1)[0]
    lines = [
        _normalize_space(line)
        for line in concise.splitlines()
        if _normalize_space(line)
    ]
    if not lines:
        return "Crawl4AI failed without a concise error message."
    if len(lines) >= 2 and lines[0].startswith("Unexpected error"):
        return lines[1][:300]
    return " ".join(lines[:2])[:300]


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
