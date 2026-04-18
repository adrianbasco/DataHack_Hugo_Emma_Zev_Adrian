"""Async website enrichment for curated date-place datasets."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})
DISCOVERY_LINK_HINTS: tuple[str, ...] = (
    "menu",
    "food",
    "drink",
    "wine",
    "cocktail",
    "book",
    "reserv",
    "event",
    "function",
    "about",
    "gallery",
)
STRUCTURED_PAGE_HINTS: tuple[str, ...] = (
    "menu",
    "booking",
    "reservation",
    "event",
    "gallery",
    "about",
)
MAX_EVIDENCE_SNIPPETS = 6
MAX_RICH_TEXT_LENGTH = 2400
STOP_TEXT_FRAGMENTS = (
    "privacy policy",
    "cookie policy",
    "terms and conditions",
    "all rights reserved",
    "subscribe to our newsletter",
)

CUISINE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "italian": ("italian", "pasta", "trattoria", "osteria"),
    "japanese": ("japanese", "sushi", "izakaya", "omakase", "yakitori", "ramen"),
    "thai": ("thai",),
    "chinese": ("chinese", "dumpling", "szechuan", "cantonese"),
    "korean": ("korean", "kimchi", "bbq"),
    "vietnamese": ("vietnamese", "pho", "banh mi"),
    "indian": ("indian", "tandoor", "curry"),
    "mexican": ("mexican", "taco", "mezcal"),
    "middle_eastern": ("middle eastern", "lebanese", "falafel", "mezze"),
    "greek": ("greek", "souvlaki"),
    "french": ("french", "bistro", "brasserie"),
    "spanish": ("spanish", "tapas", "paella"),
    "seafood": ("seafood", "oyster", "fish market"),
    "steakhouse": ("steak", "steakhouse"),
    "bakery": ("bakery", "pastry", "croissant", "patisserie"),
    "dessert": ("dessert", "gelato", "ice cream", "patisserie", "cake"),
    "coffee": ("coffee", "cafe", "espresso", "roastery", "brunch"),
}
AMBIENCE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "cozy": ("cozy", "cosy", "warm", "intimate"),
    "romantic": ("romantic", "date night", "candlelit", "moody"),
    "casual": ("casual", "relaxed", "laid-back"),
    "lively": ("lively", "buzzing", "vibrant"),
    "quiet": ("quiet", "calm", "peaceful"),
    "elegant": ("elegant", "sophisticated", "refined"),
}
SETTING_KEYWORDS: dict[str, tuple[str, ...]] = {
    "waterfront": ("waterfront", "harbour", "harbor", "beachfront", "marina"),
    "rooftop": ("rooftop", "skyline"),
    "garden": ("garden", "courtyard", "botanical"),
    "outdoor": ("outdoor seating", "alfresco", "terrace", "patio"),
    "view": ("views", "scenic", "sunset"),
}
ACTIVITY_KEYWORDS: dict[str, tuple[str, ...]] = {
    "live_music": ("live music", "dj", "band"),
    "theatre": ("theatre", "theater", "performance"),
    "gallery": ("gallery", "exhibition"),
    "museum": ("museum",),
    "arcade": ("arcade",),
    "escape_room": ("escape room",),
    "mini_golf": ("mini golf", "putt putt"),
}
AMENITY_KEYWORDS: dict[str, tuple[str, ...]] = {
    "reservations": ("reservation", "book a table", "reserve", "book now"),
    "private_dining": ("private dining", "functions", "events"),
    "outdoor_seating": ("outdoor seating", "alfresco", "terrace", "courtyard"),
    "dog_friendly": ("dog friendly", "pet friendly"),
    "wheelchair_accessible": ("wheelchair", "accessible access", "accessibility"),
    "group_friendly": ("group dining", "large groups", "functions"),
}
DRINK_KEYWORDS: dict[str, tuple[str, ...]] = {
    "wine": ("wine", "wine list", "wine bar", "cellar"),
    "cocktails": ("cocktail", "martini", "spritz"),
    "beer": ("beer", "brewery", "tap list"),
    "coffee": ("coffee", "espresso", "roastery"),
}
BOOKING_HOST_HINTS = (
    "opentable",
    "resdiary",
    "sevenrooms",
    "quandoo",
    "meandu",
    "thefork",
)
TEMPLATE_STOP_TYPE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "aquarium": ("aquarium",),
    "arcade": ("arcade",),
    "art_gallery": ("art gallery", "gallery", "exhibition"),
    "bakery": ("bakery", "pastry", "croissant", "patisserie"),
    "bakery_or_market": ("bakery", "market", "farmers market"),
    "bar": ("bar", "pub", "drinks"),
    "beach": ("beach", "ocean pool", "surf club"),
    "boardwalk_or_lookout": ("boardwalk", "lookout", "foreshore", "coastal walk"),
    "bookstore": ("bookstore", "books", "book shop"),
    "botanical_garden": ("botanical garden", "garden"),
    "bowling_alley": ("bowling", "bowling alley"),
    "brewery_or_bar": ("brewery", "taproom", "bar", "pub"),
    "brunch_restaurant": ("brunch", "all day breakfast"),
    "cafe": ("cafe", "coffee", "espresso", "brunch"),
    "casual_restaurant": ("restaurant", "bistro", "casual dining"),
    "cocktail_bar": ("cocktail", "martini", "spritz"),
    "comedy_club": ("comedy", "stand-up"),
    "dance_hall_or_club": ("dance", "club night", "nightclub"),
    "dessert_shop": ("dessert", "gelato", "ice cream", "cake"),
    "escape_room": ("escape room",),
    "ferry_ride": ("ferry", "wharf"),
    "harbor_or_pier": ("harbour", "harbor", "pier", "wharf", "marina"),
    "live_music_venue": ("live music", "band", "dj"),
    "mini_golf": ("mini golf", "putt putt"),
    "movie_theater": ("cinema", "movie", "theater", "theatre"),
    "museum": ("museum",),
    "park_or_garden": ("park", "garden", "botanical"),
    "performing_arts_venue": ("theatre", "theater", "performance", "playhouse", "opera"),
    "restaurant": ("restaurant", "dining", "eatery"),
    "rooftop_bar": ("rooftop", "sky bar"),
    "scenic_lookout": ("lookout", "scenic", "sunset views", "observation"),
    "seafood_restaurant": ("seafood", "oyster", "fish", "prawn"),
    "wine_bar": ("wine bar", "wine list", "cellar"),
}


class WebsiteProfileError(RuntimeError):
    """Base class for website enrichment failures."""


class WebsiteFetchError(WebsiteProfileError):
    """Raised when a website could not be fetched successfully."""


class WebsiteContentError(WebsiteProfileError):
    """Raised when a website returns unsupported or malformed content."""


@dataclass(frozen=True)
class WebsiteProfileSettings:
    """Settings for website enrichment."""

    timeout_seconds: float = 10.0
    retry_count: int = 1
    max_pages_per_site: int = 5
    max_response_bytes: int = 750_000
    per_domain_concurrency: int = 2
    global_concurrency: int = 8


class _HtmlDocumentParser(HTMLParser):
    """Minimal HTML parser that extracts text, metadata, links, and JSON-LD."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title_parts: list[str] = []
        self.meta_description: str | None = None
        self.og_title: str | None = None
        self.og_description: str | None = None
        self.links: list[tuple[str, str]] = []
        self.jsonld_blocks: list[str] = []
        self.heading_texts: list[str] = []
        self.body_texts: list[str] = []

        self._tag_stack: list[str] = []
        self._buffer: list[str] = []
        self._current_link_href: str | None = None
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = {name.lower(): value for name, value in attrs}
        self._tag_stack.append(tag)

        if tag in {"script", "style", "noscript"}:
            if tag == "script" and attrs_dict.get("type", "").lower() == "application/ld+json":
                self._buffer = []
            else:
                self._skip_depth += 1

        if tag == "meta":
            self._handle_meta(attrs_dict)
        elif tag == "a":
            self._current_link_href = attrs_dict.get("href")
            self._buffer = []
        elif tag in {"title", "h1", "h2", "h3", "p", "li"}:
            self._buffer = []

    def handle_endtag(self, tag: str) -> None:
        if tag in {"style", "noscript"} and self._skip_depth > 0:
            self._skip_depth -= 1

        text = _normalize_space(" ".join(self._buffer))

        if tag == "script":
            if self._tag_stack and self._tag_stack[-1] == "script":
                if text:
                    self.jsonld_blocks.append(text)
            elif self._skip_depth > 0:
                self._skip_depth -= 1
        elif tag == "title" and text:
            self.title_parts.append(text)
        elif tag in {"h1", "h2", "h3"} and text:
            self.heading_texts.append(text)
        elif tag in {"p", "li"} and text:
            self.body_texts.append(text)
        elif tag == "a":
            if self._current_link_href:
                self.links.append((self._current_link_href, text))
            self._current_link_href = None

        if tag in {"script", "title", "h1", "h2", "h3", "p", "li", "a"}:
            self._buffer = []

        if self._tag_stack:
            self._tag_stack.pop()

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        if not self._tag_stack:
            return
        current_tag = self._tag_stack[-1]
        if current_tag in {"title", "script", "h1", "h2", "h3", "p", "li", "a"}:
            self._buffer.append(data)

    def _handle_meta(self, attrs: dict[str, str | None]) -> None:
        name = (attrs.get("name") or "").strip().lower()
        prop = (attrs.get("property") or "").strip().lower()
        content = _normalize_space(attrs.get("content") or "")
        if not content:
            return
        if name == "description" and self.meta_description is None:
            self.meta_description = content
        elif prop == "og:title" and self.og_title is None:
            self.og_title = content
        elif prop == "og:description" and self.og_description is None:
            self.og_description = content


@dataclass(frozen=True)
class _FetchedPage:
    url: str
    text: str
    parser: _HtmlDocumentParser


class WebsiteProfileClient:
    """Fetch and profile place websites asynchronously."""

    def __init__(
        self,
        settings: WebsiteProfileSettings | None = None,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = settings or WebsiteProfileSettings()
        self._http_client = http_client or httpx.AsyncClient(follow_redirects=True)
        self._owns_http_client = http_client is None
        self._domain_semaphores: dict[str, asyncio.Semaphore] = {}
        self._global_semaphore = asyncio.Semaphore(self._settings.global_concurrency)

    async def __aenter__(self) -> "WebsiteProfileClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_http_client:
            await self._http_client.aclose()

    async def enrich_dataframe(self, places: pd.DataFrame) -> pd.DataFrame:
        if "website" not in places.columns:
            raise ValueError("places DataFrame must contain a website column.")

        tasks = [
            self._enrich_place(row.to_dict())
            for _, row in places.iterrows()
        ]
        results = await asyncio.gather(*tasks)
        enrichment = pd.DataFrame(results)
        return places.merge(enrichment, on="fsq_place_id", how="left", validate="one_to_one")

    async def _enrich_place(self, row: dict[str, Any]) -> dict[str, Any]:
        place_id = str(row["fsq_place_id"])
        website = _normalize_website_url(row.get("website"))
        if website is None:
            logger.error("Place %s is missing a website URL during enrichment.", place_id)
            return _error_result(place_id, "missing_website", "No website URL was provided.")

        try:
            homepage = await self._fetch_page(website)
            same_domain_urls = _discover_same_domain_urls(
                homepage.url,
                homepage.parser.links,
                max_urls=self._settings.max_pages_per_site - 1,
            )
            extra_pages = await self._fetch_additional_pages(same_domain_urls)
            profile = _build_profile(row, [homepage, *extra_pages])
            profile["fsq_place_id"] = place_id
            return profile
        except WebsiteProfileError as exc:
            logger.error("Website enrichment failed for %s (%s): %s", place_id, website, exc)
            return _error_result(place_id, exc.__class__.__name__, str(exc))
        except Exception as exc:  # pragma: no cover - loud catch for unexpected failures
            logger.exception("Unexpected website enrichment failure for %s (%s).", place_id, website)
            return _error_result(place_id, "unexpected_failure", str(exc))

    async def _fetch_additional_pages(self, urls: list[str]) -> list[_FetchedPage]:
        if not urls:
            return []
        tasks = [self._fetch_page(url) for url in urls]
        pages: list[_FetchedPage] = []
        for result in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(result, Exception):
                logger.warning("Skipping linked page after fetch failure: %s", result)
                continue
            pages.append(result)
        return pages

    async def _fetch_page(self, url: str) -> _FetchedPage:
        domain = _domain_key(url)
        semaphore = self._domain_semaphores.setdefault(
            domain, asyncio.Semaphore(self._settings.per_domain_concurrency)
        )
        async with self._global_semaphore, semaphore:
            response = await self._request(url)

        content_type = response.headers.get("content-type", "").casefold()
        if "html" not in content_type:
            raise WebsiteContentError(
                f"Unsupported content-type {content_type!r} for {url}."
            )

        if len(response.content) > self._settings.max_response_bytes:
            raise WebsiteContentError(
                f"Response body for {url} exceeded max_response_bytes="
                f"{self._settings.max_response_bytes}."
            )

        text = response.text
        parser = _HtmlDocumentParser()
        parser.feed(text)
        parser.close()

        visible_text = _aggregate_visible_text(parser)
        if not visible_text:
            raise WebsiteContentError(f"Website {url} produced no usable visible text.")

        return _FetchedPage(url=str(response.url), text=visible_text, parser=parser)

    async def _request(self, url: str) -> httpx.Response:
        attempts = self._settings.retry_count + 1
        last_error: Exception | None = None
        for attempt in range(attempts):
            try:
                response = await self._http_client.get(
                    url,
                    timeout=self._settings.timeout_seconds,
                    headers={
                        "User-Agent": (
                            "DataHackDateProfileBot/1.0 "
                            "(research crawl for venue profiling)"
                        )
                    },
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = exc
                if attempt < self._settings.retry_count:
                    logger.warning(
                        "Transient website fetch failure for %s on attempt %d/%d: %s",
                        url,
                        attempt + 1,
                        attempts,
                        exc,
                    )
                    continue
                raise WebsiteFetchError(f"Transport failure fetching {url}: {exc}") from exc

            if response.status_code < 400:
                return response
            if response.status_code in RETRYABLE_STATUS_CODES and attempt < self._settings.retry_count:
                logger.warning(
                    "Website fetch failed for %s with status=%s on attempt %d/%d; retrying.",
                    url,
                    response.status_code,
                    attempt + 1,
                    attempts,
                )
                continue
            raise WebsiteFetchError(
                f"HTTP {response.status_code} while fetching {url}."
            )

        if last_error is not None:
            raise WebsiteFetchError(f"Transport failure fetching {url}: {last_error}") from last_error
        raise WebsiteFetchError(f"Failed to fetch {url} for an unknown reason.")


def _build_profile(row: dict[str, Any], pages: list[_FetchedPage]) -> dict[str, Any]:
    if not pages:
        raise WebsiteContentError("No pages were fetched for profile building.")

    combined_text = "\n".join(page.text for page in pages)
    combined_lower = combined_text.casefold()
    jsonld_types = _extract_jsonld_types(pages)
    discovered_page_types = _discovered_page_types(pages)
    cuisines = _keyword_tags(combined_lower, CUISINE_KEYWORDS)
    ambience_tags = _keyword_tags(combined_lower, AMBIENCE_KEYWORDS)
    setting_tags = _keyword_tags(combined_lower, SETTING_KEYWORDS)
    activity_tags = _keyword_tags(combined_lower, ACTIVITY_KEYWORDS)
    amenity_tags = _keyword_tags(combined_lower, AMENITY_KEYWORDS)
    drink_tags = _keyword_tags(combined_lower, DRINK_KEYWORDS)
    template_stop_tags = _infer_template_stop_tags(
        text=combined_lower,
        category_labels=row.get("fsq_category_labels"),
    )
    booking_signals = sorted(
        set(discovered_page_types)
        | _host_based_booking_signals(pages)
        | ({"reservations"} if "reservations" in amenity_tags else set())
    )
    evidence_snippets = _evidence_snippets(pages, cuisines, ambience_tags, setting_tags, activity_tags)

    rich_profile_text = _build_rich_profile_text(
        row=row,
        pages=pages,
        jsonld_types=jsonld_types,
        cuisines=cuisines,
        ambience_tags=ambience_tags,
        setting_tags=setting_tags,
        activity_tags=activity_tags,
        amenity_tags=amenity_tags,
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
            amenity_tags,
            drink_tags,
            template_stop_tags,
            booking_signals,
        ),
        evidence_snippets=evidence_snippets,
    )

    return {
        "website_enrichment_status": "ok",
        "website_enrichment_error": None,
        "website_canonical_url": pages[0].url,
        "website_page_count": len(pages),
        "website_jsonld_types": jsonld_types,
        "website_discovered_page_types": discovered_page_types,
        "website_cuisines": cuisines,
        "website_ambience_tags": ambience_tags,
        "website_setting_tags": setting_tags,
        "website_activity_tags": activity_tags,
        "website_amenity_tags": amenity_tags,
        "website_drink_tags": drink_tags,
        "website_template_stop_tags": template_stop_tags,
        "website_booking_signals": booking_signals,
        "website_evidence_snippets": evidence_snippets,
        "website_quality_score": quality_score,
        "website_rich_profile_text": rich_profile_text,
    }


def _error_result(place_id: str, status: str, error: str) -> dict[str, Any]:
    return {
        "fsq_place_id": place_id,
        "website_enrichment_status": status,
        "website_enrichment_error": error,
        "website_canonical_url": None,
        "website_page_count": 0,
        "website_jsonld_types": [],
        "website_discovered_page_types": [],
        "website_cuisines": [],
        "website_ambience_tags": [],
        "website_setting_tags": [],
        "website_activity_tags": [],
        "website_amenity_tags": [],
        "website_drink_tags": [],
        "website_template_stop_tags": [],
        "website_booking_signals": [],
        "website_evidence_snippets": [],
        "website_quality_score": 0,
        "website_rich_profile_text": None,
    }


def _normalize_website_url(url: object) -> str | None:
    if url is None:
        return None
    text = str(url).strip()
    if not text:
        return None
    if "://" not in text:
        text = "https://" + text
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"}:
        raise WebsiteContentError(f"Unsupported website scheme {parsed.scheme!r} for {text}.")
    if not parsed.netloc:
        raise WebsiteContentError(f"Website URL {text!r} is missing a hostname.")
    cleaned = parsed._replace(fragment="")
    return urlunparse(cleaned)


def _domain_key(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc.casefold().lstrip("www.")


def _discover_same_domain_urls(
    base_url: str,
    raw_links: list[tuple[str, str]],
    *,
    max_urls: int,
) -> list[str]:
    base_domain = _domain_key(base_url)
    selected: list[str] = []
    seen: set[str] = set()
    scored: list[tuple[int, str]] = []
    for href, anchor_text in raw_links:
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue
        if _domain_key(absolute) != base_domain:
            continue
        normalized = urlunparse(parsed._replace(fragment="", query=""))
        hint_text = f"{normalized} {anchor_text}".casefold()
        score = sum(1 for hint in DISCOVERY_LINK_HINTS if hint in hint_text)
        if score <= 0:
            continue
        if normalized in seen or normalized == base_url:
            continue
        seen.add(normalized)
        scored.append((score, normalized))

    scored.sort(key=lambda item: (-item[0], item[1]))
    for _, normalized in scored:
        selected.append(normalized)
        if len(selected) >= max_urls:
            break
    return selected


def _aggregate_visible_text(parser: _HtmlDocumentParser) -> str:
    parts = [
        *parser.title_parts,
        parser.meta_description or "",
        parser.og_title or "",
        parser.og_description or "",
        *parser.heading_texts,
        *parser.body_texts,
    ]
    cleaned: list[str] = []
    for part in parts:
        normalized = _normalize_space(part)
        if not normalized:
            continue
        lowered = normalized.casefold()
        if any(fragment in lowered for fragment in STOP_TEXT_FRAGMENTS):
            continue
        cleaned.append(normalized)
    return "\n".join(cleaned)


def _extract_jsonld_types(pages: list[_FetchedPage]) -> list[str]:
    types: set[str] = set()
    for page in pages:
        for block in page.parser.jsonld_blocks:
            try:
                payload = json.loads(unescape(block))
            except json.JSONDecodeError:
                logger.warning("Skipping invalid JSON-LD block from %s", page.url)
                continue
            _collect_jsonld_types(payload, types)
    return sorted(types)


def _collect_jsonld_types(payload: object, types: set[str]) -> None:
    if isinstance(payload, dict):
        type_value = payload.get("@type")
        if isinstance(type_value, str):
            types.add(type_value)
        elif isinstance(type_value, list):
            for item in type_value:
                if isinstance(item, str):
                    types.add(item)
        for value in payload.values():
            _collect_jsonld_types(value, types)
    elif isinstance(payload, list):
        for item in payload:
            _collect_jsonld_types(item, types)


def _discovered_page_types(pages: list[_FetchedPage]) -> list[str]:
    page_types: set[str] = set()
    for page in pages[1:]:
        lowered = page.url.casefold()
        for hint in STRUCTURED_PAGE_HINTS:
            if hint in lowered:
                page_types.add(hint)
    return sorted(page_types)


def _keyword_tags(text: str, mapping: dict[str, tuple[str, ...]]) -> list[str]:
    hits: list[str] = []
    for tag, keywords in mapping.items():
        if any(keyword in text for keyword in keywords):
            hits.append(tag)
    return hits


def _infer_template_stop_tags(text: str, category_labels: object) -> list[str]:
    category_text = _join_sequence(category_labels).casefold()
    combined = f"{text}\n{category_text}"
    matches: list[str] = []
    for stop_type, keywords in TEMPLATE_STOP_TYPE_KEYWORDS.items():
        if any(keyword in combined for keyword in keywords):
            matches.append(stop_type)
    return matches


def _host_based_booking_signals(pages: list[_FetchedPage]) -> set[str]:
    signals: set[str] = set()
    for page in pages:
        lowered = page.url.casefold()
        if any(host_hint in lowered for host_hint in BOOKING_HOST_HINTS):
            signals.add("third_party_booking")
        for href, _ in page.parser.links:
            absolute = urljoin(page.url, href).casefold()
            if any(host_hint in absolute for host_hint in BOOKING_HOST_HINTS):
                signals.add("third_party_booking")
    return signals


def _evidence_snippets(
    pages: list[_FetchedPage],
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


def _build_rich_profile_text(
    *,
    row: dict[str, Any],
    pages: list[_FetchedPage],
    jsonld_types: list[str],
    cuisines: list[str],
    ambience_tags: list[str],
    setting_tags: list[str],
    activity_tags: list[str],
    amenity_tags: list[str],
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
        f"Structured page types found: {_join_sequence(_discovered_page_types(pages))}",
        f"JSON-LD types: {_join_sequence(jsonld_types)}",
        f"Cuisines: {_join_sequence(cuisines)}",
        f"Ambience tags: {_join_sequence(ambience_tags)}",
        f"Setting tags: {_join_sequence(setting_tags)}",
        f"Activity tags: {_join_sequence(activity_tags)}",
        f"Drink tags: {_join_sequence(drink_tags)}",
        f"Template stop tags: {_join_sequence(template_stop_tags)}",
        f"Amenity tags: {_join_sequence(amenity_tags)}",
        f"Booking signals: {_join_sequence(booking_signals)}",
    ]
    if evidence_snippets:
        lines.append("Evidence snippets: " + " | ".join(evidence_snippets))
    text = "\n".join(line for line in lines if not line.endswith(": "))
    return text[:MAX_RICH_TEXT_LENGTH]


def _join_nonempty(values: list[object]) -> str:
    return ", ".join(str(value) for value in values if value is not None and str(value).strip())


def _join_sequence(values: object) -> str:
    if values is None:
        return ""
    if isinstance(values, (list, tuple, set)):
        return ", ".join(str(value) for value in values if str(value).strip())
    return str(values)


def _profile_quality_score(
    *,
    page_count: int,
    rich_profile_text: str,
    feature_groups: tuple[list[str], ...],
    evidence_snippets: list[str],
) -> int:
    score = min(page_count, 3)
    score += min(len(evidence_snippets), 3)
    score += min(len(rich_profile_text) // 250, 4)
    score += sum(1 for group in feature_groups if group)
    return int(score)


_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_space(text: str) -> str:
    return _WHITESPACE_RE.sub(" ", text).strip()
