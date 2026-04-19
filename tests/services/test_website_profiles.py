from __future__ import annotations

import asyncio
import unittest

from crawl4ai.async_configs import CrawlerRunConfig

from back_end.services.crawl4ai_profiles import (
    _Crawl4AIPage,
    _Crawl4AIRequestResult,
    _build_crawl4ai_profile,
    _classify_crawl4ai_failure,
    _crawl_result_internal_links,
    _crawl_result_text,
    Crawl4AIProfileSettings,
    Crawl4AIWebsiteProfileClient,
)
from back_end.services.website_profiles import (
    _FetchedPage,
    _HtmlDocumentParser,
    _aggregate_visible_text,
    _build_profile,
    _discover_same_domain_urls,
    _normalize_website_url,
)


class _FakeMarkdown:
    def __init__(self, raw_markdown: str, fit_markdown: str | None = None) -> None:
        self.raw_markdown = raw_markdown
        self.fit_markdown = fit_markdown

    def __str__(self) -> str:
        return self.raw_markdown


class _FakeResult:
    def __init__(self) -> None:
        self.markdown = _FakeMarkdown(
            raw_markdown="# Cozy Italian Spot\nBook now for waterfront pasta and wine.",
            fit_markdown="# Cozy Italian Spot\nBook now for waterfront pasta and wine.",
        )
        self.metadata = {"title": "Cozy Italian Spot", "description": "Waterfront pasta bar"}
        self.links = {
            "internal": [
                {"href": "https://example.com/menu", "text": "Menu"},
                {"href": "https://example.com/book", "text": "Book now"},
            ]
        }


class _FakeFailureResult:
    def __init__(self, error_message: str, status_code: int | None = None) -> None:
        self.error_message = error_message
        self.status_code = status_code


class _NeverReturningCrawler:
    async def arun(self, url: str, config: object, **kwargs: object) -> object:
        await asyncio.Event().wait()


class WebsiteProfilesTestCase(unittest.TestCase):
    def test_discover_same_domain_urls_filters_cross_domain_links(self) -> None:
        urls = _discover_same_domain_urls(
            "https://example.com",
            [
                ("https://example.com/menu", "Menu"),
                ("/book", "Book now"),
                ("https://other.com/menu", "Menu"),
                ("mailto:test@example.com", "Email"),
            ],
            max_urls=5,
        )

        self.assertEqual(urls, ["https://example.com/book", "https://example.com/menu"])

    def test_build_profile_extracts_expected_tags(self) -> None:
        parser = _HtmlDocumentParser()
        parser.feed(
            """
            <html>
              <head>
                <title>Cozy Waterfront Trattoria</title>
                <meta name="description" content="Italian pasta, wine, and book a table by the harbour." />
                <script type="application/ld+json">{"@type":"Restaurant"}</script>
              </head>
              <body>
                <h1>Cozy Italian Dinner</h1>
                <p>Join us for handmade pasta, Italian wine, and a romantic waterfront setting.</p>
                <a href="/menu">Menu</a>
              </body>
            </html>
            """
        )
        parser.close()
        page = _FetchedPage(
            url="https://example.com",
            text=_aggregate_visible_text(parser),
            parser=parser,
        )

        profile = _build_profile(
            {
                "name": "Cozy Waterfront Trattoria",
                "locality": "Sydney",
                "region": "NSW",
                "postcode": "2000",
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
            },
            [page],
        )

        self.assertEqual(profile["website_enrichment_status"], "ok")
        self.assertIn("Restaurant", profile["website_jsonld_types"])
        self.assertIn("italian", profile["website_cuisines"])
        self.assertIn("cozy", profile["website_ambience_tags"])
        self.assertIn("romantic", profile["website_ambience_tags"])
        self.assertIn("waterfront", profile["website_setting_tags"])
        self.assertIn("wine", profile["website_drink_tags"])
        self.assertIn("restaurant", profile["website_template_stop_tags"])
        self.assertIn("harbor_or_pier", profile["website_template_stop_tags"])
        self.assertGreater(profile["website_quality_score"], 0)
        self.assertIn("pasta", profile["website_rich_profile_text"].casefold())

    def test_normalize_website_url_canonicalizes_root_trailing_slash(self) -> None:
        self.assertEqual(_normalize_website_url("example.com"), "https://example.com")
        self.assertEqual(_normalize_website_url("https://example.com/"), "https://example.com")

    def test_crawl_result_helpers_extract_text_and_links(self) -> None:
        result = _FakeResult()

        text = _crawl_result_text(result)
        links = _crawl_result_internal_links(result)

        self.assertIn("Cozy Italian Spot", text)
        self.assertEqual(
            links,
            [
                ("https://example.com/menu", "Menu"),
                ("https://example.com/book", "Book now"),
            ],
        )

    def test_classify_crawl4ai_failure_trims_verbose_navigation_errors(self) -> None:
        error_type, error_message = _classify_crawl4ai_failure(
            "http://dead.example",
            _FakeFailureResult(
                "Unexpected error in _crawl_web at line 778\n"
                "Error: Failed on navigating ACS-GOTO:\n"
                "Page.goto: net::ERR_NAME_NOT_RESOLVED at http://dead.example/\n"
                "Call log:\n"
                "  - navigating to dead.example\n\n"
                "Code context:\n"
                " 773 lots of noisy internals"
            ),
        )

        self.assertEqual(error_type, "dns_not_resolved")
        self.assertEqual(error_message, "DNS did not resolve for http://dead.example.")

    def test_classify_crawl4ai_failure_keeps_anti_bot_concise(self) -> None:
        error_type, error_message = _classify_crawl4ai_failure(
            "https://blocked.example",
            _FakeFailureResult(
                "Blocked by anti-bot protection: Structural: minimal_text on small page"
            ),
        )

        self.assertEqual(error_type, "anti_bot_blocked")
        self.assertIn("Blocked by anti-bot protection", error_message)

    def test_build_crawl4ai_profile_extracts_expected_tags(self) -> None:
        profile = _build_crawl4ai_profile(
            {
                "name": "Harbour Pasta Bar",
                "locality": "Sydney",
                "region": "NSW",
                "postcode": "2000",
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
            },
            [
                _Crawl4AIPage(
                    url="https://example.com",
                    text=(
                        "Harbour Pasta Bar\n"
                        "Cozy Italian wine bar with waterfront views.\n"
                        "Book now for pasta and cocktails."
                    ),
                    links=[
                        ("https://example.com/menu", "Menu"),
                        ("https://www.opentable.com/r/harbour-pasta-bar", "Book now"),
                    ],
                ),
                _Crawl4AIPage(
                    url="https://example.com/menu",
                    text="Menu\nFresh pasta, tiramisu, Italian wine list.",
                    links=[],
                ),
            ],
        )

        self.assertEqual(profile["crawl4ai_enrichment_status"], "ok")
        self.assertIn("italian", profile["crawl4ai_cuisines"])
        self.assertIn("cozy", profile["crawl4ai_ambience_tags"])
        self.assertIn("waterfront", profile["crawl4ai_setting_tags"])
        self.assertIn("wine", profile["crawl4ai_drink_tags"])
        self.assertIn("restaurant", profile["crawl4ai_template_stop_tags"])
        self.assertGreater(profile["crawl4ai_quality_score"], 0)
        self.assertIn("menu", profile["crawl4ai_discovered_page_types"])
        self.assertIn("third_party_booking", profile["crawl4ai_booking_signals"])

    def test_crawl4ai_batch_watchdog_returns_explicit_timeout_result(self) -> None:
        client = Crawl4AIWebsiteProfileClient(
            settings=Crawl4AIProfileSettings(
                semaphore_count=1,
                max_session_permit=1,
                crawl_watchdog_extra_seconds=0.0,
                batch_progress_interval_seconds=2.0,
            )
        )

        results = asyncio.run(
            client._crawl_batch(
                _NeverReturningCrawler(),
                ["https://stuck.example"],
                CrawlerRunConfig(page_timeout=1),
                batch_label="test",
            )
        )

        self.assertEqual(
            results["https://stuck.example"].error_type,
            "crawl_watchdog_timeout",
        )
        self.assertIn(
            "exceeded watchdog",
            results["https://stuck.example"].error_message,
        )

    def test_crawl4ai_watchdog_defaults_are_short_and_capped(self) -> None:
        client = Crawl4AIWebsiteProfileClient()

        self.assertEqual(
            client._crawl_watchdog_seconds(CrawlerRunConfig(page_timeout=9000)),
            12.0,
        )
        self.assertEqual(
            client._crawl_watchdog_seconds(CrawlerRunConfig(page_timeout=12000)),
            15.0,
        )
        self.assertEqual(
            client._crawl_watchdog_seconds(CrawlerRunConfig(page_timeout=18000)),
            15.0,
        )

    def test_crawl4ai_retry_skips_timeout_statuses(self) -> None:
        client = Crawl4AIWebsiteProfileClient()
        results_by_url = {
            "https://slow.example": _Crawl4AIRequestResult(
                requested_url="https://slow.example",
                page=None,
                error_type="crawl_timeout",
                error_message="Timed out.",
            ),
            "https://stuck.example": _Crawl4AIRequestResult(
                requested_url="https://stuck.example",
                page=None,
                error_type="crawl_watchdog_timeout",
                error_message="Watchdog expired.",
            ),
        }

        retried = asyncio.run(
            client._retry_failed_batch(
                results_by_url,
                run_config=CrawlerRunConfig(page_timeout=1),
                batch_label="test-retry",
            )
        )

        self.assertIs(retried, results_by_url)


if __name__ == "__main__":
    unittest.main()
