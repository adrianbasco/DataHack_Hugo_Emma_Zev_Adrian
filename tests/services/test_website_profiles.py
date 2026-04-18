from __future__ import annotations

import unittest

from back_end.services.crawl4ai_profiles import (
    _Crawl4AIPage,
    _build_crawl4ai_profile,
    _crawl_result_internal_links,
    _crawl_result_text,
)
from back_end.services.website_profiles import (
    _FetchedPage,
    _HtmlDocumentParser,
    _aggregate_visible_text,
    _build_profile,
    _discover_same_domain_urls,
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


if __name__ == "__main__":
    unittest.main()
