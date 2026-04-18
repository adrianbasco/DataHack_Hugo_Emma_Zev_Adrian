from __future__ import annotations

import unittest

import pandas as pd

from back_end.services.adaptive_website_profiles import (
    _backend_used,
    _build_canonical_profile_columns,
    _needs_crawl4ai_fallback,
)


class AdaptiveWebsiteProfilesTestCase(unittest.TestCase):
    def test_needs_crawl4ai_fallback_for_failed_or_thin_rows(self) -> None:
        failed_row = pd.Series(
            {
                "website_enrichment_status": "WebsiteFetchError",
                "website_quality_score": 0,
                "website_page_count": 0,
                "website_rich_profile_text": None,
                "website_template_stop_tags": [],
                "website_evidence_snippets": [],
            }
        )
        thin_row = pd.Series(
            {
                "website_enrichment_status": "ok",
                "website_quality_score": 3,
                "website_page_count": 1,
                "website_rich_profile_text": "short",
                "website_template_stop_tags": [],
                "website_evidence_snippets": [],
            }
        )
        strong_row = pd.Series(
            {
                "website_enrichment_status": "ok",
                "website_quality_score": 9,
                "website_page_count": 3,
                "website_rich_profile_text": "x" * 800,
                "website_template_stop_tags": ["restaurant"],
                "website_evidence_snippets": ["good evidence"],
            }
        )

        self.assertTrue(_needs_crawl4ai_fallback(failed_row, min_quality_score=6))
        self.assertTrue(_needs_crawl4ai_fallback(thin_row, min_quality_score=6))
        self.assertFalse(_needs_crawl4ai_fallback(strong_row, min_quality_score=6))

    def test_build_canonical_profile_columns_prefers_crawl4ai_when_used(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "fsq_place_id": "1",
                    "website_profile_backend_used": "heuristic",
                    "website_enrichment_status": "ok",
                    "crawl4ai_enrichment_status": None,
                    "website_enrichment_error": None,
                    "crawl4ai_enrichment_error": None,
                    "website_canonical_url": "https://heuristic.example.com",
                    "crawl4ai_canonical_url": None,
                    "website_page_count": 2,
                    "crawl4ai_page_count": 0,
                    "website_discovered_page_types": ["menu"],
                    "crawl4ai_discovered_page_types": [],
                    "website_cuisines": ["italian"],
                    "crawl4ai_cuisines": [],
                    "website_ambience_tags": ["cozy"],
                    "crawl4ai_ambience_tags": [],
                    "website_setting_tags": [],
                    "crawl4ai_setting_tags": [],
                    "website_activity_tags": [],
                    "crawl4ai_activity_tags": [],
                    "website_drink_tags": ["wine"],
                    "crawl4ai_drink_tags": [],
                    "website_template_stop_tags": ["restaurant"],
                    "crawl4ai_template_stop_tags": [],
                    "website_booking_signals": ["menu"],
                    "crawl4ai_booking_signals": [],
                    "website_evidence_snippets": ["heuristic evidence"],
                    "crawl4ai_evidence_snippets": [],
                    "website_quality_score": 8,
                    "crawl4ai_quality_score": 0,
                    "website_rich_profile_text": "heuristic profile",
                    "crawl4ai_rich_profile_text": None,
                },
                {
                    "fsq_place_id": "2",
                    "website_profile_backend_used": "crawl4ai",
                    "website_enrichment_status": "WebsiteContentError",
                    "crawl4ai_enrichment_status": "ok",
                    "website_enrichment_error": "bad",
                    "crawl4ai_enrichment_error": None,
                    "website_canonical_url": None,
                    "crawl4ai_canonical_url": "https://crawl4ai.example.com",
                    "website_page_count": 0,
                    "crawl4ai_page_count": 3,
                    "website_discovered_page_types": [],
                    "crawl4ai_discovered_page_types": ["about"],
                    "website_cuisines": [],
                    "crawl4ai_cuisines": ["coffee"],
                    "website_ambience_tags": [],
                    "crawl4ai_ambience_tags": ["casual"],
                    "website_setting_tags": [],
                    "crawl4ai_setting_tags": ["view"],
                    "website_activity_tags": [],
                    "crawl4ai_activity_tags": [],
                    "website_drink_tags": [],
                    "crawl4ai_drink_tags": ["coffee"],
                    "website_template_stop_tags": [],
                    "crawl4ai_template_stop_tags": ["cafe"],
                    "website_booking_signals": [],
                    "crawl4ai_booking_signals": ["about"],
                    "website_evidence_snippets": [],
                    "crawl4ai_evidence_snippets": ["crawl evidence"],
                    "website_quality_score": 0,
                    "crawl4ai_quality_score": 7,
                    "website_rich_profile_text": None,
                    "crawl4ai_rich_profile_text": "crawl4ai profile",
                },
            ]
        )

        combined = _build_canonical_profile_columns(df)

        self.assertEqual(_backend_used(combined.iloc[0]), "heuristic")
        self.assertEqual(_backend_used(combined.iloc[1]), "crawl4ai")
        self.assertEqual(combined.loc[0, "profile_canonical_url"], "https://heuristic.example.com")
        self.assertEqual(combined.loc[1, "profile_canonical_url"], "https://crawl4ai.example.com")
        self.assertEqual(combined.loc[0, "profile_rich_profile_text"], "heuristic profile")
        self.assertEqual(combined.loc[1, "profile_rich_profile_text"], "crawl4ai profile")


if __name__ == "__main__":
    unittest.main()
