"""Adaptive website enrichment that escalates to Crawl4AI only when needed."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from back_end.services.crawl4ai_profiles import (
    Crawl4AIProfileSettings,
    Crawl4AIWebsiteProfileClient,
)
from back_end.services.website_profiles import WebsiteProfileClient, WebsiteProfileSettings


@dataclass(frozen=True)
class AdaptiveWebsiteProfileSettings:
    """Settings for the adaptive website enrichment pipeline."""

    heuristic: WebsiteProfileSettings = WebsiteProfileSettings(
        global_concurrency=20,
        per_domain_concurrency=2,
        max_pages_per_site=5,
    )
    crawl4ai: Crawl4AIProfileSettings = Crawl4AIProfileSettings(
        max_pages_per_site=3,
        semaphore_count=4,
        max_session_permit=8,
        pruning_threshold=0.45,
    )
    min_heuristic_quality_score: int = 6


class AdaptiveWebsiteProfileClient:
    """Use cheap HTTP extraction first, then escalate thin/failed rows to Crawl4AI."""

    def __init__(self, settings: AdaptiveWebsiteProfileSettings | None = None) -> None:
        self._settings = settings or AdaptiveWebsiteProfileSettings()
        self._heuristic_client = WebsiteProfileClient(settings=self._settings.heuristic)
        self._crawl4ai_client = Crawl4AIWebsiteProfileClient(settings=self._settings.crawl4ai)

    async def aclose(self) -> None:
        await self._heuristic_client.aclose()

    async def enrich_dataframe(self, places: pd.DataFrame) -> pd.DataFrame:
        heuristic_df = await self._heuristic_client.enrich_dataframe(places)
        fallback_subset = heuristic_df.loc[
            heuristic_df.apply(
                lambda row: _needs_crawl4ai_fallback(
                    row,
                    min_quality_score=self._settings.min_heuristic_quality_score,
                ),
                axis=1,
            )
        ].copy()

        if fallback_subset.empty:
            combined = heuristic_df.copy()
            combined["website_profile_backend_used"] = "heuristic"
            return _build_canonical_profile_columns(combined)

        crawl4ai_df = await self._crawl4ai_client.enrich_dataframe(fallback_subset)
        crawl4ai_cols = [
            "fsq_place_id",
            "crawl4ai_enrichment_status",
            "crawl4ai_enrichment_error",
            "crawl4ai_canonical_url",
            "crawl4ai_page_count",
            "crawl4ai_discovered_page_types",
            "crawl4ai_cuisines",
            "crawl4ai_ambience_tags",
            "crawl4ai_setting_tags",
            "crawl4ai_activity_tags",
            "crawl4ai_drink_tags",
            "crawl4ai_template_stop_tags",
            "crawl4ai_booking_signals",
            "crawl4ai_evidence_snippets",
            "crawl4ai_quality_score",
            "crawl4ai_rich_profile_text",
        ]
        combined = heuristic_df.merge(
            crawl4ai_df.loc[:, crawl4ai_cols],
            on="fsq_place_id",
            how="left",
            validate="one_to_one",
        )
        combined["website_profile_backend_used"] = combined.apply(
            _backend_used,
            axis=1,
        )
        return _build_canonical_profile_columns(combined)


def _needs_crawl4ai_fallback(row: pd.Series, *, min_quality_score: int) -> bool:
    if row["website_enrichment_status"] != "ok":
        return True
    if int(row.get("website_quality_score", 0)) < min_quality_score:
        return True

    page_count = int(row.get("website_page_count", 0) or 0)
    rich_len = len(str(row.get("website_rich_profile_text") or ""))
    template_tags = row.get("website_template_stop_tags") or []
    evidence_snippets = row.get("website_evidence_snippets") or []

    return page_count <= 1 and rich_len < 500 and not template_tags and not evidence_snippets


def _backend_used(row: pd.Series) -> str:
    if row.get("crawl4ai_enrichment_status") == "ok":
        return "crawl4ai"
    return "heuristic"


def _build_canonical_profile_columns(df: pd.DataFrame) -> pd.DataFrame:
    canonical_fields = [
        ("enrichment_status", "website_enrichment_status", "crawl4ai_enrichment_status"),
        ("enrichment_error", "website_enrichment_error", "crawl4ai_enrichment_error"),
        ("canonical_url", "website_canonical_url", "crawl4ai_canonical_url"),
        ("page_count", "website_page_count", "crawl4ai_page_count"),
        ("discovered_page_types", "website_discovered_page_types", "crawl4ai_discovered_page_types"),
        ("cuisines", "website_cuisines", "crawl4ai_cuisines"),
        ("ambience_tags", "website_ambience_tags", "crawl4ai_ambience_tags"),
        ("setting_tags", "website_setting_tags", "crawl4ai_setting_tags"),
        ("activity_tags", "website_activity_tags", "crawl4ai_activity_tags"),
        ("drink_tags", "website_drink_tags", "crawl4ai_drink_tags"),
        ("template_stop_tags", "website_template_stop_tags", "crawl4ai_template_stop_tags"),
        ("booking_signals", "website_booking_signals", "crawl4ai_booking_signals"),
        ("evidence_snippets", "website_evidence_snippets", "crawl4ai_evidence_snippets"),
        ("quality_score", "website_quality_score", "crawl4ai_quality_score"),
        ("rich_profile_text", "website_rich_profile_text", "crawl4ai_rich_profile_text"),
    ]
    combined = df.copy()
    for canonical_name, heuristic_col, crawl4ai_col in canonical_fields:
        combined[f"profile_{canonical_name}"] = combined.apply(
            lambda row: row[crawl4ai_col]
            if row.get("website_profile_backend_used") == "crawl4ai"
            else row[heuristic_col],
            axis=1,
        )
    return combined
