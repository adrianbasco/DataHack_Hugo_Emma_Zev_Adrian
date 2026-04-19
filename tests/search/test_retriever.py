from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from back_end.precache.output import OUTPUT_COLUMNS
from back_end.search.models import LocationInput, ResolvedLocationFilter, StructuredFilters, WeatherPreference
from back_end.search.retriever import PlanRetriever


def _plan_row(
    *,
    plan_id: str,
    bucket_id: str,
    bucket_label: str,
    bucket_latitude: float,
    bucket_longitude: float,
    template_id: str,
    vibe: list[str],
    time_of_day: str,
    weather_sensitive: bool,
    search_text: str | None,
    card_payload: dict | None,
    fsq_ids: list[str],
) -> dict[str, object]:
    return {
        "plan_id": plan_id,
        "bucket_id": bucket_id,
        "bucket_label": bucket_label,
        "bucket_latitude": bucket_latitude,
        "bucket_longitude": bucket_longitude,
        "bucket_radius_km": 2.5,
        "bucket_transport_mode": "WALK",
        "bucket_tags_json": "[]",
        "bucket_metadata_json": json.dumps({"label": bucket_label}),
        "template_id": template_id,
        "template_title": template_id.replace("_", " ").title(),
        "vibe": json.dumps(vibe),
        "time_of_day": time_of_day,
        "weather_sensitive": weather_sensitive,
        "template_duration_hours": 2.0,
        "template_description": f"{template_id} description",
        "template_metadata_json": json.dumps({"description": f"{template_id} description"}),
        "plan_title": f"{template_id} title",
        "plan_hook": "hook",
        "plan_time_iso": "2026-04-24T19:00:00+10:00",
        "stops_json": "[]",
        "search_text": search_text,
        "card_json": json.dumps(card_payload) if card_payload is not None else None,
        "fsq_place_ids_sorted": json.dumps(fsq_ids),
        "fsq_place_id_count": len(fsq_ids),
        "verification_json": "{}",
        "generated_at_utc": "2026-04-18T00:00:00+00:00",
        "written_at_utc": "2026-04-18T00:00:00+00:00",
        "model": "test-model",
    }


class PlanRetrieverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.plans_path = self.root / "plans.parquet"
        rows = [
            _plan_row(
                plan_id="plan-1",
                bucket_id="sydney_cbd",
                bucket_label="Sydney CBD",
                bucket_latitude=-33.8688,
                bucket_longitude=151.2093,
                template_id="drinks_dinner_dessert",
                vibe=["romantic", "foodie"],
                time_of_day="evening",
                weather_sensitive=False,
                search_text="romantic cocktails dessert in sydney cbd",
                card_payload={"plan_title": "Card 1", "search_text": "romantic cocktails dessert in sydney cbd"},
                fsq_ids=["a", "b", "c"],
            ),
            _plan_row(
                plan_id="plan-2",
                bucket_id="sydney_cbd",
                bucket_label="Sydney CBD",
                bucket_latitude=-33.8688,
                bucket_longitude=151.2093,
                template_id="drinks_dinner_dessert",
                vibe=["romantic", "foodie"],
                time_of_day="evening",
                weather_sensitive=False,
                search_text="romantic cocktails dessert in sydney cbd alt",
                card_payload={"plan_title": "Card 2", "search_text": "romantic cocktails dessert in sydney cbd alt"},
                fsq_ids=["a", "b", "d"],
            ),
            _plan_row(
                plan_id="plan-3",
                bucket_id="bondi",
                bucket_label="Bondi",
                bucket_latitude=-33.8915,
                bucket_longitude=151.2767,
                template_id="beach_picnic",
                vibe=["outdoorsy", "romantic"],
                time_of_day="afternoon",
                weather_sensitive=True,
                search_text="beach picnic outdoorsy bondi",
                card_payload={"plan_title": "Card 3", "search_text": "beach picnic outdoorsy bondi"},
                fsq_ids=["x", "y", "z"],
            ),
            _plan_row(
                plan_id="plan-4",
                bucket_id="missing",
                bucket_label="Missing",
                bucket_latitude=-33.9,
                bucket_longitude=151.2,
                template_id="broken",
                vibe=["casual"],
                time_of_day="evening",
                weather_sensitive=False,
                search_text="broken row",
                card_payload=None,
                fsq_ids=["m"],
            ),
        ]
        pd.DataFrame(rows, columns=list(OUTPUT_COLUMNS)).to_parquet(self.plans_path, index=False)
        self.retriever = PlanRetriever(self.plans_path)

    def test_filter_candidates_drops_invalid_cards_and_applies_indoors_only(self) -> None:
        filtered = self.retriever.filter_candidates(
            filters=StructuredFilters(weather_ok=WeatherPreference.INDOORS_ONLY),
            resolved_location=None,
        )

        self.assertEqual(1, filtered.filter_stage_counts[0].rejected)
        self.assertEqual(["plan-1", "plan-2"], [candidate.plan_id for candidate in filtered.candidates])

    def test_score_and_rerank_is_stable_and_deduplicates_shared_prefixes(self) -> None:
        filtered = self.retriever.filter_candidates(
            filters=StructuredFilters(vibes=("romantic",), time_of_day="evening"),
            resolved_location=ResolvedLocationFilter(
                text="Sydney",
                radius_km=5.0,
                anchor_latitude=-33.8688,
                anchor_longitude=151.2093,
                resolved_label="Sydney",
            ),
        )

        first = self.retriever.score_and_rerank(
            candidates=filtered.candidates,
            query_text="romantic cocktails dessert",
            template_hints=("dessert",),
            limit=5,
        )
        second = self.retriever.score_and_rerank(
            candidates=filtered.candidates,
            query_text="romantic cocktails dessert",
            template_hints=("dessert",),
            limit=5,
        )

        self.assertEqual([result.plan_id for result in first], [result.plan_id for result in second])
        self.assertEqual(["plan-1"], [result.plan_id for result in first])


if __name__ == "__main__":
    unittest.main()
