from __future__ import annotations

import tempfile
import unittest
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from back_end.agents.date_idea_agent import (
    DEFAULT_DATE_IDEA_AGENT_MODEL,
    DEFAULT_REASONING_EFFORT,
)
from back_end.catalog.repository import PlacesRepository
from back_end.query.settings import QuerySettings
from back_end.search.parser import LLM_RESPONSE_SCHEMA, QueryParser


def _schema_type_values(schema: object):
    if isinstance(schema, dict):
        if "type" in schema:
            yield schema["type"]
        for value in schema.values():
            yield from _schema_type_values(value)
    elif isinstance(schema, list):
        for item in schema:
            yield from _schema_type_values(item)


def _places_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fsq_place_id": "place-1",
                "name": "Bondi Cafe",
                "latitude": -33.8915,
                "longitude": 151.2767,
                "address": "1 Beach Rd",
                "locality": "Bondi",
                "region": "NSW",
                "postcode": "2026",
                "fsq_category_ids": ["cat_food"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
                "date_closed": None,
            },
            {
                "fsq_place_id": "place-2",
                "name": "Newtown Books",
                "latitude": -33.8981,
                "longitude": 151.1748,
                "address": "2 King St",
                "locality": "Newtown",
                "region": "NSW",
                "postcode": "2042",
                "fsq_category_ids": ["cat_books"],
                "fsq_category_labels": ["Retail > Bookstore"],
                "date_closed": None,
            },
        ]
    )


class QueryParserTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.places_path = self.root / "places.parquet"
        _places_df().to_parquet(self.places_path)
        settings = QuerySettings(
            places_parquet_path=self.places_path,
            categories_parquet_path=self.root / "unused_categories.parquet",
            allowlist_seed_path=self.root / "unused_allowlist.yaml",
        )
        self.repository = PlacesRepository(settings)
        self.parser = QueryParser(self.repository)

    async def test_parser_extracts_high_signal_filters_with_rule_layer(self) -> None:
        parsed = await self.parser.parse(
            "romantic bookstore walk in Newtown tonight indoors only"
        )

        self.assertEqual(("romantic",), parsed.filters.vibes)
        self.assertEqual("evening", parsed.filters.time_of_day)
        self.assertEqual("walking", parsed.filters.transport_mode)
        self.assertEqual("Newtown, NSW", parsed.filters.location.text)
        self.assertEqual("indoors_only", parsed.filters.weather_ok.value)
        self.assertIn("bookstore", parsed.filters.template_hints)
        self.assertIsNotNone(parsed.free_text_residual)

    async def test_parser_warns_when_no_structured_filters_are_found(self) -> None:
        parsed = await self.parser.parse("surprise me with something different")

        self.assertEqual((), parsed.filters.vibes)
        self.assertIsNone(parsed.filters.time_of_day)
        self.assertIn("No structured filters were extracted.", parsed.warnings)

    async def test_llm_parser_uses_default_sonnet_thinking_model(self) -> None:
        observed: dict = {}

        class _StubLlmClient:
            async def create_chat_completion(self, **kwargs):
                observed.update(kwargs)
                return SimpleNamespace(
                    output_text=json.dumps(
                        {
                            "vibes": ["energetic"],
                            "time_of_day": "night",
                            "weather_ok": None,
                            "location_text": None,
                            "transport_mode": "walking",
                            "template_hints": ["pub"],
                            "free_text_residual": "pub crawl",
                            "warnings": [],
                        }
                    )
                )

        parser = QueryParser(self.repository, llm_client=_StubLlmClient())
        parsed = await parser.parse("I'd love to go on an exciting pub crawl")

        self.assertEqual(DEFAULT_DATE_IDEA_AGENT_MODEL, observed["model"])
        self.assertEqual(
            {"reasoning": {"effort": DEFAULT_REASONING_EFFORT, "exclude": True}},
            observed["extra_body"],
        )
        self.assertTrue(parsed.llm_succeeded)
        self.assertEqual(("energetic",), parsed.filters.vibes)

    def test_llm_response_schema_uses_provider_safe_nullable_fields(self) -> None:
        type_values = list(_schema_type_values(LLM_RESPONSE_SCHEMA))
        self.assertFalse(
            any(isinstance(value, list) for value in type_values),
            "OpenRouter providers can reject JSON Schema type arrays.",
        )

        properties = LLM_RESPONSE_SCHEMA["properties"]
        for field_name in (
            "time_of_day",
            "weather_ok",
            "location_text",
            "transport_mode",
            "free_text_residual",
        ):
            field_schema = properties[field_name]
            self.assertIn("anyOf", field_schema)
            self.assertIn({"type": "null"}, field_schema["anyOf"])


if __name__ == "__main__":
    unittest.main()
