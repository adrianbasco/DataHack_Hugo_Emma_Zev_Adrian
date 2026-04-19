from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from back_end.catalog.repository import PlacesRepository
from back_end.query.settings import QuerySettings
from back_end.search.models import (
    FilteredCandidatePool,
    LocationInput,
    ParsedQuery,
    RetrieverCandidate,
    ScoreBreakdown,
    SearchContext,
    SearchCoordinates,
    SearchResult,
    SearchRequest,
    StructuredFilters,
    WeatherPreference,
)
from back_end.search.service import SearchService


def _places_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fsq_place_id": "place-1",
                "name": "Sydney Place",
                "latitude": -33.8688,
                "longitude": 151.2093,
                "address": "1 George St",
                "locality": "Sydney",
                "region": "NSW",
                "postcode": "2000",
                "fsq_category_ids": ["cat_food"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
                "date_closed": None,
            }
        ]
    )


class _StubParser:
    def __init__(self, parsed: ParsedQuery) -> None:
        self._parsed = parsed

    async def parse(self, query: str | None, *, context: SearchContext | None = None) -> ParsedQuery:
        return self._parsed


class _StubRetriever:
    def __init__(self, candidates: tuple[RetrieverCandidate, ...]) -> None:
        self._candidates = candidates

    def filter_candidates(self, *, filters: StructuredFilters, resolved_location, exclude_plan_ids=()):
        self.last_filters = filters
        self.last_resolved_location = resolved_location
        return FilteredCandidatePool(candidates=self._candidates, filter_stage_counts=())

    def score_and_rerank(self, *, candidates, query_text, template_hints=(), limit=20):
        return tuple(
            SearchResult(
                plan_id=candidate.plan_id,
                score=1.0,
                match_reasons=("stub",),
                score_breakdown=ScoreBreakdown(lexical=1.0, total=1.0),
                card=candidate.card,
            )
            for candidate in candidates[:limit]
        )


class _TransportFilteringStubRetriever(_StubRetriever):
    def filter_candidates(self, *, filters: StructuredFilters, resolved_location, exclude_plan_ids=()):
        self.last_filters = filters
        self.last_resolved_location = resolved_location
        candidates = () if filters.transport_mode == "public_transport" else self._candidates
        return FilteredCandidatePool(candidates=candidates, filter_stage_counts=())


class _FailingWeatherService:
    def __init__(self) -> None:
        self.calls = 0

    async def evaluate_window(self, coordinates, *, exposure, start_at, end_at):
        from back_end.domain.models import (
            WeatherAssessmentStatus,
            WeatherCheckFailure,
            WeatherWindowAssessment,
        )

        self.calls += 1
        return WeatherWindowAssessment(
            exposure=exposure,
            start_at=start_at,
            end_at=end_at,
            status=WeatherAssessmentStatus.UPSTREAM_FAILURE,
            should_reject=True,
            summary="upstream failed",
            considered_points=(),
            failure=WeatherCheckFailure(
                reason="weather_upstream_failure",
                message="provider unavailable",
            ),
        )


class SearchServiceTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_overrides_win_over_parser_and_context(self) -> None:
        candidate = RetrieverCandidate(
            plan_id="plan-1",
            bucket_id="sydney_cbd",
            template_id="drinks_dinner_dessert",
            bucket_label="Sydney CBD",
            bucket_latitude=-33.8688,
            bucket_longitude=151.2093,
            time_of_day="morning",
            weather_sensitive=False,
            template_duration_hours=2.0,
            template_title="Title",
            template_description=None,
            search_text="romantic drinks",
            card={"plan_title": "Plan 1"},
            vibes=("romantic",),
            transport_mode="WALK",
            plan_time_iso="2026-04-24T09:00:00+10:00",
            fsq_place_ids_sorted=("a", "b"),
        )
        service = SearchService(
            repository=self.repository,
            query_settings=self.repository._settings,
            parser=_StubParser(
                ParsedQuery(
                    free_text_residual="romantic drinks",
                    filters=StructuredFilters(
                        time_of_day="evening",
                        location=None,
                    ),
                )
            ),
            retriever=_StubRetriever((candidate,)),
            weather_service=_FailingWeatherService(),
        )

        response = await service.search(
            SearchRequest(
                query="romantic drinks tonight",
                context=SearchContext(
                    now_iso="2026-04-24T18:30:00+10:00",
                    user_location=SearchCoordinates(lat=-33.87, lng=151.21),
                ),
                overrides=StructuredFilters(
                    time_of_day="morning",
                    vibes=("romantic",),
                ),
            )
        )

        self.assertEqual("morning", response.parsed.time_of_day.value)
        self.assertEqual("override", response.parsed.time_of_day.source.value)
        self.assertEqual(("romantic",), response.parsed.vibes.value)

    async def test_location_resolution_failure_is_reported_not_hidden(self) -> None:
        candidate = RetrieverCandidate(
            plan_id="plan-1",
            bucket_id="sydney_cbd",
            template_id="drinks_dinner_dessert",
            bucket_label="Sydney CBD",
            bucket_latitude=-33.8688,
            bucket_longitude=151.2093,
            time_of_day="evening",
            weather_sensitive=False,
            template_duration_hours=2.0,
            template_title="Title",
            template_description=None,
            search_text="romantic drinks",
            card={"plan_title": "Plan 1"},
            vibes=("romantic",),
            transport_mode="WALK",
            plan_time_iso="2026-04-24T19:00:00+10:00",
            fsq_place_ids_sorted=("a", "b"),
        )
        service = SearchService(
            repository=self.repository,
            query_settings=self.repository._settings,
            parser=_StubParser(
                ParsedQuery(
                    filters=StructuredFilters(location=None),
                )
            ),
            retriever=_StubRetriever((candidate,)),
            weather_service=_FailingWeatherService(),
        )

        response = await service.search(
            SearchRequest(
                overrides=StructuredFilters(location=LocationInput(text="Nowhere", radius_km=5.0)),
            )
        )

        self.assertTrue(any("could not be resolved" in warning for warning in response.parsed.warnings))

    async def test_weather_upstream_failure_falls_back_to_closest_related_plan_and_surfaces_warning(self) -> None:
        weather_sensitive_candidate = RetrieverCandidate(
            plan_id="plan-1",
            bucket_id="bondi",
            template_id="beach_picnic",
            bucket_label="Bondi",
            bucket_latitude=-33.8915,
            bucket_longitude=151.2767,
            time_of_day="afternoon",
            weather_sensitive=True,
            template_duration_hours=2.0,
            template_title="Beach Picnic",
            template_description="outdoor picnic",
            search_text="beach picnic",
            card={"plan_title": "Plan 1"},
            vibes=("romantic",),
            transport_mode="WALK",
            plan_time_iso="2026-04-24T14:00:00+10:00",
            fsq_place_ids_sorted=("a", "b"),
        )
        weather_service = _FailingWeatherService()
        service = SearchService(
            repository=self.repository,
            query_settings=self.repository._settings,
            parser=_StubParser(ParsedQuery()),
            retriever=_StubRetriever((weather_sensitive_candidate,)),
            weather_service=weather_service,
        )

        response = await service.search(SearchRequest(query="beach picnic"))

        self.assertEqual(1, weather_service.calls)
        self.assertEqual(("plan-1",), tuple(result.plan_id for result in response.results))
        self.assertEqual(0, response.diagnostics.weather_gate_stats.upstream_failures)
        self.assertTrue(any("Weather upstream failure" in warning for warning in response.parsed.warnings))
        self.assertTrue(any("closest related" in warning for warning in response.parsed.warnings))

    async def test_empty_strict_transport_match_returns_closest_related_candidates(self) -> None:
        candidate = RetrieverCandidate(
            plan_id="plan-1",
            bucket_id="newtown",
            template_id="dinner_dessert",
            bucket_label="Newtown",
            bucket_latitude=-33.897,
            bucket_longitude=151.18,
            time_of_day="night",
            weather_sensitive=False,
            template_duration_hours=2.0,
            template_title="Dinner Dessert",
            template_description=None,
            search_text="romantic dinner dessert newtown",
            card={"plan_title": "Plan 1"},
            vibes=("romantic",),
            transport_mode="WALK",
            plan_time_iso="2026-04-24T21:00:00+10:00",
            fsq_place_ids_sorted=("a", "b"),
        )
        service = SearchService(
            repository=self.repository,
            query_settings=self.repository._settings,
            parser=_StubParser(
                ParsedQuery(
                    free_text_residual="romantic dinner",
                    filters=StructuredFilters(transport_mode="public_transport"),
                )
            ),
            retriever=_TransportFilteringStubRetriever((candidate,)),
            weather_service=_FailingWeatherService(),
        )

        response = await service.search(SearchRequest(query="romantic dinner by train"))

        self.assertEqual(("plan-1",), tuple(result.plan_id for result in response.results))
        self.assertTrue(
            any(stage.stage == "closest_related_fallback" for stage in response.diagnostics.filter_stage_counts)
        )
        self.assertTrue(any("closest related" in warning for warning in response.parsed.warnings))


if __name__ == "__main__":
    unittest.main()
