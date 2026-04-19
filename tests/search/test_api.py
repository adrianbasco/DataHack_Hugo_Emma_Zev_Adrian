from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace

import httpx

from back_end.api.app import create_app
from back_end.search.models import (
    FinalParsedFilters,
    SearchDiagnostics,
    SearchResponse,
    SearchResult,
    ScoreBreakdown,
    SourcedValue,
    WeatherGateStats,
)
from back_end.search.models import FilterSource


class _StubSearchService:
    async def search(self, request):
        return SearchResponse(
            parsed=FinalParsedFilters(
                vibes=SourcedValue(value=("romantic",), source=FilterSource.PARSED),
                time_of_day=SourcedValue(value="evening", source=FilterSource.PARSED),
                weather_ok=SourcedValue(value=None, source=FilterSource.UNSET),
                location=SourcedValue(value=None, source=FilterSource.UNSET),
                transport_mode=SourcedValue(value=None, source=FilterSource.UNSET),
                template_hints=SourcedValue(value=(), source=FilterSource.UNSET),
                free_text_residual=SourcedValue(value="romantic dinner", source=FilterSource.DERIVED),
            ),
            results=(
                SearchResult(
                    plan_id="plan-1",
                    score=1.23,
                    match_reasons=("stub",),
                    score_breakdown=ScoreBreakdown(lexical=1.0, total=1.23),
                    card={"plan_title": "A plan"},
                ),
            ),
            diagnostics=SearchDiagnostics(
                total_matched_before_limit=1,
                filter_stage_counts=(),
                weather_gate_stats=WeatherGateStats(),
            ),
        )


class _EmptySearchService:
    async def search(self, request):
        return SearchResponse(
            parsed=FinalParsedFilters(
                vibes=SourcedValue(value=(), source=FilterSource.UNSET),
                time_of_day=SourcedValue(value=None, source=FilterSource.UNSET),
                weather_ok=SourcedValue(value=None, source=FilterSource.UNSET),
                location=SourcedValue(value=None, source=FilterSource.UNSET),
                transport_mode=SourcedValue(value=None, source=FilterSource.UNSET),
                template_hints=SourcedValue(value=(), source=FilterSource.UNSET),
                free_text_residual=SourcedValue(value=request.query, source=FilterSource.DERIVED),
                warnings=("No cached cards matched the request.",),
            ),
            results=(),
            diagnostics=SearchDiagnostics(
                total_matched_before_limit=0,
                filter_stage_counts=(),
                weather_gate_stats=WeatherGateStats(),
                warnings=("No cached cards matched the request.",),
            ),
        )


class _PlanImageService:
    assets_dir = Path(__file__).parent

    async def get_plan(self, *, plan_id: str, public_base_url: str):
        if plan_id != "plan-1":
            raise KeyError(plan_id)
        return SimpleNamespace(
            heroImageUrl=f"{public_base_url}/static/precache-images/google-1/hero.jpg"
        )


class SearchApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_search_endpoint_rejects_empty_request(self) -> None:
        app = create_app(search_service=_StubSearchService())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            response = await client.post("/dates/search", json={})

        self.assertEqual(400, response.status_code)
        self.assertIn("Invalid request body.", response.text)

    async def test_search_endpoint_returns_service_payload(self) -> None:
        app = create_app(
            service=_PlanImageService(),
            search_service=_StubSearchService(),
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            response = await client.post("/dates/search", json={"query": "romantic dinner"})

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual("plan-1", payload["results"][0]["plan_id"])
        self.assertEqual("evening", payload["parsed"]["time_of_day"]["value"])
        self.assertEqual(
            "http://testserver/static/precache-images/google-1/hero.jpg",
            payload["results"][0]["card"]["hero_image_url"],
        )

    async def test_search_endpoint_preserves_explicit_empty_results(self) -> None:
        app = create_app(search_service=_EmptySearchService())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            response = await client.post("/dates/search", json={"query": "nonexistent plan"})

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual([], payload["results"])
        self.assertIn("No cached cards matched the request.", payload["diagnostics"]["warnings"])


if __name__ == "__main__":
    unittest.main()
