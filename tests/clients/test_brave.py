from __future__ import annotations

import json
import os
import unittest

import httpx

from back_end.clients.brave import BraveSearchClient, BraveUpstreamError
from back_end.clients.settings import BraveConfigurationError, BraveSettings


def _make_response(
    request: httpx.Request,
    status_code: int,
    payload: dict | None = None,
) -> httpx.Response:
    return httpx.Response(
        status_code=status_code,
        headers={"Content-Type": "application/json"},
        content=json.dumps(payload or {}).encode("utf-8"),
        request=request,
    )


class BraveSettingsTests(unittest.TestCase):
    def test_from_env_requires_api_key(self) -> None:
        old_value = os.environ.pop("BRAVE_API_KEY", None)
        self.addCleanup(self._restore_env, "BRAVE_API_KEY", old_value)

        with self.assertRaises(BraveConfigurationError):
            BraveSettings.from_env()

    @staticmethod
    def _restore_env(name: str, value: str | None) -> None:
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value


class BraveSearchClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.settings = BraveSettings(api_key="test-key", retry_count=1)

    async def test_search_web_sends_expected_headers_and_parses_results(self) -> None:
        captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["token"] = request.headers.get("X-Subscription-Token")
            captured["path"] = request.url.path
            captured["query"] = dict(request.url.params)
            return _make_response(
                request,
                200,
                {
                    "web": {
                        "results": [
                            {
                                "title": "Barrenjoey Lighthouse",
                                "url": "https://www.nationalparks.nsw.gov.au/example",
                                "description": "Scenic walk.",
                                "extra_snippets": ["Whale watching in season."],
                            }
                        ]
                    }
                },
            )

        client = BraveSearchClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        results = await client.search_web('"Barrenjoey Track" Palm Beach', count=3)

        self.assertEqual("test-key", captured["token"])
        self.assertEqual("/res/v1/web/search", captured["path"])
        self.assertEqual("3", captured["query"]["count"])
        self.assertEqual("Barrenjoey Lighthouse", results[0].title)
        self.assertEqual(("Whale watching in season.",), results[0].extra_snippets)

    async def test_search_local_retries_retryable_status_codes(self) -> None:
        attempts = {"count": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            attempts["count"] += 1
            if attempts["count"] == 1:
                return _make_response(request, 503, {"error": "temporary"})
            return _make_response(
                request,
                200,
                {
                    "results": [
                        {
                            "id": "loc_1",
                            "title": "Louis Fruit Market",
                            "coordinates": [-33.8773, 151.1852],
                            "categories": ["grocery"],
                            "rating": {"ratingValue": 4.7, "reviewCount": 40},
                        }
                    ]
                },
            )

        client = BraveSearchClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        results = await client.search_local(
            "Louis Fruit Market",
            latitude=-33.8773,
            longitude=151.1852,
        )

        self.assertEqual(2, attempts["count"])
        self.assertEqual("loc_1", results[0].brave_id)
        self.assertEqual(4.7, results[0].rating.rating_value)

    async def test_non_retryable_error_raises(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(request, 400, {"error": "bad request"})

        client = BraveSearchClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(BraveUpstreamError):
            await client.search_web("bad query")
