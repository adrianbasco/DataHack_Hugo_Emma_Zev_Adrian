from __future__ import annotations

import json
import os
import unittest

import httpx

from back_end.clients.bland import (
    BlandAIClient,
    BlandAIResponseSchemaError,
    BlandAIUpstreamError,
    BlandCallRequest,
)
from back_end.clients.settings import BlandAIConfigurationError, BlandAISettings


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


class BlandAISettingsTests(unittest.TestCase):
    def test_from_env_requires_api_key(self) -> None:
        old_value = os.environ.pop("BLAND_AI_API_KEY", None)
        self.addCleanup(self._restore_env, "BLAND_AI_API_KEY", old_value)

        with self.assertRaises(BlandAIConfigurationError):
            BlandAISettings.from_env()

    def test_from_env_rejects_invalid_boolean(self) -> None:
        old_key = os.environ.get("BLAND_AI_API_KEY")
        old_record = os.environ.get("BLAND_AI_RECORD_CALLS")
        os.environ["BLAND_AI_API_KEY"] = "test-key"
        os.environ["BLAND_AI_RECORD_CALLS"] = "maybe"
        self.addCleanup(self._restore_env, "BLAND_AI_API_KEY", old_key)
        self.addCleanup(self._restore_env, "BLAND_AI_RECORD_CALLS", old_record)

        with self.assertRaises(BlandAIConfigurationError):
            BlandAISettings.from_env()

    @staticmethod
    def _restore_env(name: str, value: str | None) -> None:
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value


class BlandAIClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.settings = BlandAISettings(
            api_key="test-key",
            base_url="https://api.test.bland.ai/v1",
            status_retry_count=1,
        )

    async def test_send_call_sends_expected_headers_and_body(self) -> None:
        captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["path"] = request.url.path
            captured["authorization"] = request.headers.get("authorization")
            captured["body"] = json.loads(request.content.decode("utf-8"))
            return _make_response(
                request,
                200,
                {
                    "status": "success",
                    "message": "Call successfully queued.",
                    "call_id": "call_123",
                    "batch_id": None,
                },
            )

        client = BlandAIClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        queued = await client.send_call(
            BlandCallRequest(
                phone_number="+61412345678",
                task="Book a table.",
                request_data={"restaurant_name": "Cafe Test"},
                metadata={"purpose": "restaurant_booking"},
                dispositions=("booking_confirmed",),
            )
        )

        self.assertEqual("/v1/calls", captured["path"])
        self.assertEqual("test-key", captured["authorization"])
        self.assertEqual("+61412345678", captured["body"]["phone_number"])
        self.assertEqual("Book a table.", captured["body"]["task"])
        self.assertEqual(
            {"restaurant_name": "Cafe Test"},
            captured["body"]["request_data"],
        )
        self.assertEqual("call_123", queued.call_id)

    async def test_send_call_does_not_retry_to_avoid_duplicate_phone_calls(self) -> None:
        attempts = {"count": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            attempts["count"] += 1
            return _make_response(request, 503, {"status": "error", "message": "down"})

        client = BlandAIClient(
            BlandAISettings(api_key="test-key", status_retry_count=3),
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(BlandAIUpstreamError):
            await client.send_call(
                BlandCallRequest(phone_number="+61412345678", task="Book a table.")
            )
        self.assertEqual(1, attempts["count"])

    async def test_get_call_details_retries_retryable_status(self) -> None:
        attempts = {"count": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            attempts["count"] += 1
            if attempts["count"] == 1:
                return _make_response(request, 503, {"status": "error"})
            return _make_response(
                request,
                200,
                {
                    "call_id": "call_123",
                    "to": "+61412345678",
                    "completed": True,
                    "status": "completed",
                    "queue_status": "complete",
                    "request_data": {"restaurant_name": "Cafe Test"},
                    "metadata": {"purpose": "restaurant_booking"},
                },
            )

        client = BlandAIClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        details = await client.get_call_details("call_123")

        self.assertEqual(2, attempts["count"])
        self.assertEqual("call_123", details.call_id)
        self.assertEqual("completed", details.status)
        self.assertEqual("Cafe Test", details.request_data["restaurant_name"])

    async def test_send_call_raises_on_success_payload_without_call_id(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {"status": "success", "message": "queued"},
            )

        client = BlandAIClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(BlandAIResponseSchemaError):
            await client.send_call(
                BlandCallRequest(phone_number="+61412345678", task="Book a table.")
            )

    async def test_send_call_rejects_pathway_and_task_together(self) -> None:
        client = BlandAIClient(
            self.settings,
            http_client=httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda request: _make_response(request, 200)
                )
            ),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(ValueError):
            await client.send_call(
                BlandCallRequest(
                    phone_number="+61412345678",
                    task="Book a table.",
                    pathway_id="pathway_123",
                )
            )
