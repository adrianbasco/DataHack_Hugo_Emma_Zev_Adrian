from __future__ import annotations

import json
import os
import unittest

import httpx

from back_end.clients.openrouter import (
    OpenRouterAgentLoopError,
    OpenRouterClient,
    OpenRouterResponseSchemaError,
    OpenRouterToolExecutionError,
    OpenRouterUnknownToolError,
    OpenRouterUpstreamError,
)
from back_end.clients.settings import OpenRouterConfigurationError, OpenRouterSettings
from back_end.llm.models import AgentTool, OpenRouterFunctionTool, OpenRouterMessage


def _make_response(
    request: httpx.Request,
    status_code: int,
    payload: dict | None = None,
) -> httpx.Response:
    headers = {"Content-Type": "application/json"}
    content = json.dumps(payload or {}).encode("utf-8")
    return httpx.Response(
        status_code=status_code,
        headers=headers,
        content=content,
        request=request,
    )


class OpenRouterSettingsTests(unittest.TestCase):
    def test_from_env_requires_api_key(self) -> None:
        old_value = os.environ.pop("OPENROUTER_API_KEY", None)
        self.addCleanup(self._restore_env, "OPENROUTER_API_KEY", old_value)

        with self.assertRaises(OpenRouterConfigurationError):
            OpenRouterSettings.from_env()

    @staticmethod
    def _restore_env(name: str, value: str | None) -> None:
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value


class OpenRouterClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.settings = OpenRouterSettings(
            api_key="test-key",
            default_model="openai/gpt-4o-mini",
            http_referer="https://example.com",
            app_title="Date Night",
            retry_count=1,
            max_tool_round_trips=2,
        )
        self.messages = [
            OpenRouterMessage(role="system", content="You are helpful."),
            OpenRouterMessage(role="user", content="Find date ideas."),
        ]

    async def test_create_chat_completion_sends_expected_headers_and_body(self) -> None:
        captured: dict[str, object] = {}
        tool = OpenRouterFunctionTool(
            name="lookup_places",
            description="Find places for a vibe.",
            parameters_json_schema={
                "type": "object",
                "properties": {"vibe": {"type": "string"}},
                "required": ["vibe"],
            },
        )

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            captured["auth"] = request.headers.get("Authorization")
            captured["referer"] = request.headers.get("HTTP-Referer")
            captured["title"] = request.headers.get("X-OpenRouter-Title")
            captured["request_id"] = request.headers.get("X-Request-Id")
            captured["body"] = json.loads(request.content.decode("utf-8"))
            return _make_response(
                request,
                200,
                {
                    "id": "resp_1",
                    "model": "openai/gpt-4o-mini",
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {
                                "role": "assistant",
                                "content": "Here are some options.",
                            },
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 10,
                        "completion_tokens": 5,
                        "total_tokens": 15,
                    },
                },
            )

        client = OpenRouterClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        response = await client.create_chat_completion(
            messages=self.messages,
            tools=[tool],
            temperature=0.2,
            tool_choice="auto",
            response_format={"type": "json_object"},
            parallel_tool_calls=True,
        )

        self.assertEqual("Here are some options.", response.output_text)
        self.assertEqual(
            "https://openrouter.ai/api/v1/chat/completions",
            captured["url"],
        )
        self.assertEqual("Bearer test-key", captured["auth"])
        self.assertEqual("https://example.com", captured["referer"])
        self.assertEqual("Date Night", captured["title"])
        self.assertIsInstance(captured["request_id"], str)
        self.assertEqual("openai/gpt-4o-mini", captured["body"]["model"])
        self.assertEqual(False, captured["body"]["stream"])
        self.assertEqual("lookup_places", captured["body"]["tools"][0]["function"]["name"])
        self.assertEqual("auto", captured["body"]["tool_choice"])
        self.assertEqual({"type": "json_object"}, captured["body"]["response_format"])

    async def test_create_chat_completion_retries_retryable_status_codes(self) -> None:
        attempts = {"count": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            attempts["count"] += 1
            if attempts["count"] == 1:
                return _make_response(
                    request,
                    503,
                    {"error": {"message": "temporary outage"}},
                )
            return _make_response(
                request,
                200,
                {
                    "id": "resp_1",
                    "model": "openai/gpt-4o-mini",
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {
                                "role": "assistant",
                                "content": "Recovered.",
                            },
                        }
                    ],
                },
            )

        client = OpenRouterClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        response = await client.create_chat_completion(messages=self.messages)

        self.assertEqual(2, attempts["count"])
        self.assertEqual("Recovered.", response.output_text)

    async def test_create_chat_completion_raises_on_non_retryable_http_error(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                400,
                {"error": {"message": "bad tool schema"}},
            )

        client = OpenRouterClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(OpenRouterUpstreamError):
            await client.create_chat_completion(messages=self.messages)

    async def test_create_chat_completion_raises_on_malformed_payload(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {"id": "resp_1", "model": "openai/gpt-4o-mini", "choices": []},
            )

        client = OpenRouterClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(OpenRouterResponseSchemaError):
            await client.create_chat_completion(messages=self.messages)

    async def test_run_agent_executes_tool_and_returns_final_answer(self) -> None:
        call_counter = {"count": 0}
        observed_tool_messages: list[dict[str, object]] = []
        tool = AgentTool(
            definition=OpenRouterFunctionTool(
                name="lookup_places",
                description="Find places for a vibe.",
                parameters_json_schema={
                    "type": "object",
                    "properties": {"vibe": {"type": "string"}},
                    "required": ["vibe"],
                },
            ),
            handler=self._lookup_places_tool,
        )

        def handler(request: httpx.Request) -> httpx.Response:
            call_counter["count"] += 1
            body = json.loads(request.content.decode("utf-8"))
            if call_counter["count"] == 1:
                return _make_response(
                    request,
                    200,
                    {
                        "id": "resp_tool",
                        "model": "openai/gpt-4o-mini",
                        "choices": [
                            {
                                "finish_reason": "tool_calls",
                                "message": {
                                    "role": "assistant",
                                    "content": None,
                                    "tool_calls": [
                                        {
                                            "id": "call_1",
                                            "type": "function",
                                            "function": {
                                                "name": "lookup_places",
                                                "arguments": "{\"vibe\":\"romantic\"}",
                                            },
                                        }
                                    ],
                                },
                            }
                        ],
                    },
                )

            observed_tool_messages.extend(body["messages"][-2:])
            return _make_response(
                request,
                200,
                {
                    "id": "resp_final",
                    "model": "openai/gpt-4o-mini",
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {
                                "role": "assistant",
                                "content": "Try Bar Clara, Marion, and Waxflower.",
                            },
                        }
                    ],
                },
            )

        client = OpenRouterClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        result = await client.run_agent(messages=self.messages, tools=[tool])

        self.assertEqual(2, call_counter["count"])
        self.assertEqual(
            "Try Bar Clara, Marion, and Waxflower.",
            result.final_response.output_text,
        )
        self.assertEqual(1, len(result.tool_executions))
        self.assertEqual("lookup_places", result.tool_executions[0].tool_name)
        self.assertEqual("assistant", observed_tool_messages[0]["role"])
        self.assertEqual("tool", observed_tool_messages[1]["role"])
        self.assertEqual("call_1", observed_tool_messages[1]["tool_call_id"])

    async def test_run_agent_raises_on_invalid_tool_arguments(self) -> None:
        tool = AgentTool(
            definition=OpenRouterFunctionTool(
                name="lookup_places",
                description="Find places for a vibe.",
                parameters_json_schema={"type": "object"},
            ),
            handler=self._lookup_places_tool,
        )

        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "id": "resp_tool",
                    "model": "openai/gpt-4o-mini",
                    "choices": [
                        {
                            "finish_reason": "tool_calls",
                            "message": {
                                "role": "assistant",
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": "call_1",
                                        "type": "function",
                                        "function": {
                                            "name": "lookup_places",
                                            "arguments": "not-json",
                                        },
                                    }
                                ],
                            },
                        }
                    ],
                },
            )

        client = OpenRouterClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(OpenRouterToolExecutionError):
            await client.run_agent(messages=self.messages, tools=[tool])

    async def test_run_agent_raises_on_unknown_tool(self) -> None:
        tool = AgentTool(
            definition=OpenRouterFunctionTool(
                name="lookup_places",
                description="Find places for a vibe.",
                parameters_json_schema={"type": "object"},
            ),
            handler=self._lookup_places_tool,
        )

        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "id": "resp_tool",
                    "model": "openai/gpt-4o-mini",
                    "choices": [
                        {
                            "finish_reason": "tool_calls",
                            "message": {
                                "role": "assistant",
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": "call_1",
                                        "type": "function",
                                        "function": {
                                            "name": "unknown_tool",
                                            "arguments": "{}",
                                        },
                                    }
                                ],
                            },
                        }
                    ],
                },
            )

        client = OpenRouterClient(
            self.settings,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(OpenRouterUnknownToolError):
            await client.run_agent(messages=self.messages, tools=[tool])

    async def test_run_agent_raises_when_model_exceeds_tool_round_trip_limit(self) -> None:
        tool = AgentTool(
            definition=OpenRouterFunctionTool(
                name="lookup_places",
                description="Find places for a vibe.",
                parameters_json_schema={"type": "object"},
            ),
            handler=self._lookup_places_tool,
        )

        def handler(request: httpx.Request) -> httpx.Response:
            return _make_response(
                request,
                200,
                {
                    "id": "resp_tool",
                    "model": "openai/gpt-4o-mini",
                    "choices": [
                        {
                            "finish_reason": "tool_calls",
                            "message": {
                                "role": "assistant",
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": "call_1",
                                        "type": "function",
                                        "function": {
                                            "name": "lookup_places",
                                            "arguments": "{\"vibe\":\"romantic\"}",
                                        },
                                    }
                                ],
                            },
                        }
                    ],
                },
            )

        client = OpenRouterClient(
            OpenRouterSettings(
                api_key="test-key",
                default_model="openai/gpt-4o-mini",
                max_tool_round_trips=1,
            ),
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(OpenRouterAgentLoopError):
            await client.run_agent(messages=self.messages, tools=[tool])

    @staticmethod
    def _lookup_places_tool(arguments: dict[str, object]) -> dict[str, object]:
        vibe = arguments["vibe"]
        return {
            "vibe": vibe,
            "places": ["Bar Clara", "Marion", "Waxflower"],
        }
