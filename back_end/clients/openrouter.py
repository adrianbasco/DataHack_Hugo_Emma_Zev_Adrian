"""OpenRouter chat-completions client with client-side tool execution."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
from typing import Any
from uuid import uuid4

import httpx

from back_end.clients.settings import OpenRouterConfigurationError, OpenRouterSettings
from back_end.llm.models import (
    AgentRunResult,
    AgentTool,
    AgentToolExecution,
    OpenRouterChatResponse,
    OpenRouterFunctionTool,
    OpenRouterMessage,
    OpenRouterServerTool,
    OpenRouterToolCall,
    OpenRouterUsage,
)

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})


class OpenRouterClientError(RuntimeError):
    """Base class for OpenRouter client failures."""


class OpenRouterUpstreamError(OpenRouterClientError):
    """Raised when OpenRouter rejects a request or returns invalid HTTP."""


class OpenRouterResponseSchemaError(OpenRouterClientError):
    """Raised when OpenRouter returns an unexpected payload shape."""


class OpenRouterToolExecutionError(OpenRouterClientError):
    """Raised when a model-requested tool cannot be executed safely."""


class OpenRouterUnknownToolError(OpenRouterToolExecutionError):
    """Raised when the model calls a tool that is not registered."""


class OpenRouterAgentLoopError(OpenRouterClientError):
    """Raised when the client-side agent loop cannot reach a terminal answer."""


class OpenRouterClient:
    """Purpose-built OpenRouter client for non-streaming chat completions."""

    def __init__(
        self,
        settings: OpenRouterSettings,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = settings
        self._http_client = http_client or httpx.AsyncClient()
        self._owns_http_client = http_client is None

    async def __aenter__(self) -> "OpenRouterClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_http_client:
            await self._http_client.aclose()

    def close(self) -> None:
        """Compatibility shim for non-async callers."""

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self.aclose())
            return
        raise RuntimeError(
            "OpenRouterClient.close() was called inside a running event loop. "
            "Use 'await client.aclose()' instead."
        )

    async def create_chat_completion(
        self,
        *,
        messages: tuple[OpenRouterMessage, ...] | list[OpenRouterMessage],
        model: str | None = None,
        tools: tuple[OpenRouterFunctionTool | OpenRouterServerTool, ...]
        | list[OpenRouterFunctionTool | OpenRouterServerTool]
        | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        temperature: float | None = None,
        response_format: dict[str, Any] | None = None,
        parallel_tool_calls: bool | None = None,
        max_tokens: int | None = None,
        plugins: tuple[dict[str, Any], ...] | list[dict[str, Any]] | None = None,
        extra_body: dict[str, Any] | None = None,
        request_id: str | None = None,
    ) -> OpenRouterChatResponse:
        """Send one non-streaming chat completion request."""

        model_name = model or self._settings.default_model
        if model_name is None:
            raise OpenRouterConfigurationError(
                "No OpenRouter model was supplied. Set OPENROUTER_MODEL or pass "
                "model= explicitly."
            )
        if not messages:
            raise ValueError("messages must contain at least one item.")

        client_request_id = request_id or str(uuid4())
        payload = self._build_chat_payload(
            model=model_name,
            messages=messages,
            tools=tools,
            tool_choice=tool_choice,
            temperature=temperature,
            response_format=response_format,
            parallel_tool_calls=parallel_tool_calls,
            max_tokens=max_tokens,
            plugins=plugins,
            extra_body=extra_body,
        )
        response_payload = await self._request_json(
            "POST",
            f"{self._settings.base_url.rstrip('/')}/chat/completions",
            body=payload,
            request_id=client_request_id,
        )
        return self._parse_chat_completion(
            response_payload,
            client_request_id=client_request_id,
        )

    async def run_agent(
        self,
        *,
        messages: tuple[OpenRouterMessage, ...] | list[OpenRouterMessage],
        tools: tuple[AgentTool, ...] | list[AgentTool],
        model: str | None = None,
        tool_choice: str | dict[str, Any] | None = "auto",
        temperature: float | None = None,
        response_format: dict[str, Any] | None = None,
        parallel_tool_calls: bool = True,
        max_tokens: int | None = None,
        max_round_trips: int | None = None,
        plugins: tuple[dict[str, Any], ...] | list[dict[str, Any]] | None = None,
        extra_body: dict[str, Any] | None = None,
    ) -> AgentRunResult:
        """Run a client-side tool execution loop until the model stops."""

        if not tools:
            raise ValueError("tools must contain at least one callable tool.")

        tool_registry = self._index_tools(tools)
        round_trip_limit = max_round_trips or self._settings.max_tool_round_trips
        transcript = list(messages)
        executions: list[AgentToolExecution] = []

        for round_index in range(round_trip_limit + 1):
            response = await self.create_chat_completion(
                messages=transcript,
                model=model,
                tools=[tool.definition for tool in tools],
                tool_choice=tool_choice,
                temperature=temperature,
                response_format=response_format,
                parallel_tool_calls=parallel_tool_calls,
                max_tokens=max_tokens,
                plugins=plugins,
                extra_body=extra_body,
            )
            transcript.append(response.message)

            if not response.tool_calls:
                return AgentRunResult(
                    final_response=response,
                    transcript=tuple(transcript),
                    tool_executions=tuple(executions),
                )

            if round_index >= round_trip_limit:
                raise OpenRouterAgentLoopError(
                    "Model exceeded the configured OpenRouter tool round-trip limit "
                    f"of {round_trip_limit}."
                )

            tool_executions = await self._execute_tool_calls(
                response.tool_calls,
                tool_registry,
                parallel=parallel_tool_calls,
            )
            for execution in tool_executions:
                transcript.append(execution.tool_message)
            executions.extend(tool_executions)

        raise OpenRouterAgentLoopError(
            "Agent loop terminated unexpectedly without returning a final response."
        )

    def _build_chat_payload(
        self,
        *,
        model: str,
        messages: tuple[OpenRouterMessage, ...] | list[OpenRouterMessage],
        tools: tuple[OpenRouterFunctionTool | OpenRouterServerTool, ...]
        | list[OpenRouterFunctionTool | OpenRouterServerTool]
        | None,
        tool_choice: str | dict[str, Any] | None,
        temperature: float | None,
        response_format: dict[str, Any] | None,
        parallel_tool_calls: bool | None,
        max_tokens: int | None,
        plugins: tuple[dict[str, Any], ...] | list[dict[str, Any]] | None,
        extra_body: dict[str, Any] | None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": model,
            "messages": [message.to_api_dict() for message in messages],
            "stream": False,
        }
        if tools:
            payload["tools"] = [tool.to_api_dict() for tool in tools]
        if tool_choice is not None:
            payload["tool_choice"] = tool_choice
        if temperature is not None:
            payload["temperature"] = temperature
        if response_format is not None:
            payload["response_format"] = response_format
        if parallel_tool_calls is not None:
            payload["parallel_tool_calls"] = parallel_tool_calls
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        if plugins:
            payload["plugins"] = list(plugins)
        if extra_body:
            overlapping_keys = sorted(set(payload).intersection(extra_body))
            if overlapping_keys:
                raise ValueError(
                    "extra_body attempted to overwrite explicit chat-completion "
                    f"parameters: {overlapping_keys}."
                )
            payload.update(extra_body)
        return payload

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        body: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        attempts = self._settings.retry_count + 1
        last_response: httpx.Response | None = None

        for attempt_index in range(attempts):
            response = await self._http_client.request(
                method,
                url,
                json=body,
                headers=self._build_headers(request_id),
                timeout=self._settings.timeout_seconds,
            )
            last_response = response
            if response.status_code < 400:
                break

            if (
                response.status_code in RETRYABLE_STATUS_CODES
                and attempt_index < self._settings.retry_count
            ):
                logger.warning(
                    "OpenRouter request_id=%s failed with status=%s on attempt %s/%s; "
                    "retrying.",
                    request_id,
                    response.status_code,
                    attempt_index + 1,
                    attempts,
                )
                continue

            error_message = self._extract_error_message(response)
            logger.error(
                "OpenRouter request_id=%s failed with status=%s body=%r",
                request_id,
                response.status_code,
                response.text[:1000],
            )
            raise OpenRouterUpstreamError(
                "OpenRouter request failed "
                f"(request_id={request_id}, status={response.status_code}): "
                f"{error_message}"
            )

        if last_response is None:
            raise OpenRouterUpstreamError(
                f"OpenRouter request_id={request_id} did not produce a response."
            )

        try:
            payload = last_response.json()
        except ValueError as exc:
            logger.error(
                "OpenRouter request_id=%s returned non-JSON response: %r",
                request_id,
                last_response.text[:1000],
            )
            raise OpenRouterResponseSchemaError(
                f"OpenRouter request_id={request_id} returned a non-JSON response."
            ) from exc

        if not isinstance(payload, dict):
            raise OpenRouterResponseSchemaError(
                f"OpenRouter request_id={request_id} returned a top-level payload "
                "that was not an object."
            )

        error_payload = payload.get("error")
        if isinstance(error_payload, dict) and error_payload:
            logger.error(
                "OpenRouter request_id=%s returned explicit error payload: %r",
                request_id,
                error_payload,
            )
            message = error_payload.get("message") or "unknown error"
            raise OpenRouterUpstreamError(
                f"OpenRouter request failed (request_id={request_id}): {message}"
            )
        return payload

    def _build_headers(self, request_id: str) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._settings.api_key}",
            "Content-Type": "application/json",
            "X-Request-Id": request_id,
        }
        if self._settings.http_referer is not None:
            headers["HTTP-Referer"] = self._settings.http_referer
        if self._settings.app_title is not None:
            headers["X-OpenRouter-Title"] = self._settings.app_title
        return headers

    def _parse_chat_completion(
        self,
        payload: dict[str, Any],
        *,
        client_request_id: str,
    ) -> OpenRouterChatResponse:
        model = payload.get("model")
        choices = payload.get("choices")
        response_id = payload.get("id")
        usage = payload.get("usage")

        if not isinstance(model, str) or not model:
            raise OpenRouterResponseSchemaError(
                "OpenRouter response did not include a valid model string."
            )
        if not isinstance(choices, list) or not choices:
            raise OpenRouterResponseSchemaError(
                "OpenRouter response did not include a non-empty choices list."
            )

        choice = choices[0]
        if not isinstance(choice, dict):
            raise OpenRouterResponseSchemaError(
                "OpenRouter response choice was not an object."
            )

        finish_reason = choice.get("finish_reason")
        if finish_reason is not None and not isinstance(finish_reason, str):
            raise OpenRouterResponseSchemaError(
                "OpenRouter response finish_reason was not a string."
            )

        message_payload = choice.get("message")
        if not isinstance(message_payload, dict):
            raise OpenRouterResponseSchemaError(
                "OpenRouter response choice did not include a message object."
            )

        message = self._parse_message(message_payload)
        return OpenRouterChatResponse(
            response_id=response_id if isinstance(response_id, str) else None,
            client_request_id=client_request_id,
            model=model,
            finish_reason=finish_reason,
            message=message,
            usage=self._parse_usage(usage),
        )

    def _parse_message(self, payload: dict[str, Any]) -> OpenRouterMessage:
        role = payload.get("role")
        content = payload.get("content")
        raw_tool_calls = payload.get("tool_calls")

        if not isinstance(role, str) or not role:
            raise OpenRouterResponseSchemaError(
                "OpenRouter message did not include a valid role string."
            )

        if content is None:
            parsed_content = None
        elif isinstance(content, str):
            parsed_content = content
        elif isinstance(content, list):
            parsed_content = self._coerce_content_parts(content)
        else:
            raise OpenRouterResponseSchemaError(
                "OpenRouter message content was neither a string, list, nor null."
            )

        tool_calls: tuple[OpenRouterToolCall, ...] = ()
        if raw_tool_calls is not None:
            if not isinstance(raw_tool_calls, list):
                raise OpenRouterResponseSchemaError(
                    "OpenRouter message tool_calls was not a list."
                )
            tool_calls = tuple(self._parse_tool_call(item) for item in raw_tool_calls)

        if parsed_content is None and not tool_calls:
            raise OpenRouterResponseSchemaError(
                "OpenRouter message contained neither content nor tool_calls."
            )

        return OpenRouterMessage(
            role=role,
            content=parsed_content,
            tool_calls=tool_calls,
        )

    def _parse_tool_call(self, payload: Any) -> OpenRouterToolCall:
        if not isinstance(payload, dict):
            raise OpenRouterResponseSchemaError(
                "OpenRouter tool call entry was not an object."
            )

        function = payload.get("function")
        if not isinstance(function, dict):
            raise OpenRouterResponseSchemaError(
                "OpenRouter tool call did not include a function object."
            )

        name = function.get("name")
        arguments_json = function.get("arguments")
        raw_id = payload.get("id")
        if not isinstance(name, str) or not name:
            raise OpenRouterResponseSchemaError(
                "OpenRouter tool call function.name was invalid."
            )
        if not isinstance(arguments_json, str):
            raise OpenRouterResponseSchemaError(
                "OpenRouter tool call function.arguments was not a string."
            )

        call_id = payload.get("id")
        if not isinstance(call_id, str) or not call_id:
            call_id = f"call_{uuid4()}"

        return OpenRouterToolCall(
            call_id=call_id,
            raw_id=raw_id if isinstance(raw_id, str) else None,
            name=name,
            arguments_json=arguments_json,
        )

    def _parse_usage(self, payload: Any) -> OpenRouterUsage | None:
        if payload is None:
            return None
        if not isinstance(payload, dict):
            raise OpenRouterResponseSchemaError(
                "OpenRouter usage payload was present but not an object."
            )
        return OpenRouterUsage(
            prompt_tokens=_coerce_optional_int(payload.get("prompt_tokens")),
            completion_tokens=_coerce_optional_int(payload.get("completion_tokens")),
            total_tokens=_coerce_optional_int(payload.get("total_tokens")),
        )

    def _coerce_content_parts(self, content_parts: list[Any]) -> str:
        collected_text: list[str] = []
        for index, item in enumerate(content_parts):
            if isinstance(item, str):
                collected_text.append(item)
                continue
            if not isinstance(item, dict):
                raise OpenRouterResponseSchemaError(
                    f"OpenRouter message content part at index {index} was invalid."
                )
            if "text" in item and isinstance(item["text"], str):
                collected_text.append(item["text"])
                continue
            raise OpenRouterResponseSchemaError(
                "OpenRouter message content parts included a non-text fragment."
            )
        return "".join(collected_text)

    def _index_tools(
        self,
        tools: tuple[AgentTool, ...] | list[AgentTool],
    ) -> dict[str, AgentTool]:
        registry: dict[str, AgentTool] = {}
        for tool in tools:
            name = tool.definition.name
            if name in registry:
                raise ValueError(f"Duplicate agent tool name {name!r}.")
            registry[name] = tool
        return registry

    async def _execute_tool_calls(
        self,
        tool_calls: tuple[OpenRouterToolCall, ...],
        tool_registry: dict[str, AgentTool],
        *,
        parallel: bool,
    ) -> list[AgentToolExecution]:
        if parallel and len(tool_calls) > 1:
            executions = await asyncio.gather(
                *[
                    self._execute_one_tool_call(tool_call, tool_registry)
                    for tool_call in tool_calls
                ]
            )
            return list(executions)
        return [
            await self._execute_one_tool_call(tool_call, tool_registry)
            for tool_call in tool_calls
        ]

    async def _execute_one_tool_call(
        self,
        tool_call: OpenRouterToolCall,
        tool_registry: dict[str, AgentTool],
    ) -> AgentToolExecution:
        tool = tool_registry.get(tool_call.name)
        if tool is None:
            raise OpenRouterUnknownToolError(
                f"Model requested unknown tool {tool_call.name!r}."
            )

        try:
            arguments = json.loads(tool_call.arguments_json)
        except ValueError as exc:
            logger.error(
                "OpenRouter tool %s returned invalid JSON arguments: %r",
                tool_call.name,
                tool_call.arguments_json,
            )
            raise OpenRouterToolExecutionError(
                f"Tool {tool_call.name!r} received invalid JSON arguments from the model."
            ) from exc

        if not isinstance(arguments, dict):
            raise OpenRouterToolExecutionError(
                f"Tool {tool_call.name!r} expected an object of arguments, got "
                f"{type(arguments).__name__}."
            )

        try:
            result = tool.handler(arguments)
            if inspect.isawaitable(result):
                result = await result
        except Exception as exc:  # pragma: no cover - defensive logging path
            logger.exception("Agent tool %s raised unexpectedly.", tool_call.name)
            raise OpenRouterToolExecutionError(
                f"Tool {tool_call.name!r} raised an exception."
            ) from exc

        output_text = _serialize_tool_result(result)
        return AgentToolExecution(
            call_id=tool_call.call_id,
            tool_name=tool_call.name,
            arguments=arguments,
            output_text=output_text,
            tool_message=OpenRouterMessage(
                role="tool",
                tool_call_id=tool_call.call_id,
                content=output_text,
            ),
        )

    @staticmethod
    def _extract_error_message(response: httpx.Response) -> str:
        try:
            payload = response.json()
        except ValueError:
            return response.text[:500] or "unknown error"

        if isinstance(payload, dict):
            error = payload.get("error")
            if isinstance(error, dict):
                message = error.get("message")
                if isinstance(message, str) and message:
                    return message
            if isinstance(error, str) and error:
                return error
            message = payload.get("message")
            if isinstance(message, str) and message:
                return message
        return response.text[:500] or "unknown error"


def _coerce_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int):
        raise OpenRouterResponseSchemaError(
            f"Expected usage value to be an integer, got {type(value).__name__}."
        )
    return value


def _serialize_tool_result(result: Any) -> str:
    if isinstance(result, str):
        return result
    try:
        return json.dumps(result, separators=(",", ":"), sort_keys=True)
    except TypeError as exc:
        raise OpenRouterToolExecutionError(
            "Tool result was not JSON-serializable; return a string, dict, list, "
            "number, boolean, or null."
        ) from exc
