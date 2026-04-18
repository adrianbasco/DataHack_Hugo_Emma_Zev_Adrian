"""Typed models for OpenRouter-backed chat completions and tool use."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable


JsonValue = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
ToolHandler = Callable[[dict[str, Any]], Awaitable[JsonValue] | JsonValue]


@dataclass(frozen=True)
class OpenRouterFunctionTool:
    """User-defined function tool exposed to the model."""

    name: str
    description: str
    parameters_json_schema: dict[str, Any]
    strict: bool | None = None

    def to_api_dict(self) -> dict[str, Any]:
        function_payload: dict[str, Any] = {
            "name": self.name,
            "description": self.description,
            "parameters": self.parameters_json_schema,
        }
        if self.strict is not None:
            function_payload["strict"] = self.strict
        return {"type": "function", "function": function_payload}


@dataclass(frozen=True)
class OpenRouterServerTool:
    """OpenRouter-managed tool that executes server-side."""

    type: str
    parameters: dict[str, Any] | None = None

    def to_api_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"type": self.type}
        if self.parameters is not None:
            payload["parameters"] = self.parameters
        return payload


@dataclass(frozen=True)
class OpenRouterToolCall:
    """Tool call suggested by the model."""

    call_id: str
    name: str
    arguments_json: str
    raw_id: str | None = None


@dataclass(frozen=True)
class OpenRouterMessage:
    """Single chat-completions message."""

    role: str
    content: str | None = None
    name: str | None = None
    tool_call_id: str | None = None
    tool_calls: tuple[OpenRouterToolCall, ...] = field(default_factory=tuple)

    def to_api_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"role": self.role, "content": self.content}
        if self.name is not None:
            payload["name"] = self.name
        if self.tool_call_id is not None:
            payload["tool_call_id"] = self.tool_call_id
        if self.tool_calls:
            payload["tool_calls"] = [
                {
                    "id": tool_call.raw_id or tool_call.call_id,
                    "type": "function",
                    "function": {
                        "name": tool_call.name,
                        "arguments": tool_call.arguments_json,
                    },
                }
                for tool_call in self.tool_calls
            ]
        return payload


@dataclass(frozen=True)
class OpenRouterUsage:
    """Token usage returned by OpenRouter."""

    prompt_tokens: int | None
    completion_tokens: int | None
    total_tokens: int | None


@dataclass(frozen=True)
class OpenRouterChatResponse:
    """Parsed non-streaming chat completion result."""

    response_id: str | None
    client_request_id: str
    model: str
    finish_reason: str | None
    message: OpenRouterMessage
    usage: OpenRouterUsage | None

    @property
    def output_text(self) -> str | None:
        return self.message.content

    @property
    def tool_calls(self) -> tuple[OpenRouterToolCall, ...]:
        return self.message.tool_calls


@dataclass(frozen=True)
class AgentTool:
    """A callable tool made available to the agent loop."""

    definition: OpenRouterFunctionTool
    handler: ToolHandler


@dataclass(frozen=True)
class AgentToolExecution:
    """One executed tool call and the message fed back to the model."""

    call_id: str
    tool_name: str
    arguments: dict[str, Any]
    output_text: str
    tool_message: OpenRouterMessage


@dataclass(frozen=True)
class AgentRunResult:
    """Terminal result from the client-side tool execution loop."""

    final_response: OpenRouterChatResponse
    transcript: tuple[OpenRouterMessage, ...]
    tool_executions: tuple[AgentToolExecution, ...]


def make_json_schema_response_format(
    schema_name: str,
    schema: dict[str, Any],
    *,
    strict: bool = True,
) -> dict[str, Any]:
    """Build an OpenAI/OpenRouter-compatible JSON schema response format."""

    return {
        "type": "json_schema",
        "json_schema": {
            "name": schema_name,
            "strict": strict,
            "schema": schema,
        },
    }
