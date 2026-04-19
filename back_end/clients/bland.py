"""Async Bland AI client for outbound restaurant booking calls."""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Any

import httpx

from back_end.clients.settings import BlandAISettings

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})
E164_PHONE_RE = re.compile(r"^\+[1-9]\d{6,14}$")


class BlandAIClientError(RuntimeError):
    """Base class for Bland AI client failures."""


class BlandAIUpstreamError(BlandAIClientError):
    """Raised when Bland AI rejects a request or returns invalid HTTP."""


class BlandAIResponseSchemaError(BlandAIClientError):
    """Raised when Bland AI returns an unexpected payload shape."""


@dataclass(frozen=True)
class BlandCallRequest:
    """Request body for a Bland AI outbound call.

    Call creation is intentionally not retried by the client because a retry can
    create duplicate real-world phone calls.
    """

    phone_number: str
    task: str | None = None
    pathway_id: str | None = None
    pathway_version: int | None = None
    first_sentence: str | None = None
    voice: str | None = None
    model: str | None = None
    language: str | None = None
    timezone: str | None = None
    max_duration: int | None = None
    wait_for_greeting: bool = True
    record: bool = False
    voicemail: dict[str, Any] | None = None
    request_data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    webhook: str | None = None
    webhook_events: tuple[str, ...] = ()
    dispositions: tuple[str, ...] = ()
    keywords: tuple[str, ...] = ()
    summary_prompt: str | None = None

    def to_payload(self) -> dict[str, Any]:
        """Return a validated JSON payload for Bland AI."""

        _validate_e164(self.phone_number, "phone_number")
        task = _optional_str(self.task)
        pathway_id = _optional_str(self.pathway_id)
        if task is None and pathway_id is None:
            raise ValueError("Either task or pathway_id is required for a Bland AI call.")
        if task is not None and pathway_id is not None:
            raise ValueError("Bland AI calls must not specify both task and pathway_id.")
        if self.pathway_version is not None and self.pathway_version < 1:
            raise ValueError("pathway_version must be positive when provided.")
        if self.max_duration is not None and self.max_duration < 1:
            raise ValueError("max_duration must be at least 1 minute.")

        payload: dict[str, Any] = {
            "phone_number": self.phone_number,
            "wait_for_greeting": self.wait_for_greeting,
            "record": self.record,
        }
        _put_if_not_none(payload, "task", task)
        _put_if_not_none(payload, "pathway_id", pathway_id)
        _put_if_not_none(payload, "pathway_version", self.pathway_version)
        _put_if_not_none(payload, "first_sentence", _optional_str(self.first_sentence))
        _put_if_not_none(payload, "voice", _optional_str(self.voice))
        _put_if_not_none(payload, "model", _optional_str(self.model))
        _put_if_not_none(payload, "language", _optional_str(self.language))
        _put_if_not_none(payload, "timezone", _optional_str(self.timezone))
        _put_if_not_none(payload, "max_duration", self.max_duration)
        _put_if_not_none(payload, "voicemail", self.voicemail)
        _put_if_not_none(payload, "summary_prompt", _optional_str(self.summary_prompt))

        if self.request_data:
            payload["request_data"] = dict(self.request_data)
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        if self.webhook:
            payload["webhook"] = self.webhook
        if self.webhook_events:
            payload["webhook_events"] = list(self.webhook_events)
        if self.dispositions:
            payload["dispositions"] = list(self.dispositions)
        if self.keywords:
            payload["keywords"] = list(self.keywords)
        return payload


@dataclass(frozen=True)
class BlandCallQueued:
    """Normalized response returned after Bland AI queues a call."""

    call_id: str
    message: str | None
    batch_id: str | None


@dataclass(frozen=True)
class BlandCallDetails:
    """Subset of Bland AI call details used by booking status code."""

    call_id: str
    to: str | None
    from_number: str | None
    completed: bool | None
    queue_status: str | None
    status: str | None
    answered_by: str | None
    error_message: str | None
    summary: str | None
    disposition_tag: str | None
    concatenated_transcript: str | None
    request_data: dict[str, Any]
    metadata: dict[str, Any]
    raw_payload: dict[str, Any]


class BlandAIClient:
    """Thin async client around the Bland AI calls API."""

    def __init__(
        self,
        settings: BlandAISettings,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = settings
        self._http_client = http_client or httpx.AsyncClient()
        self._owns_http_client = http_client is None

    async def __aenter__(self) -> "BlandAIClient":
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
            "BlandAIClient.close() was called inside a running event loop. "
            "Use 'await client.aclose()' instead."
        )

    async def send_call(self, call_request: BlandCallRequest) -> BlandCallQueued:
        """Queue one outbound Bland AI call.

        This method deliberately makes a single HTTP attempt. If Bland receives
        the request but the response is lost, retrying here could call the same
        restaurant twice.
        """

        payload = await self._request_json(
            "POST",
            f"{self._settings.base_url.rstrip('/')}/calls",
            json=call_request.to_payload(),
            retry_count=0,
            operation="Bland AI call creation",
        )
        return _parse_call_queued(payload)

    async def get_call_details(self, call_id: str) -> BlandCallDetails:
        """Fetch Bland AI's current details for a queued or completed call."""

        normalized_call_id = call_id.strip()
        if not normalized_call_id:
            raise ValueError("call_id must not be empty.")

        payload = await self._request_json(
            "GET",
            f"{self._settings.base_url.rstrip('/')}/calls/{normalized_call_id}",
            retry_count=self._settings.status_retry_count,
            operation="Bland AI call details",
        )
        return _parse_call_details(payload)

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        retry_count: int,
        operation: str,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        attempts = retry_count + 1
        last_response: httpx.Response | None = None
        for attempt_index in range(attempts):
            response = await self._http_client.request(
                method,
                url,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "authorization": self._settings.api_key,
                },
                json=json,
                timeout=self._settings.timeout_seconds,
            )
            last_response = response
            if response.status_code < 400:
                break
            if (
                response.status_code in RETRYABLE_STATUS_CODES
                and attempt_index < retry_count
            ):
                logger.warning(
                    "%s failed with status=%s on attempt %s/%s; retrying.",
                    operation,
                    response.status_code,
                    attempt_index + 1,
                    attempts,
                )
                continue

            logger.error(
                "%s failed with status=%s body=%r",
                operation,
                response.status_code,
                response.text[:1000],
            )
            raise BlandAIUpstreamError(
                f"{operation} failed with status {response.status_code}."
            )

        if last_response is None:
            raise BlandAIUpstreamError(f"{operation} did not produce a response.")

        try:
            payload = last_response.json()
        except ValueError as exc:
            logger.error("%s returned non-JSON response: %r", operation, last_response.text[:1000])
            raise BlandAIResponseSchemaError(
                f"{operation} returned a non-JSON response."
            ) from exc

        if not isinstance(payload, dict):
            raise BlandAIResponseSchemaError(
                f"{operation} returned a top-level payload that was not an object."
            )
        if payload.get("status") == "error":
            message = _optional_str(payload.get("message")) or "unknown error"
            logger.error("%s returned explicit error payload: %r", operation, payload)
            raise BlandAIUpstreamError(f"{operation} was rejected: {message}.")
        return payload


def _parse_call_queued(payload: dict[str, Any]) -> BlandCallQueued:
    status = payload.get("status")
    if status != "success":
        raise BlandAIResponseSchemaError(
            f"Bland AI call creation response status was {status!r}, expected 'success'."
        )
    call_id = payload.get("call_id")
    if not isinstance(call_id, str) or not call_id.strip():
        raise BlandAIResponseSchemaError(
            "Bland AI call creation response did not include a valid call_id."
        )
    batch_id = payload.get("batch_id")
    if batch_id is not None and not isinstance(batch_id, str):
        raise BlandAIResponseSchemaError("Bland AI batch_id was not a string or null.")
    return BlandCallQueued(
        call_id=call_id,
        message=_optional_str(payload.get("message")),
        batch_id=batch_id,
    )


def _parse_call_details(payload: dict[str, Any]) -> BlandCallDetails:
    call_id = payload.get("call_id")
    if not isinstance(call_id, str) or not call_id.strip():
        raise BlandAIResponseSchemaError(
            "Bland AI call details did not include a valid call_id."
        )
    request_data = payload.get("request_data") or {}
    metadata = payload.get("metadata") or {}
    if not isinstance(request_data, dict):
        raise BlandAIResponseSchemaError("Bland AI request_data was not an object.")
    if not isinstance(metadata, dict):
        raise BlandAIResponseSchemaError("Bland AI metadata was not an object.")
    completed = payload.get("completed")
    if completed is not None and not isinstance(completed, bool):
        raise BlandAIResponseSchemaError("Bland AI completed field was not a bool.")

    return BlandCallDetails(
        call_id=call_id,
        to=_optional_str(payload.get("to")),
        from_number=_optional_str(payload.get("from")),
        completed=completed,
        queue_status=_optional_str(payload.get("queue_status")),
        status=_optional_str(payload.get("status")),
        answered_by=_optional_str(payload.get("answered_by")),
        error_message=_optional_str(payload.get("error_message")),
        summary=_optional_str(payload.get("summary")),
        disposition_tag=_optional_str(payload.get("disposition_tag")),
        concatenated_transcript=_optional_str(payload.get("concatenated_transcript")),
        request_data=dict(request_data),
        metadata=dict(metadata),
        raw_payload=dict(payload),
    )


def _validate_e164(value: str, field_name: str) -> None:
    if not E164_PHONE_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be in E.164 format, got {value!r}.")


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _put_if_not_none(payload: dict[str, Any], key: str, value: Any) -> None:
    if value is not None:
        payload[key] = value
