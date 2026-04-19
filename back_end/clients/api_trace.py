"""Append-only request/response tracing for outbound API calls."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

logger = logging.getLogger(__name__)

_REDACTED = "__REDACTED__"
_SECRET_HEADER_NAMES = frozenset({"authorization", "x-goog-api-key"})


@dataclass(slots=True)
class ApiTraceLogger:
    """Write one JSON object per outbound HTTP exchange."""

    path: Path
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)

    async def log_http_exchange(
        self,
        *,
        service: str,
        method: str,
        url: str,
        request_headers: Mapping[str, Any] | None,
        request_body: Any = None,
        attempt: int | None = None,
        response: Any | None = None,
        error: BaseException | None = None,
        duration_ms: float | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        entry: dict[str, Any] = {
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "service": service,
            "request": {
                "method": method,
                "url": url,
                "headers": _sanitize_headers(request_headers),
                "body": _normalize_jsonish_value(request_body),
            },
        }
        if attempt is not None:
            entry["attempt"] = attempt
        if duration_ms is not None:
            entry["duration_ms"] = round(duration_ms, 2)
        if metadata:
            entry["metadata"] = _normalize_jsonish_value(dict(metadata))
        if response is not None:
            entry["response"] = {
                "status_code": getattr(response, "status_code", None),
                "headers": _sanitize_headers(getattr(response, "headers", None)),
                "body": _response_body(response),
            }
        if error is not None:
            entry["error"] = {
                "type": type(error).__name__,
                "message": str(error),
            }
        serialized = json.dumps(entry, ensure_ascii=True, sort_keys=True)
        async with self._lock:
            try:
                with self.path.open("a", encoding="utf-8") as handle:
                    handle.write(serialized)
                    handle.write("\n")
            except OSError:
                logger.exception("Failed to append API trace entry at %s.", self.path)


def _sanitize_headers(headers: Mapping[str, Any] | None) -> dict[str, str]:
    if not headers:
        return {}
    sanitized: dict[str, str] = {}
    for name, value in headers.items():
        clean_name = str(name)
        if clean_name.casefold() in _SECRET_HEADER_NAMES:
            sanitized[clean_name] = _REDACTED
        else:
            sanitized[clean_name] = str(value)
    return sanitized


def _normalize_jsonish_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {
            str(key): _normalize_jsonish_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_normalize_jsonish_value(item) for item in value]
    return repr(value)


def _response_body(response: Any) -> Any:
    try:
        return _normalize_jsonish_value(response.json())
    except ValueError:
        return response.text
