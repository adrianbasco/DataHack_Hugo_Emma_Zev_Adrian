"""Helpers for turning RAG document rows into domain candidate places."""

from __future__ import annotations

import logging
import math
from collections.abc import Mapping
from typing import Any

from back_end.domain.models import CandidatePlace

logger = logging.getLogger(__name__)


class RagCandidateRowError(ValueError):
    """Raised when a RAG document row cannot identify a Maps candidate."""


def candidate_place_from_rag_row(row: Mapping[str, Any]) -> CandidatePlace:
    """Build a Maps candidate from one RAG document row."""

    fsq_place_id = _required_text(row, "fsq_place_id")
    name = _required_text(row, "name")
    latitude = _required_float(row, "latitude", fsq_place_id=fsq_place_id)
    longitude = _required_float(row, "longitude", fsq_place_id=fsq_place_id)
    return CandidatePlace(
        fsq_place_id=fsq_place_id,
        name=name,
        latitude=latitude,
        longitude=longitude,
        address=_optional_text(row.get("address")),
        locality=_optional_text(row.get("locality")),
        region=_optional_text(row.get("region")),
        postcode=_optional_text(row.get("postcode")),
        fsq_category_ids=tuple(_string_list(row.get("fsq_category_ids"))),
    )


def _required_text(row: Mapping[str, Any], field_name: str) -> str:
    value = row.get(field_name)
    if _is_missing(value) or not str(value).strip():
        logger.error("RAG document row is missing required field %s.", field_name)
        raise RagCandidateRowError(f"RAG document row is missing {field_name}.")
    return str(value).strip()


def _required_float(
    row: Mapping[str, Any],
    field_name: str,
    *,
    fsq_place_id: str,
) -> float:
    value = row.get(field_name)
    if _is_missing(value):
        logger.error(
            "RAG document row for fsq_place_id=%s is missing %s.",
            fsq_place_id,
            field_name,
        )
        raise RagCandidateRowError(
            f"RAG document row for {fsq_place_id!r} is missing {field_name}."
        )
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        logger.error(
            "RAG document row for fsq_place_id=%s has non-numeric %s=%r.",
            fsq_place_id,
            field_name,
            value,
        )
        raise RagCandidateRowError(
            f"RAG document row for {fsq_place_id!r} has invalid {field_name}."
        ) from exc
    if not math.isfinite(parsed):
        logger.error(
            "RAG document row for fsq_place_id=%s has non-finite %s=%r.",
            fsq_place_id,
            field_name,
            value,
        )
        raise RagCandidateRowError(
            f"RAG document row for {fsq_place_id!r} has invalid {field_name}."
        )
    return parsed


def _optional_text(value: Any) -> str | None:
    if _is_missing(value):
        return None
    text = str(value).strip()
    return text or None


def _string_list(value: Any) -> list[str]:
    if _is_missing(value):
        return []
    if isinstance(value, str):
        values = [value]
    else:
        try:
            values = list(value)
        except TypeError:
            values = [value]
    return [str(item).strip() for item in values if not _is_missing(item) and str(item).strip()]


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(value != value)
    except Exception:
        return False
