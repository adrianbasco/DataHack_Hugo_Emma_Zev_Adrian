"""Build API-facing card payloads for persisted pre-cache plans."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

_MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
_WHITESPACE_RE = re.compile(r"\s+")


def build_plan_card_payload(
    *,
    plan_title: str,
    plan_hook: str,
    plan_time_iso: str,
    bucket_id: str,
    bucket_label: str,
    template_id: str,
    template_title: str,
    template_description: str | None,
    vibe: Sequence[str],
    transport_mode: str,
    model: str,
    stops: Sequence[Mapping[str, Any]],
    verification: Mapping[str, Any],
    rag_documents: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    stop_verification = verification.get("stops_verification")
    verification_stops = (
        list(stop_verification)
        if isinstance(stop_verification, list)
        else []
    )
    card_stops: list[dict[str, Any]] = []
    for index, stop in enumerate(stops, start=1):
        matched_stop = (
            verification_stops[index - 1]
            if index - 1 < len(verification_stops)
            and isinstance(verification_stops[index - 1], Mapping)
            else {}
        )
        fsq_place_id = _optional_text(stop.get("fsq_place_id"))
        rag_row = rag_documents.get(fsq_place_id or "", {})
        source_summary = _clean_display_text(_source_summary(rag_row))
        evidence_snippets = [
            cleaned
            for cleaned in (
                _clean_display_text(snippet)
                for snippet in _string_list(rag_row.get("crawl4ai_evidence_snippets"))[:3]
            )
            if cleaned is not None
        ]
        categories = _string_list(rag_row.get("fsq_category_labels"))
        stop_payload: dict[str, Any] = {
            "index": index,
            "kind": _optional_text(stop.get("kind")),
            "stop_type": _optional_text(stop.get("stop_type")),
            "fsq_place_id": fsq_place_id,
            "name": _optional_text(stop.get("name")),
            "llm_description": _optional_text(stop.get("description")),
            "why_it_fits": _optional_text(stop.get("why_it_fits")),
            "source_summary": source_summary,
            "evidence_snippets": evidence_snippets,
            "categories": categories,
            "locality": _optional_text(rag_row.get("locality")),
            "region": _optional_text(rag_row.get("region")),
            "postcode": _optional_text(rag_row.get("postcode")),
            "address": _optional_text(rag_row.get("address"))
            or _optional_text(matched_stop.get("formatted_address")),
            "quality_score": _optional_int(rag_row.get("crawl4ai_quality_score")),
            "template_stop_tags": _string_list(rag_row.get("crawl4ai_template_stop_tags")),
            "ambience_tags": _string_list(rag_row.get("crawl4ai_ambience_tags")),
            "setting_tags": _string_list(rag_row.get("crawl4ai_setting_tags")),
            "activity_tags": _string_list(rag_row.get("crawl4ai_activity_tags")),
            "drink_tags": _string_list(rag_row.get("crawl4ai_drink_tags")),
            "booking_signals": _string_list(rag_row.get("crawl4ai_booking_signals")),
            "google_place_id": _optional_text(matched_stop.get("google_place_id")),
            "google_maps_uri": _optional_text(matched_stop.get("google_maps_uri")),
            "website_uri": _optional_text(matched_stop.get("website_uri")),
            "business_status": _optional_text(matched_stop.get("business_status")),
            "match_kind": _optional_text(matched_stop.get("match_kind")),
            "matched": matched_stop.get("matched"),
            "open_at_plan_time": matched_stop.get("open_at_plan_time"),
            "rating": _optional_number(matched_stop.get("rating")),
            "user_rating_count": _optional_int(matched_stop.get("user_rating_count")),
            "weekday_descriptions": _string_list(matched_stop.get("weekday_descriptions")),
            "location": _location_payload(matched_stop.get("location")),
            "primary_photo": _photo_payload(matched_stop.get("primary_photo")),
            "photos": _photo_list(matched_stop.get("photos")),
        }
        card_stops.append(stop_payload)

    search_text = _build_search_text(
        plan_title=plan_title,
        plan_hook=plan_hook,
        bucket_label=bucket_label,
        template_title=template_title,
        template_description=template_description,
        vibe=vibe,
        card_stops=card_stops,
    )
    return {
        "version": 1,
        "plan_title": plan_title,
        "plan_hook": plan_hook,
        "plan_time_iso": plan_time_iso,
        "bucket_id": bucket_id,
        "bucket_label": bucket_label,
        "template_id": template_id,
        "template_title": template_title,
        "template_description": template_description,
        "vibe": list(vibe),
        "transport_mode": transport_mode,
        "model": model,
        "search_text": search_text,
        "stops": card_stops,
        "legs": verification.get("legs", []),
        "feasibility": verification.get("feasibility", {}),
    }


def _build_search_text(
    *,
    plan_title: str,
    plan_hook: str,
    bucket_label: str,
    template_title: str,
    template_description: str | None,
    vibe: Sequence[str],
    card_stops: Sequence[Mapping[str, Any]],
) -> str:
    parts = [
        plan_title,
        plan_hook,
        template_title,
        template_description or "",
        bucket_label,
        " ".join(vibe),
    ]
    for stop in card_stops:
        parts.extend(
            [
                _optional_text(stop.get("name")) or "",
                _optional_text(stop.get("stop_type")) or "",
                _optional_text(stop.get("llm_description")) or "",
                _optional_text(stop.get("why_it_fits")) or "",
                _optional_text(stop.get("source_summary")) or "",
                " ".join(_string_list(stop.get("categories"))),
                " ".join(_string_list(stop.get("ambience_tags"))),
                " ".join(_string_list(stop.get("setting_tags"))),
                " ".join(_string_list(stop.get("activity_tags"))),
                " ".join(_string_list(stop.get("drink_tags"))),
                " ".join(_string_list(stop.get("evidence_snippets"))),
            ]
        )
    return "\n".join(part for part in parts if part)


def _source_summary(rag_row: Mapping[str, Any]) -> str | None:
    snippets = _string_list(rag_row.get("crawl4ai_evidence_snippets"))
    if snippets:
        return snippets[0]
    document_text = _optional_text(rag_row.get("document_text"))
    if document_text is None:
        return None
    if "Website profile:" in document_text:
        _, _, profile = document_text.partition("Website profile:")
        profile_text = _optional_text(profile)
        if profile_text is not None:
            return profile_text[:320]
    return document_text[:320]


def _clean_display_text(value: str | None) -> str | None:
    text = _optional_text(value)
    if text is None:
        return None
    text = _MARKDOWN_LINK_RE.sub(r"\1", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text or None


def _photo_payload(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    name = _optional_text(value.get("name"))
    if name is None:
        return None
    return {
        "name": name,
        "width_px": _optional_int(value.get("width_px")),
        "height_px": _optional_int(value.get("height_px")),
        "author_attributions": [
            {
                "display_name": _optional_text(item.get("display_name")),
                "uri": _optional_text(item.get("uri")),
                "photo_uri": _optional_text(item.get("photo_uri")),
            }
            for item in value.get("author_attributions", [])
            if isinstance(item, Mapping)
        ],
    }


def _photo_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    photos: list[dict[str, Any]] = []
    for item in value:
        payload = _photo_payload(item)
        if payload is not None:
            photos.append(payload)
    return photos


def _location_payload(value: Any) -> dict[str, float] | None:
    if not isinstance(value, Mapping):
        return None
    latitude = _optional_number(value.get("latitude"))
    longitude = _optional_number(value.get("longitude"))
    if latitude is None or longitude is None:
        return None
    return {
        "latitude": latitude,
        "longitude": longitude,
    }


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    try:
        items = list(value)
    except TypeError:
        return []
    out: list[str] = []
    for item in items:
        text = _optional_text(item)
        if text is not None:
            out.append(text)
    return out


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
