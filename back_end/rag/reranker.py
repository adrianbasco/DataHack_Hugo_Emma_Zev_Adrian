"""Deterministic reranking for semantic place hits."""

from __future__ import annotations

from dataclasses import replace

from back_end.rag.models import RagSearchHit


def rerank_hits(
    hits: tuple[RagSearchHit, ...],
    *,
    stop_type: str,
    query_text: str,
) -> tuple[RagSearchHit, ...]:
    """Apply transparent project-specific boosts on top of semantic similarity."""

    query_terms = _tokenize(query_text)
    reranked: list[RagSearchHit] = []
    for hit in hits:
        metadata = hit.metadata
        stop_tags = _string_set(metadata.get("crawl4ai_template_stop_tags"))
        categories = _string_set(metadata.get("fsq_category_labels"))
        ambience = _string_set(metadata.get("crawl4ai_ambience_tags"))
        setting = _string_set(metadata.get("crawl4ai_setting_tags"))
        activity = _string_set(metadata.get("crawl4ai_activity_tags"))
        drinks = _string_set(metadata.get("crawl4ai_drink_tags"))
        evidence = _string_set(metadata.get("crawl4ai_evidence_snippets"))

        breakdown = dict(hit.score_breakdown)
        if stop_type in stop_tags:
            breakdown["stop_tag_match"] = 0.18
        elif _stop_keyword_match(stop_type, categories):
            breakdown["category_keyword_match"] = 0.10
        else:
            breakdown["weak_stop_match_penalty"] = -0.20

        tag_terms = ambience | setting | activity | drinks
        tag_overlap = len(query_terms & tag_terms)
        if tag_overlap:
            breakdown["query_tag_overlap"] = min(0.12, 0.03 * tag_overlap)

        text_blob = " ".join(evidence).casefold()
        evidence_overlap = sum(1 for term in query_terms if term in text_blob)
        if evidence_overlap:
            breakdown["evidence_overlap"] = min(0.08, 0.02 * evidence_overlap)

        quality = _safe_int(metadata.get("crawl4ai_quality_score"))
        if quality > 0:
            breakdown["profile_quality"] = min(0.08, quality * 0.01)

        final_score = hit.semantic_score + sum(
            value for key, value in breakdown.items() if key != "semantic"
        )
        reranked.append(
            replace(
                hit,
                final_score=final_score,
                score_breakdown=breakdown,
            )
        )

    return tuple(sorted(reranked, key=lambda item: item.final_score, reverse=True))


def _tokenize(text: str) -> set[str]:
    return {
        token.strip(".,;:!?()[]{}\"'").casefold()
        for token in text.split()
        if len(token.strip(".,;:!?()[]{}\"'")) >= 3
    }


def _string_set(value: object) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        values = [value]
    else:
        try:
            values = list(value)  # type: ignore[arg-type]
        except TypeError:
            values = [value]
    return {str(item).strip().casefold() for item in values if str(item).strip()}


def _stop_keyword_match(stop_type: str, categories: set[str]) -> bool:
    stop_words = set(stop_type.casefold().split("_"))
    for category in categories:
        if stop_words & set(category.replace(">", " ").split()):
            return True
    return False


def _safe_int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0

