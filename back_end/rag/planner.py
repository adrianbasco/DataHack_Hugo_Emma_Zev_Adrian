"""Grounded date-plan candidate assembly from template-aware retrieval."""

from __future__ import annotations

from back_end.rag.models import DatePlanCandidate, StopRetrievalRequest, StopRetrievalResult
from back_end.rag.retriever import CONNECTIVE_STOP_TYPES, RagRetriever, build_stop_query


class DatePlanner:
    """Assemble candidate plans without inventing venues."""

    def __init__(self, retriever: RagRetriever) -> None:
        self._retriever = retriever

    async def build_candidate_plan(
        self,
        *,
        template: dict[str, object],
        user_query: str,
        top_k_per_stop: int = 5,
        city: str = "Sydney",
    ) -> DatePlanCandidate:
        """Retrieve grounded candidates for each stop in a template.

        ``city`` is forwarded to ``build_stop_query``. Date templates are
        location-agnostic; pass the target city here to localise search.
        """

        if top_k_per_stop <= 0:
            raise ValueError("top_k_per_stop must be positive.")
        template_id = str(template.get("id") or "")
        template_title = str(template.get("title") or template_id)
        stops = template.get("stops")
        if not isinstance(stops, list) or not stops:
            return DatePlanCandidate(
                template_id=template_id,
                template_title=template_title,
                stop_results=(),
                empty_reason=f"Template {template_id!r} has no stops.",
            )

        results: list[StopRetrievalResult] = []
        used_place_ids: set[str] = set()
        for stop in stops:
            if not isinstance(stop, dict) or not stop.get("type"):
                return DatePlanCandidate(
                    template_id=template_id,
                    template_title=template_title,
                    stop_results=tuple(results),
                    empty_reason=f"Template {template_id!r} contains a malformed stop.",
                )
            stop_type = str(stop["type"])
            if stop.get("kind") == "connective" or stop_type in CONNECTIVE_STOP_TYPES:
                results.append(
                    StopRetrievalResult(
                        stop_type=stop_type,
                        query_text=str(stop.get("note") or stop_type),
                        hits=(),
                        empty_reason="Connective stop; no venue retrieval required.",
                        is_connective=True,
                    )
                )
                continue

            query_text = build_stop_query(
                user_query=user_query,
                template=template,
                stop_type=stop_type,
                city=city,
            )
            result = await self._retriever.retrieve_stop(
                StopRetrievalRequest(
                    stop_type=stop_type,
                    query_text=query_text,
                    top_k=top_k_per_stop,
                )
            )
            filtered_hits = tuple(
                hit for hit in result.hits if hit.fsq_place_id not in used_place_ids
            )
            if not filtered_hits:
                return DatePlanCandidate(
                    template_id=template_id,
                    template_title=template_title,
                    stop_results=tuple(results + [result]),
                    empty_reason=(
                        f"Could not fill required stop {stop_type!r} for template "
                        f"{template_id!r}: {result.empty_reason or 'all hits were duplicates'}."
                    ),
                )
            used_place_ids.add(filtered_hits[0].fsq_place_id)
            results.append(
                StopRetrievalResult(
                    stop_type=result.stop_type,
                    query_text=result.query_text,
                    hits=filtered_hits,
                    empty_reason=None,
                    is_connective=False,
                )
            )

        return DatePlanCandidate(
            template_id=template_id,
            template_title=template_title,
            stop_results=tuple(results),
            empty_reason=None,
        )

