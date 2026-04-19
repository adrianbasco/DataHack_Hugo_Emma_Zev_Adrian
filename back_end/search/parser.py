"""Natural-language query parsing for cached-card retrieval."""

from __future__ import annotations

import json
import logging
import math
import re
from collections import Counter
from collections.abc import Iterable

from back_end.catalog import VIBES
from back_end.agents.date_idea_agent import (
    DEFAULT_DATE_IDEA_AGENT_MODEL,
    DEFAULT_REASONING_EFFORT,
)
from back_end.catalog.repository import PlacesRepository
from back_end.clients.openrouter import OpenRouterClient, OpenRouterClientError
from back_end.clients.settings import OpenRouterConfigurationError, OpenRouterSettings
from back_end.llm.models import OpenRouterMessage, make_json_schema_response_format
from back_end.search.models import (
    LocationInput,
    ParsedQuery,
    SearchContext,
    StructuredFilters,
    WeatherPreference,
)

logger = logging.getLogger(__name__)

TOKEN_PATTERN = re.compile(r"[a-z0-9']+")
POSTCODE_PATTERN = re.compile(r"\b\d{4}\b")

TIME_OF_DAY_RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\b(brunch|breakfast|this morning|morning)\b", re.I), "morning"),
    (re.compile(r"\b(lunch|midday|noon)\b", re.I), "midday"),
    (re.compile(r"\b(afternoon|this arvo|arvo)\b", re.I), "afternoon"),
    (re.compile(r"\b(tonight|this evening|evening|dinner|date night|after work)\b", re.I), "evening"),
    (re.compile(r"\b(late night|nightcap|night out|night)\b", re.I), "night"),
)
TRANSPORT_RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\b(walk|walking|on foot)\b", re.I), "walking"),
    (re.compile(r"\b(public transport|transit|train|tram|bus|ferry)\b", re.I), "public_transport"),
    (re.compile(r"\b(drive|driving|car|uber|taxi)\b", re.I), "driving"),
)
INDOOR_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(indoors only|indoor only|inside only|rainy day|keep it indoors|not outside)\b", re.I),
    re.compile(r"\b(indoor|indoors|inside)\b", re.I),
)
OUTDOOR_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(outside is fine|outdoors is fine|outdoor is fine)\b", re.I),
    re.compile(r"\b(outdoor|outdoors|outside|beach|picnic|hike|walk)\b", re.I),
)
TEMPLATE_HINT_PATTERNS: dict[str, re.Pattern[str]] = {
    "bookstore": re.compile(r"\b(bookstore|books|book shop)\b", re.I),
    "dessert": re.compile(r"\b(dessert|gelato|ice cream|sweet)\b", re.I),
    "rooftop": re.compile(r"\b(rooftop)\b", re.I),
    "beach": re.compile(r"\b(beach|ocean pool|swim)\b", re.I),
    "picnic": re.compile(r"\b(picnic)\b", re.I),
    "ferry": re.compile(r"\b(ferry)\b", re.I),
    "movie": re.compile(r"\b(movie|cinema)\b", re.I),
    "theatre": re.compile(r"\b(theatre|theater|show|comedy|live music)\b", re.I),
    "museum": re.compile(r"\b(museum|gallery|aquarium|zoo)\b", re.I),
    "brunch": re.compile(r"\b(brunch|breakfast|coffee)\b", re.I),
}


def _nullable_string_schema() -> dict[str, object]:
    return {"anyOf": [{"type": "string"}, {"type": "null"}]}


def _nullable_enum_schema(values: list[str]) -> dict[str, object]:
    return {
        "anyOf": [
            {"type": "string", "enum": values},
            {"type": "null"},
        ]
    }


LLM_RESPONSE_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "vibes": {
            "type": "array",
            "items": {"type": "string", "enum": sorted(VIBES)},
        },
        "time_of_day": _nullable_enum_schema(
            ["morning", "midday", "afternoon", "evening", "night", "flexible"]
        ),
        "weather_ok": _nullable_enum_schema(
            [WeatherPreference.INDOORS_ONLY.value, WeatherPreference.OUTDOORS_OK.value]
        ),
        "location_text": _nullable_string_schema(),
        "transport_mode": _nullable_enum_schema(
            ["walking", "public_transport", "driving"]
        ),
        "template_hints": {
            "type": "array",
            "items": {"type": "string"},
        },
        "free_text_residual": _nullable_string_schema(),
        "warnings": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "required": [
        "vibes",
        "time_of_day",
        "weather_ok",
        "location_text",
        "transport_mode",
        "template_hints",
        "free_text_residual",
        "warnings",
    ],
}


class QueryParser:
    """Parse free-form query text into structured filters."""

    def __init__(
        self,
        repository: PlacesRepository,
        *,
        llm_client: OpenRouterClient | None = None,
        model: str | None = DEFAULT_DATE_IDEA_AGENT_MODEL,
        reasoning_effort: str | None = DEFAULT_REASONING_EFFORT,
    ) -> None:
        self._repository = repository
        self._llm_client = llm_client
        self._model = model
        self._reasoning_effort = reasoning_effort
        self._known_localities = self._load_known_localities(repository)
        self._canonical_regions = self._load_canonical_regions(repository)

    async def parse(
        self,
        query: str | None,
        *,
        context: SearchContext | None = None,
    ) -> ParsedQuery:
        if query is None or not query.strip():
            return ParsedQuery()

        clean_query = query.strip()
        rule_parse = self._parse_with_rules(clean_query)
        llm_warnings: list[str] = []
        llm_parse: ParsedQuery | None = None
        llm_attempted = False

        llm_client, owns_llm_client = self._resolve_llm_client()
        if llm_client is None:
            llm_warnings.append(
                "LLM parser was unavailable; using deterministic query parsing only."
            )
        else:
            llm_attempted = True
            try:
                llm_parse = await self._parse_with_llm(
                    clean_query,
                    context=context,
                    llm_client=llm_client,
                )
            except (OpenRouterClientError, RuntimeError, ValueError, TypeError) as exc:
                logger.error("LLM query parsing failed for query=%r: %s", clean_query, exc)
                llm_warnings.append(f"LLM parser failed: {exc}")
            finally:
                if owns_llm_client:
                    await llm_client.aclose()

        merged = _merge_rule_and_llm(rule_parse, llm_parse)
        warnings = tuple(
            _dedupe_strings(
                [*merged.warnings, *llm_warnings]
            )
        )
        if (
            not merged.filters.vibes
            and merged.filters.time_of_day is None
            and merged.filters.weather_ok is None
            and merged.filters.location is None
            and merged.filters.transport_mode is None
            and not merged.filters.template_hints
        ):
            logger.warning("Parser returned no structured filters for query=%r.", clean_query)
            warnings = tuple(_dedupe_strings([*warnings, "No structured filters were extracted."]))

        return ParsedQuery(
            free_text_residual=merged.free_text_residual,
            filters=merged.filters,
            warnings=warnings,
            llm_attempted=llm_attempted,
            llm_succeeded=llm_parse is not None,
        )

    def _resolve_llm_client(self) -> tuple[OpenRouterClient | None, bool]:
        if self._llm_client is not None:
            return self._llm_client, False
        try:
            settings = OpenRouterSettings.from_env()
        except OpenRouterConfigurationError as exc:
            logger.warning("Search parser LLM disabled: %s", exc)
            return None, False
        return OpenRouterClient(settings), True

    async def _parse_with_llm(
        self,
        query: str,
        *,
        context: SearchContext | None,
        llm_client: OpenRouterClient,
    ) -> ParsedQuery:
        now_iso = context.now_iso if context is not None else None
        response = await llm_client.create_chat_completion(
            model=self._model,
            messages=(
                OpenRouterMessage(
                    role="system",
                    content=(
                        "You parse date-plan search queries into a compact JSON object. "
                        "Only extract fields that are clearly supported by the query. "
                        "Do not invent localities. Preserve any unmatched terms in "
                        "free_text_residual. If a relative time like tonight appears, "
                        f"use now_iso={now_iso!r} for interpretation."
                    ),
                ),
                OpenRouterMessage(
                    role="user",
                    content=(
                        "Return JSON only.\n"
                        f"query={query!r}\n"
                        f"known_vibes={sorted(VIBES)!r}\n"
                    ),
                ),
            ),
            temperature=0,
            response_format=make_json_schema_response_format(
                "search_query_parse",
                LLM_RESPONSE_SCHEMA,
            ),
            max_tokens=300,
            extra_body={
                "reasoning": {
                    "effort": self._reasoning_effort,
                    "exclude": True,
                }
            }
            if self._reasoning_effort is not None
            else None,
        )
        payload = json.loads(response.output_text or "{}")
        if not isinstance(payload, dict):
            raise ValueError("LLM parser returned a non-object payload.")
        return ParsedQuery(
            free_text_residual=_optional_text(payload.get("free_text_residual")),
            filters=StructuredFilters(
                vibes=_normalize_string_list(payload.get("vibes")),
                time_of_day=_optional_time_of_day(payload.get("time_of_day")),
                weather_ok=_optional_weather_preference(payload.get("weather_ok")),
                location=LocationInput(text=_optional_text(payload.get("location_text")))
                if _optional_text(payload.get("location_text")) is not None
                else None,
                transport_mode=_optional_transport_mode(payload.get("transport_mode")),
                template_hints=_normalize_string_list(payload.get("template_hints")),
            ),
            warnings=_normalize_string_list(payload.get("warnings")),
            llm_attempted=True,
            llm_succeeded=True,
        )

    def _parse_with_rules(self, query: str) -> ParsedQuery:
        lowered = query.casefold()
        phrases_to_strip: list[str] = []
        warnings: list[str] = []

        postcode_match = POSTCODE_PATTERN.search(query)
        matched_localities = self._matched_localities(lowered)
        location_text: str | None = None
        if postcode_match is not None:
            location_text = postcode_match.group(0)
            phrases_to_strip.append(location_text)
        elif matched_localities:
            matched_locality = matched_localities[0]
            canonical_region = self._canonical_regions.get(matched_locality.casefold())
            location_text = (
                f"{matched_locality}, {canonical_region}"
                if canonical_region is not None
                else matched_locality
            )
            phrases_to_strip.append(matched_locality)
            if len(matched_localities) > 1:
                warnings.append(
                    "Multiple locality names were detected; using the longest match."
                )

        vibes = tuple(sorted(vibe for vibe in VIBES if re.search(rf"\b{re.escape(vibe)}\b", lowered)))
        phrases_to_strip.extend(vibes)

        time_of_day: str | None = None
        for pattern, value in TIME_OF_DAY_RULES:
            if pattern.search(query):
                time_of_day = value
                phrases_to_strip.extend(pattern.findall(query))
                break

        transport_mode: str | None = None
        for pattern, value in TRANSPORT_RULES:
            if pattern.search(query):
                transport_mode = value
                phrases_to_strip.extend(pattern.findall(query))
                break

        weather_ok: WeatherPreference | None = None
        if any(pattern.search(query) for pattern in INDOOR_PATTERNS):
            weather_ok = WeatherPreference.INDOORS_ONLY
        elif any(pattern.search(query) for pattern in OUTDOOR_PATTERNS):
            weather_ok = WeatherPreference.OUTDOORS_OK

        template_hints = tuple(
            sorted(
                hint
                for hint, pattern in TEMPLATE_HINT_PATTERNS.items()
                if pattern.search(query)
            )
        )
        phrases_to_strip.extend(template_hints)
        residual = _strip_phrases(query, phrases_to_strip)
        if residual == "":
            residual = None

        return ParsedQuery(
            free_text_residual=residual,
            filters=StructuredFilters(
                vibes=vibes,
                time_of_day=time_of_day,
                weather_ok=weather_ok,
                location=LocationInput(text=location_text) if location_text is not None else None,
                transport_mode=transport_mode,
                template_hints=template_hints,
            ),
            warnings=tuple(warnings),
        )

    @staticmethod
    def _load_known_localities(repository: PlacesRepository) -> tuple[str, ...]:
        localities = repository.open_places_df["locality"].dropna().astype(str)
        unique = {value.strip() for value in localities if value and value.strip()}
        return tuple(sorted(unique, key=lambda item: (-len(item), item.casefold())))

    @staticmethod
    def _load_canonical_regions(repository: PlacesRepository) -> dict[str, str]:
        allowed_regions = {"nsw", "vic", "qld", "wa", "sa", "tas", "act", "nt"}
        frame = repository.open_places_df.loc[:, ["locality", "region"]].dropna()
        counts: dict[str, Counter[str]] = {}
        for locality, region in frame.itertuples(index=False):
            locality_text = str(locality).strip()
            region_text = str(region).strip().casefold()
            if not locality_text or region_text not in allowed_regions:
                continue
            key = locality_text.casefold()
            counter = counts.setdefault(key, Counter())
            counter[region_text] += 1

        canonical: dict[str, str] = {}
        for locality_key, counter in counts.items():
            region, count = counter.most_common(1)[0]
            if count <= 0:
                continue
            canonical[locality_key] = region.upper()
        return canonical

    def _matched_localities(self, lowered_query: str) -> tuple[str, ...]:
        matches: list[str] = []
        for locality in self._known_localities:
            pattern = rf"(?<![a-z0-9]){re.escape(locality.casefold())}(?![a-z0-9])"
            if re.search(pattern, lowered_query):
                if any(locality.casefold() in existing.casefold() for existing in matches):
                    continue
                matches.append(locality)
        return tuple(matches)


def _merge_rule_and_llm(rule_parse: ParsedQuery, llm_parse: ParsedQuery | None) -> ParsedQuery:
    if llm_parse is None:
        return rule_parse

    llm_filters = llm_parse.filters
    rule_filters = rule_parse.filters
    merged_filters = StructuredFilters(
        vibes=rule_filters.vibes or llm_filters.vibes,
        time_of_day=rule_filters.time_of_day or llm_filters.time_of_day,
        weather_ok=rule_filters.weather_ok or llm_filters.weather_ok,
        location=rule_filters.location or llm_filters.location,
        transport_mode=rule_filters.transport_mode or llm_filters.transport_mode,
        template_hints=tuple(
            _dedupe_strings([*rule_filters.template_hints, *llm_filters.template_hints])
        ),
    )
    residual = rule_parse.free_text_residual or llm_parse.free_text_residual
    return ParsedQuery(
        free_text_residual=residual,
        filters=merged_filters,
        warnings=tuple(_dedupe_strings([*rule_parse.warnings, *llm_parse.warnings])),
        llm_attempted=llm_parse.llm_attempted,
        llm_succeeded=llm_parse.llm_succeeded,
    )


def _normalize_string_list(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f"Expected a list of strings, got {type(value).__name__}.")
    result: list[str] = []
    for item in value:
        text = _optional_text(item)
        if text is not None:
            result.append(text)
    return tuple(_dedupe_strings(result))


def _optional_time_of_day(value: object) -> str | None:
    text = _optional_text(value)
    if text is None:
        return None
    normalized = text.casefold()
    if normalized not in {"morning", "midday", "afternoon", "evening", "night", "flexible"}:
        raise ValueError(f"Unsupported time_of_day {value!r}.")
    return normalized


def _optional_transport_mode(value: object) -> str | None:
    text = _optional_text(value)
    if text is None:
        return None
    normalized = text.casefold()
    if normalized not in {"walking", "public_transport", "driving"}:
        raise ValueError(f"Unsupported transport_mode {value!r}.")
    return normalized


def _optional_weather_preference(value: object) -> WeatherPreference | None:
    text = _optional_text(value)
    if text is None:
        return None
    normalized = text.casefold()
    if normalized == WeatherPreference.INDOORS_ONLY.value:
        return WeatherPreference.INDOORS_ONLY
    if normalized == WeatherPreference.OUTDOORS_OK.value:
        return WeatherPreference.OUTDOORS_OK
    raise ValueError(f"Unsupported weather preference {value!r}.")


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip()
    return text or None


def _strip_phrases(text: str, phrases: Iterable[str]) -> str:
    result = text
    for phrase in sorted(
        {item.strip() for item in phrases if isinstance(item, str) and item.strip()},
        key=len,
        reverse=True,
    ):
        result = re.sub(rf"\b{re.escape(phrase)}\b", " ", result, flags=re.I)
    result = re.sub(r"\s+", " ", result).strip(" ,.-")
    tokens = TOKEN_PATTERN.findall(result.casefold())
    return " ".join(tokens)


def _dedupe_strings(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = value.strip()
        key = text.casefold()
        if not text or key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result
