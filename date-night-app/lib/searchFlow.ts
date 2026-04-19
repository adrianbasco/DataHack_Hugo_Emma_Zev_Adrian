import {
  GenerateChatRequest,
  GenerateFormRequest,
  Plan,
  PlanStop,
  TimeOfDay,
  TransportLeg,
} from "./types";

const VALID_TIME_OF_DAY = new Set<TimeOfDay>([
  "morning",
  "midday",
  "afternoon",
  "evening",
  "night",
  "flexible",
]);

export type SearchRequestPayload = {
  query?: string;
  context: {
    now_iso: string;
    limit: number;
  };
  overrides?: {
    vibes?: string[];
    time_of_day?: TimeOfDay;
    location?: {
      text: string;
      radius_km?: number;
    };
    transport_mode?: string;
    template_hints?: string[];
  };
};

export type SearchResponseContract = {
  parsed: {
    warnings: string[];
    autoAppliedNotes: string[];
  };
  results: SearchResultContract[];
  diagnostics: {
    warnings: string[];
    unsupportedConstraints: string[];
  };
};

export type SearchResultContract = {
  planId: string;
  matchReasons: string[];
  card: Record<string, unknown>;
};

export function buildSearchPayloadFromForm(
  request: GenerateFormRequest,
  now: Date = new Date()
): SearchRequestPayload {
  const templateHints = deriveTemplateHints(request);
  const query = compactQueryParts([
    request.vibes.join(" "),
    request.selectedTemplateTitle,
    ...templateHints,
    request.notes,
    request.dietaryConstraints,
    request.accessibilityConstraints,
  ]);

  return finalizeSearchPayload({
    query,
    limit: request.desiredIdeaCount,
    now,
    overrides: {
      vibes: normalizeStringList(request.vibes),
      time_of_day: normalizeTimeOfDay(request.timeWindow),
      location: buildLocationOverride(request.location, request.radiusKm),
      template_hints: templateHints,
    },
  });
}

export function buildSearchPayloadFromChat(
  request: GenerateChatRequest,
  now: Date = new Date()
): SearchRequestPayload {
  const templateHints = deriveTemplateHints(request);
  const query = compactQueryParts([
    request.prompt,
    request.constraints,
    request.vibe,
    request.selectedTemplateTitle,
    ...templateHints,
  ]);

  return finalizeSearchPayload({
    query,
    limit: request.desiredIdeaCount,
    now,
    overrides: {
      vibes: normalizeStringList(request.vibe ? [request.vibe] : []),
      time_of_day: normalizeTimeOfDay(request.timeWindow),
      location: buildLocationOverride(request.location),
      template_hints: templateHints,
    },
  });
}

export function mapSearchResponseToPlans(
  response: SearchResponseContract,
  options: {
    requestPartySize: number;
  }
): { plans: Plan[]; warning?: string } {
  const plans = response.results.map((result) =>
    mapSearchResultToPlan(result, options.requestPartySize)
  );
  const warnings = [
    ...response.parsed.warnings,
    ...response.diagnostics.warnings,
    ...response.diagnostics.unsupportedConstraints.map(
      (constraint) => `Unsupported constraint: ${constraint}`
    ),
  ];

  return {
    plans,
    warning: warnings.length > 0 ? dedupeStrings(warnings).join(" ") : undefined,
  };
}

type SearchPayloadDraft = {
  query?: string;
  limit: number;
  now: Date;
  overrides: {
    vibes?: string[];
    time_of_day?: TimeOfDay;
    location?: {
      text: string;
      radius_km?: number;
    };
    transport_mode?: string;
    template_hints?: string[];
  };
};

function finalizeSearchPayload(draft: SearchPayloadDraft): SearchRequestPayload {
  const overrides = compactOverrides(draft.overrides);
  const query = draft.query?.trim();

  if (!query && !overrides) {
    throw new Error(
      "Planner request did not include enough information to search the cached cards."
    );
  }

  return {
    query: query || undefined,
    context: {
      now_iso: draft.now.toISOString(),
      limit: clampLimit(draft.limit),
    },
    overrides,
  };
}

function compactOverrides(
  overrides: SearchPayloadDraft["overrides"]
): SearchRequestPayload["overrides"] | undefined {
  const compacted: NonNullable<SearchRequestPayload["overrides"]> = {};

  if (overrides.vibes?.length) {
    compacted.vibes = dedupeStrings(overrides.vibes);
  }
  if (overrides.time_of_day) {
    compacted.time_of_day = overrides.time_of_day;
  }
  if (overrides.location) {
    compacted.location = overrides.location;
  }
  if (overrides.transport_mode?.trim()) {
    compacted.transport_mode = overrides.transport_mode.trim();
  }
  if (overrides.template_hints?.length) {
    compacted.template_hints = dedupeStrings(overrides.template_hints);
  }

  return Object.keys(compacted).length > 0 ? compacted : undefined;
}

function buildLocationOverride(location?: string, radiusKm?: number) {
  const text = location?.trim();
  if (!text) {
    return undefined;
  }

  const normalizedRadius =
    typeof radiusKm === "number" && Number.isFinite(radiusKm) && radiusKm > 0
      ? radiusKm
      : undefined;

  return normalizedRadius === undefined
    ? { text }
    : {
        text,
        radius_km: normalizedRadius,
      };
}

function deriveTemplateHints(
  request:
    | Pick<GenerateFormRequest, "selectedTemplateTitle" | "selectedTemplateStopTypes">
    | Pick<GenerateChatRequest, "selectedTemplateTitle" | "selectedTemplateStopTypes">
) {
  return dedupeStrings([
    ...(request.selectedTemplateTitle ? [request.selectedTemplateTitle] : []),
    ...normalizeStringList(request.selectedTemplateStopTypes ?? []).map(humanizeToken),
  ]);
}

function mapSearchResultToPlan(
  result: SearchResultContract,
  requestPartySize: number
): Plan {
  const card = result.card;
  const planTitle = requireString(card.plan_title, `${result.planId}.card.plan_title`);
  const stops = parseStops(card.stops, result.planId);
  const hook = optionalString(card.plan_hook) || "A cached date idea matched your planner request.";
  const summary =
    optionalString(card.template_description) ||
    (result.matchReasons.length > 0 ? result.matchReasons.join(" · ") : undefined);
  const durationLabel = buildDurationLabel(
    card.template_duration_hours,
    stops.length
  );
  const transportLegs = parseTransportLegs(card.legs, result.planId);

  return {
    id: result.planId,
    title: planTitle,
    hook,
    summary,
    vibes: normalizeStringList(card.vibe),
    templateHint: optionalString(card.template_title) || optionalString(card.template_id),
    templateId: optionalString(card.template_id),
    durationLabel,
    costBand: undefined,
    weather: undefined,
    heroImageUrl: resolveHeroImageUrl(card, stops),
    mapsVerificationNeeded: mapsVerificationNeeded(card.feasibility),
    constraintsConsidered: [],
    stops,
    transportLegs: transportLegs.length > 0 ? transportLegs : undefined,
    bookingContext: buildBookingContext(card, stops, requestPartySize, result.planId),
    source: "api",
  };
}

function parseStops(rawStops: unknown, planId: string): PlanStop[] {
  if (!Array.isArray(rawStops)) {
    throw new Error(`${planId}.card.stops must be an array.`);
  }

  return rawStops
    .filter((stop) => {
      if (typeof stop !== "object" || stop === null || Array.isArray(stop)) {
        console.error("Discarding invalid stop payload from cached card.", planId, stop);
        return false;
      }
      return true;
    })
    .map((rawStop, index) => {
      const stop = rawStop as Record<string, unknown>;
      return {
        id:
          optionalString(stop.fsq_place_id) ||
          optionalString(stop.google_place_id) ||
          `${planId}-stop-${index + 1}`,
        kind: stop.kind === "connective" ? "connective" : "venue",
        stopType: optionalString(stop.stop_type) || "venue",
        name: requireString(stop.name, `${planId}.card.stops[${index}].name`),
        description: optionalString(stop.llm_description) || "",
        whyItFits: optionalString(stop.why_it_fits),
        fsqPlaceId: optionalString(stop.fsq_place_id) || null,
        time: undefined,
        transport: undefined,
        mapsUrl: optionalString(stop.google_maps_uri),
        address: optionalString(stop.address),
        phoneNumber: undefined,
      };
    });
}

function parseTransportLegs(rawLegs: unknown, planId: string): TransportLeg[] {
  if (!Array.isArray(rawLegs)) {
    return [];
  }

  return rawLegs
    .filter((leg) => {
      if (typeof leg !== "object" || leg === null || Array.isArray(leg)) {
        console.error("Discarding invalid transport leg payload from cached card.", planId, leg);
        return false;
      }
      return true;
    })
    .map((rawLeg, index) => {
      const leg = rawLeg as Record<string, unknown>;
      return {
        mode: humanizeToken(optionalString(leg.transport_mode) || `leg_${index + 1}`),
        durationText: formatDurationText(leg.duration_seconds),
      };
    });
}

function resolveHeroImageUrl(card: Record<string, unknown>, stops: PlanStop[]) {
  const directUrl = optionalString(card.hero_image_url);
  if (directUrl) {
    return directUrl;
  }

  const heroImage = optionalObject(card.hero_image);
  if (heroImage) {
    const heroUrl = optionalString(heroImage.public_url);
    if (heroUrl) {
      return heroUrl;
    }
  }

  for (const stop of stops) {
    const originalStop = (card.stops as Array<Record<string, unknown>>).find(
      (entry) => optionalString(entry.name) === stop.name
    );
    const primaryImage = optionalObject(originalStop?.primary_image);
    const primaryUrl = optionalString(primaryImage?.public_url);
    if (primaryUrl) {
      return primaryUrl;
    }
  }

  return undefined;
}

function buildBookingContext(
  card: Record<string, unknown>,
  stops: PlanStop[],
  requestPartySize: number,
  planId: string
) {
  const rawStops = Array.isArray(card.stops)
    ? (card.stops as Array<Record<string, unknown>>)
    : [];
  let chosenIndex = rawStops.findIndex((stop) =>
    (optionalString(stop.stop_type) || "").toLowerCase().includes("restaurant")
  );

  if (chosenIndex < 0) {
    chosenIndex = rawStops.findIndex((stop) => {
      const signals = normalizeStringList(stop.booking_signals).map((value) =>
        value.toLowerCase()
      );
      return signals.includes("booking") || signals.includes("third_party_booking");
    });
  }

  if (chosenIndex < 0) {
    return undefined;
  }

  const chosenStop = rawStops[chosenIndex];
  const matchedStop = stops[chosenIndex];
  if (!matchedStop) {
    console.error("Search result booking context pointed at a missing stop.", planId, chosenIndex);
    return undefined;
  }

  return {
    planId,
    restaurantName: matchedStop.name,
    restaurantPhoneNumber: undefined,
    restaurantAddress: matchedStop.address,
    suggestedArrivalTimeIso: optionalString(card.plan_time_iso),
    partySize: requestPartySize,
  };
}

function buildDurationLabel(rawDurationHours: unknown, stopCount: number) {
  const durationHours = optionalNumber(rawDurationHours);
  if (durationHours && durationHours > 0) {
    const totalMinutes = Math.max(90, Math.round(durationHours * 60));
    return formatDurationLabel(totalMinutes);
  }
  if (stopCount <= 0) {
    return undefined;
  }
  return formatDurationLabel(Math.max(90, stopCount * 60));
}

function formatDurationLabel(totalMinutes: number) {
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  if (minutes === 0) {
    return hours === 1 ? "1 hour" : `${hours} hours`;
  }
  if (hours === 0) {
    return `${minutes} mins`;
  }
  return `${hours}h ${minutes}m`;
}

function formatDurationText(rawDurationSeconds: unknown) {
  const durationSeconds = optionalNumber(rawDurationSeconds);
  if (!durationSeconds || durationSeconds <= 0) {
    return "Unknown";
  }

  const totalMinutes = Math.round(durationSeconds / 60);
  if (totalMinutes < 60) {
    return `${totalMinutes} min`;
  }

  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  return minutes === 0 ? `${hours} hr` : `${hours} hr ${minutes} min`;
}

function mapsVerificationNeeded(rawFeasibility: unknown) {
  const feasibility = optionalObject(rawFeasibility);
  if (!feasibility) {
    return false;
  }

  const flags = [
    feasibility.all_legs_under_threshold,
    feasibility.all_open_at_plan_time,
    feasibility.all_venues_matched,
  ].filter((value): value is boolean => typeof value === "boolean");

  return flags.length > 0 ? !flags.every(Boolean) : false;
}

function normalizeTimeOfDay(value?: string) {
  const normalized = value?.trim().toLowerCase();
  if (!normalized || !VALID_TIME_OF_DAY.has(normalized as TimeOfDay)) {
    return undefined;
  }
  return normalized as TimeOfDay;
}

function compactQueryParts(parts: Array<string | undefined>) {
  const normalized = parts
    .flatMap((part) => normalizeSearchPhrase(part))
    .filter((part) => part.length > 0);
  return normalized.length > 0 ? dedupeStrings(normalized).join(". ") : undefined;
}

function normalizeSearchPhrase(value?: string) {
  const trimmed = value?.trim();
  if (!trimmed) {
    return [];
  }
  return [trimmed.replace(/\s+/g, " ")];
}

function normalizeStringList(value: unknown) {
  if (!Array.isArray(value)) {
    return typeof value === "string" && value.trim() ? [value.trim()] : [];
  }
  return value
    .filter((item): item is string => typeof item === "string")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

function humanizeToken(value: string) {
  return value
    .split("_")
    .flatMap((part) => part.split(/\s+/))
    .map((part) => part.trim())
    .filter((part) => part.length > 0)
    .join(" ");
}

function dedupeStrings(values: string[]) {
  return Array.from(
    new Set(
      values
        .map((value) => value.trim())
        .filter((value) => value.length > 0)
    )
  );
}

function clampLimit(limit: number) {
  if (!Number.isFinite(limit)) {
    return 4;
  }
  return Math.min(50, Math.max(1, Math.round(limit)));
}

function requireString(value: unknown, context: string) {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${context} must be a non-empty string.`);
  }
  return value.trim();
}

function optionalString(value: unknown) {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function optionalNumber(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function optionalObject(value: unknown) {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return undefined;
  }
  return value as Record<string, unknown>;
}
