import {
  BlandCallDescription,
  BookingContext,
  DateTemplate,
  Plan,
  PlanStop,
  RestaurantBookingJob,
  RestaurantBookingPreview,
  RestaurantBookingStatus,
  RestaurantBookingStatusValue,
  TemplateStop,
  TimeOfDay,
} from "./types";

const TIME_OF_DAY_VALUES = new Set<TimeOfDay>([
  "morning",
  "midday",
  "afternoon",
  "evening",
  "night",
  "flexible",
]);
const BOOKING_STATUS_VALUES = new Set<RestaurantBookingStatusValue>([
  "queued",
  "in_progress",
  "confirmed",
  "declined",
  "no_answer",
  "needs_human_follow_up",
  "failed",
  "unknown",
]);

type GeneratePlansContract = {
  plans: Plan[];
  warnings: string[];
};

type SearchResponseContract = {
  parsed: {
    warnings: string[];
    autoAppliedNotes: string[];
  };
  results: Array<{
    planId: string;
    matchReasons: string[];
    card: Record<string, unknown>;
  }>;
  diagnostics: {
    warnings: string[];
    unsupportedConstraints: string[];
  };
};

export function parseTemplatesResponse(payload: unknown): DateTemplate[] {
  const root = expectObject(payload, "templates response");
  const rawTemplates = expectArray(root.templates, "templates response.templates");
  return rawTemplates.map((item, index) =>
    parseTemplate(item, `templates response.templates[${index}]`)
  );
}

export function parseGeneratePlansResponse(payload: unknown): GeneratePlansContract {
  const root = expectObject(payload, "generate response");
  const rawPlans = expectArray(root.plans, "generate response.plans");
  const warnings = expectOptionalStringArray(root.warnings, "generate response.warnings") ?? [];
  const meta = expectObject(root.meta, "generate response.meta");

  requireFiniteNumber(meta.matchedCount, "generate response.meta.matchedCount");
  requireFiniteNumber(meta.returnedCount, "generate response.meta.returnedCount");
  requireFiniteNumber(meta.totalAvailable, "generate response.meta.totalAvailable");

  return {
    plans: rawPlans.map((item, index) => parseApiPlan(item, `generate response.plans[${index}]`)),
    warnings,
  };
}

export function parsePlanDetailResponse(payload: unknown): Plan {
  const root = expectObject(payload, "plan detail response");
  return parseApiPlan(root.plan, "plan detail response.plan");
}

export function parseSearchResponse(payload: unknown): SearchResponseContract {
  const root = expectObject(payload, "search response");
  const parsed = expectObject(root.parsed, "search response.parsed");
  const diagnostics = expectObject(root.diagnostics, "search response.diagnostics");
  const rawResults = expectArray(root.results, "search response.results");

  return {
    parsed: {
      warnings: expectOptionalStringArray(parsed.warnings, "search response.parsed.warnings") ?? [],
      autoAppliedNotes:
        expectOptionalStringArray(
          parsed.auto_applied_notes,
          "search response.parsed.auto_applied_notes"
        ) ?? [],
    },
    results: rawResults.map((item, index) => {
      const result = expectObject(item, `search response.results[${index}]`);
      return {
        planId: requireString(result.plan_id, `search response.results[${index}].plan_id`),
        matchReasons:
          expectOptionalStringArray(
            result.match_reasons,
            `search response.results[${index}].match_reasons`
          ) ?? [],
        card: expectObject(result.card, `search response.results[${index}].card`),
      };
    }),
    diagnostics: {
      warnings:
        expectOptionalStringArray(
          diagnostics.warnings,
          "search response.diagnostics.warnings"
        ) ?? [],
      unsupportedConstraints:
        expectOptionalStringArray(
          diagnostics.unsupported_constraints,
          "search response.diagnostics.unsupported_constraints"
        ) ?? [],
    },
  };
}

export function parseBookingJobResponse(payload: unknown): RestaurantBookingJob {
  const root = expectObject(payload, "booking job response");
  return {
    callId: requireString(root.callId, "booking job response.callId"),
    status: requireBookingStatus(root.status, "booking job response.status"),
    provider: requireBookingProvider(root.provider, "booking job response.provider"),
    restaurantName: requireString(
      root.restaurantName,
      "booking job response.restaurantName"
    ),
    restaurantPhoneNumber: optionalNullableString(root.restaurantPhoneNumber),
    arrivalTimeIso: requireString(
      root.arrivalTimeIso,
      "booking job response.arrivalTimeIso"
    ),
    partySize: requireFiniteNumber(root.partySize, "booking job response.partySize"),
  };
}

export function parseBookingStatusResponse(payload: unknown): RestaurantBookingStatus {
  const root = expectObject(payload, "booking status response");
  return {
    callId: requireString(root.callId, "booking status response.callId"),
    status: requireBookingStatus(root.status, "booking status response.status"),
    providerStatus: optionalString(root.providerStatus),
    queueStatus: optionalString(root.queueStatus),
    answeredBy: optionalString(root.answeredBy),
    summary: optionalString(root.summary),
    errorMessage: optionalString(root.errorMessage),
  };
}

export function parseBookingPreviewResponse(payload: unknown): RestaurantBookingPreview {
  const root = expectObject(payload, "booking preview response");
  return {
    bookingContext: parseRequiredBookingContext(
      root.bookingContext,
      "booking preview response.bookingContext"
    ),
    callDescription: parseBlandCallDescription(
      root.callDescription,
      "booking preview response.callDescription"
    ),
    liveCallEnabled: requireBoolean(
      root.liveCallEnabled,
      "booking preview response.liveCallEnabled"
    ),
    liveCallDisabledReason: optionalNullableString(root.liveCallDisabledReason),
  };
}

export function normalizeStoredPlan(payload: unknown): Plan | null {
  try {
    return parseStoredPlan(payload);
  } catch (error) {
    console.error("Discarding invalid stored plan payload.", error, payload);
    return null;
  }
}

function parseStoredPlan(payload: unknown): Plan {
  const root = expectObject(payload, "stored plan");
  const source = root.source;
  if (source !== undefined && source !== "api" && source !== "fallback") {
    throw new Error("stored plan.source must be 'api' or 'fallback' when present.");
  }

  return parsePlan(root, "stored plan", source === "fallback" ? "fallback" : "api");
}

function parseApiPlan(payload: unknown, context: string): Plan {
  const root = expectObject(payload, context);
  if (root.source !== "api") {
    throw new Error(`${context}.source must be 'api'.`);
  }
  return parsePlan(root, context, "api");
}

function parsePlan(payload: Record<string, unknown>, context: string, source: Plan["source"]): Plan {
  const rawStops = expectArray(payload.stops, `${context}.stops`);

  return {
    id: requireString(payload.id, `${context}.id`),
    title: requireString(payload.title, `${context}.title`),
    hook: requireString(payload.hook, `${context}.hook`),
    summary: optionalString(payload.summary),
    vibes: expectOptionalStringArray(payload.vibes, `${context}.vibes`) ?? [],
    templateHint: optionalString(payload.templateHint),
    templateId: optionalString(payload.templateId),
    durationLabel: optionalString(payload.durationLabel),
    costBand: optionalString(payload.costBand),
    weather: optionalString(payload.weather),
    heroImageUrl: optionalString(payload.heroImageUrl),
    mapsVerificationNeeded: optionalBoolean(payload.mapsVerificationNeeded) ?? false,
    constraintsConsidered:
      expectOptionalStringArray(payload.constraintsConsidered, `${context}.constraintsConsidered`) ??
      [],
    stops: rawStops.map((item, index) => parsePlanStop(item, `${context}.stops[${index}]`)),
    transportLegs: parseTransportLegs(payload.transportLegs, `${context}.transportLegs`),
    bookingContext: parseBookingContext(payload.bookingContext, `${context}.bookingContext`),
    source,
  };
}

function parseTemplate(payload: unknown, context: string): DateTemplate {
  const root = expectObject(payload, context);
  const timeOfDay = requireString(root.timeOfDay, `${context}.timeOfDay`);
  if (!TIME_OF_DAY_VALUES.has(timeOfDay as TimeOfDay)) {
    throw new Error(`${context}.timeOfDay must be one of the supported values.`);
  }

  return {
    id: requireString(root.id, `${context}.id`),
    title: requireString(root.title, `${context}.title`),
    vibes: expectStringArray(root.vibes, `${context}.vibes`) as DateTemplate["vibes"],
    timeOfDay: timeOfDay as TimeOfDay,
    durationHours: requirePositiveNumber(root.durationHours, `${context}.durationHours`),
    meaningfulVariations: requireNonNegativeNumber(
      root.meaningfulVariations,
      `${context}.meaningfulVariations`
    ),
    weatherSensitive: requireBoolean(root.weatherSensitive, `${context}.weatherSensitive`),
    description: requireString(root.description, `${context}.description`),
    stops: expectArray(root.stops, `${context}.stops`).map((item, index) =>
      parseTemplateStop(item, `${context}.stops[${index}]`)
    ),
  };
}

function parseTemplateStop(payload: unknown, context: string): TemplateStop {
  const root = expectObject(payload, context);
  const kind = root.kind as TemplateStop["kind"] | undefined;
  if (kind !== undefined && kind !== "connective" && kind !== "venue") {
    throw new Error(`${context}.kind must be 'connective' or 'venue' when present.`);
  }
  return {
    type: requireString(root.type, `${context}.type`),
    kind,
    note: optionalString(root.note),
  };
}

function parsePlanStop(payload: unknown, context: string): PlanStop {
  const root = expectObject(payload, context);
  const kind = root.kind;
  if (kind !== "connective" && kind !== "venue") {
    throw new Error(`${context}.kind must be 'connective' or 'venue'.`);
  }
  return {
    id: requireString(root.id, `${context}.id`),
    kind,
    stopType: requireString(root.stopType, `${context}.stopType`),
    name: requireString(root.name, `${context}.name`),
    description: requireString(root.description, `${context}.description`),
    whyItFits: optionalString(root.whyItFits),
    fsqPlaceId: optionalNullableString(root.fsqPlaceId),
    time: optionalString(root.time),
    transport: optionalString(root.transport),
    mapsUrl: optionalString(root.mapsUrl),
    address: optionalString(root.address),
    phoneNumber: optionalString(root.phoneNumber),
  };
}

function parseTransportLegs(payload: unknown, context: string): Plan["transportLegs"] {
  if (payload === undefined) {
    return undefined;
  }
  return expectArray(payload, context).map((item, index) => {
    const root = expectObject(item, `${context}[${index}]`);
    return {
      mode: requireString(root.mode, `${context}[${index}].mode`),
      durationText: requireString(root.durationText, `${context}[${index}].durationText`),
    };
  });
}

function parseBookingContext(payload: unknown, context: string): BookingContext | undefined {
  if (payload === undefined || payload === null) {
    return undefined;
  }
  return parseRequiredBookingContext(payload, context);
}

function parseRequiredBookingContext(payload: unknown, context: string): BookingContext {
  const root = expectObject(payload, context);
  return {
    planId: optionalString(root.planId),
    restaurantName: optionalString(root.restaurantName),
    restaurantPhoneNumber: optionalString(root.restaurantPhoneNumber),
    restaurantAddress: optionalString(root.restaurantAddress),
    suggestedArrivalTimeIso: optionalString(root.suggestedArrivalTimeIso),
    partySize:
      root.partySize === undefined ? undefined : requireFiniteNumber(root.partySize, `${context}.partySize`),
  };
}

function parseBlandCallDescription(payload: unknown, context: string): BlandCallDescription {
  const root = expectObject(payload, context);
  return {
    provider: requireBookingProvider(root.provider, `${context}.provider`),
    phoneNumber: requireString(root.phoneNumber, `${context}.phoneNumber`),
    firstSentence: optionalNullableString(root.firstSentence),
    task: optionalNullableString(root.task),
    voice: optionalNullableString(root.voice),
    model: optionalNullableString(root.model),
    language: optionalNullableString(root.language),
    timezone: optionalNullableString(root.timezone),
    maxDurationMinutes:
      root.maxDurationMinutes === undefined || root.maxDurationMinutes === null
        ? undefined
        : requirePositiveNumber(root.maxDurationMinutes, `${context}.maxDurationMinutes`),
    waitForGreeting: requireBoolean(root.waitForGreeting, `${context}.waitForGreeting`),
    record: requireBoolean(root.record, `${context}.record`),
    voicemail:
      root.voicemail === undefined || root.voicemail === null
        ? undefined
        : expectObject(root.voicemail, `${context}.voicemail`),
    requestData: expectObject(root.requestData, `${context}.requestData`),
    metadata: expectObject(root.metadata, `${context}.metadata`),
    dispositions:
      expectOptionalStringArray(root.dispositions, `${context}.dispositions`) ?? [],
    keywords: expectOptionalStringArray(root.keywords, `${context}.keywords`) ?? [],
    summaryPrompt: optionalNullableString(root.summaryPrompt),
  };
}

function expectObject(value: unknown, context: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${context} must be an object.`);
  }
  return value as Record<string, unknown>;
}

function expectArray(value: unknown, context: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new Error(`${context} must be an array.`);
  }
  return value;
}

function expectStringArray(value: unknown, context: string): string[] {
  const items = expectArray(value, context);
  return items.map((item, index) => requireString(item, `${context}[${index}]`));
}

function expectOptionalStringArray(value: unknown, context: string): string[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  return expectStringArray(value, context);
}

function requireString(value: unknown, context: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${context} must be a non-empty string.`);
  }
  return value;
}

function requireBookingProvider(value: unknown, context: string): "bland_ai" {
  if (value !== "bland_ai") {
    throw new Error(`${context} must be 'bland_ai'.`);
  }
  return value;
}

function requireBookingStatus(
  value: unknown,
  context: string
): RestaurantBookingStatusValue {
  const status = requireString(value, context);
  if (!BOOKING_STATUS_VALUES.has(status as RestaurantBookingStatusValue)) {
    throw new Error(`${context} must be a supported booking status.`);
  }
  return status as RestaurantBookingStatusValue;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function optionalNullableString(value: unknown): string | null | undefined {
  if (value === null) {
    return null;
  }
  return optionalString(value);
}

function requireBoolean(value: unknown, context: string): boolean {
  if (typeof value !== "boolean") {
    throw new Error(`${context} must be a boolean.`);
  }
  return value;
}

function optionalBoolean(value: unknown): boolean | undefined {
  return typeof value === "boolean" ? value : undefined;
}

function requireFiniteNumber(value: unknown, context: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`${context} must be a finite number.`);
  }
  return value;
}

function requirePositiveNumber(value: unknown, context: string): number {
  const number = requireFiniteNumber(value, context);
  if (number <= 0) {
    throw new Error(`${context} must be greater than zero.`);
  }
  return number;
}

function requireNonNegativeNumber(value: unknown, context: string): number {
  const number = requireFiniteNumber(value, context);
  if (number < 0) {
    throw new Error(`${context} must be zero or greater.`);
  }
  return number;
}
