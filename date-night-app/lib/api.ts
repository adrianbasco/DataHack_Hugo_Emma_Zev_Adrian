import { fallbackPlans } from "./mockPlans";
import { fallbackTemplates } from "./mockTemplates";
import {
  DataResult,
  DateTemplate,
  GenerateChatRequest,
  GenerateFormRequest,
  Plan,
  PlanStop,
  RestaurantBookingJob,
  RestaurantBookingRequest,
  RestaurantBookingStatus,
  TemplateStop,
} from "./types";

const env = (globalThis as { process?: { env?: Record<string, string | undefined> } })
  .process?.env;
const API_BASE = (env?.EXPO_PUBLIC_API_BASE_URL || "").trim().replace(/\/+$/, "");

const ENDPOINTS = {
  templates: "/templates",
  formPlanner: "/dates/form",
  legacyFormPlanner: "/dates/generate",
  chatPlanner: "/dates/chat",
  bookingCreate: "/booking/restaurants",
  bookingStatus: (callId: string) => `/booking/restaurants/${callId}`,
};

export async function fetchTemplates(): Promise<DataResult<DateTemplate[]>> {
  if (!API_BASE) {
    console.warn("Template API base URL is not configured. Using bundled template fallback.");
    return {
      data: fallbackTemplates,
      source: "fallback",
      warning: "Template API is not configured yet, showing bundled templates.",
    };
  }

  try {
    const payload = await requestJson(ENDPOINTS.templates);
    const templates = normalizeTemplatesPayload(payload);
    return { data: templates, source: "api" };
  } catch (error) {
    console.warn("Template fetch failed. Falling back to bundled templates.", error);
    return {
      data: fallbackTemplates,
      source: "fallback",
      warning: toMessage(error, "Could not load live templates, showing bundled templates."),
    };
  }
}

export async function generatePlansFromForm(
  request: GenerateFormRequest
): Promise<DataResult<Plan[]>> {
  if (!API_BASE) {
    console.warn("Planner API base URL is not configured. Using local plan fallback.");
    return {
      data: makeFallbackPlans(request.selectedTemplateId, request.vibes),
      source: "fallback",
      warning: "Planner API is not configured yet, showing local demo plans.",
    };
  }

  const payload = {
    location: request.location,
    vibes: request.vibes,
    radius_km: request.radiusKm,
    budget: request.budget || undefined,
    transport_mode: request.transportMode,
    party_size: request.partySize,
    time_window: request.timeWindow || undefined,
    time_of_day: request.timeWindow || undefined,
    desired_idea_count: request.desiredIdeaCount,
    max_candidates: request.desiredIdeaCount,
    dietary_constraints: request.dietaryConstraints || undefined,
    accessibility_constraints: request.accessibilityConstraints || undefined,
    notes: request.notes || undefined,
    selected_template_id: request.selectedTemplateId || undefined,
    template_id: request.selectedTemplateId || undefined,
  };

  try {
    const payloadJson = await requestJson(ENDPOINTS.formPlanner, {
      method: "POST",
      body: JSON.stringify(payload),
    }).catch(async (error) => {
      if (isProbablyMissingEndpoint(error)) {
        return requestJson(ENDPOINTS.legacyFormPlanner, {
          method: "POST",
          body: JSON.stringify(payload),
        });
      }
      throw error;
    });

    return {
      data: normalizePlansPayload(payloadJson),
      source: "api",
    };
  } catch (error) {
    console.warn("Live form planner request failed. Using local fallback plans.", error);
    return {
      data: makeFallbackPlans(request.selectedTemplateId, request.vibes),
      source: "fallback",
      warning: toMessage(error, "Could not reach the live planner, showing local demo plans."),
    };
  }
}

export async function generatePlansFromChat(
  request: GenerateChatRequest
): Promise<DataResult<Plan[]>> {
  if (!API_BASE) {
    console.warn("Chat planner API base URL is not configured. Using local plan fallback.");
    return {
      data: makeFallbackPlans(request.selectedTemplateId, request.vibe ? [request.vibe] : []),
      source: "fallback",
      warning: "Chat planner API is not configured yet, showing local demo plans.",
    };
  }

  const payload = {
    prompt: request.prompt,
    transcript: request.transcript,
    messages: request.transcript,
    location: request.location || undefined,
    time_window: request.timeWindow || undefined,
    time_of_day: request.timeWindow || undefined,
    vibe: request.vibe || undefined,
    budget: request.budget || undefined,
    transport_mode: request.transportMode || undefined,
    party_size: request.partySize,
    constraints: request.constraints || undefined,
    desired_idea_count: request.desiredIdeaCount,
    max_candidates: request.desiredIdeaCount,
    selected_template_id: request.selectedTemplateId || undefined,
    template_id: request.selectedTemplateId || undefined,
  };

  try {
    const payloadJson = await requestJson(ENDPOINTS.chatPlanner, {
      method: "POST",
      body: JSON.stringify(payload),
    });

    return {
      data: normalizePlansPayload(payloadJson),
      source: "api",
    };
  } catch (error) {
    console.warn("Live chat planner request failed. Using local fallback plans.", error);
    return {
      data: makeFallbackPlans(request.selectedTemplateId, request.vibe ? [request.vibe] : []),
      source: "fallback",
      warning: toMessage(error, "Could not reach the live chat planner, showing local demo plans."),
    };
  }
}

export async function createRestaurantBooking(
  request: RestaurantBookingRequest
): Promise<RestaurantBookingJob> {
  const payload = {
    restaurant_name: request.restaurantName,
    restaurant_phone_number: request.restaurantPhoneNumber,
    arrival_time: request.arrivalTimeIso,
    party_size: request.partySize,
    booking_name: request.bookingName,
    customer_phone_number: request.customerPhoneNumber || undefined,
    restaurant_address: request.restaurantAddress || undefined,
    dietary_constraints: request.dietaryConstraints || undefined,
    accessibility_constraints: request.accessibilityConstraints || undefined,
    special_occasion: request.specialOccasion || undefined,
    notes: request.notes || undefined,
    acceptable_time_window_minutes: request.acceptableTimeWindowMinutes,
    plan_id: request.planId || undefined,
  };

  const result = await requestJson(ENDPOINTS.bookingCreate, {
    method: "POST",
    body: JSON.stringify(payload),
  });

  return normalizeBookingJob(result);
}

export async function fetchRestaurantBookingStatus(
  callId: string
): Promise<RestaurantBookingStatus> {
  const result = await requestJson(ENDPOINTS.bookingStatus(callId));
  return normalizeBookingStatus(result);
}

async function requestJson(path: string, init?: RequestInit) {
  if (!API_BASE) {
    throw new Error("Set EXPO_PUBLIC_API_BASE_URL to enable live backend requests.");
  }

  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers || {}),
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Request failed with status ${response.status}`);
  }

  return response.json();
}

function normalizeTemplatesPayload(payload: unknown): DateTemplate[] {
  const rawTemplates = Array.isArray(payload)
    ? payload
    : isObject(payload) && Array.isArray(payload.templates)
      ? payload.templates
      : isObject(payload) && isObject(payload.data) && Array.isArray(payload.data.templates)
        ? payload.data.templates
        : isObject(payload) && Array.isArray(payload.data)
          ? payload.data
      : [];

  return rawTemplates.map((template, index) => normalizeTemplate(template, index));
}

function normalizeTemplate(raw: unknown, index: number): DateTemplate {
  const template = isObject(raw) ? raw : {};
  const rawVibes = Array.isArray(template.vibe)
    ? template.vibe
    : Array.isArray(template.vibes)
      ? template.vibes
      : [];
  const rawStops = Array.isArray(template.stops) ? template.stops : [];

  return {
    id: stringOr(template.id, `template-${index}`),
    title: stringOr(template.title, `Template ${index + 1}`),
    vibes: rawVibes.map((value) => String(value)) as DateTemplate["vibes"],
    timeOfDay: stringOr(template.time_of_day ?? template.timeOfDay, "flexible") as DateTemplate["timeOfDay"],
    durationHours: numberOr(template.duration_hours ?? template.durationHours, 0),
    meaningfulVariations: numberOr(
      template.meaningful_variations ?? template.meaningfulVariations,
      0
    ),
    weatherSensitive: booleanOr(
      template.weather_sensitive ?? template.weatherSensitive,
      false
    ),
    description: stringOr(template.description, ""),
    stops: rawStops.map(normalizeTemplateStop),
  };
}

function normalizeTemplateStop(raw: unknown): TemplateStop {
  const stop = isObject(raw) ? raw : {};
  return {
    type: stringOr(stop.type, "unknown"),
    kind: (stop.kind === "connective" ? "connective" : "venue") as TemplateStop["kind"],
    note: optionalString(stop.note),
  };
}

function normalizePlansPayload(payload: unknown): Plan[] {
  const rawPlans = extractRawPlans(payload);
  if (!rawPlans.length) {
    console.error(
      "Planner payload did not include any usable plans. Falling back to bundled plans.",
      payload
    );
    return makeFallbackPlans();
  }
  return rawPlans.map((rawPlan, index) => normalizePlan(rawPlan, index, "api"));
}

function extractRawPlans(payload: unknown): unknown[] {
  if (Array.isArray(payload)) {
    return payload;
  }
  if (!isObject(payload)) {
    return [];
  }
  if (Array.isArray(payload.plans)) {
    return payload.plans;
  }
  if (Array.isArray(payload.ideas)) {
    return payload.ideas;
  }
  if (Array.isArray(payload.date_ideas)) {
    return payload.date_ideas;
  }
  if (isObject(payload.result)) {
    return extractRawPlans(payload.result);
  }
  if (isObject(payload.data)) {
    return extractRawPlans(payload.data);
  }
  return [];
}

function normalizePlan(raw: unknown, index: number, source: Plan["source"]): Plan {
  const plan = isObject(raw) ? raw : {};
  const rawStops = Array.isArray(plan.stops) ? plan.stops : [];
  const stops = rawStops.map((stop, stopIndex) => normalizePlanStop(stop, index, stopIndex));
  const templateHint = optionalString(plan.template_hint ?? plan.templateHint);
  const title = stringOr(plan.title, `Date idea ${index + 1}`);
  const hook = stringOr(plan.hook ?? plan.vibeLine ?? plan.summary, "A tailored date idea.");
  const bookingContext = extractBookingContext(plan, stops, index);

  return {
    id: stringOr(plan.id, `plan-${index + 1}-${slugify(title)}`),
    title,
    hook,
    summary: optionalString(plan.summary),
    vibes: arrayOfStrings(plan.vibes ?? plan.vibe),
    templateHint,
    templateId: optionalString(plan.template_id ?? plan.templateId ?? templateHint),
    durationLabel: optionalString(plan.duration_label ?? plan.durationLabel),
    costBand: optionalString(plan.cost_band ?? plan.costBand ?? plan.budget),
    weather: optionalString(plan.weather),
    heroImageUrl: optionalString(plan.hero_image_url ?? plan.heroImageUrl),
    mapsVerificationNeeded: booleanOr(
      plan.maps_verification_needed ?? plan.mapsVerificationNeeded,
      false
    ),
    constraintsConsidered: arrayOfStrings(
      plan.constraints_considered ?? plan.constraintsConsidered
    ),
    stops,
    transportLegs: normalizeTransportLegs(plan.transport_legs ?? plan.transportLegs),
    bookingContext,
    source,
  };
}

function normalizePlanStop(raw: unknown, planIndex: number, stopIndex: number): PlanStop {
  const stop = isObject(raw) ? raw : {};
  const stopType = stringOr(stop.stop_type ?? stop.stopType, "venue");
  return {
    id: stringOr(stop.id, `plan-${planIndex}-stop-${stopIndex}`),
    kind: stop.kind === "connective" ? "connective" : "venue",
    stopType,
    name: stringOr(stop.name, humanizeToken(stopType)),
    description: stringOr(stop.description, ""),
    whyItFits: optionalString(stop.why_it_fits ?? stop.whyItFits),
    fsqPlaceId: optionalString(stop.fsq_place_id ?? stop.fsqPlaceId),
    time: optionalString(stop.time),
    transport: optionalString(stop.transport),
    mapsUrl: optionalString(stop.maps_url ?? stop.mapsUrl ?? stop.google_maps_uri),
    address: optionalString(stop.address ?? stop.formatted_address),
    phoneNumber: optionalString(stop.phone_number ?? stop.restaurant_phone_number),
  };
}

function normalizeTransportLegs(raw: unknown): Plan["transportLegs"] {
  if (!Array.isArray(raw)) {
    return undefined;
  }
  return raw.map((leg, index) => {
    const record = isObject(leg) ? leg : {};
    return {
      mode: stringOr(record.mode, `Leg ${index + 1}`),
      durationText: stringOr(record.duration_text ?? record.durationText, ""),
    };
  });
}

function extractBookingContext(
  rawPlan: Record<string, unknown>,
  stops: PlanStop[],
  index: number
) {
  const restaurantStop =
    stops.find((stop) => stop.stopType.includes("restaurant")) ??
    stops.find((stop) => stop.stopType.includes("bar"));

  const restaurantName = optionalString(
    rawPlan.restaurant_name ?? rawPlan.restaurantName ?? restaurantStop?.name
  );
  const restaurantPhoneNumber = optionalString(
    rawPlan.restaurant_phone_number ??
      rawPlan.restaurantPhoneNumber ??
      restaurantStop?.phoneNumber
  );
  const restaurantAddress = optionalString(
    rawPlan.restaurant_address ?? rawPlan.restaurantAddress ?? restaurantStop?.address
  );

  if (!restaurantName && !restaurantPhoneNumber && !restaurantAddress) {
    return undefined;
  }

  return {
    planId: stringOr(rawPlan.plan_id ?? rawPlan.id, `plan-${index + 1}`),
    restaurantName,
    restaurantPhoneNumber,
    restaurantAddress,
    suggestedArrivalTimeIso: optionalString(
      rawPlan.arrival_time_iso ?? rawPlan.arrivalTimeIso
    ),
    partySize: numberOr(rawPlan.party_size ?? rawPlan.partySize, 2),
  };
}

function normalizeBookingJob(raw: unknown): RestaurantBookingJob {
  const payload = isObject(raw) ? raw : {};
  return {
    callId: stringOr(payload.call_id ?? payload.callId, "unknown-call"),
    status: stringOr(payload.status, "queued"),
    provider: stringOr(payload.provider, "unknown"),
    restaurantName: stringOr(payload.restaurant_name ?? payload.restaurantName, ""),
    restaurantPhoneNumber: stringOr(
      payload.restaurant_phone_number ?? payload.restaurantPhoneNumber,
      ""
    ),
    arrivalTimeIso: stringOr(payload.arrival_time ?? payload.arrivalTimeIso, ""),
    partySize: numberOr(payload.party_size ?? payload.partySize, 2),
  };
}

function normalizeBookingStatus(raw: unknown): RestaurantBookingStatus {
  const payload = isObject(raw) ? raw : {};
  return {
    callId: stringOr(payload.call_id ?? payload.callId, "unknown-call"),
    status: stringOr(payload.status, "unknown"),
    providerStatus: optionalString(payload.provider_status ?? payload.providerStatus),
    queueStatus: optionalString(payload.queue_status ?? payload.queueStatus),
    answeredBy: optionalString(payload.answered_by ?? payload.answeredBy),
    summary: optionalString(payload.summary),
    errorMessage: optionalString(payload.error_message ?? payload.errorMessage),
  };
}

function makeFallbackPlans(selectedTemplateId?: string, vibes: string[] = []): Plan[] {
  const scored = fallbackPlans
    .map((plan) => ({
      plan,
      score:
        (selectedTemplateId && plan.templateId === selectedTemplateId ? 4 : 0) +
        plan.vibes.filter((vibe) => vibes.includes(vibe)).length,
    }))
    .sort((left, right) => right.score - left.score)
    .map((item) => item.plan);

  return scored.slice(0, 4).map((plan) => ({
    ...plan,
    source: "fallback",
  }));
}

function humanizeToken(value: string) {
  return value
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function slugify(value: string) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "-");
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function stringOr(value: unknown, fallback: string) {
  return typeof value === "string" && value.trim() ? value : fallback;
}

function optionalString(value: unknown) {
  return typeof value === "string" && value.trim() ? value : undefined;
}

function numberOr(value: unknown, fallback: number) {
  return typeof value === "number" && Number.isFinite(value)
    ? value
    : typeof value === "string" && value.trim() && !Number.isNaN(Number(value))
      ? Number(value)
      : fallback;
}

function booleanOr(value: unknown, fallback: boolean) {
  return typeof value === "boolean" ? value : fallback;
}

function arrayOfStrings(value: unknown) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item)).filter(Boolean);
  }
  if (typeof value === "string" && value.trim()) {
    return [value];
  }
  return [];
}

function isProbablyMissingEndpoint(error: unknown) {
  const message = toMessage(error, "");
  return message.includes("404") || message.includes("Not Found");
}

function toMessage(error: unknown, fallback: string) {
  if (error instanceof Error && error.message.trim()) {
    return error.message;
  }
  return fallback;
}
