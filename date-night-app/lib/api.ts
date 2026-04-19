import Constants from "expo-constants";
import { Platform } from "react-native";

import {
  parseBookingJobResponse,
  parseBookingPreviewResponse,
  parseBookingStatusResponse,
  parsePlanDetailResponse,
  parseSearchResponse,
  parseTemplatesResponse,
} from "./contracts";
import {
  buildSearchPayloadFromChat,
  mapSearchResponseToPlans,
} from "./searchFlow";
import {
  DataResult,
  DateTemplate,
  GenerateChatRequest,
  Plan,
  RestaurantBookingJob,
  RestaurantBookingPreview,
  RestaurantBookingRequest,
  RestaurantBookingStatus,
} from "./types";
import { resolveApiBaseUrl } from "./apiConfig";
import { reportClientError } from "./clientErrors";

const env = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process
  ?.env;

const API_BASE = resolveApiBaseUrl({
  env,
  expoDevServerHostUri: Constants.expoConfig?.hostUri,
  isDev: typeof __DEV__ === "boolean" ? __DEV__ : false,
  platformOS: Platform.OS,
});

const ENDPOINTS = {
  templates: "/templates",
  plansSearch: "/dates/search",
  planDetail: (planId: string) => `/dates/${encodeURIComponent(planId)}`,
  bookingPreview: "/booking/restaurants/preview",
  bookingCreate: "/booking/restaurants",
  bookingStatus: (callId: string) => `/booking/restaurants/${encodeURIComponent(callId)}`,
};

export async function fetchTemplates(): Promise<DataResult<DateTemplate[]>> {
  const payload = await requestJson(ENDPOINTS.templates);
  return {
    data: parseTemplatesResponse(payload),
    source: "api",
  };
}

export async function searchPlansFromChat(
  request: GenerateChatRequest
): Promise<DataResult<Plan[]>> {
  const payload = await requestJson(ENDPOINTS.plansSearch, {
    method: "POST",
    body: JSON.stringify(buildSearchPayloadFromChat(request)),
  });
  const response = mapSearchResponseToPlans(parseSearchResponse(payload), {
    requestPartySize: request.partySize,
  });
  return {
    data: response.plans,
    source: "api",
    warning: response.warning,
  };
}

export async function fetchPlan(planId: string): Promise<Plan> {
  const payload = await requestJson(ENDPOINTS.planDetail(planId));
  return parsePlanDetailResponse(payload);
}

export async function previewRestaurantBooking(
  request: RestaurantBookingRequest
): Promise<RestaurantBookingPreview> {
  const payload = await requestJson(ENDPOINTS.bookingPreview, {
    method: "POST",
    body: JSON.stringify(request),
  });
  return parseBookingPreviewResponse(payload);
}

export async function createRestaurantBooking(
  request: RestaurantBookingRequest
): Promise<RestaurantBookingJob> {
  const payload = await requestJson(ENDPOINTS.bookingCreate, {
    method: "POST",
    body: JSON.stringify(request),
  });
  return parseBookingJobResponse(payload);
}

export async function fetchRestaurantBookingStatus(
  callId: string
): Promise<RestaurantBookingStatus> {
  const payload = await requestJson(ENDPOINTS.bookingStatus(callId));
  return parseBookingStatusResponse(payload);
}

async function requestJson(path: string, init?: RequestInit) {
  if (!API_BASE) {
    const message =
      "Set EXPO_PUBLIC_API_BASE_URL for native builds, or run Expo in LAN mode so the app can infer the local backend URL.";
    reportClientError({
      source: "api.missing_base_url",
      error: new Error(message),
      context: { path },
    });
    throw new Error(message);
  }

  let response: Response;
  try {
    response = await fetch(`${API_BASE}${path}`, {
      ...init,
      headers: {
        "Content-Type": "application/json",
        ...(init?.headers || {}),
      },
    });
  } catch (error) {
    reportClientError({
      source: "api.network_error",
      error,
      context: { path, method: init?.method || "GET" },
    });
    throw error;
  }

  if (!response.ok) {
    const message = await parseError(response);
    const error = new Error(message);
    reportClientError({
      source: "api.http_error",
      error,
      context: {
        path,
        method: init?.method || "GET",
        status: response.status,
      },
    });
    throw error;
  }

  return response.json();
}

async function parseError(response: Response): Promise<string> {
  const contentType = response.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    try {
      const data = (await response.json()) as { detail?: unknown };
      if (typeof data.detail === "string" && data.detail.trim()) {
        return data.detail;
      }
      if (Array.isArray(data.detail) && data.detail.length > 0) {
        return JSON.stringify(data.detail);
      }
    } catch (error) {
      console.error("Failed to parse JSON error response from the API.", error);
    }
  }
  const text = await response.text();
  return text || `Request failed with status ${response.status}`;
}
