import { Platform } from "react-native";

import { GenerateRequest, Plan } from "./types";

type GeneratePlansResponse = {
  plans: Plan[];
  warnings: string[];
  meta: {
    matchedCount: number;
    returnedCount: number;
    totalAvailable: number;
  };
};

type PlanDetailResponse = {
  plan: Plan;
};

function getApiBaseUrl(): string {
  const configured = process.env.EXPO_PUBLIC_API_BASE_URL?.trim();
  if (configured) {
    return configured.replace(/\/+$/, "");
  }
  if (Platform.OS === "web") {
    return "http://127.0.0.1:8000";
  }
  throw new Error(
    "EXPO_PUBLIC_API_BASE_URL is required for native builds so the app can reach the local API."
  );
}

async function parseError(response: Response): Promise<string> {
  const contentType = response.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    try {
      const data = (await response.json()) as { detail?: string };
      if (data.detail) {
        return data.detail;
      }
    } catch {}
  }
  const text = await response.text();
  return text || `Request failed with status ${response.status}`;
}

export async function generatePlans(payload: GenerateRequest): Promise<GeneratePlansResponse> {
  const response = await fetch(`${getApiBaseUrl()}/dates/generate`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    throw new Error(await parseError(response));
  }

  return (await response.json()) as GeneratePlansResponse;
}

export async function fetchPlan(planId: string): Promise<Plan> {
  const response = await fetch(`${getApiBaseUrl()}/dates/${encodeURIComponent(planId)}`);
  if (!response.ok) {
    throw new Error(await parseError(response));
  }
  const data = (await response.json()) as PlanDetailResponse;
  return data.plan;
}
