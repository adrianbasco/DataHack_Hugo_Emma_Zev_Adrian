import { GenerateRequest, Plan } from "./types";

const API_BASE = "http://YOUR_COMPUTER_IP:8000";

export async function generatePlans(payload: GenerateRequest): Promise<Plan[]> {
  const response = await fetch(`${API_BASE}/dates/generate`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || "Failed to generate plans");
  }

  const data = await response.json();
  return data.plans;
}