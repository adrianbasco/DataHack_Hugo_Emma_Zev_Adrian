import AsyncStorage from "@react-native-async-storage/async-storage";

import { normalizeStoredPlan } from "./contracts";
import { Plan } from "./types";

const SAVED_PLANS_KEY = "saved_plans";
const GENERATED_PLANS_KEY = "generated_plans";

export async function clearSavedPlans() {
  await AsyncStorage.removeItem(SAVED_PLANS_KEY);
}

export async function savePlan(plan: Plan) {
  const normalizedPlan = normalizeStoredPlan(plan);
  if (!normalizedPlan) {
    throw new Error("Refusing to save an invalid plan payload to local storage.");
  }

  const existing = await getSavedPlans();
  const alreadyExists = existing.some((savedPlan) => savedPlan.id === normalizedPlan.id);
  if (alreadyExists) {
    return;
  }

  await AsyncStorage.setItem(
    SAVED_PLANS_KEY,
    JSON.stringify([...existing, normalizedPlan])
  );
}

export async function getSavedPlans(): Promise<Plan[]> {
  return readPlans(SAVED_PLANS_KEY);
}

export async function cacheGeneratedPlans(plans: Plan[]) {
  const normalized = plans
    .map((plan) => normalizeStoredPlan(plan))
    .filter((plan): plan is Plan => plan !== null);

  await AsyncStorage.setItem(GENERATED_PLANS_KEY, JSON.stringify(normalized));
}

export async function getGeneratedPlans(): Promise<Plan[]> {
  return readPlans(GENERATED_PLANS_KEY);
}

export async function getPlanById(id: string): Promise<Plan | undefined> {
  const generated = await getGeneratedPlans();
  const saved = await getSavedPlans();

  return generated.find((plan) => plan.id === id) || saved.find((plan) => plan.id === id);
}

async function readPlans(key: string): Promise<Plan[]> {
  const raw = await AsyncStorage.getItem(key);
  if (!raw) {
    return [];
  }

  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      console.error(`Stored plan cache "${key}" did not contain an array.`);
      return [];
    }

    return parsed
      .map((plan) => normalizeStoredPlan(plan))
      .filter((plan): plan is Plan => plan !== null);
  } catch (error) {
    console.error(`Failed to parse plan cache for storage key "${key}".`, error);
    return [];
  }
}
