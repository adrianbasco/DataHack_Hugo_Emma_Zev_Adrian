import AsyncStorage from "@react-native-async-storage/async-storage";
import { fallbackPlans } from "./mockPlans";
import { Plan } from "./types";

const SAVED_PLANS_KEY = "saved_plans";
const GENERATED_PLANS_KEY = "generated_plans";

export async function savePlan(plan: Plan) {
  const existing = await getSavedPlans();
  const alreadyExists = existing.some((savedPlan) => savedPlan.id === plan.id);

  if (alreadyExists) {
    return;
  }

  const updated = [...existing, plan];
  await AsyncStorage.setItem(SAVED_PLANS_KEY, JSON.stringify(updated));
}

export async function getSavedPlans(): Promise<Plan[]> {
  return readPlans(SAVED_PLANS_KEY);
}

export async function cacheGeneratedPlans(plans: Plan[]) {
  await AsyncStorage.setItem(GENERATED_PLANS_KEY, JSON.stringify(plans));
}

export async function getGeneratedPlans(): Promise<Plan[]> {
  return readPlans(GENERATED_PLANS_KEY);
}

export async function getPlanById(id: string): Promise<Plan | undefined> {
  const generated = await getGeneratedPlans();
  const saved = await getSavedPlans();
  return (
    generated.find((plan) => plan.id === id) ||
    saved.find((plan) => plan.id === id) ||
    fallbackPlans.find((plan) => plan.id === id)
  );
}

async function readPlans(key: string): Promise<Plan[]> {
  const raw = await AsyncStorage.getItem(key);
  if (!raw) {
    return [];
  }

  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? (parsed as Plan[]) : [];
  } catch (error) {
    console.error(`Failed to parse plan cache for storage key "${key}".`, error);
    return [];
  }
}
