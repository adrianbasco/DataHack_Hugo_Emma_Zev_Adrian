import AsyncStorage from "@react-native-async-storage/async-storage";
import { Plan } from "./types";

const SAVED_PLANS_KEY = "saved_plans";

export async function savePlan(plan: Plan) {
  const existing = await getSavedPlans();
  const alreadyExists = existing.some((p) => p.id === plan.id);

  if (alreadyExists) return;

  const updated = [...existing, plan];
  await AsyncStorage.setItem(SAVED_PLANS_KEY, JSON.stringify(updated));
}

export async function getSavedPlans(): Promise<Plan[]> {
  const raw = await AsyncStorage.getItem(SAVED_PLANS_KEY);
  if (!raw) return [];
  return JSON.parse(raw);
}