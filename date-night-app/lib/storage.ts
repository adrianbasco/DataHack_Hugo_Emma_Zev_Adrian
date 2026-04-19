import AsyncStorage from "@react-native-async-storage/async-storage";
import { fallbackPlans } from "./mockPlans";
import { Plan } from "./types";

const SAVED_PLANS_KEY = "saved_plans";
const GENERATED_PLANS_KEY = "generated_plans";
export async function clearSavedPlans() {
  await AsyncStorage.removeItem("saved_plans");
}

export async function savePlan(plan: Plan) {
  const existing = await getSavedPlans();
  const normalisedPlan = normalisePlan(plan);
  const alreadyExists = existing.some((savedPlan) => savedPlan.id === normalisedPlan.id);

  if (alreadyExists) {
    return;
  }

  const updated = [...existing, normalisedPlan];
  await AsyncStorage.setItem(SAVED_PLANS_KEY, JSON.stringify(updated));
}

export async function getSavedPlans(): Promise<Plan[]> {
  return readPlans(SAVED_PLANS_KEY);
}

export async function cacheGeneratedPlans(plans: Plan[]) {
  const normalised = Array.isArray(plans) ? plans.map(normalisePlan) : [];
  await AsyncStorage.setItem(GENERATED_PLANS_KEY, JSON.stringify(normalised));
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
    return Array.isArray(parsed) ? parsed.map(normalisePlan) : [];
  } catch (error) {
    console.error(`Failed to parse plan cache for storage key "${key}".`, error);
    return [];
  }
}

function normalisePlan(plan: any): Plan {
  return {
    ...plan,
    id: typeof plan?.id === "string" ? plan.id : `plan-${Math.random().toString(36).slice(2)}`,
    title: typeof plan?.title === "string" ? plan.title : "Untitled plan",
    hook: typeof plan?.hook === "string" ? plan.hook : "",
    summary: typeof plan?.summary === "string" ? plan.summary : "",
    vibes: Array.isArray(plan?.vibes) ? plan.vibes : [],
    templateHint: typeof plan?.templateHint === "string" ? plan.templateHint : "",
    templateId: typeof plan?.templateId === "string" ? plan.templateId : "",
    durationLabel: typeof plan?.durationLabel === "string" ? plan.durationLabel : "",
    costBand: typeof plan?.costBand === "string" ? plan.costBand : "",
    weather: typeof plan?.weather === "string" ? plan.weather : "",
    heroImageUrl: typeof plan?.heroImageUrl === "string" ? plan.heroImageUrl : "",
    mapsVerificationNeeded: Boolean(plan?.mapsVerificationNeeded),
    constraintsConsidered: Array.isArray(plan?.constraintsConsidered)
      ? plan.constraintsConsidered
      : [],
    stops: Array.isArray(plan?.stops) ? plan.stops : [],
    transportLegs: Array.isArray(plan?.transportLegs) ? plan.transportLegs : [],
    bookingContext:
      plan?.bookingContext && typeof plan.bookingContext === "object"
        ? plan.bookingContext
        : undefined,
    source: typeof plan?.source === "string" ? plan.source : "generated",
  };
}