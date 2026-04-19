import { BookingContext, Plan } from "./types";

export function mergePlanWithCachedContext(plan: Plan, cachedPlan?: Plan): Plan {
  if (!cachedPlan) {
    return plan;
  }

  return {
    ...plan,
    summary: plan.summary ?? cachedPlan.summary,
    durationLabel: plan.durationLabel ?? cachedPlan.durationLabel,
    costBand: plan.costBand ?? cachedPlan.costBand,
    weather: plan.weather ?? cachedPlan.weather,
    heroImageUrl: plan.heroImageUrl ?? cachedPlan.heroImageUrl,
    constraintsConsidered:
      plan.constraintsConsidered.length > 0
        ? plan.constraintsConsidered
        : cachedPlan.constraintsConsidered,
    transportLegs:
      plan.transportLegs && plan.transportLegs.length > 0
        ? plan.transportLegs
        : cachedPlan.transportLegs,
    bookingContext: mergeBookingContext(plan.bookingContext, cachedPlan.bookingContext),
  };
}

function mergeBookingContext(
  primary?: BookingContext,
  cached?: BookingContext
): BookingContext | undefined {
  if (!primary && !cached) {
    return undefined;
  }

  return {
    planId: primary?.planId ?? cached?.planId,
    restaurantName: primary?.restaurantName ?? cached?.restaurantName,
    restaurantPhoneNumber:
      primary?.restaurantPhoneNumber ?? cached?.restaurantPhoneNumber,
    restaurantAddress: primary?.restaurantAddress ?? cached?.restaurantAddress,
    suggestedArrivalTimeIso:
      primary?.suggestedArrivalTimeIso ?? cached?.suggestedArrivalTimeIso,
    partySize: primary?.partySize ?? cached?.partySize,
  };
}
