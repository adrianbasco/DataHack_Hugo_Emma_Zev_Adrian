import assert from "node:assert/strict";
import test from "node:test";

import {
  normalizeStoredPlan,
  parseGeneratePlansResponse,
  parsePlanDetailResponse,
  parseTemplatesResponse,
} from "../lib/contracts";

test("parseTemplatesResponse accepts the backend templates contract", () => {
  const templates = parseTemplatesResponse({
    templates: [
      {
        id: "sunset_dinner",
        title: "Sunset dinner",
        vibes: ["romantic", "foodie"],
        timeOfDay: "evening",
        durationHours: 3.5,
        meaningfulVariations: 12,
        weatherSensitive: true,
        description: "Golden hour, then dinner.",
        stops: [
          { type: "scenic_lookout", kind: "connective", note: "sunset stop" },
          { type: "restaurant", kind: "venue" },
        ],
      },
    ],
  });

  assert.equal(templates[0]?.id, "sunset_dinner");
  assert.equal(templates[0]?.stops[0]?.kind, "connective");
});

test("parseTemplatesResponse rejects legacy fallback-style payloads", () => {
  assert.throws(
    () =>
      parseTemplatesResponse([
        {
          id: "fallback-template",
        },
      ]),
    /templates response must be an object/
  );
});

test("parseGeneratePlansResponse enforces the exact generate contract", () => {
  const response = parseGeneratePlansResponse({
    plans: [
      {
        id: "plan-1",
        title: "Sunset Dinner",
        hook: "A Maps-verified date night plan.",
        summary: "Golden hour, then dinner.",
        vibes: ["romantic", "foodie"],
        templateHint: "Sunset dinner",
        templateId: "sunset_dinner",
        durationLabel: "3.5 hours",
        costBand: "$$",
        weather: null,
        heroImageUrl: "http://127.0.0.1:8000/static/precache-images/hero.jpg",
        mapsVerificationNeeded: false,
        constraintsConsidered: [],
        stops: [
          {
            id: "stop-1",
            kind: "venue",
            stopType: "restaurant",
            name: "Dinner Spot",
            description: "Dinner afterwards.",
            whyItFits: "The main meal stop.",
            fsqPlaceId: null,
            time: "7:00 PM",
            transport: "Walk · 9 min",
            mapsUrl: "https://maps.google.com/?cid=2",
            address: "99 George St",
            phoneNumber: null,
          },
        ],
        transportLegs: [{ mode: "Walk", durationText: "9 min" }],
        bookingContext: {
          planId: "plan-1",
          restaurantName: "Dinner Spot",
          restaurantPhoneNumber: "+61491114073",
          restaurantAddress: "99 George St",
          suggestedArrivalTimeIso: "2026-04-25T19:00:00+10:00",
          partySize: 2,
        },
        source: "api",
      },
    ],
    warnings: ["Budget filtering is not applied."],
    meta: {
      matchedCount: 1,
      returnedCount: 1,
      totalAvailable: 10,
    },
  });

  assert.equal(response.plans[0]?.source, "api");
  assert.deepEqual(response.warnings, ["Budget filtering is not applied."]);
});

test("parsePlanDetailResponse rejects a plan detail payload without a plan object", () => {
  assert.throws(
    () => parsePlanDetailResponse({}),
    /plan detail response\.plan must be an object/
  );
});

test("normalizeStoredPlan discards invalid stored plans instead of inventing defaults", () => {
  const plan = normalizeStoredPlan({
    id: "plan-1",
    title: "Cached plan",
    hook: "A cached copy.",
    constraintsConsidered: [],
    stops: [],
    source: "generated",
  });

  assert.equal(plan, null);
});
