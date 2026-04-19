import assert from "node:assert/strict";
import test from "node:test";

import {
  buildSearchPayloadFromChat,
  buildSearchPayloadFromForm,
  mapSearchResponseToPlans,
  type SearchResponseContract,
} from "./searchFlow";

test("buildSearchPayloadFromForm maps structured fields into cached search overrides", () => {
  const payload = buildSearchPayloadFromForm(
    {
      location: "Surry Hills, NSW",
      vibes: ["romantic", "foodie"],
      radiusKm: 6,
      budget: "$$",
      transportMode: "walking",
      partySize: 2,
      timeWindow: "evening",
      desiredIdeaCount: 5,
      dietaryConstraints: "vegetarian",
      accessibilityConstraints: "low walking",
      notes: "quiet atmosphere",
      selectedTemplateId: "drinks_dinner_dessert",
      selectedTemplateTitle: "Drinks, Dinner, Dessert",
      selectedTemplateStopTypes: ["cocktail_bar", "restaurant", "dessert_shop"],
      selectedTemplateDurationHours: 3.5,
    },
    new Date("2026-04-19T18:00:00.000Z")
  );

  assert.equal(payload.context.limit, 5);
  assert.equal(payload.context.now_iso, "2026-04-19T18:00:00.000Z");
  assert.equal(payload.overrides?.time_of_day, "evening");
  assert.deepEqual(payload.overrides?.vibes, ["romantic", "foodie"]);
  assert.deepEqual(payload.overrides?.location, {
    text: "Surry Hills, NSW",
    radius_km: 6,
  });
  assert.equal(payload.overrides?.transport_mode, undefined);
  assert.ok(payload.overrides?.template_hints?.includes("Drinks, Dinner, Dessert"));
  assert.ok(payload.overrides?.template_hints?.includes("cocktail bar"));
  assert.match(payload.query ?? "", /quiet atmosphere/);
  assert.match(payload.query ?? "", /vegetarian/);
});

test("buildSearchPayloadFromChat keeps the natural-language prompt as cached search query", () => {
  const payload = buildSearchPayloadFromChat(
    {
      prompt: "Romantic dinner in Newtown tonight",
      transcript: [{ id: "user-1", role: "user", content: "Romantic dinner in Newtown tonight" }],
      location: "Newtown, NSW",
      timeWindow: "night",
      vibe: "romantic",
      budget: "$$",
      transportMode: "public_transport",
      partySize: 3,
      constraints: "not too loud",
      desiredIdeaCount: 4,
      selectedTemplateId: "dinner_then_dessert",
      selectedTemplateTitle: "Dinner then Dessert",
      selectedTemplateStopTypes: ["restaurant", "dessert_shop"],
      selectedTemplateDurationHours: 3,
    },
    new Date("2026-04-19T19:30:00.000Z")
  );

  assert.equal(payload.context.limit, 4);
  assert.equal(payload.overrides?.time_of_day, "night");
  assert.equal(payload.overrides?.transport_mode, undefined);
  assert.deepEqual(payload.overrides?.location, {
    text: "Newtown, NSW",
  });
  assert.deepEqual(payload.overrides?.vibes, ["romantic"]);
  assert.match(payload.query ?? "", /Romantic dinner in Newtown tonight/);
  assert.match(payload.query ?? "", /not too loud/);
});

test("mapSearchResponseToPlans adapts cached card payloads into swipe-deck plans", () => {
  const response: SearchResponseContract = {
    parsed: {
      warnings: ["Parser warning."],
      autoAppliedNotes: [],
    },
    results: [
      {
        planId: "plan-123",
        matchReasons: ["Matched query terms: dinner, romantic"],
        card: {
          plan_title: "Dinner and Dessert",
          plan_hook: "A polished date night.",
          plan_time_iso: "2026-04-24T19:00:00+10:00",
          template_id: "dinner_then_dessert",
          template_title: "Dinner then Dessert",
          template_description: "Classic pacing for an easy night.",
          template_duration_hours: 3.5,
          vibe: ["romantic", "foodie"],
          hero_image_url: "http://127.0.0.1:8000/static/precache-images/hero.jpg",
          feasibility: {
            all_legs_under_threshold: true,
            all_open_at_plan_time: true,
            all_venues_matched: true,
          },
          legs: [
            {
              transport_mode: "WALK",
              duration_seconds: 480,
            },
          ],
          stops: [
            {
              kind: "venue",
              stop_type: "restaurant",
              fsq_place_id: "restaurant-1",
              name: "The Dining Room",
              llm_description: "Seasonal menu in a quiet room.",
              why_it_fits: "It anchors the night.",
              address: "1 Date Street",
              google_maps_uri: "https://maps.google.com/?cid=1",
              booking_signals: ["third_party_booking"],
            },
            {
              kind: "venue",
              stop_type: "dessert_shop",
              fsq_place_id: "dessert-1",
              name: "Late Gelato",
              llm_description: "A short dessert finish.",
              address: "2 Date Street",
              google_maps_uri: "https://maps.google.com/?cid=2",
            },
          ],
        },
      },
    ],
    diagnostics: {
      warnings: ["Weather upstream warning."],
      unsupportedConstraints: ["hard budget filtering"],
    },
  };

  const normalized = mapSearchResponseToPlans(response, { requestPartySize: 3 });

  assert.equal(normalized.plans.length, 1);
  assert.match(normalized.warning ?? "", /Parser warning/);
  assert.match(normalized.warning ?? "", /Weather upstream warning/);
  assert.match(normalized.warning ?? "", /Unsupported constraint: hard budget filtering/);

  const [plan] = normalized.plans;
  assert.equal(plan.id, "plan-123");
  assert.equal(plan.title, "Dinner and Dessert");
  assert.equal(plan.templateHint, "Dinner then Dessert");
  assert.equal(plan.durationLabel, "3h 30m");
  assert.equal(plan.heroImageUrl, "http://127.0.0.1:8000/static/precache-images/hero.jpg");
  assert.equal(plan.mapsVerificationNeeded, false);
  assert.deepEqual(plan.vibes, ["romantic", "foodie"]);
  assert.equal(plan.transportLegs?.[0].mode, "WALK");
  assert.equal(plan.transportLegs?.[0].durationText, "8 min");
  assert.equal(plan.bookingContext?.restaurantName, "The Dining Room");
  assert.equal(plan.bookingContext?.partySize, 3);
  assert.equal(plan.stops[0].whyItFits, "It anchors the night.");
});
