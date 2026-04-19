import assert from "node:assert/strict";
import test from "node:test";

import {
  buildChatPlannerRequest,
  DEFAULT_CHAT_IDEA_COUNT,
  DEFAULT_CHAT_PARTY_SIZE,
} from "../lib/chatPlanner";

test("buildChatPlannerRequest trims the prompt and creates a single user transcript entry", () => {
  const request = buildChatPlannerRequest({
    prompt: "  Quiet date in Newtown with dessert after dinner  ",
  });

  assert.equal(request.prompt, "Quiet date in Newtown with dessert after dinner");
  assert.equal(request.partySize, DEFAULT_CHAT_PARTY_SIZE);
  assert.equal(request.desiredIdeaCount, DEFAULT_CHAT_IDEA_COUNT);
  assert.deepEqual(request.transcript, [
    {
      id: "user-1",
      role: "user",
      content: "Quiet date in Newtown with dessert after dinner",
    },
  ]);
});

test("buildChatPlannerRequest carries template metadata without inventing extra chat fields", () => {
  const request = buildChatPlannerRequest({
    prompt: "Rain-proof anniversary plan",
    selectedTemplate: {
      id: "rainy_night",
      title: "Rainy Night",
      durationHours: 3.5,
      stops: [
        {
          type: "restaurant",
        },
      ],
    },
  });

  assert.equal(request.selectedTemplateId, "rainy_night");
  assert.equal(request.selectedTemplateTitle, "Rainy Night");
  assert.deepEqual(request.selectedTemplateStopTypes, ["restaurant"]);
  assert.equal(request.selectedTemplateDurationHours, 3.5);
  assert.equal(request.location, undefined);
  assert.equal(request.vibe, undefined);
  assert.equal(request.transportMode, undefined);
  assert.equal(request.budget, undefined);
});

test("buildChatPlannerRequest rejects an empty prompt", () => {
  assert.throws(
    () =>
      buildChatPlannerRequest({
        prompt: "   ",
      }),
    /Chat prompt must not be empty/
  );
});
