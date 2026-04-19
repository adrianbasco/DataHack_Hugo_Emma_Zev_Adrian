import { DateTemplate, GenerateChatRequest, PlannerMessage } from "./types";

export const DEFAULT_CHAT_PARTY_SIZE = 2;
export const DEFAULT_CHAT_IDEA_COUNT = 4;

type BuildChatPlannerRequestArgs = {
  prompt: string;
  selectedTemplate?: Pick<DateTemplate, "id" | "title" | "durationHours" | "stops">;
};

export function buildChatPlannerRequest({
  prompt,
  selectedTemplate,
}: BuildChatPlannerRequestArgs): GenerateChatRequest {
  const normalizedPrompt = prompt.trim();
  if (!normalizedPrompt) {
    throw new Error("Chat prompt must not be empty.");
  }

  return {
    prompt: normalizedPrompt,
    transcript: buildTranscript(normalizedPrompt),
    partySize: DEFAULT_CHAT_PARTY_SIZE,
    desiredIdeaCount: DEFAULT_CHAT_IDEA_COUNT,
    selectedTemplateId: selectedTemplate?.id,
    selectedTemplateTitle: selectedTemplate?.title,
    selectedTemplateStopTypes: selectedTemplate?.stops.map((stop) => stop.type),
    selectedTemplateDurationHours: selectedTemplate?.durationHours,
  };
}

function buildTranscript(prompt: string): PlannerMessage[] {
  return [
    {
      id: "user-1",
      role: "user",
      content: prompt,
    },
  ];
}
