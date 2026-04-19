import { useEffect, useMemo, useState, type ReactNode } from "react";
import { StyleSheet, Text, TextInput, View } from "react-native";
import {
  DateTemplate,
  GenerateChatRequest,
  PlannerMessage,
  TransportMode,
  Vibe,
} from "../lib/types";
import {
  ActionButton,
  Eyebrow,
  SelectChip,
  SurfaceCard,
  palette,
} from "./ui";

type Props = {
  selectedTemplate?: DateTemplate;
  onSubmit: (payload: GenerateChatRequest) => void;
};

const starterPrompts = [
  "Romantic but not too formal",
  "Rain-proof date for two tonight",
  "Something playful with food",
  "A date that feels special on a budget",
];

const followUps = [
  "Nice direction. Which area should I optimize for, and is there a hard budget ceiling?",
  "Helpful. What matters more from here: scenery, food, activity, or low effort logistics?",
  "Great. Any hard no's like loud bars, lots of walking, or weather risk?",
  "That is enough to brief the planner. Add anything else you care about, or generate ideas now.",
];

const vibeOptions: Vibe[] = [
  "romantic",
  "foodie",
  "nightlife",
  "nerdy",
  "outdoorsy",
  "active",
  "casual",
];

export default function ChatPlanner({ selectedTemplate, onSubmit }: Props) {
  const [messages, setMessages] = useState<PlannerMessage[]>([
    {
      id: "assistant-0",
      role: "assistant",
      content:
        "Tell me what kind of date you want. Mention the vibe, location, timing, budget, and any must-have or must-avoid details.",
    },
  ]);
  const [draft, setDraft] = useState("");
  const [location, setLocation] = useState("");
  const [partySize, setPartySize] = useState("2");
  const [budget, setBudget] = useState<GenerateChatRequest["budget"]>("$$");
  const [vibe, setVibe] = useState<GenerateChatRequest["vibe"]>(
    selectedTemplate?.vibes[0]
  );
  const [transportMode, setTransportMode] = useState<TransportMode>("driving");
  const [timeWindow, setTimeWindow] = useState<string>(
    selectedTemplate?.timeOfDay ?? "evening"
  );
  const [desiredIdeaCount, setDesiredIdeaCount] = useState("4");
  const [constraints, setConstraints] = useState("");

  useEffect(() => {
    if (!selectedTemplate) {
      return;
    }
    setMessages((current) => [
      current[0],
      {
        id: `assistant-template-${selectedTemplate.id}`,
        role: "assistant",
        content: `I can use the "${selectedTemplate.title}" template as a starting arc while still tailoring the actual venues to your request.`,
      },
      ...current.slice(1).filter((message) => !message.id.startsWith("assistant-template-")),
    ]);
    setVibe(selectedTemplate.vibes[0]);
    setTimeWindow(selectedTemplate.timeOfDay);
  }, [selectedTemplate?.id]);

  const userPrompt = useMemo(
    () =>
      messages
        .filter((message) => message.role === "user")
        .map((message) => message.content)
        .join("\n"),
    [messages]
  );

  function sendMessage(content: string) {
    const trimmed = content.trim();
    if (!trimmed) {
      return;
    }

    const nextMessages: PlannerMessage[] = [
      ...messages,
      {
        id: `user-${messages.length + 1}`,
        role: "user",
        content: trimmed,
      },
    ];

    const assistantReply =
      followUps[Math.min(nextMessages.filter((item) => item.role === "user").length - 1, followUps.length - 1)];

    setMessages([
      ...nextMessages,
      {
        id: `assistant-${messages.length + 2}`,
        role: "assistant",
        content: assistantReply,
      },
    ]);
    setDraft("");
  }

  function handleSubmit() {
    const prompt = draft.trim() ? `${userPrompt}\n${draft.trim()}`.trim() : userPrompt;
    if (!prompt.trim()) {
      return;
    }

    onSubmit({
      prompt,
      transcript:
        draft.trim() && messages[messages.length - 1]?.role !== "user"
          ? [
              ...messages,
              {
                id: `user-${messages.length + 1}`,
                role: "user",
                content: draft.trim(),
              },
            ]
          : messages,
      location: location || undefined,
      timeWindow,
      vibe: vibe || undefined,
      budget,
      transportMode,
      partySize: Number(partySize) || 2,
      constraints: constraints || undefined,
      desiredIdeaCount: Number(desiredIdeaCount) || 4,
      selectedTemplateId: selectedTemplate?.id,
    });
  }

  return (
    <View style={styles.container}>
      <View style={styles.heroWrap}>
        <Eyebrow>Chat with the planner</Eyebrow>
        <Text style={styles.heroTitle}>Describe the date in your own words.</Text>
        <Text style={styles.heroSubtitle}>
          Use natural language when you want the backend LLM to interpret tone, priorities, and tradeoffs.
        </Text>
      </View>

      <SurfaceCard style={styles.chatCard}>
        <View style={styles.chatThread}>
          {messages.map((message) => (
            <View
              key={message.id}
              style={[
                styles.messageBubble,
                message.role === "assistant" ? styles.assistantBubble : styles.userBubble,
              ]}
            >
              <Text style={styles.messageRole}>
                {message.role === "assistant" ? "Planner" : "You"}
              </Text>
              <Text style={styles.messageText}>{message.content}</Text>
            </View>
          ))}
        </View>

        <View style={styles.quickPrompts}>
          {starterPrompts.map((prompt) => (
            <SelectChip key={prompt} label={prompt} onPress={() => sendMessage(prompt)} />
          ))}
        </View>

        <TextInput
          style={[styles.input, styles.notesInput]}
          value={draft}
          onChangeText={setDraft}
          placeholder="Example: We want something intimate in Surry Hills tonight, not too expensive, and ideally not a loud bar."
          placeholderTextColor={palette.textMuted}
          multiline
        />

        <View style={styles.row}>
          <Field label="Location">
            <TextInput
              style={styles.input}
              value={location}
              onChangeText={setLocation}
              placeholder="Optional city or suburb"
              placeholderTextColor={palette.textMuted}
            />
          </Field>

          <Field label="Party size">
            <TextInput
              style={styles.input}
              value={partySize}
              onChangeText={setPartySize}
              keyboardType="numeric"
              placeholder="2"
              placeholderTextColor={palette.textMuted}
            />
          </Field>
        </View>

        <View style={styles.row}>
          <Field label="Vibe">
            <View style={styles.chipWrap}>
              {vibeOptions.map((option) => (
                <SelectChip
                  key={option}
                  label={capitalize(option)}
                  selected={vibe === option}
                  onPress={() => setVibe(option)}
                />
              ))}
            </View>
          </Field>
        </View>

        <View style={styles.row}>
          <Field label="Budget">
            <View style={styles.chipWrap}>
              {["$", "$$", "$$$", "$$$$"].map((value) => (
                <SelectChip
                  key={value}
                  label={value}
                  selected={budget === value}
                  onPress={() => setBudget(value as GenerateChatRequest["budget"])}
                />
              ))}
            </View>
          </Field>

          <Field label="Transport">
            <View style={styles.chipWrap}>
              {[
                { label: "Walk", value: "walking" },
                { label: "Transit", value: "public_transport" },
                { label: "Drive", value: "driving" },
              ].map((option) => (
                <SelectChip
                  key={option.value}
                  label={option.label}
                  selected={transportMode === option.value}
                  onPress={() => setTransportMode(option.value as TransportMode)}
                />
              ))}
            </View>
          </Field>
        </View>

        <View style={styles.row}>
          <Field label="Time window">
            <TextInput
              style={styles.input}
              value={timeWindow}
              onChangeText={setTimeWindow}
              placeholder="evening"
              placeholderTextColor={palette.textMuted}
            />
          </Field>

          <Field label="Idea count">
            <TextInput
              style={styles.input}
              value={desiredIdeaCount}
              onChangeText={setDesiredIdeaCount}
              keyboardType="numeric"
              placeholder="4"
              placeholderTextColor={palette.textMuted}
            />
          </Field>
        </View>

        <Field label="Constraints">
          <TextInput
            style={styles.input}
            value={constraints}
            onChangeText={setConstraints}
            placeholder="No seafood, low walking, quiet atmosphere..."
            placeholderTextColor={palette.textMuted}
          />
        </Field>

        <View style={styles.actions}>
          <ActionButton
            label="Add message"
            variant="secondary"
            onPress={() => sendMessage(draft)}
            style={styles.actionButton}
          />
          <ActionButton
            label="Generate ideas from chat"
            onPress={handleSubmit}
            style={styles.actionButton}
          />
        </View>
      </SurfaceCard>
    </View>
  );
}

function capitalize(value: string) {
  return value.charAt(0).toUpperCase() + value.slice(1);
}

function Field({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <View style={styles.field}>
      <Text style={styles.fieldLabel}>{label}</Text>
      {children}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    gap: 16,
  },
  heroWrap: {
    gap: 10,
  },
  heroTitle: {
    color: palette.text,
    fontSize: 34,
    lineHeight: 40,
    fontWeight: "900",
  },
  heroSubtitle: {
    color: palette.textMuted,
    fontSize: 15,
    lineHeight: 23,
  },
  chatCard: {
    gap: 14,
  },
  chatThread: {
    gap: 10,
  },
  messageBubble: {
    borderRadius: 20,
    paddingHorizontal: 14,
    paddingVertical: 12,
    gap: 4,
  },
  assistantBubble: {
    backgroundColor: "rgba(255, 255, 255, 0.06)",
    borderWidth: 1,
    borderColor: palette.border,
    alignSelf: "flex-start",
  },
  userBubble: {
    backgroundColor: "rgba(255, 122, 89, 0.16)",
    borderWidth: 1,
    borderColor: "rgba(255, 151, 124, 0.34)",
    alignSelf: "flex-end",
  },
  messageRole: {
    color: palette.accentWarm,
    fontSize: 12,
    fontWeight: "800",
    textTransform: "uppercase",
  },
  messageText: {
    color: palette.text,
    lineHeight: 21,
  },
  quickPrompts: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 10,
  },
  input: {
    borderWidth: 1,
    borderColor: palette.border,
    backgroundColor: palette.panelSoft,
    borderRadius: 18,
    paddingHorizontal: 16,
    paddingVertical: 14,
    fontSize: 15,
    color: palette.text,
  },
  notesInput: {
    minHeight: 96,
    textAlignVertical: "top",
  },
  row: {
    flexDirection: "row",
    gap: 12,
    flexWrap: "wrap",
  },
  field: {
    flex: 1,
    minWidth: 160,
    gap: 8,
  },
  fieldLabel: {
    color: palette.textSoft,
    fontSize: 14,
    fontWeight: "700",
  },
  chipWrap: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 10,
  },
  actions: {
    flexDirection: "row",
    gap: 12,
  },
  actionButton: {
    flex: 1,
  },
});
