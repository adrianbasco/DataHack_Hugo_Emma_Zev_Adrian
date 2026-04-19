import { useEffect, useMemo, useRef, useState } from "react";
import {
  KeyboardAvoidingView,
  Platform,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import { SafeAreaView } from "react-native-safe-area-context";
import { StatusBar } from "expo-status-bar";
import { findFallbackTemplateById } from "../../lib/mockTemplates";
import {
  DateTemplate,
  GenerateChatRequest,
  PlannerMessage,
  TransportMode,
} from "../../lib/types";
import { palette } from "../../components/ui";

// One suggestion only
const SUGGESTION = "Romantic but not too formal";

const FOLLOW_UPS = [
  "Nice direction. Which area should I optimize for, and is there a hard budget ceiling?",
  "Helpful. What matters more: scenery, food, activity, or low-effort logistics?",
  "Great. Any hard no's — loud bars, lots of walking, weather risk?",
  "That's enough to brief the planner. Add anything else, or tap Generate.",
];

export default function HomeScreen() {
  const router = useRouter();
  const scrollRef = useRef<ScrollView>(null);

  const params = useLocalSearchParams<{ templateId?: string; template?: string }>();
  const selectedTemplate = useMemo(() => {
    if (typeof params.template === "string" && params.template) {
      try { return JSON.parse(params.template) as DateTemplate; }
      catch { return findFallbackTemplateById(params.templateId); }
    }
    return findFallbackTemplateById(params.templateId);
  }, [params.template, params.templateId]);

  const [messages, setMessages] = useState<PlannerMessage[]>([]);
  const [draft, setDraft] = useState("");

  useEffect(() => {
    if (!selectedTemplate) return;
    setMessages((prev) => {
      const filtered = prev.filter((m) => !m.id.startsWith("tpl-"));
      return [
        {
          id: `tpl-${selectedTemplate.id}`,
          role: "assistant",
          content: `I'll use the "${selectedTemplate.title}" arc as a starting point and tailor the venues to your brief.`,
        },
        ...filtered,
      ];
    });
  }, [selectedTemplate?.id]);

  useEffect(() => {
    setTimeout(() => scrollRef.current?.scrollToEnd({ animated: true }), 80);
  }, [messages]);

  const userTurns = messages.filter((m) => m.role === "user").length;
  const hasMessages = messages.length > 0;

  function sendMessage(text: string) {
    const trimmed = text.trim();
    if (!trimmed) return;
    const reply = FOLLOW_UPS[Math.min(userTurns, FOLLOW_UPS.length - 1)];
    setMessages((prev) => [
      ...prev,
      { id: `user-${Date.now()}`, role: "user", content: trimmed },
      { id: `asst-${Date.now() + 1}`, role: "assistant", content: reply },
    ]);
    setDraft("");
  }

  function handleGenerate() {
    const allText = messages
      .filter((m) => m.role === "user")
      .map((m) => m.content)
      .join("\n");
    const prompt = draft.trim() ? `${allText}\n${draft.trim()}`.trim() : allText;
    if (!prompt.trim()) return;

    const finalMessages: PlannerMessage[] = draft.trim()
      ? [...messages, { id: `user-final-${Date.now()}`, role: "user", content: draft.trim() }]
      : messages;

    const payload: GenerateChatRequest = {
      prompt,
      transcript: finalMessages,
      partySize: 2,
      desiredIdeaCount: 4,
      transportMode: "driving" as TransportMode,
      selectedTemplateId: selectedTemplate?.id,
    };

    router.push({
      pathname: "/results",
      params: { mode: "chat", request: JSON.stringify(payload) },
    });
  }

  const canGenerate = draft.trim().length > 0 || userTurns > 0;

  return (
    <SafeAreaView style={styles.root} edges={["top", "left", "right"]}>
      <StatusBar style="light" />

      <View pointerEvents="none" style={StyleSheet.absoluteFill}>
        <View style={[styles.orb, styles.orbCool]} />
        <View style={[styles.orb, styles.orbWarm]} />
      </View>

      <KeyboardAvoidingView
        style={styles.flex}
        behavior={Platform.OS === "ios" ? "padding" : "height"}
        keyboardVerticalOffset={0}
      >
        {/* Header */}
        <View style={styles.header}>
          <Text style={styles.headerTitle}>Date Night</Text>
          <Text style={styles.headerSub}>AI date planner</Text>
        </View>

        {/* Thread / empty state */}
        <ScrollView
          ref={scrollRef}
          style={styles.flex}
          contentContainerStyle={[
            styles.threadContent,
            !hasMessages && styles.threadEmpty,
          ]}
          keyboardShouldPersistTaps="handled"
          showsVerticalScrollIndicator={false}
        >
          {!hasMessages ? (
            <View style={styles.emptyWrap}>
              <Text style={styles.emptyTitle}>Plan your perfect date.</Text>
              <Text style={styles.emptySub}>
                Describe the vibe, location, and budget. I'll build the itinerary.
              </Text>
              {/* Single suggestion chip */}
              <Pressable
                style={({ pressed }) => [
                  styles.suggestionChip,
                  pressed && styles.suggestionChipPressed,
                ]}
                onPress={() => sendMessage(SUGGESTION)}
              >
                <Text style={styles.suggestionText}>{SUGGESTION}</Text>
              </Pressable>
            </View>
          ) : (
            <View style={styles.thread}>
              {messages.map((msg) => (
                <View
                  key={msg.id}
                  style={[
                    styles.bubble,
                    msg.role === "user" ? styles.bubbleUser : styles.bubbleAssistant,
                  ]}
                >
                  {msg.role === "assistant" && (
                    <View style={styles.avatar}>
                      <Text style={styles.avatarText}>DN</Text>
                    </View>
                  )}
                  <View style={[
                    styles.bubbleBody,
                    msg.role === "user" ? styles.bubbleBodyUser : styles.bubbleBodyAssistant,
                  ]}>
                    <Text style={styles.bubbleText}>{msg.content}</Text>
                  </View>
                </View>
              ))}
            </View>
          )}
        </ScrollView>

        {/* Bottom area — input + generate button + tab bar room */}
        <View style={styles.bottomArea}>
          <TextInput
            style={styles.textInput}
            value={draft}
            onChangeText={setDraft}
            placeholder="Describe your ideal date..."
            placeholderTextColor={palette.textMuted}
            multiline
            maxLength={500}
          />

          <Pressable
            style={({ pressed }) => [
              styles.generateBtn,
              !canGenerate && styles.generateBtnDim,
              pressed && canGenerate && styles.generateBtnPressed,
            ]}
            onPress={handleGenerate}
            disabled={!canGenerate}
          >
            <Text style={[
              styles.generateBtnText,
              !canGenerate && styles.generateBtnTextDim,
            ]}>
              Generate date ideas →
            </Text>
          </Pressable>

          {/* Enough room for the tab bar — 80px covers most tab bar heights */}
          <View style={styles.tabBarSpacer} />
        </View>
      </KeyboardAvoidingView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  root: {
    flex: 1,
    backgroundColor: palette.bg,
  },
  flex: { flex: 1 },
  orb: { position: "absolute", borderRadius: 999 },
  orbCool: {
    width: 280, height: 280, right: -60, top: -40,
    backgroundColor: "rgba(103,232,249,0.09)",
  },
  orbWarm: {
    width: 220, height: 220, left: -70, top: 200,
    backgroundColor: "rgba(255,122,89,0.09)",
  },

  header: {
    paddingHorizontal: 20,
    paddingTop: 12,
    paddingBottom: 10,
    borderBottomWidth: 0.5,
    borderBottomColor: "rgba(148,163,184,0.12)",
    flexDirection: "row",
    alignItems: "baseline",
    gap: 8,
  },
  headerTitle: { color: palette.text, fontSize: 18, fontWeight: "900" },
  headerSub: { color: palette.textMuted, fontSize: 12, fontWeight: "600" },

  threadContent: {
    flexGrow: 1,
    paddingHorizontal: 16,
    paddingTop: 16,
    paddingBottom: 8,
  },
  threadEmpty: { justifyContent: "flex-end" },
  thread: { gap: 16 },

  emptyWrap: { gap: 12 },
  emptyTitle: {
    color: palette.text,
    fontSize: 26,
    fontWeight: "900",
    lineHeight: 32,
  },
  emptySub: {
    color: palette.textMuted,
    fontSize: 14,
    lineHeight: 21,
  },
  suggestionChip: {
    alignSelf: "flex-start",
    backgroundColor: "rgba(255,255,255,0.05)",
    borderWidth: 1,
    borderColor: "rgba(148,163,184,0.18)",
    borderRadius: 14,
    paddingHorizontal: 16,
    paddingVertical: 11,
    marginTop: 4,
  },
  suggestionChipPressed: {
    backgroundColor: "rgba(255,122,89,0.12)",
    borderColor: "rgba(255,122,89,0.3)",
  },
  suggestionText: {
    color: palette.textSoft,
    fontSize: 14,
    fontWeight: "600",
  },

  bubble: { flexDirection: "row", gap: 10 },
  bubbleAssistant: { alignItems: "flex-start" },
  bubbleUser: { flexDirection: "row-reverse" },
  avatar: {
    width: 30, height: 30, borderRadius: 10,
    backgroundColor: "rgba(255,122,89,0.2)",
    borderWidth: 1, borderColor: "rgba(255,122,89,0.3)",
    alignItems: "center", justifyContent: "center",
    flexShrink: 0, marginTop: 2,
  },
  avatarText: { color: palette.accentWarm, fontSize: 9, fontWeight: "900" },
  bubbleBody: {
    maxWidth: "82%",
    borderRadius: 18,
    paddingHorizontal: 14,
    paddingVertical: 10,
  },
  bubbleBodyAssistant: {
    backgroundColor: "rgba(255,255,255,0.06)",
    borderWidth: 1,
    borderColor: "rgba(148,163,184,0.16)",
    borderTopLeftRadius: 4,
  },
  bubbleBodyUser: {
    backgroundColor: "rgba(255,122,89,0.18)",
    borderWidth: 1,
    borderColor: "rgba(255,151,124,0.3)",
    borderTopRightRadius: 4,
  },
  bubbleText: { color: palette.text, fontSize: 14, lineHeight: 21 },

  bottomArea: {
    paddingHorizontal: 14,
    paddingTop: 10,
    gap: 8,
    borderTopWidth: 0.5,
    borderTopColor: "rgba(148,163,184,0.14)",
    backgroundColor: "rgba(7,17,31,0.98)",
  },
  textInput: {
    minHeight: 46,
    maxHeight: 120,
    backgroundColor: "rgba(18,36,58,0.9)",
    borderWidth: 1,
    borderColor: "rgba(148,163,184,0.2)",
    borderRadius: 16,
    paddingHorizontal: 16,
    paddingTop: 12,
    paddingBottom: 12,
    fontSize: 15,
    color: palette.text,
    textAlignVertical: "top",
  },
  generateBtn: {
    height: 50,
    backgroundColor: palette.accentWarm,
    borderRadius: 16,
    alignItems: "center",
    justifyContent: "center",
  },
  generateBtnDim: {
    backgroundColor: "rgba(255,122,89,0.3)",
  },
  generateBtnPressed: {
    opacity: 0.85,
    transform: [{ scale: 0.99 }],
  },
  generateBtnText: {
    color: "#101826",
    fontSize: 15,
    fontWeight: "900",
    letterSpacing: 0.2,
  },
  generateBtnTextDim: {
    color: "rgba(16,24,38,0.45)",
  },
  // 80px clears most tab bars — adjust if yours is taller
  tabBarSpacer: { height: 120 },
});