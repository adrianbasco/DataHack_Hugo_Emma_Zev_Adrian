import { useEffect, useMemo, useState } from "react";
import { StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import ChatPlanner from "../components/ChatPlanner";
import InputForm from "../components/InputForm";
import {
  Eyebrow,
  ScreenShell,
  SelectChip,
  SurfaceCard,
  palette,
} from "../components/ui";
import { findFallbackTemplateById } from "../lib/mockTemplates";
import {
  DateTemplate,
  GenerateChatRequest,
  GenerateFormRequest,
  PlannerMode,
} from "../lib/types";

export default function HomeScreen() {
  const router = useRouter();
  const params = useLocalSearchParams<{ mode?: string; templateId?: string; template?: string }>();

  const selectedTemplate = useMemo(() => {
    if (typeof params.template === "string" && params.template) {
      try {
        return JSON.parse(params.template) as DateTemplate;
      } catch {
        return findFallbackTemplateById(params.templateId);
      }
    }
    return findFallbackTemplateById(params.templateId);
  }, [params.template, params.templateId]);

  const [mode, setMode] = useState<PlannerMode>(
    params.mode === "chat" ? "chat" : "form"
  );

  useEffect(() => {
    if (params.mode === "chat" || params.mode === "form") {
      setMode(params.mode);
    }
  }, [params.mode]);

  function handleFormSubmit(payload: GenerateFormRequest) {
    router.push({
      pathname: "/results",
      params: { mode: "form", request: JSON.stringify(payload) },
    });
  }

  function handleChatSubmit(payload: GenerateChatRequest) {
    router.push({
      pathname: "/results",
      params: { mode: "chat", request: JSON.stringify(payload) },
    });
  }

  return (
    <ScreenShell scroll contentContainerStyle={styles.container}>
      {/* ── Hero ── */}
      <View style={styles.heroSection}>
        <Eyebrow tone="warm">Date Night</Eyebrow>
        <Text style={styles.heroTitle}>Plan your perfect date.</Text>
        <Text style={styles.heroSubtitle}>
          Tell us the vibe, location, and budget — we'll build the itinerary.
        </Text>
      </View>

      {/* ── Mode toggle — prominent, directly under hero ── */}
      <View style={styles.modeToggle}>
        <SelectChip
          label="Structured form"
          selected={mode === "form"}
          onPress={() => setMode("form")}
          style={styles.modeChip}
        />
        <SelectChip
          label="Chat with AI"
          selected={mode === "chat"}
          onPress={() => setMode("chat")}
          style={styles.modeChip}
        />
      </View>

      {/* ── Selected template badge (only when one is active) ── */}
      {selectedTemplate ? (
        <SurfaceCard style={styles.templateBadge}>
          <Text style={styles.templateBadgeLabel}>Template selected</Text>
          <Text style={styles.templateBadgeTitle}>{selectedTemplate.title}</Text>
          <View style={styles.templateMeta}>
            <SelectChip label={selectedTemplate.timeOfDay} selected />
            <SelectChip label={`${selectedTemplate.durationHours}h`} selected />
            {selectedTemplate.vibes.map((vibe) => (
              <SelectChip key={vibe} label={vibe} selected />
            ))}
          </View>
        </SurfaceCard>
      ) : null}

      {/* ── Active planner ── */}
      {mode === "form" ? (
        <InputForm selectedTemplate={selectedTemplate} onSubmit={handleFormSubmit} />
      ) : (
        <ChatPlanner selectedTemplate={selectedTemplate} onSubmit={handleChatSubmit} />
      )}
    </ScreenShell>
  );
}

const styles = StyleSheet.create({
  container: {
    gap: 16,
  },
  heroSection: {
    gap: 8,
    paddingTop: 4,
  },
  heroTitle: {
    color: palette.text,
    fontSize: 36,
    lineHeight: 42,
    fontWeight: "900",
  },
  heroSubtitle: {
    color: palette.textMuted,
    fontSize: 15,
    lineHeight: 23,
  },
  // Mode toggle sits right under the hero — big and obvious
  modeToggle: {
    flexDirection: "row",
    gap: 10,
  },
  modeChip: {
    flex: 1,
    minHeight: 46,
  },
  // Template badge is compact — doesn't compete with the form
  templateBadge: {
    gap: 6,
    paddingVertical: 14,
  },
  templateBadgeLabel: {
    color: palette.accentWarm,
    fontSize: 11,
    fontWeight: "800",
    letterSpacing: 0.8,
    textTransform: "uppercase",
  },
  templateBadgeTitle: {
    color: palette.text,
    fontSize: 18,
    fontWeight: "800",
  },
  templateMeta: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
    marginTop: 4,
  },
});