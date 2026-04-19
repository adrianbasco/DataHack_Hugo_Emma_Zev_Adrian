import { useEffect, useMemo, useState } from "react";
import { StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import ChatPlanner from "../components/ChatPlanner";
import InputForm from "../components/InputForm";
import {
  ActionButton,
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
      params: {
        mode: "form",
        request: JSON.stringify(payload),
      },
    });
  }

  function handleChatSubmit(payload: GenerateChatRequest) {
    router.push({
      pathname: "/results",
      params: {
        mode: "chat",
        request: JSON.stringify(payload),
      },
    });
  }

  return (
    <ScreenShell scroll contentContainerStyle={styles.container}>
      <View style={styles.heroSection}>
        <Eyebrow tone="warm">Planner workspace</Eyebrow>
        <Text style={styles.heroTitle}>Plan around the backend, not around mock UI shortcuts.</Text>
        <Text style={styles.heroSubtitle}>
          Browse reusable date templates, generate plans from a structured form, or hand the brief to a planner-style chat flow.
        </Text>

        <View style={styles.topActions}>
          <ActionButton
            label="Browse templates"
            variant="secondary"
            onPress={() => router.push("/templates")}
            style={styles.topActionButton}
          />
          <ActionButton
            label="Saved dates"
            variant="secondary"
            onPress={() => router.push("/saved")}
            style={styles.topActionButton}
          />
        </View>
      </View>

      {selectedTemplate ? (
        <SurfaceCard style={styles.selectedTemplateCard}>
          <Text style={styles.selectedTemplateLabel}>Template in focus</Text>
          <Text style={styles.selectedTemplateTitle}>{selectedTemplate.title}</Text>
          <Text style={styles.selectedTemplateText}>{selectedTemplate.description}</Text>
          <View style={styles.templateMeta}>
            <SelectChip label={selectedTemplate.timeOfDay} selected />
            <SelectChip label={`${selectedTemplate.durationHours}h`} selected />
            {selectedTemplate.vibes.map((vibe) => (
              <SelectChip key={vibe} label={vibe} selected />
            ))}
          </View>
        </SurfaceCard>
      ) : null}

      <SurfaceCard style={styles.modeCard}>
        <Text style={styles.modeTitle}>Choose your planner input</Text>
        <Text style={styles.modeText}>
          Form mode is best for structured constraints. Chat mode is best when the request is fuzzy and you want the LLM to interpret it.
        </Text>
        <View style={styles.modeToggle}>
          <SelectChip
            label="Structured form"
            selected={mode === "form"}
            onPress={() => setMode("form")}
          />
          <SelectChip
            label="Planner chat"
            selected={mode === "chat"}
            onPress={() => setMode("chat")}
          />
        </View>
      </SurfaceCard>

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
    gap: 18,
  },
  heroSection: {
    gap: 12,
  },
  heroTitle: {
    color: palette.text,
    fontSize: 38,
    lineHeight: 44,
    fontWeight: "900",
  },
  heroSubtitle: {
    color: palette.textMuted,
    fontSize: 16,
    lineHeight: 24,
    maxWidth: 720,
  },
  topActions: {
    flexDirection: "row",
    gap: 12,
    flexWrap: "wrap",
  },
  topActionButton: {
    minWidth: 160,
  },
  selectedTemplateCard: {
    gap: 6,
  },
  selectedTemplateLabel: {
    color: palette.accentWarm,
    fontSize: 12,
    fontWeight: "800",
    letterSpacing: 0.7,
    textTransform: "uppercase",
  },
  selectedTemplateTitle: {
    color: palette.text,
    fontSize: 22,
    fontWeight: "800",
  },
  selectedTemplateText: {
    color: palette.textSoft,
    lineHeight: 21,
  },
  templateMeta: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 10,
    marginTop: 8,
  },
  modeCard: {
    gap: 12,
  },
  modeTitle: {
    color: palette.text,
    fontSize: 22,
    fontWeight: "800",
  },
  modeText: {
    color: palette.textMuted,
    lineHeight: 22,
  },
  modeToggle: {
    flexDirection: "row",
    gap: 10,
    flexWrap: "wrap",
  },
});
