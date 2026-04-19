import { useEffect, useMemo, useState } from "react";
import { ActivityIndicator, StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import SwipeDeck from "../components/SwipeDeck";
import {
  ActionButton,
  Eyebrow,
  ScreenShell,
  SelectChip,
  SurfaceCard,
  palette,
} from "../components/ui";
import { generatePlansFromChat, generatePlansFromForm } from "../lib/api";
import { cacheGeneratedPlans, savePlan } from "../lib/storage";
import {
  GenerateChatRequest,
  GenerateFormRequest,
  PlannerMode,
  Plan,
} from "../lib/types";

export default function ResultsScreen() {
  const router = useRouter();
  const params = useLocalSearchParams<{ mode?: string; request?: string }>();
  const [plans, setPlans] = useState<Plan[]>([]);
  const [loading, setLoading] = useState(true);
  const [warning, setWarning] = useState<string | undefined>();
  const [error, setError] = useState<string | undefined>();

  const mode: PlannerMode = params.mode === "chat" ? "chat" : "form";
  const parsedRequest = useMemo(() => {
    if (!params.request) return null;
    try {
      return JSON.parse(params.request as string) as GenerateFormRequest | GenerateChatRequest;
    } catch {
      return null;
    }
  }, [params.request]);

  useEffect(() => {
    let active = true;

    async function loadPlans() {
      if (!parsedRequest) {
        setError("The planner request was missing or malformed.");
        setLoading(false);
        return;
      }

      setLoading(true);
      setError(undefined);

      try {
        const result =
          mode === "chat"
            ? await generatePlansFromChat(parsedRequest as GenerateChatRequest)
            : await generatePlansFromForm(parsedRequest as GenerateFormRequest);

        if (!active) return;

        setPlans(result.data);
        setWarning(result.warning);
        await cacheGeneratedPlans(result.data);
      } catch (loadError) {
        if (!active) return;
        setError(loadError instanceof Error ? loadError.message : "Failed to generate plans.");
      } finally {
        if (active) setLoading(false);
      }
    }

    void loadPlans();
    return () => { active = false; };
  }, [mode, parsedRequest]);

  // ── Loading state ──────────────────────────────────────────────────────────
  if (loading) {
    return (
      <ScreenShell contentContainerStyle={styles.centered}>
        <SurfaceCard style={styles.centerCard}>
          <ActivityIndicator color={palette.accent} />
          <Text style={styles.centerTitle}>Finding your date ideas</Text>
          <Text style={styles.centerText}>
            Pulling together venues, timing, and plan details.
          </Text>
        </SurfaceCard>
      </ScreenShell>
    );
  }

  // ── Error state ────────────────────────────────────────────────────────────
  if (error) {
    return (
      <ScreenShell contentContainerStyle={styles.centered}>
        <SurfaceCard style={styles.centerCard}>
          <Text style={styles.centerTitle}>Couldn't generate ideas</Text>
          <Text style={styles.centerText}>{error}</Text>
          <ActionButton label="Go back" variant="secondary" onPress={() => router.replace("/")} />
        </SurfaceCard>
      </ScreenShell>
    );
  }

  // ── Empty state ────────────────────────────────────────────────────────────
  if (plans.length === 0) {
    return (
      <ScreenShell contentContainerStyle={styles.centered}>
        <SurfaceCard style={styles.centerCard}>
          <Text style={styles.centerTitle}>No plans returned</Text>
          <Text style={styles.centerText}>
            Try loosening the constraints or switching to chat mode.
          </Text>
          <ActionButton label="Go back" variant="secondary" onPress={() => router.replace("/")} />
        </SurfaceCard>
      </ScreenShell>
    );
  }

  // ── Main swipe deck — NON-scrolling so buttons are always visible ──────────
  return (
    <ScreenShell contentContainerStyle={styles.container}>
      {/* Minimal header — just a back button and count */}
      <View style={styles.header}>
        <ActionButton
          label="← Edit"
          variant="secondary"
          onPress={() => router.replace("/")}
          style={styles.backButton}
        />
        {warning ? (
          <Text style={styles.warningTag}>Demo data</Text>
        ) : null}
      </View>

      {/* Deck takes ALL remaining space — save/skip always on screen */}
      <SwipeDeck
        plans={plans}
        onSavePlan={async (plan) => { await savePlan(plan); }}
        onOpenPlan={(plan) =>
          router.push({
            pathname: "/plan/[id]",
            params: { id: plan.id },
          })
        }
        onFinished={() => router.replace("/saved")}
      />
    </ScreenShell>
  );
}

const styles = StyleSheet.create({
  // Non-scrolling container — flex:1 so deck fills screen
  container: {
    flex: 1,
    gap: 12,
  },
  centered: {
    flex: 1,
    justifyContent: "center",
    gap: 12,
  },
  centerCard: {
    gap: 10,
    alignItems: "center",
    paddingVertical: 28,
  },
  centerTitle: {
    color: palette.text,
    fontSize: 22,
    fontWeight: "800",
    textAlign: "center",
  },
  centerText: {
    color: palette.textMuted,
    textAlign: "center",
    lineHeight: 22,
  },
  // Compact header row
  header: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
  },
  backButton: {
    minHeight: 38,
    paddingHorizontal: 14,
  },
  headerCount: {
    flex: 1,
    color: palette.text,
    fontSize: 17,
    fontWeight: "900",
  },
  warningTag: {
    color: palette.textMuted,
    fontSize: 12,
    fontWeight: "700",
    backgroundColor: "rgba(255,255,255,0.06)",
    borderRadius: 8,
    paddingHorizontal: 8,
    paddingVertical: 4,
  },
});