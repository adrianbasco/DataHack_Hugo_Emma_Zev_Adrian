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
    if (!params.request) {
      return null;
    }

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

        if (!active) {
          return;
        }

        setPlans(result.data);
        setWarning(result.warning);
        await cacheGeneratedPlans(result.data);
      } catch (loadError) {
        if (!active) {
          return;
        }
        setError(loadError instanceof Error ? loadError.message : "Failed to generate plans.");
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    }

    void loadPlans();

    return () => {
      active = false;
    };
  }, [mode, parsedRequest]);

  const summaryChips = buildSummaryChips(mode, parsedRequest);

  return (
    <ScreenShell contentContainerStyle={styles.container}>
      <View style={styles.header}>
        <Eyebrow>{mode === "chat" ? "Chat planner" : "Structured planner"}</Eyebrow>
        <Text style={styles.title}>Generated date ideas</Text>
        <Text style={styles.subtitle}>
          These cards are ready for swipe-save, detail view, and booking handoff.
        </Text>

        <View style={styles.summaryWrap}>
          {summaryChips.map((chip) => (
            <SelectChip key={chip} label={chip} selected />
          ))}
        </View>

        <View style={styles.headerActions}>
          <ActionButton
            label="Edit request"
            variant="secondary"
            onPress={() => router.back()}
            style={styles.headerButton}
          />
          <ActionButton
            label="Saved"
            variant="secondary"
            onPress={() => router.push("/saved")}
            style={styles.headerButton}
          />
        </View>
      </View>

      {warning ? (
        <SurfaceCard style={styles.bannerCard}>
          <Text style={styles.bannerTitle}>Using local fallback data</Text>
          <Text style={styles.bannerText}>{warning}</Text>
        </SurfaceCard>
      ) : null}

      {loading ? (
        <SurfaceCard style={styles.centerCard}>
          <ActivityIndicator color={palette.accent} />
          <Text style={styles.centerTitle}>Generating plans</Text>
          <Text style={styles.centerText}>
            Pulling together template logic, constraints, and plan details.
          </Text>
        </SurfaceCard>
      ) : null}

      {!loading && error ? (
        <SurfaceCard style={styles.centerCard}>
          <Text style={styles.centerTitle}>Planner request failed</Text>
          <Text style={styles.centerText}>{error}</Text>
          <ActionButton label="Go back" variant="secondary" onPress={() => router.back()} />
        </SurfaceCard>
      ) : null}

      {!loading && !error && plans.length > 0 ? (
        <SwipeDeck
          plans={plans}
          onSavePlan={async (plan) => {
            await savePlan(plan);
          }}
          onOpenPlan={(plan) =>
            router.push({
              pathname: "/plan/[id]",
              params: { id: plan.id },
            })
          }
          onFinished={() => router.push("/saved")}
        />
      ) : null}

      {!loading && !error && plans.length === 0 ? (
        <SurfaceCard style={styles.centerCard}>
          <Text style={styles.centerTitle}>No plans returned</Text>
          <Text style={styles.centerText}>
            Try loosening the constraints or switching to chat mode for a more open-ended brief.
          </Text>
        </SurfaceCard>
      ) : null}
    </ScreenShell>
  );
}

function buildSummaryChips(
  mode: PlannerMode,
  request: GenerateFormRequest | GenerateChatRequest | null
) {
  if (!request) {
    return [mode];
  }

  if (mode === "chat") {
    const chatRequest = request as GenerateChatRequest;
    return [
      chatRequest.location || "Anywhere",
      chatRequest.timeWindow || "Flexible",
      chatRequest.budget || "Open budget",
      `${chatRequest.desiredIdeaCount} ideas`,
    ];
  }

  const formRequest = request as GenerateFormRequest;
  return [
    formRequest.location || "Anywhere",
    ...(formRequest.vibes.slice(0, 2) || []),
    formRequest.timeWindow || "Flexible",
    `${formRequest.desiredIdeaCount} ideas`,
  ];
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    gap: 16,
  },
  header: {
    gap: 10,
  },
  title: {
    color: palette.text,
    fontSize: 34,
    lineHeight: 40,
    fontWeight: "900",
  },
  subtitle: {
    color: palette.textMuted,
    lineHeight: 22,
    fontSize: 15,
  },
  summaryWrap: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 10,
  },
  headerActions: {
    flexDirection: "row",
    gap: 12,
    flexWrap: "wrap",
  },
  headerButton: {
    minWidth: 120,
  },
  bannerCard: {
    gap: 6,
  },
  bannerTitle: {
    color: palette.text,
    fontSize: 18,
    fontWeight: "800",
  },
  bannerText: {
    color: palette.textMuted,
    lineHeight: 21,
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
});
