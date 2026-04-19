import { useEffect, useMemo, useState } from "react";
import { ActivityIndicator, StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";

import SwipeDeck from "../components/SwipeDeck";
import {
  ActionButton,
  ScreenShell,
  SurfaceCard,
  palette,
} from "../components/ui";
import { searchPlansFromChat } from "../lib/api";
import { cacheGeneratedPlans, savePlan } from "../lib/storage";
import {
  GenerateChatRequest,
  Plan,
  PlannerMessage,
} from "../lib/types";

export default function ResultsScreen() {
  const router = useRouter();
  const params = useLocalSearchParams<{ request?: string }>();
  const [plans, setPlans] = useState<Plan[]>([]);
  const [loading, setLoading] = useState(true);
  const [warning, setWarning] = useState<string | undefined>();
  const [error, setError] = useState<string | undefined>();

  const parsedRequest = useMemo(() => {
    if (!params.request) return null;
    try {
      const parsed = JSON.parse(params.request as string);
      if (!isGenerateChatRequest(parsed)) {
        console.error("Planner results received a non-chat request payload.", parsed);
        return null;
      }
      return parsed;
    } catch (parseError) {
      console.error("Planner request route param could not be parsed.", parseError);
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
      setWarning(undefined);

      try {
        const result = await searchPlansFromChat(parsedRequest);

        if (!active) return;

        setPlans(result.data);
        setWarning(result.data.length === 0 ? result.warning : undefined);
        try {
          await cacheGeneratedPlans(result.data);
        } catch (storageError) {
          console.error("Failed to cache planner search results locally.", storageError);
        }
      } catch (loadError) {
        if (!active) return;
        setError(
          loadError instanceof Error
            ? loadError.message
            : "Planner search failed against the cached date cards."
        );
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
  }, [parsedRequest]);

  if (loading) {
    return (
      <ScreenShell contentContainerStyle={styles.centered}>
        <SurfaceCard style={styles.centerCard}>
          <ActivityIndicator color={palette.accent} />
          <Text style={styles.centerTitle}>Searching cached date cards</Text>
          <Text style={styles.centerText}>
            Matching your planner input against the local demo snapshot.
          </Text>
        </SurfaceCard>
      </ScreenShell>
    );
  }

  if (error) {
    return (
      <ScreenShell contentContainerStyle={styles.centered}>
        <SurfaceCard style={styles.centerCard}>
          <Text style={styles.centerTitle}>Planner search failed</Text>
          <Text style={styles.centerText}>{error}</Text>
          <ActionButton
            label="Go back"
            variant="secondary"
            onPress={() => router.replace("/")}
          />
        </SurfaceCard>
      </ScreenShell>
    );
  }

  if (plans.length === 0) {
    return (
      <ScreenShell contentContainerStyle={styles.centered}>
        <SurfaceCard style={styles.centerCard}>
          <Text style={styles.centerTitle}>No cached matches found</Text>
          <Text style={styles.centerText}>
            The planner request did not match any cached demo plans. Try broadening the location,
            loosening the constraints, or changing the vibe.
          </Text>
          {warning ? <Text style={styles.centerText}>{warning}</Text> : null}
          <ActionButton
            label="Go back"
            variant="secondary"
            onPress={() => router.replace("/")}
          />
        </SurfaceCard>
      </ScreenShell>
    );
  }

  return (
      <ScreenShell contentContainerStyle={styles.container}>
      <View style={styles.header}>
        <ActionButton
          label="Edit"
          variant="secondary"
          onPress={() => router.replace("/")}
          style={styles.backButton}
        />
      </View>

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
        onFinished={() => router.replace("/saved")}
      />
    </ScreenShell>
  );
}

function isGenerateChatRequest(value: unknown): value is GenerateChatRequest {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }

  const request = value as Partial<GenerateChatRequest>;
  return (
    typeof request.prompt === "string" &&
    request.prompt.trim().length > 0 &&
    Array.isArray(request.transcript) &&
    request.transcript.every(isPlannerMessage) &&
    typeof request.partySize === "number" &&
    Number.isFinite(request.partySize) &&
    typeof request.desiredIdeaCount === "number" &&
    Number.isFinite(request.desiredIdeaCount)
  );
}

function isPlannerMessage(value: unknown): value is PlannerMessage {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }

  const message = value as Partial<PlannerMessage>;
  return (
    typeof message.id === "string" &&
    (message.role === "assistant" || message.role === "user") &&
    typeof message.content === "string"
  );
}

const styles = StyleSheet.create({
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
    paddingHorizontal: 24,
  },
  centerTitle: {
    color: palette.text,
    fontSize: 22,
    fontWeight: "800",
  },
  centerText: {
    color: palette.textMuted,
    textAlign: "center",
    lineHeight: 22,
  },
  header: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  backButton: {
    minWidth: 96,
  },
});
