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
import { searchPlansFromChat, searchPlansFromForm } from "../lib/api";
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
      setWarning(undefined);

      try {
        const result =
          mode === "chat"
            ? await searchPlansFromChat(parsedRequest as GenerateChatRequest)
            : await searchPlansFromForm(parsedRequest as GenerateFormRequest);

        if (!active) return;

        setPlans(result.data);
        setWarning(result.warning);
        if (result.warning) {
          console.error("Planner search returned non-fatal warnings.", result.warning);
        }
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
  }, [mode, parsedRequest]);

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
