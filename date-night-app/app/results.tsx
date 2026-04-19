import { useEffect, useState } from "react";
import { View, Text, Pressable, StyleSheet, ActivityIndicator } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import SwipeDeck from "../components/SwipeDeck";
import { generatePlans } from "../lib/api";
import { savePlan } from "../lib/storage";
import { GenerateRequest, Plan } from "../lib/types";

export default function ResultsScreen() {
  const router = useRouter();
  const params = useLocalSearchParams();

  const payload = params.payload
    ? (JSON.parse(params.payload as string) as GenerateRequest)
    : null;
  const [plans, setPlans] = useState<Plan[]>([]);
  const [warnings, setWarnings] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState<boolean>(Boolean(payload));

  useEffect(() => {
    let cancelled = false;

    async function loadPlans() {
      if (!payload) {
        setIsLoading(false);
        setError("Missing request payload.");
        return;
      }
      setIsLoading(true);
      setError(null);
      try {
        const response = await generatePlans(payload);
        if (cancelled) return;
        setPlans(response.plans);
        setWarnings(response.warnings);
        if (response.plans.length === 0) {
          setError("No cached date plans matched that request.");
        }
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Failed to load date plans.");
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    }

    void loadPlans();
    return () => {
      cancelled = true;
    };
  }, [payload]);

  return (
    <View style={styles.container}>
      <View style={styles.header}>
        <View>
          <Text style={styles.title}>Swipe Date Ideas</Text>
          {payload ? (
            <Text style={styles.subtitle}>
              {payload.location || "Anywhere"} · {payload.vibe} · {payload.budget}
            </Text>
          ) : (
            <Text style={styles.subtitle}>Swipe right to save</Text>
          )}
        </View>

        <Pressable style={styles.savedButton} onPress={() => router.push("/saved")}>
          <Text style={styles.savedButtonText}>Saved</Text>
        </Pressable>
      </View>

      {isLoading ? (
        <View style={styles.centeredCard}>
          <ActivityIndicator size="large" color="#be185d" />
          <Text style={styles.statusText}>Loading cached date ideas...</Text>
        </View>
      ) : error ? (
        <View style={styles.centeredCard}>
          <Text style={styles.errorTitle}>Could not load plans</Text>
          <Text style={styles.errorText}>{error}</Text>
        </View>
      ) : (
        <>
          {warnings.length > 0 ? (
            <View style={styles.warningCard}>
              {warnings.map((warning) => (
                <Text key={warning} style={styles.warningText}>
                  {warning}
                </Text>
              ))}
            </View>
          ) : null}

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
        </>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#fff7f8",
    paddingHorizontal: 16,
    paddingTop: 18,
    paddingBottom: 16,
  },
  header: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: 18,
  },
  title: {
    fontSize: 28,
    fontWeight: "700",
    color: "#881337",
  },
  subtitle: {
    marginTop: 4,
    color: "#64748b",
    fontSize: 14,
  },
  savedButton: {
    backgroundColor: "white",
    paddingHorizontal: 16,
    paddingVertical: 10,
    borderRadius: 999,
  },
  savedButtonText: {
    color: "#be185d",
    fontWeight: "700",
  },
  centeredCard: {
    backgroundColor: "white",
    borderRadius: 20,
    padding: 24,
    alignItems: "center",
    justifyContent: "center",
    gap: 12,
    minHeight: 220,
  },
  statusText: {
    color: "#475569",
    fontSize: 15,
  },
  errorTitle: {
    color: "#881337",
    fontWeight: "700",
    fontSize: 18,
  },
  errorText: {
    color: "#475569",
    fontSize: 14,
    textAlign: "center",
  },
  warningCard: {
    backgroundColor: "#fff1f2",
    borderRadius: 16,
    padding: 14,
    marginBottom: 14,
  },
  warningText: {
    color: "#9f1239",
    fontSize: 13,
  },
});
