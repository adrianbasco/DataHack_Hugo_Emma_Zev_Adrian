import { View, Text, Pressable, StyleSheet } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import SwipeDeck from "../components/SwipeDeck";
import { mockPlans } from "../lib/mockPlans";
import { savePlan } from "../lib/storage";
import { GenerateRequest } from "../lib/types";

export default function ResultsScreen() {
  const router = useRouter();
  const params = useLocalSearchParams();

  const payload = params.payload
    ? (JSON.parse(params.payload as string) as GenerateRequest)
    : null;

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

      <SwipeDeck
        plans={mockPlans}
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
});