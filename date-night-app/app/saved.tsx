import { useCallback, useState } from "react";
import { ScrollView, Text, View, StyleSheet } from "react-native";
import { useFocusEffect, useRouter } from "expo-router";
import PlanCard from "../components/PlanCard";
import { getSavedPlans } from "../lib/storage";
import { Plan } from "../lib/types";

export default function SavedScreen() {
  const router = useRouter();
  const [plans, setPlans] = useState<Plan[]>([]);

  useFocusEffect(
    useCallback(() => {
      getSavedPlans().then(setPlans);
    }, [])
  );

  return (
    <ScrollView contentContainerStyle={styles.container}>
      <Text style={styles.title}>Saved Dates</Text>

      {plans.length === 0 ? (
        <View style={styles.emptyCard}>
          <Text style={styles.emptyTitle}>No saved dates yet</Text>
          <Text style={styles.emptyText}>
            Save a plan from the results screen and it will appear here.
          </Text>
        </View>
      ) : (
        plans.map((plan) => (
          <PlanCard
            key={plan.id}
            plan={plan}
            onView={() =>
              router.push({
                pathname: "/plan/[id]",
                params: { id: plan.id },
              })
            }
          />
        ))
      )}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: {
    padding: 16,
    backgroundColor: "#fff7f8",
  },
  title: {
    fontSize: 28,
    fontWeight: "700",
    color: "#881337",
    marginBottom: 16,
  },
  emptyCard: {
    backgroundColor: "white",
    borderRadius: 18,
    padding: 18,
  },
  emptyTitle: {
    fontSize: 18,
    fontWeight: "700",
    marginBottom: 8,
    color: "#334155",
  },
  emptyText: {
    color: "#64748b",
    lineHeight: 20,
  },
});