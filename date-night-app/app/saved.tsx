import { useCallback, useState } from "react";
import { StyleSheet, Text, View } from "react-native";
import { useFocusEffect, useRouter } from "expo-router";
import PlanCard from "../components/PlanCard";
import {
  ActionButton,
  Eyebrow,
  ScreenShell,
  SurfaceCard,
  palette,
} from "../components/ui";
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
    <ScreenShell scroll>
      <View style={styles.header}>
        <Eyebrow tone="success">Saved plans</Eyebrow>
        <Text style={styles.title}>Your shortlist</Text>
        <Text style={styles.subtitle}>
          Plans you swipe-saved from the planner stay here for deeper review and booking handoff.
        </Text>
      </View>

      {plans.length === 0 ? (
        <SurfaceCard style={styles.emptyCard}>
          <Text style={styles.emptyTitle}>No saved dates yet</Text>
          <Text style={styles.emptyText}>
            Save a plan from the results screen and it will appear here ready for detail view.
          </Text>
          <ActionButton
            label="Back to planner"
            variant="secondary"
            onPress={() => router.push("/")}
          />
        </SurfaceCard>
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
    </ScreenShell>
  );
}

const styles = StyleSheet.create({
  header: {
    gap: 8,
  },
  title: {
    color: palette.text,
    fontSize: 34,
    fontWeight: "900",
  },
  subtitle: {
    color: palette.textMuted,
    lineHeight: 22,
    fontSize: 15,
  },
  emptyCard: {
    gap: 14,
  },
  emptyTitle: {
    color: palette.text,
    fontSize: 22,
    fontWeight: "800",
  },
  emptyText: {
    color: palette.textMuted,
    lineHeight: 22,
    fontSize: 14,
  },
});
