import { useCallback, useState } from "react";
import { StyleSheet, Text, View } from "react-native";
import { useFocusEffect, useRouter } from "expo-router";
import PlanCard from "../components/PlanCard";
import { getSavedPlans } from "../lib/storage";
import { Plan } from "../lib/types";
import {
  ActionButton,
  Eyebrow,
  ScreenShell,
  SurfaceCard,
  palette,
} from "../components/ui";

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
        <Eyebrow tone="success">Your shortlist</Eyebrow>
        <Text style={styles.title}>Saved date ideas</Text>
        <Text style={styles.subtitle}>
          Keep the strongest options close, compare them later, and share your favorites.
        </Text>
      </View>

      {plans.length === 0 ? (
        <SurfaceCard style={styles.emptyCard}>
          <Text style={styles.emptyTitle}>Nothing saved yet</Text>
          <Text style={styles.emptyText}>
            Swipe right on any result and it will show up here ready for a closer look.
          </Text>
          <ActionButton
            label="Browse results"
            variant="secondary"
            onPress={() => router.push("/results")}
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
    marginBottom: 4,
  },
  title: {
    fontSize: 32,
    fontWeight: "900",
    color: palette.text,
  },
  subtitle: {
    color: palette.textMuted,
    fontSize: 15,
    lineHeight: 22,
  },
  emptyCard: {
    gap: 14,
  },
  emptyTitle: {
    fontSize: 22,
    fontWeight: "800",
    color: palette.text,
  },
  emptyText: {
    color: palette.textMuted,
    lineHeight: 22,
    fontSize: 14,
  },
});
