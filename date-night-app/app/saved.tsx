import { useCallback, useState } from "react";
import { StyleSheet, Text, View, useWindowDimensions } from "react-native";
import { useFocusEffect, useRouter } from "expo-router";
import PlanCard from "../components/PlanCard";
import {
  ActionButton,
  Eyebrow,
  ScreenShell,
  SurfaceCard,
  palette,
} from "../components/ui";
import { clearSavedPlans, getSavedPlans } from "../lib/storage";
import { Plan } from "../lib/types";

const MAX_CONTENT_WIDTH = 420;

export default function SavedScreen() {
  const router = useRouter();
  const { width } = useWindowDimensions();
  const [plans, setPlans] = useState<Plan[]>([]);

  const contentWidth = Math.min(Math.max(width - 24, 280), MAX_CONTENT_WIDTH);
  const isSmallScreen = width < 420;

  const handleClear = async () => {
    try {
      await clearSavedPlans();
      setPlans([]);
    } catch (error) {
      console.error("Failed to clear saved plans.", error);
    }
  };

  useFocusEffect(
    useCallback(() => {
      let isActive = true;

      (async () => {
        try {
          const savedPlans = await getSavedPlans();
          console.log("SAVED SCREEN loaded plans:", savedPlans);
          console.log("SAVED SCREEN count:", savedPlans?.length);

          if (isActive) {
            setPlans(Array.isArray(savedPlans) ? savedPlans : []);
          }
        } catch (error) {
          console.error("Failed to load saved plans.", error);
          if (isActive) {
            setPlans([]);
          }
        }
      })();
      console.log("RENDERING saved screen plans:", plans);
      return () => {
        isActive = false;
      };
    }, [])
  );

  return (
    <ScreenShell scroll>
      <View style={styles.screenContent}>
        <View style={[styles.frame, { maxWidth: contentWidth }]}>
          <View style={styles.header}>
            <Eyebrow tone="success">Saved plans</Eyebrow>

            <Text
              style={[
                styles.title,
                {
                  fontSize: isSmallScreen ? 30 : 34,
                  lineHeight: isSmallScreen ? 36 : 40,
                },
              ]}
            >
              Your shortlist
            </Text>

            <Text style={styles.subtitle}>
              Plans you swipe-saved from the planner stay here for deeper review and
              booking handoff.
            </Text>

            {plans.length > 0 ? (
              <ActionButton
                label="Clear saved plans"
                variant="secondary"
                onPress={handleClear}
              />
            ) : null}
          </View>

          {plans.length === 0 ? (
            <SurfaceCard style={styles.emptyCard}>
              <Text style={styles.emptyTitle}>No saved dates yet</Text>
              <Text style={styles.emptyText}>
                Save a plan from the results screen and it will appear here ready for
                detail view.
              </Text>
              <ActionButton
                label="Back to planner"
                variant="secondary"
                onPress={() => router.push("/")}
              />
            </SurfaceCard>
          ) : (
            <View style={styles.list}>
              {plans.map((plan, index) => (
                <View key={plan?.id ?? `saved-plan-${index}`} style={styles.cardWrap}>
                  <PlanCard
                    plan={plan}
                    onView={() =>
                      router.push({
                        pathname: "/plan/[id]",
                        params: { id: plan.id },
                      })
                    }
                  />
                </View>
              ))}
            </View>
          )}
        </View>
      </View>
    </ScreenShell>
  );
}

const styles = StyleSheet.create({
  screenContent: {
    width: "100%",
    alignItems: "center",
  },
  frame: {
    width: "100%",
    alignSelf: "center",
    gap: 18,
    paddingBottom: 20,
  },
  header: {
    gap: 8,
  },
  title: {
    color: palette.text,
    fontWeight: "900",
  },
  subtitle: {
    color: palette.textMuted,
    lineHeight: 22,
    fontSize: 15,
  },
  list: {
    width: "100%",
  },
  cardWrap: {
    width: "100%",
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