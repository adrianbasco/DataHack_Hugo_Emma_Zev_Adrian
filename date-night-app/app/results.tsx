import { StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import SwipeDeck from "../components/SwipeDeck";
import { mockPlans } from "../lib/mockPlans";
import { savePlan } from "../lib/storage";
import { GenerateRequest } from "../lib/types";
import {
  ActionButton,
  Eyebrow,
  ScreenShell,
  SelectChip,
  palette,
} from "../components/ui";

export default function ResultsScreen() {
  const router = useRouter();
  const params = useLocalSearchParams();

  const payload = params.payload
    ? (JSON.parse(params.payload as string) as GenerateRequest)
    : null;

  return (
    <ScreenShell contentContainerStyle={styles.container}>
      <View style={styles.header}>
        <View style={styles.headerCopy}>
          <Eyebrow>Swipe and shortlist</Eyebrow>
          <Text style={styles.title}>Date ideas worth sending.</Text>
          <Text style={styles.subtitle}>
            Swipe right to save the plans that feel like an instant yes.
          </Text>
          {payload ? (
            <View style={styles.filters}>
              <SelectChip label={payload.location || "Anywhere"} selected />
              <SelectChip label={payload.vibe} selected />
              <SelectChip label={payload.budget} selected />
            </View>
          ) : null}
        </View>

        <ActionButton
          label="Saved"
          variant="secondary"
          onPress={() => router.push("/saved")}
          style={styles.savedButton}
        />
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
    </ScreenShell>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    gap: 18,
  },
  header: {
    gap: 14,
  },
  headerCopy: {
    gap: 8,
  },
  title: {
    fontSize: 32,
    lineHeight: 38,
    fontWeight: "900",
    color: palette.text,
  },
  subtitle: {
    color: palette.textMuted,
    fontSize: 15,
    lineHeight: 22,
    maxWidth: 540,
  },
  filters: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 10,
  },
  savedButton: {
    alignSelf: "flex-start",
  },
});
