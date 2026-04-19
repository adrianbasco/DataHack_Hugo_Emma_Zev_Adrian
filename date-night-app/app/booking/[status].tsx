import { StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import { ActionButton, Eyebrow, ScreenShell, SurfaceCard, palette } from "../../components/ui";

export default function BookingStatusScreen() {
  const router = useRouter();
  const { status } = useLocalSearchParams<{ status?: string }>();

  const config = {
    pending: {
      title: "Booking in progress",
      description: "We are reaching out to the restaurant to confirm your reservation.",
      action: "We will notify you once it is confirmed.",
      tone: "warm" as const,
    },
    confirmed: {
      title: "Booking confirmed",
      description: "Your table has been reserved and the date is ready to go.",
      action: "Have a wonderful night out.",
      tone: "success" as const,
    },
    declined: {
      title: "Booking declined",
      description: "Unfortunately the restaurant could not accommodate your request.",
      action: "Try another date plan or venue.",
      tone: "default" as const,
    },
    "no-answer": {
      title: "No response yet",
      description: "We could not reach the restaurant in time.",
      action: "You may want to contact them directly.",
      tone: "default" as const,
    },
  } as const;

  const current =
    config[(status as keyof typeof config) || "pending"] ?? config.pending;

  return (
    <ScreenShell contentContainerStyle={styles.container}>
      <SurfaceCard style={styles.card}>
        <Eyebrow tone={current.tone}>{status || "pending"}</Eyebrow>
        <Text style={styles.title}>{current.title}</Text>
        <Text style={styles.description}>{current.description}</Text>
        <Text style={styles.action}>{current.action}</Text>

        <View style={styles.actions}>
          {status === "confirmed" ? (
            <ActionButton label="View my dates" onPress={() => router.push("/saved")} />
          ) : null}

          {status === "declined" ? (
            <ActionButton
              label="Find another date"
              onPress={() => router.push("/results")}
            />
          ) : null}

          <ActionButton
            label="Back to home"
            variant="secondary"
            onPress={() => router.push("/")}
          />
        </View>
      </SurfaceCard>
    </ScreenShell>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
  },
  card: {
    width: "100%",
    maxWidth: 420,
    gap: 12,
    paddingVertical: 24,
  },
  title: {
    fontSize: 32,
    lineHeight: 38,
    fontWeight: "900",
    textAlign: "center",
    color: palette.text,
  },
  description: {
    fontSize: 16,
    textAlign: "center",
    color: palette.textSoft,
    lineHeight: 24,
  },
  action: {
    fontSize: 14,
    textAlign: "center",
    color: palette.textMuted,
    marginBottom: 8,
  },
  actions: {
    gap: 12,
    marginTop: 6,
  },
});
