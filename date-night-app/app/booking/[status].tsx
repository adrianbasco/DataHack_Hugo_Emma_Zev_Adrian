import { View, Text, Pressable, StyleSheet } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";

export default function BookingStatusScreen() {
  const router = useRouter();
  const { status } = useLocalSearchParams<{ status?: string }>();

  const config = {
    pending: {
      title: "Booking in Progress",
      description: "We’re reaching out to the restaurant to confirm your reservation.",
      action: "We’ll notify you once confirmed.",
    },
    confirmed: {
      title: "Booking Confirmed!",
      description: "Your table has been reserved.",
      action: "Have a wonderful date night.",
    },
    declined: {
      title: "Booking Declined",
      description: "Unfortunately the restaurant couldn’t accommodate your request.",
      action: "Try another date or venue.",
    },
    "no-answer": {
      title: "No Response",
      description: "We couldn’t reach the restaurant.",
      action: "You may want to contact them directly.",
    },
  } as const;

  const current =
    config[(status as keyof typeof config) || "pending"] ?? config.pending;

  return (
    <View style={styles.container}>
      <View style={styles.card}>
        <Text style={styles.title}>{current.title}</Text>
        <Text style={styles.description}>{current.description}</Text>
        <Text style={styles.action}>{current.action}</Text>

        {status === "confirmed" ? (
          <Pressable style={styles.primaryButton} onPress={() => router.push("/saved")}>
            <Text style={styles.primaryButtonText}>View My Dates</Text>
          </Pressable>
        ) : null}

        {status === "declined" ? (
          <Pressable style={styles.primaryButton} onPress={() => router.push("/results")}>
            <Text style={styles.primaryButtonText}>Find Another Date</Text>
          </Pressable>
        ) : null}

        <Pressable style={styles.secondaryButton} onPress={() => router.push("/")}>
          <Text style={styles.secondaryButtonText}>Back to Home</Text>
        </Pressable>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#fff7f8",
    justifyContent: "center",
    alignItems: "center",
    padding: 20,
  },
  card: {
    width: "100%",
    maxWidth: 380,
    backgroundColor: "white",
    borderRadius: 24,
    padding: 24,
    shadowColor: "#000",
    shadowOpacity: 0.08,
    shadowRadius: 12,
    shadowOffset: { width: 0, height: 4 },
    elevation: 4,
  },
  title: {
    fontSize: 28,
    fontWeight: "700",
    textAlign: "center",
    color: "#881337",
    marginBottom: 12,
  },
  description: {
    fontSize: 16,
    textAlign: "center",
    color: "#475569",
    marginBottom: 8,
    lineHeight: 22,
  },
  action: {
    fontSize: 14,
    textAlign: "center",
    color: "#64748b",
    marginBottom: 24,
  },
  primaryButton: {
    backgroundColor: "#ec4899",
    borderRadius: 999,
    paddingVertical: 14,
    marginBottom: 12,
  },
  primaryButtonText: {
    textAlign: "center",
    color: "white",
    fontWeight: "700",
  },
  secondaryButton: {
    backgroundColor: "#f8fafc",
    borderRadius: 999,
    paddingVertical: 14,
  },
  secondaryButtonText: {
    textAlign: "center",
    color: "#334155",
    fontWeight: "700",
  },
});