import { useEffect, useMemo, useState } from "react";
import { ActivityIndicator, StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import { fetchRestaurantBookingStatus } from "../../lib/api";
import {
  ActionButton,
  Eyebrow,
  ScreenShell,
  SurfaceCard,
  palette,
} from "../../components/ui";

const terminalStatuses = new Set(["confirmed", "declined", "no_answer", "failed", "unknown"]);

export default function BookingStatusScreen() {
  const router = useRouter();
  const { status, callId } = useLocalSearchParams<{ status?: string; callId?: string }>();
  const [currentStatus, setCurrentStatus] = useState(status || "queued");
  const [summary, setSummary] = useState<string | undefined>();
  const [errorMessage, setErrorMessage] = useState<string | undefined>();
  const [loading, setLoading] = useState(Boolean(callId));

  useEffect(() => {
    if (!callId || terminalStatuses.has(currentStatus)) {
      setLoading(false);
      return;
    }

    let active = true;
    let timer: ReturnType<typeof setTimeout> | undefined;

    async function poll() {
      if (!callId) {
        return;
      }
      try {
        const result = await fetchRestaurantBookingStatus(callId);
        if (!active) {
          return;
        }
        setCurrentStatus(result.status);
        setSummary(result.summary || undefined);
        setErrorMessage(result.errorMessage || undefined);
        setLoading(false);

        if (!terminalStatuses.has(result.status)) {
          timer = setTimeout(() => {
            void poll();
          }, 5000);
        }
      } catch (pollError) {
        if (!active) {
          return;
        }
        setErrorMessage(
          pollError instanceof Error
            ? pollError.message
            : "Could not fetch the live booking status."
        );
        setLoading(false);
      }
    }

    void poll();

    return () => {
      active = false;
      if (timer) {
        clearTimeout(timer);
      }
    };
  }, [callId, currentStatus]);

  const copy = useMemo(() => getCopy(currentStatus), [currentStatus]);

  return (
    <ScreenShell contentContainerStyle={styles.container}>
      <SurfaceCard style={styles.card}>
        <Eyebrow tone={copy.tone}>{currentStatus.replace(/_/g, " ")}</Eyebrow>
        <Text style={styles.title}>{copy.title}</Text>
        <Text style={styles.description}>{copy.description}</Text>

        {loading ? <ActivityIndicator color={palette.accent} /> : null}
        {summary ? <Text style={styles.summary}>Summary: {summary}</Text> : null}
        {errorMessage ? <Text style={styles.errorText}>{errorMessage}</Text> : null}
        {callId ? <Text style={styles.metaText}>Call ID: {callId}</Text> : null}

        <View style={styles.actions}>
          {currentStatus === "confirmed" ? (
            <ActionButton label="View my saved dates" onPress={() => router.push("/saved")} />
          ) : null}

          {currentStatus === "declined" || currentStatus === "failed" ? (
            <ActionButton
              label="Return to planner"
              onPress={() => router.push("/")}
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

function getCopy(status: string) {
  switch (status) {
    case "confirmed":
      return {
        title: "Booking confirmed",
        description: "The reservation has been confirmed and the backend call flow is complete.",
        tone: "success" as const,
      };
    case "declined":
      return {
        title: "Booking declined",
        description: "The restaurant could not accept the booking request in its current form.",
        tone: "default" as const,
      };
    case "no_answer":
      return {
        title: "No answer yet",
        description: "The booking service could not reach the venue successfully.",
        tone: "default" as const,
      };
    case "failed":
      return {
        title: "Booking failed",
        description: "The booking handoff failed before it could be completed.",
        tone: "default" as const,
      };
    default:
      return {
        title: "Booking in progress",
        description: "The app is polling the backend booking service for an updated call status.",
        tone: "warm" as const,
      };
  }
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
  },
  card: {
    width: "100%",
    maxWidth: 460,
    gap: 12,
  },
  title: {
    color: palette.text,
    fontSize: 32,
    lineHeight: 38,
    fontWeight: "900",
    textAlign: "center",
  },
  description: {
    color: palette.textSoft,
    lineHeight: 23,
    fontSize: 16,
    textAlign: "center",
  },
  summary: {
    color: palette.text,
    lineHeight: 22,
    textAlign: "center",
  },
  errorText: {
    color: "#fecdd3",
    textAlign: "center",
    lineHeight: 21,
  },
  metaText: {
    color: palette.textMuted,
    textAlign: "center",
    fontSize: 13,
  },
  actions: {
    gap: 12,
    marginTop: 6,
  },
});
