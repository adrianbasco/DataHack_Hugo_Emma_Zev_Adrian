import { useMemo } from "react";
import {
  Image,
  Linking,
  Share,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import { mockPlans } from "../../lib/mockPlans";
import {
  ActionButton,
  Eyebrow,
  ScreenShell,
  SurfaceCard,
  palette,
} from "../../components/ui";

export default function PlanDetailScreen() {
  const router = useRouter();
  const { id } = useLocalSearchParams<{ id?: string }>();

  const plan = useMemo(() => mockPlans.find((p) => p.id === id), [id]);

  if (!plan) {
    return (
      <ScreenShell contentContainerStyle={styles.centered}>
        <SurfaceCard style={styles.notFoundCard}>
          <Text style={styles.notFoundTitle}>Plan not found</Text>
          <Text style={styles.notFoundText}>
            This date idea is unavailable right now. Try another saved plan.
          </Text>
        </SurfaceCard>
      </ScreenShell>
    );
  }

  async function handleShare() {
    await Share.share({
      message: `${plan.title}\n${plan.vibeLine}\nStops: ${plan.stops
        .map((s) => s.name)
        .join(" -> ")}`,
    });
  }

  return (
    <ScreenShell scroll>
      {plan.heroImageUrl ? (
        <Image source={{ uri: plan.heroImageUrl }} style={styles.heroImage} />
      ) : null}

      <View style={styles.heroCopy}>
        <Eyebrow tone="warm">Full itinerary</Eyebrow>
        <Text style={styles.title}>{plan.title}</Text>
        <Text style={styles.subtitle}>{plan.vibeLine}</Text>
        {plan.summary ? <Text style={styles.summary}>{plan.summary}</Text> : null}
      </View>

      <View style={styles.statsRow}>
        <StatCard label="Duration" value={plan.durationLabel} />
        <StatCard label="Estimate" value={plan.costBand} />
        <StatCard label="Weather" value={plan.weather || "-"} />
      </View>

      <View style={styles.section}>
        <Text style={styles.sectionTitle}>Your itinerary</Text>
        {plan.stops.map((stop, index) => (
          <SurfaceCard key={stop.id} style={styles.stopCard}>
            <View style={styles.stopHeader}>
              <View style={styles.stopBadge}>
                <Text style={styles.stopBadgeText}>{index + 1}</Text>
              </View>
              <View style={styles.stopHeaderCopy}>
                <Text style={styles.stopTitle}>{stop.name}</Text>
                {stop.time ? <Text style={styles.stopTime}>{stop.time}</Text> : null}
              </View>
            </View>

            {stop.description ? (
              <Text style={styles.stopDescription}>{stop.description}</Text>
            ) : null}

            {stop.transport ? (
              <Text style={styles.transportLabel}>Transit: {stop.transport}</Text>
            ) : null}

            {stop.mapsUrl ? (
              <ActionButton
                label="Open in maps"
                variant="secondary"
                onPress={() => Linking.openURL(stop.mapsUrl!)}
              />
            ) : null}
          </SurfaceCard>
        ))}
      </View>

      {plan.transportLegs?.length ? (
        <View style={styles.section}>
          <Text style={styles.sectionTitle}>Movement summary</Text>
          <SurfaceCard style={styles.transportCard}>
            {plan.transportLegs.map((leg, index) => (
              <View key={index} style={styles.transportRow}>
                <Text style={styles.transportMode}>{leg.mode}</Text>
                <Text style={styles.transportDuration}>{leg.durationText}</Text>
              </View>
            ))}
          </SurfaceCard>
        </View>
      ) : null}

      <View style={styles.actions}>
        <ActionButton label="Book restaurant" onPress={() => router.push("/booking/pending")} />
        <ActionButton label="Share plan" variant="secondary" onPress={handleShare} />
      </View>
    </ScreenShell>
  );
}

function StatCard({ label, value }: { label: string; value: string }) {
  return (
    <SurfaceCard style={styles.statCard}>
      <Text style={styles.statValue}>{value}</Text>
      <Text style={styles.statLabel}>{label}</Text>
    </SurfaceCard>
  );
}

const styles = StyleSheet.create({
  centered: {
    flex: 1,
    justifyContent: "center",
  },
  notFoundCard: {
    gap: 8,
  },
  notFoundTitle: {
    color: palette.text,
    fontSize: 24,
    fontWeight: "900",
  },
  notFoundText: {
    color: palette.textMuted,
    fontSize: 15,
    lineHeight: 22,
  },
  heroImage: {
    width: "100%",
    height: 280,
    borderRadius: 30,
    marginTop: 4,
  },
  heroCopy: {
    gap: 8,
  },
  title: {
    fontSize: 34,
    lineHeight: 40,
    fontWeight: "900",
    color: palette.text,
  },
  subtitle: {
    fontSize: 16,
    color: palette.textSoft,
  },
  summary: {
    fontSize: 15,
    color: palette.textMuted,
    lineHeight: 23,
  },
  statsRow: {
    flexDirection: "row",
    gap: 12,
  },
  statCard: {
    flex: 1,
    gap: 6,
    alignItems: "center",
    paddingVertical: 18,
  },
  statValue: {
    fontWeight: "900",
    color: palette.text,
    fontSize: 16,
    textAlign: "center",
  },
  statLabel: {
    color: palette.textMuted,
    fontSize: 12,
  },
  section: {
    gap: 12,
  },
  sectionTitle: {
    fontSize: 24,
    fontWeight: "900",
    color: palette.text,
  },
  stopCard: {
    gap: 12,
  },
  stopHeader: {
    flexDirection: "row",
    alignItems: "center",
    gap: 12,
  },
  stopHeaderCopy: {
    flex: 1,
    gap: 2,
  },
  stopBadge: {
    width: 38,
    height: 38,
    borderRadius: 19,
    backgroundColor: "rgba(255, 122, 89, 0.18)",
    borderWidth: 1,
    borderColor: "rgba(255, 151, 124, 0.34)",
    justifyContent: "center",
    alignItems: "center",
  },
  stopBadgeText: {
    color: palette.text,
    fontWeight: "900",
  },
  stopTitle: {
    fontSize: 17,
    fontWeight: "800",
    color: palette.text,
  },
  stopTime: {
    color: palette.textMuted,
    fontSize: 13,
  },
  stopDescription: {
    color: palette.textSoft,
    lineHeight: 22,
  },
  transportLabel: {
    color: palette.accentWarm,
    fontSize: 13,
    fontWeight: "700",
  },
  transportCard: {
    gap: 10,
  },
  transportRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 12,
  },
  transportMode: {
    color: palette.text,
    fontWeight: "700",
    textTransform: "capitalize",
  },
  transportDuration: {
    color: palette.textMuted,
  },
  actions: {
    gap: 12,
    marginBottom: 6,
  },
});
