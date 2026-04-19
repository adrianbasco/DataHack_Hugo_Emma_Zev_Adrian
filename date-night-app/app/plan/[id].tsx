import { useEffect, useState } from "react";
import { ActivityIndicator, Image, Linking, StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import {
  ActionButton,
  Eyebrow,
  ScreenShell,
  SurfaceCard,
  palette,
} from "../../components/ui";
import { getPlanById } from "../../lib/storage";
import { Plan } from "../../lib/types";

export default function PlanDetailScreen() {
  const router = useRouter();
  const { id } = useLocalSearchParams<{ id?: string }>();
  const [plan, setPlan] = useState<Plan | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let active = true;

    async function loadPlan() {
      if (!id) {
        setLoading(false);
        return;
      }

      const result = await getPlanById(id);
      if (!active) {
        return;
      }
      setPlan(result ?? null);
      setLoading(false);
    }

    void loadPlan();

    return () => {
      active = false;
    };
  }, [id]);

  if (loading) {
    return (
      <ScreenShell contentContainerStyle={styles.centered}>
        <SurfaceCard style={styles.centerCard}>
          <ActivityIndicator color={palette.accent} />
          <Text style={styles.centerTitle}>Loading plan</Text>
        </SurfaceCard>
      </ScreenShell>
    );
  }

  if (!plan) {
    return (
      <ScreenShell contentContainerStyle={styles.centered}>
        <SurfaceCard style={styles.centerCard}>
          <Text style={styles.centerTitle}>Plan not found</Text>
          <Text style={styles.centerText}>
            This plan is no longer in the active or saved cache.
          </Text>
          <ActionButton label="Back to results" variant="secondary" onPress={() => router.back()} />
        </SurfaceCard>
      </ScreenShell>
    );
  }

  return (
    <ScreenShell scroll>
      {plan.heroImageUrl ? (
        <Image source={{ uri: plan.heroImageUrl }} style={styles.heroImage} />
      ) : null}

      <View style={styles.heroCopy}>
        <Eyebrow tone="warm">{plan.templateHint || "Planner idea"}</Eyebrow>
        <Text style={styles.title}>{plan.title}</Text>
        <Text style={styles.subtitle}>{plan.hook}</Text>
        {plan.summary ? <Text style={styles.summary}>{plan.summary}</Text> : null}
      </View>

      <View style={styles.metaWrap}>
        {plan.durationLabel ? <MetaCard label="Duration" value={plan.durationLabel} /> : null}
        {plan.costBand ? <MetaCard label="Budget" value={plan.costBand} /> : null}
        {plan.weather ? <MetaCard label="Weather" value={plan.weather} /> : null}
      </View>

      {plan.mapsVerificationNeeded ? (
        <SurfaceCard style={styles.sectionCard}>
          <Text style={styles.sectionTitle}>Manual map check recommended</Text>
          <Text style={styles.summary}>
            The backend flagged this plan for a quick maps sanity check before treating travel timing or venue availability as final.
          </Text>
        </SurfaceCard>
      ) : null}

      {plan.constraintsConsidered.length ? (
        <SurfaceCard style={styles.sectionCard}>
          <Text style={styles.sectionTitle}>Planner constraints considered</Text>
          <View style={styles.tagWrap}>
            {plan.constraintsConsidered.map((constraint) => (
              <View key={constraint} style={styles.tag}>
                <Text style={styles.tagText}>{constraint}</Text>
              </View>
            ))}
          </View>
        </SurfaceCard>
      ) : null}

      <View style={styles.section}>
        <Text style={styles.sectionTitle}>Itinerary</Text>
        {plan.stops.map((stop, index) => (
          <SurfaceCard key={stop.id} style={styles.stopCard}>
            <View style={styles.stopHeader}>
              <View style={styles.stopBadge}>
                <Text style={styles.stopBadgeText}>{index + 1}</Text>
              </View>
              <View style={styles.stopHeaderCopy}>
                <Text style={styles.stopTitle}>{stop.name}</Text>
                <Text style={styles.stopType}>{humanizeToken(stop.stopType)}</Text>
              </View>
            </View>

            <Text style={styles.stopDescription}>{stop.description}</Text>
            {stop.whyItFits ? (
              <Text style={styles.whyItFits}>Why it fits: {stop.whyItFits}</Text>
            ) : null}

            <View style={styles.stopMeta}>
              {stop.time ? <Text style={styles.stopMetaText}>Time: {stop.time}</Text> : null}
              {stop.transport ? (
                <Text style={styles.stopMetaText}>Transit: {stop.transport}</Text>
              ) : null}
              {stop.address ? (
                <Text style={styles.stopMetaText}>Address: {stop.address}</Text>
              ) : null}
            </View>

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
        <SurfaceCard style={styles.sectionCard}>
          <Text style={styles.sectionTitle}>Transport summary</Text>
          {plan.transportLegs.map((leg, index) => (
            <View key={`${leg.mode}-${index}`} style={styles.transportRow}>
              <Text style={styles.transportMode}>{leg.mode}</Text>
              <Text style={styles.transportDuration}>{leg.durationText}</Text>
            </View>
          ))}
        </SurfaceCard>
      ) : null}

      <View style={styles.actions}>
        {plan.bookingContext?.restaurantName ? (
          <ActionButton
            label="Request restaurant booking"
            onPress={() =>
              router.push({
                pathname: "/booking/request",
                params: { planId: plan.id },
              })
            }
          />
        ) : null}
        <ActionButton label="Saved dates" variant="secondary" onPress={() => router.push("/saved")} />
      </View>
    </ScreenShell>
  );
}

function MetaCard({ label, value }: { label: string; value: string }) {
  return (
    <SurfaceCard style={styles.metaCard}>
      <Text style={styles.metaValue}>{value}</Text>
      <Text style={styles.metaLabel}>{label}</Text>
    </SurfaceCard>
  );
}

function humanizeToken(value: string) {
  return value
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

const styles = StyleSheet.create({
  centered: {
    flex: 1,
    justifyContent: "center",
  },
  centerCard: {
    gap: 10,
    alignItems: "center",
  },
  centerTitle: {
    color: palette.text,
    fontSize: 22,
    fontWeight: "800",
  },
  centerText: {
    color: palette.textMuted,
    textAlign: "center",
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
    lineHeight: 24,
  },
  summary: {
    fontSize: 15,
    color: palette.textMuted,
    lineHeight: 23,
  },
  metaWrap: {
    flexDirection: "row",
    gap: 12,
    flexWrap: "wrap",
  },
  metaCard: {
    flex: 1,
    minWidth: 140,
    alignItems: "center",
    gap: 6,
    paddingVertical: 18,
  },
  metaValue: {
    color: palette.text,
    fontWeight: "900",
    fontSize: 16,
    textAlign: "center",
  },
  metaLabel: {
    color: palette.textMuted,
    fontSize: 12,
  },
  section: {
    gap: 12,
  },
  sectionCard: {
    gap: 12,
  },
  sectionTitle: {
    color: palette.text,
    fontSize: 24,
    fontWeight: "900",
  },
  tagWrap: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 10,
  },
  tag: {
    borderRadius: 999,
    paddingHorizontal: 12,
    paddingVertical: 8,
    backgroundColor: "rgba(255, 255, 255, 0.06)",
    borderWidth: 1,
    borderColor: palette.border,
  },
  tagText: {
    color: palette.textSoft,
    fontWeight: "700",
    fontSize: 12,
  },
  stopCard: {
    gap: 12,
  },
  stopHeader: {
    flexDirection: "row",
    gap: 12,
    alignItems: "center",
  },
  stopHeaderCopy: {
    flex: 1,
    gap: 3,
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
    color: palette.text,
    fontWeight: "800",
    fontSize: 17,
  },
  stopType: {
    color: palette.textMuted,
    fontSize: 13,
  },
  stopDescription: {
    color: palette.textSoft,
    lineHeight: 22,
  },
  whyItFits: {
    color: palette.accentWarm,
    lineHeight: 21,
  },
  stopMeta: {
    gap: 6,
  },
  stopMetaText: {
    color: palette.textMuted,
    lineHeight: 20,
  },
  transportRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 12,
  },
  transportMode: {
    color: palette.text,
    fontWeight: "700",
  },
  transportDuration: {
    color: palette.textMuted,
  },
  actions: {
    gap: 12,
    marginBottom: 6,
  },
});
