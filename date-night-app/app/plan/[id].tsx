import { useMemo } from "react";
import {
  ScrollView,
  Text,
  View,
  Image,
  Pressable,
  StyleSheet,
  Share,
  Linking,
} from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import { mockPlans } from "../../lib/mockPlans";

export default function PlanDetailScreen() {
  const router = useRouter();
  const { id } = useLocalSearchParams<{ id?: string }>();

  const plan = useMemo(() => mockPlans.find((p) => p.id === id), [id]);

  if (!plan) {
    return (
      <View style={styles.centered}>
        <Text>Plan not found.</Text>
      </View>
    );
  }

  async function handleShare() {
    await Share.share({
      message: `${plan.title}\n${plan.vibeLine}\nStops: ${plan.stops
        .map((s) => s.name)
        .join(" → ")}`,
    });
  }

  return (
    <ScrollView contentContainerStyle={styles.container}>
      {plan.heroImageUrl ? (
        <Image source={{ uri: plan.heroImageUrl }} style={styles.heroImage} />
      ) : null}

      <Text style={styles.title}>{plan.title}</Text>
      <Text style={styles.subtitle}>{plan.vibeLine}</Text>
      {plan.summary ? <Text style={styles.summary}>{plan.summary}</Text> : null}

      <View style={styles.statsCard}>
        <View style={styles.statItem}>
          <Text style={styles.statValue}>{plan.durationLabel}</Text>
          <Text style={styles.statLabel}>Duration</Text>
        </View>
        <View style={styles.statItem}>
          <Text style={styles.statValue}>{plan.costBand}</Text>
          <Text style={styles.statLabel}>Estimate</Text>
        </View>
        <View style={styles.statItem}>
          <Text style={styles.statValue}>{plan.weather || "-"}</Text>
          <Text style={styles.statLabel}>Weather</Text>
        </View>
      </View>

      <Text style={styles.sectionTitle}>Your Itinerary</Text>
      {plan.stops.map((stop, index) => (
        <View key={stop.id} style={styles.stopCard}>
          <View style={styles.stopHeader}>
            <View style={styles.stopBadge}>
              <Text style={styles.stopBadgeText}>{index + 1}</Text>
            </View>
            <View style={{ flex: 1 }}>
              <Text style={styles.stopTitle}>{stop.name}</Text>
              {stop.time ? <Text style={styles.stopTime}>{stop.time}</Text> : null}
            </View>
          </View>

          {stop.description ? (
            <Text style={styles.stopDescription}>{stop.description}</Text>
          ) : null}

          {stop.transport ? (
            <Text style={styles.transportLabel}>Transport: {stop.transport}</Text>
          ) : null}

          {stop.mapsUrl ? (
            <Pressable
              onPress={() => Linking.openURL(stop.mapsUrl!)}
              style={styles.mapButton}
            >
              <Text style={styles.mapButtonText}>View on map</Text>
            </Pressable>
          ) : null}
        </View>
      ))}

      {plan.transportLegs?.length ? (
        <>
          <Text style={styles.sectionTitle}>Transport Summary</Text>
          <View style={styles.transportCard}>
            {plan.transportLegs.map((leg, index) => (
              <Text key={index} style={styles.transportRow}>
                {leg.mode} · {leg.durationText}
              </Text>
            ))}
          </View>
        </>
      ) : null}

      <View style={styles.actions}>
        <Pressable
          style={styles.primaryButton}
          onPress={() => router.push("/booking/pending")}
        >
          <Text style={styles.primaryButtonText}>Book Restaurant</Text>
        </Pressable>

        <Pressable style={styles.secondaryButton} onPress={handleShare}>
          <Text style={styles.secondaryButtonText}>Share Plan</Text>
        </Pressable>
      </View>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  centered: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
  },
  container: {
    padding: 16,
    backgroundColor: "#fff7f8",
  },
  heroImage: {
    width: "100%",
    height: 260,
    borderRadius: 24,
    marginBottom: 16,
  },
  title: {
    fontSize: 30,
    fontWeight: "700",
    color: "#881337",
    marginBottom: 6,
  },
  subtitle: {
    fontSize: 16,
    color: "#475569",
    marginBottom: 10,
  },
  summary: {
    fontSize: 15,
    color: "#334155",
    lineHeight: 22,
    marginBottom: 16,
  },
  statsCard: {
    backgroundColor: "white",
    borderRadius: 20,
    padding: 16,
    marginBottom: 20,
    flexDirection: "row",
    justifyContent: "space-between",
  },
  statItem: {
    alignItems: "center",
    flex: 1,
  },
  statValue: {
    fontWeight: "700",
    color: "#0f172a",
    marginBottom: 4,
    textAlign: "center",
  },
  statLabel: {
    color: "#64748b",
    fontSize: 12,
  },
  sectionTitle: {
    fontSize: 22,
    fontWeight: "700",
    color: "#0f172a",
    marginBottom: 12,
    marginTop: 4,
  },
  stopCard: {
    backgroundColor: "white",
    borderRadius: 20,
    padding: 16,
    marginBottom: 12,
  },
  stopHeader: {
    flexDirection: "row",
    alignItems: "center",
    gap: 12,
    marginBottom: 8,
  },
  stopBadge: {
    width: 34,
    height: 34,
    borderRadius: 17,
    backgroundColor: "#ec4899",
    justifyContent: "center",
    alignItems: "center",
  },
  stopBadgeText: {
    color: "white",
    fontWeight: "700",
  },
  stopTitle: {
    fontSize: 16,
    fontWeight: "700",
    color: "#0f172a",
  },
  stopTime: {
    color: "#64748b",
    marginTop: 2,
    fontSize: 13,
  },
  stopDescription: {
    color: "#475569",
    lineHeight: 21,
    marginBottom: 8,
  },
  transportLabel: {
    color: "#be185d",
    fontSize: 13,
    marginBottom: 8,
  },
  mapButton: {
    backgroundColor: "#fff1f2",
    borderRadius: 999,
    paddingVertical: 10,
  },
  mapButtonText: {
    color: "#be185d",
    fontWeight: "600",
    textAlign: "center",
  },
  transportCard: {
    backgroundColor: "white",
    borderRadius: 18,
    padding: 16,
    marginBottom: 18,
  },
  transportRow: {
    color: "#475569",
    marginBottom: 8,
  },
  actions: {
    gap: 12,
    marginBottom: 30,
  },
  primaryButton: {
    backgroundColor: "#ec4899",
    borderRadius: 999,
    paddingVertical: 15,
  },
  primaryButtonText: {
    color: "white",
    textAlign: "center",
    fontWeight: "700",
    fontSize: 16,
  },
  secondaryButton: {
    backgroundColor: "white",
    borderRadius: 999,
    paddingVertical: 15,
  },
  secondaryButtonText: {
    color: "#334155",
    textAlign: "center",
    fontWeight: "700",
    fontSize: 16,
  },
});