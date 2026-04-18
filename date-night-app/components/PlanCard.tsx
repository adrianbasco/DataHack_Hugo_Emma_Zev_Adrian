import { View, Text, Image, Pressable, StyleSheet } from "react-native";
import { Plan } from "../lib/types";

type Props = {
  plan: Plan;
  onView: () => void;
  onSave?: () => void;
};

export default function PlanCard({ plan, onView, onSave }: Props) {
  return (
    <View style={styles.card}>
      {plan.heroImageUrl ? (
        <Image source={{ uri: plan.heroImageUrl }} style={styles.image} />
      ) : null}

      <View style={styles.content}>
        <Text style={styles.title}>{plan.title}</Text>
        <Text style={styles.subtitle}>{plan.vibeLine}</Text>

        <View style={styles.metaRow}>
          <Text style={styles.metaText}>{plan.durationLabel}</Text>
          <Text style={styles.metaDot}>•</Text>
          <Text style={styles.metaText}>{plan.costBand}</Text>
          {plan.weather ? (
            <>
              <Text style={styles.metaDot}>•</Text>
              <Text style={styles.metaText}>{plan.weather}</Text>
            </>
          ) : null}
        </View>

        <View style={styles.stops}>
          {plan.stops.map((stop, index) => (
            <Text key={stop.id} style={styles.stopText}>
              {index + 1}. {stop.name}
            </Text>
          ))}
        </View>

        <View style={styles.actions}>
          <Pressable style={styles.secondaryButton} onPress={onView}>
            <Text style={styles.secondaryButtonText}>View Details</Text>
          </Pressable>

          {onSave ? (
            <Pressable style={styles.primaryButton} onPress={onSave}>
              <Text style={styles.primaryButtonText}>Save</Text>
            </Pressable>
          ) : null}
        </View>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: "white",
    borderRadius: 24,
    overflow: "hidden",
    marginBottom: 18,
    shadowColor: "#000",
    shadowOpacity: 0.08,
    shadowRadius: 12,
    shadowOffset: { width: 0, height: 4 },
    elevation: 4,
  },
  image: {
    width: "100%",
    height: 210,
  },
  content: {
    padding: 16,
  },
  title: {
    fontSize: 22,
    fontWeight: "700",
    color: "#881337",
    marginBottom: 6,
  },
  subtitle: {
    color: "#475569",
    marginBottom: 10,
    fontSize: 15,
  },
  metaRow: {
    flexDirection: "row",
    alignItems: "center",
    flexWrap: "wrap",
    marginBottom: 12,
  },
  metaText: {
    color: "#64748b",
    fontSize: 13,
  },
  metaDot: {
    marginHorizontal: 6,
    color: "#cbd5e1",
  },
  stops: {
    gap: 6,
    marginBottom: 16,
  },
  stopText: {
    color: "#334155",
    fontSize: 14,
  },
  actions: {
    flexDirection: "row",
    gap: 10,
  },
  secondaryButton: {
    flex: 1,
    backgroundColor: "#fff1f2",
    borderRadius: 999,
    paddingVertical: 12,
  },
  secondaryButtonText: {
    textAlign: "center",
    color: "#be185d",
    fontWeight: "600",
  },
  primaryButton: {
    flex: 1,
    backgroundColor: "#ec4899",
    borderRadius: 999,
    paddingVertical: 12,
  },
  primaryButtonText: {
    textAlign: "center",
    color: "white",
    fontWeight: "700",
  },
});