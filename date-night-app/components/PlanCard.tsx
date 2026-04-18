import { View, Text, Image, Button, StyleSheet } from "react-native";
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

      <Text style={styles.title}>{plan.title}</Text>
      <Text style={styles.subtitle}>{plan.vibeLine}</Text>
      <Text>{plan.durationLabel} · {plan.costBand}</Text>

      <View style={{ marginTop: 8 }}>
        {plan.stops.map((stop) => (
          <Text key={stop.id}>• {stop.name}</Text>
        ))}
      </View>

      <View style={styles.actions}>
        <Button title="View details" onPress={onView} />
        {onSave ? <Button title="Save" onPress={onSave} /> : null}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    borderWidth: 1,
    borderColor: "#ddd",
    borderRadius: 12,
    padding: 16,
    marginBottom: 16,
    backgroundColor: "white",
  },
  image: {
    width: "100%",
    height: 180,
    borderRadius: 10,
    marginBottom: 12,
  },
  title: {
    fontSize: 20,
    fontWeight: "700",
  },
  subtitle: {
    color: "#666",
    marginBottom: 8,
  },
  actions: {
    flexDirection: "row",
    justifyContent: "space-between",
    marginTop: 16,
  },
});