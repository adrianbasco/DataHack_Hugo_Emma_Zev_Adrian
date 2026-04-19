import { Image, Pressable, StyleSheet, Text, View } from "react-native";
import { Plan } from "../lib/types";
import { ActionButton, SurfaceCard, palette } from "./ui";

type Props = {
  plan: Plan;
  onView: () => void;
  onSave?: () => void;
};

export default function PlanCard({ plan, onView, onSave }: Props) {
  return (
    <SurfaceCard style={styles.card}>
      {plan.heroImageUrl ? (
        <Image source={{ uri: plan.heroImageUrl }} style={styles.image} />
      ) : null}

      <View style={styles.content}>
        <View style={styles.titleRow}>
          <Text style={styles.title}>{plan.title}</Text>
          <View style={styles.priceBadge}>
            <Text style={styles.priceText}>{plan.costBand}</Text>
          </View>
        </View>

        <Text style={styles.subtitle}>{plan.vibeLine}</Text>

        <View style={styles.metaRow}>
          <MetaBadge label={plan.durationLabel} />
          {plan.weather ? <MetaBadge label={plan.weather} tone="cool" /> : null}
        </View>

        <View style={styles.stops}>
          {plan.stops.map((stop, index) => (
            <View key={stop.id} style={styles.stopRow}>
              <View style={styles.stopIndex}>
                <Text style={styles.stopIndexText}>{index + 1}</Text>
              </View>
              <Text style={styles.stopText}>{stop.name}</Text>
            </View>
          ))}
        </View>

        <View style={styles.actions}>
          <ActionButton
            label="View details"
            variant="secondary"
            onPress={onView}
            style={styles.actionButton}
          />
          {onSave ? (
            <ActionButton label="Save plan" onPress={onSave} style={styles.actionButton} />
          ) : null}
        </View>
      </View>
    </SurfaceCard>
  );
}

function MetaBadge({
  label,
  tone = "default",
}: {
  label: string;
  tone?: "default" | "cool";
}) {
  return (
    <Pressable disabled style={[styles.metaBadge, tone === "cool" && styles.metaBadgeCool]}>
      <Text style={[styles.metaBadgeText, tone === "cool" && styles.metaBadgeTextCool]}>
        {label}
      </Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  card: {
    padding: 0,
    overflow: "hidden",
    marginBottom: 18,
  },
  image: {
    width: "100%",
    height: 220,
  },
  content: {
    padding: 18,
    gap: 14,
  },
  titleRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "flex-start",
    gap: 12,
  },
  title: {
    flex: 1,
    fontSize: 24,
    lineHeight: 29,
    fontWeight: "800",
    color: palette.text,
  },
  priceBadge: {
    borderRadius: 999,
    paddingHorizontal: 12,
    paddingVertical: 8,
    backgroundColor: "rgba(255, 122, 89, 0.18)",
    borderWidth: 1,
    borderColor: "rgba(255, 151, 124, 0.35)",
  },
  priceText: {
    color: palette.text,
    fontWeight: "800",
    fontSize: 12,
  },
  subtitle: {
    color: palette.textMuted,
    fontSize: 15,
    lineHeight: 22,
  },
  metaRow: {
    flexDirection: "row",
    gap: 10,
    flexWrap: "wrap",
  },
  metaBadge: {
    borderRadius: 999,
    paddingHorizontal: 12,
    paddingVertical: 8,
    backgroundColor: "rgba(255, 255, 255, 0.06)",
    borderWidth: 1,
    borderColor: palette.border,
  },
  metaBadgeCool: {
    backgroundColor: "rgba(52, 211, 153, 0.14)",
    borderColor: "rgba(52, 211, 153, 0.26)",
  },
  metaBadgeText: {
    color: palette.textSoft,
    fontWeight: "700",
    fontSize: 12,
  },
  metaBadgeTextCool: {
    color: "#b4f5dd",
  },
  stops: {
    gap: 10,
  },
  stopRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
  },
  stopIndex: {
    width: 26,
    height: 26,
    borderRadius: 13,
    backgroundColor: "rgba(249, 199, 79, 0.18)",
    borderWidth: 1,
    borderColor: "rgba(249, 199, 79, 0.28)",
    justifyContent: "center",
    alignItems: "center",
  },
  stopIndexText: {
    color: palette.text,
    fontSize: 12,
    fontWeight: "800",
  },
  stopText: {
    color: palette.textSoft,
    fontSize: 14,
    flex: 1,
  },
  actions: {
    flexDirection: "row",
    gap: 10,
    marginTop: 4,
  },
  actionButton: {
    flex: 1,
  },
});
