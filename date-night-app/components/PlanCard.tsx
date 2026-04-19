import { Image, StyleSheet, Text, View } from "react-native";
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
        <View style={styles.header}>
          <View style={styles.headerCopy}>
            <Text style={styles.title}>{plan.title}</Text>
            <Text style={styles.hook}>{plan.hook}</Text>
          </View>
          {plan.costBand ? (
            <View style={styles.badge}>
              <Text style={styles.badgeText}>{plan.costBand}</Text>
            </View>
          ) : null}
        </View>

        <View style={styles.metaWrap}>
          {plan.templateHint ? <MetaPill label={plan.templateHint} /> : null}
          {plan.durationLabel ? <MetaPill label={plan.durationLabel} /> : null}
          {plan.weather ? <MetaPill label={plan.weather} tone="cool" /> : null}
          {plan.mapsVerificationNeeded ? <MetaPill label="Check maps" tone="cool" /> : null}
        </View>

        <View style={styles.vibesWrap}>
          {plan.vibes.map((vibe) => (
            <MetaPill key={vibe} label={vibe} />
          ))}
        </View>

        <View style={styles.stops}>
          {plan.stops.slice(0, 3).map((stop, index) => (
            <View key={stop.id} style={styles.stopRow}>
              <Text style={styles.stopIndex}>{index + 1}</Text>
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

function MetaPill({
  label,
  tone = "default",
}: {
  label: string;
  tone?: "default" | "cool";
}) {
  return (
    <View style={[styles.metaPill, tone === "cool" && styles.metaPillCool]}>
      <Text style={[styles.metaText, tone === "cool" && styles.metaTextCool]}>{label}</Text>
    </View>
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
  header: {
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
  },
  headerCopy: {
    flex: 1,
    gap: 6,
  },
  title: {
    color: palette.text,
    fontSize: 24,
    fontWeight: "800",
    lineHeight: 30,
  },
  hook: {
    color: palette.textMuted,
    lineHeight: 22,
    fontSize: 15,
  },
  badge: {
    borderRadius: 999,
    paddingHorizontal: 12,
    paddingVertical: 8,
    backgroundColor: "rgba(255, 122, 89, 0.16)",
    borderWidth: 1,
    borderColor: "rgba(255, 151, 124, 0.34)",
    alignSelf: "flex-start",
  },
  badgeText: {
    color: palette.text,
    fontWeight: "800",
    fontSize: 12,
  },
  metaWrap: {
    flexDirection: "row",
    gap: 10,
    flexWrap: "wrap",
  },
  vibesWrap: {
    flexDirection: "row",
    gap: 10,
    flexWrap: "wrap",
  },
  metaPill: {
    borderRadius: 999,
    paddingHorizontal: 12,
    paddingVertical: 8,
    backgroundColor: "rgba(255, 255, 255, 0.06)",
    borderWidth: 1,
    borderColor: palette.border,
  },
  metaPillCool: {
    backgroundColor: "rgba(52, 211, 153, 0.12)",
    borderColor: "rgba(52, 211, 153, 0.24)",
  },
  metaText: {
    color: palette.textSoft,
    fontWeight: "700",
    fontSize: 12,
  },
  metaTextCool: {
    color: "#b4f5dd",
  },
  stops: {
    gap: 10,
  },
  stopRow: {
    flexDirection: "row",
    gap: 10,
    alignItems: "center",
  },
  stopIndex: {
    width: 18,
    color: palette.accent,
    fontWeight: "900",
  },
  stopText: {
    color: palette.textSoft,
    fontSize: 14,
    flex: 1,
  },
  actions: {
    flexDirection: "row",
    gap: 10,
  },
  actionButton: {
    flex: 1,
  },
});
