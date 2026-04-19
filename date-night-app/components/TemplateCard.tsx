import { StyleSheet, Text, View } from "react-native";
import { DateTemplate } from "../lib/types";
import { ActionButton, SelectChip, SurfaceCard, palette } from "./ui";

type Props = {
  template: DateTemplate;
  onUse: () => void;
};

export default function TemplateCard({ template, onUse }: Props) {
  return (
    <SurfaceCard style={styles.card}>
      <View style={styles.header}>
        <View style={styles.headerCopy}>
          <Text style={styles.title}>{template.title}</Text>
          <Text style={styles.description}>{template.description}</Text>
        </View>
        <View style={styles.metaWrap}>
          <SelectChip label={capitalize(template.timeOfDay)} selected />
          <SelectChip label={`${template.durationHours}h`} selected />
          {template.weatherSensitive ? <SelectChip label="Weather aware" selected /> : null}
        </View>
      </View>

      <View style={styles.vibesWrap}>
        {template.vibes.map((vibe) => (
          <SelectChip key={vibe} label={capitalize(vibe)} selected />
        ))}
      </View>

      <View style={styles.stopList}>
        {template.stops.map((stop, index) => (
          <View key={`${template.id}-${stop.type}-${index}`} style={styles.stopRow}>
            <Text style={styles.stopIndex}>{index + 1}</Text>
            <View style={styles.stopCopy}>
              <Text style={styles.stopType}>{humanizeToken(stop.type)}</Text>
              {stop.note ? <Text style={styles.stopNote}>{stop.note}</Text> : null}
            </View>
          </View>
        ))}
      </View>

      <View style={styles.footer}>
        <Text style={styles.variationText}>
          {template.meaningfulVariations} meaningful variations in a mid-to-large city.
        </Text>
        <ActionButton label="Use template" onPress={onUse} />
      </View>
    </SurfaceCard>
  );
}

function humanizeToken(value: string) {
  return value
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function capitalize(value: string) {
  return value.charAt(0).toUpperCase() + value.slice(1);
}

const styles = StyleSheet.create({
  card: {
    gap: 14,
  },
  header: {
    gap: 12,
  },
  headerCopy: {
    gap: 6,
  },
  title: {
    color: palette.text,
    fontSize: 24,
    lineHeight: 30,
    fontWeight: "800",
  },
  description: {
    color: palette.textMuted,
    lineHeight: 21,
  },
  metaWrap: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 10,
  },
  vibesWrap: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 10,
  },
  stopList: {
    gap: 10,
  },
  stopRow: {
    flexDirection: "row",
    gap: 12,
    alignItems: "flex-start",
  },
  stopIndex: {
    color: palette.accent,
    fontWeight: "900",
    width: 18,
    marginTop: 1,
  },
  stopCopy: {
    flex: 1,
    gap: 2,
  },
  stopType: {
    color: palette.text,
    fontWeight: "700",
  },
  stopNote: {
    color: palette.textMuted,
    lineHeight: 20,
    fontSize: 13,
  },
  footer: {
    gap: 12,
  },
  variationText: {
    color: palette.textMuted,
    fontSize: 13,
    lineHeight: 20,
  },
});
