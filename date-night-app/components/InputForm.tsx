import { useState, type ReactNode } from "react";
import {
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";
import { Budget, GenerateRequest, TransportMode } from "../lib/types";
import {
  ActionButton,
  Eyebrow,
  SelectChip,
  SurfaceCard,
  palette,
} from "./ui";

type Props = {
  onSubmit: (payload: GenerateRequest) => void;
};

const transportOptions: TransportMode[] = ["walking", "driving", "transit"];
const vibeOptions = ["romantic", "playful", "foodie", "cozy", "surprise"];
const budgetOptions: Budget[] = ["$", "$$", "$$$"];

export default function InputForm({ onSubmit }: Props) {
  const [location, setLocation] = useState("");
  const [radiusKm, setRadiusKm] = useState("10");
  const [transportMode, setTransportMode] = useState<TransportMode>("walking");
  const [vibe, setVibe] = useState("romantic");
  const [budget, setBudget] = useState<Budget>("$$");
  const [startTime, setStartTime] = useState("18:00");
  const [durationMinutes, setDurationMinutes] = useState("180");
  const [partySize, setPartySize] = useState("2");
  const [constraintsNote, setConstraintsNote] = useState("");

  return (
    <View style={styles.container}>
      <View style={styles.heroWrap}>
        <Eyebrow tone="warm">Curated date generator</Eyebrow>
        <Text style={styles.heroTitle}>Build a date night that feels custom.</Text>
        <Text style={styles.heroSubtitle}>
          Stylish picks, realistic logistics, and a plan that still feels spontaneous.
        </Text>

        <View style={styles.heroStats}>
          <StatPill label="Fast setup" value="60 sec" />
          <StatPill label="Best for" value="Tonight" />
          <StatPill label="Energy" value="Fun + polished" />
        </View>
      </View>

      <SurfaceCard style={styles.card}>
        <View style={styles.cardHeader}>
          <Text style={styles.cardTitle}>Set the mood</Text>
          <Text style={styles.cardSubtitle}>
            Pick the vibe, budget, and timing for your ideal night out.
          </Text>
        </View>

        <FormField label="Location" helper="Suburb, postcode, or city">
          <TextInput
            style={styles.input}
            value={location}
            onChangeText={setLocation}
            placeholder="Surry Hills, 2010"
            placeholderTextColor={palette.textMuted}
          />
        </FormField>

        <View style={styles.row}>
          <FormField label="Travel radius" helper="Kilometres">
            <TextInput
              style={styles.input}
              value={radiusKm}
              onChangeText={setRadiusKm}
              keyboardType="numeric"
              placeholder="10"
              placeholderTextColor={palette.textMuted}
            />
          </FormField>

          <FormField label="Party size" helper="People">
            <TextInput
              style={styles.input}
              value={partySize}
              onChangeText={setPartySize}
              keyboardType="numeric"
              placeholder="2"
              placeholderTextColor={palette.textMuted}
            />
          </FormField>
        </View>

        <FormField label="Transport" helper="How you want to move around">
          <View style={styles.chipRow}>
            {transportOptions.map((option) => (
              <SelectChip
                key={option}
                label={capitalize(option)}
                selected={transportMode === option}
                onPress={() => setTransportMode(option)}
              />
            ))}
          </View>
        </FormField>

        <FormField label="Date vibe" helper="Choose the overall feel">
          <View style={styles.chipWrap}>
            {vibeOptions.map((option) => (
              <SelectChip
                key={option}
                label={capitalize(option)}
                selected={vibe === option}
                onPress={() => setVibe(option)}
              />
            ))}
          </View>
        </FormField>

        <FormField label="Budget" helper="Estimated spend level">
          <View style={styles.chipRow}>
            {budgetOptions.map((option) => (
              <SelectChip
                key={option}
                label={option}
                selected={budget === option}
                onPress={() => setBudget(option)}
              />
            ))}
          </View>
        </FormField>

        <View style={styles.row}>
          <FormField label="Start time" helper="24-hour clock">
            <TextInput
              style={styles.input}
              value={startTime}
              onChangeText={setStartTime}
              placeholder="18:00"
              placeholderTextColor={palette.textMuted}
            />
          </FormField>

          <FormField label="Duration" helper="Minutes">
            <TextInput
              style={styles.input}
              value={durationMinutes}
              onChangeText={setDurationMinutes}
              keyboardType="numeric"
              placeholder="180"
              placeholderTextColor={palette.textMuted}
            />
          </FormField>
        </View>

        <FormField label="Special notes" helper="Dietary, accessibility, or must-avoid notes">
          <TextInput
            style={[styles.input, styles.notesInput]}
            value={constraintsNote}
            onChangeText={setConstraintsNote}
            placeholder="Vegetarian-friendly, low walking, no loud bars..."
            placeholderTextColor={palette.textMuted}
            multiline
          />
        </FormField>

        <View style={styles.footerCallout}>
          <Text style={styles.footerCalloutTitle}>What you will get</Text>
          <Text style={styles.footerCalloutText}>
            A swipeable shortlist of date plans with pacing, stops, cost feel, and shareable details.
          </Text>
        </View>

        <ActionButton
          label="Generate date ideas"
          onPress={() =>
            onSubmit({
              location,
              radiusKm: Number(radiusKm) || 10,
              transportMode,
              vibe,
              budget,
              startTime,
              durationMinutes: Number(durationMinutes) || 180,
              partySize: Number(partySize) || 2,
              constraintsNote,
            })
          }
        />
      </SurfaceCard>
    </View>
  );
}

function FormField({
  label,
  helper,
  children,
}: {
  label: string;
  helper?: string;
  children: ReactNode;
}) {
  return (
    <View style={styles.field}>
      <View style={styles.fieldHeader}>
        <Text style={styles.label}>{label}</Text>
        {helper ? <Text style={styles.helper}>{helper}</Text> : null}
      </View>
      {children}
    </View>
  );
}

function StatPill({ label, value }: { label: string; value: string }) {
  return (
    <View style={styles.statPill}>
      <Text style={styles.statValue}>{value}</Text>
      <Text style={styles.statLabel}>{label}</Text>
    </View>
  );
}

function capitalize(value: string) {
  return value.charAt(0).toUpperCase() + value.slice(1);
}

const styles = StyleSheet.create({
  container: {
    gap: 18,
    paddingBottom: 24,
  },
  heroWrap: {
    paddingTop: 8,
    gap: 12,
  },
  heroTitle: {
    fontSize: 36,
    lineHeight: 42,
    fontWeight: "900",
    color: palette.text,
    maxWidth: 520,
  },
  heroSubtitle: {
    color: palette.textMuted,
    fontSize: 16,
    lineHeight: 24,
    maxWidth: 560,
  },
  heroStats: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 10,
  },
  statPill: {
    minWidth: 104,
    paddingHorizontal: 14,
    paddingVertical: 12,
    borderRadius: 20,
    backgroundColor: "rgba(255, 255, 255, 0.06)",
    borderWidth: 1,
    borderColor: palette.border,
  },
  statValue: {
    color: palette.text,
    fontSize: 16,
    fontWeight: "800",
  },
  statLabel: {
    color: palette.textMuted,
    fontSize: 12,
    marginTop: 4,
  },
  card: {
    gap: 16,
  },
  cardHeader: {
    gap: 6,
  },
  cardTitle: {
    color: palette.text,
    fontSize: 24,
    fontWeight: "800",
  },
  cardSubtitle: {
    color: palette.textMuted,
    fontSize: 14,
    lineHeight: 20,
  },
  row: {
    flexDirection: "row",
    gap: 12,
  },
  field: {
    flex: 1,
    gap: 8,
  },
  fieldHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: 8,
  },
  label: {
    fontSize: 14,
    fontWeight: "700",
    color: palette.textSoft,
  },
  helper: {
    fontSize: 12,
    color: palette.textMuted,
  },
  input: {
    borderWidth: 1,
    borderColor: palette.border,
    backgroundColor: palette.panelSoft,
    borderRadius: 18,
    paddingHorizontal: 16,
    paddingVertical: 14,
    fontSize: 15,
    color: palette.text,
  },
  notesInput: {
    minHeight: 110,
    textAlignVertical: "top",
  },
  chipRow: {
    flexDirection: "row",
    gap: 10,
    flexWrap: "wrap",
  },
  chipWrap: {
    flexDirection: "row",
    gap: 10,
    flexWrap: "wrap",
  },
  footerCallout: {
    borderRadius: 22,
    backgroundColor: "rgba(255, 255, 255, 0.04)",
    borderWidth: 1,
    borderColor: palette.border,
    padding: 16,
    gap: 6,
  },
  footerCalloutTitle: {
    color: palette.text,
    fontSize: 15,
    fontWeight: "800",
  },
  footerCalloutText: {
    color: palette.textMuted,
    lineHeight: 21,
    fontSize: 14,
  },
});
