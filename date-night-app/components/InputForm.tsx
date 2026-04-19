import { useEffect, useState, type ReactNode } from "react";
import { StyleSheet, Text, TextInput, View } from "react-native";
import { DateTemplate, GenerateFormRequest, TransportMode, Vibe } from "../lib/types";
import {
  ActionButton,
  Eyebrow,
  SelectChip,
  SurfaceCard,
  palette,
} from "./ui";

type Props = {
  selectedTemplate?: DateTemplate;
  onSubmit: (payload: GenerateFormRequest) => void;
};

const transportOptions: { label: string; value: TransportMode }[] = [
  { label: "Walk", value: "walking" },
  { label: "Transit", value: "public_transport" },
  { label: "Drive", value: "driving" },
];

const vibeOptions: Vibe[] = [
  "romantic",
  "foodie",
  "nightlife",
  "nerdy",
  "outdoorsy",
  "active",
  "casual",
];

const timeWindows = ["Morning", "Afternoon", "Evening", "Night", "Flexible"];

export default function InputForm({ selectedTemplate, onSubmit }: Props) {
  const [location, setLocation] = useState("");
  const [radiusKm, setRadiusKm] = useState("10");
  const [transportMode, setTransportMode] = useState<TransportMode>("driving");
  const [vibes, setVibes] = useState<Vibe[]>(selectedTemplate?.vibes ?? ["romantic"]);
  const [budget, setBudget] = useState<GenerateFormRequest["budget"]>("$$");
  const [timeWindow, setTimeWindow] = useState<string>(
    selectedTemplate?.timeOfDay ?? "evening"
  );
  const [partySize, setPartySize] = useState("2");
  const [desiredIdeaCount, setDesiredIdeaCount] = useState("4");
  const [dietaryConstraints, setDietaryConstraints] = useState("");
  const [accessibilityConstraints, setAccessibilityConstraints] = useState("");
  const [notes, setNotes] = useState("");
  const [showAdvanced, setShowAdvanced] = useState(false);

  useEffect(() => {
    if (!selectedTemplate) return;
    setVibes(selectedTemplate.vibes);
    setTimeWindow(selectedTemplate.timeOfDay);
  }, [selectedTemplate?.id]);

  function toggleVibe(vibe: Vibe) {
    setVibes((current) =>
      current.includes(vibe)
        ? current.filter((entry) => entry !== vibe)
        : [...current, vibe]
    );
  }

  function handleSubmit() {
    onSubmit({
      location,
      vibes,
      radiusKm: Number(radiusKm) || 10,
      budget,
      transportMode,
      partySize: Number(partySize) || 2,
      timeWindow,
      desiredIdeaCount: Number(desiredIdeaCount) || 4,
      dietaryConstraints: dietaryConstraints || undefined,
      accessibilityConstraints: accessibilityConstraints || undefined,
      notes: notes || undefined,
      selectedTemplateId: selectedTemplate?.id,
    });
  }

  return (
    <View style={styles.container}>
      <SurfaceCard style={styles.card}>

        {/* ── Primary field: Location ── */}
        <FormField label="Location" helper="City or suburb">
          <TextInput
            style={styles.input}
            value={location}
            onChangeText={setLocation}
            placeholder="Sydney CBD"
            placeholderTextColor={palette.textMuted}
          />
        </FormField>

        {/* ── Primary field: Vibe ── */}
        <FormField label="Vibe" helper="Pick one or several">
          <View style={styles.chipWrap}>
            {vibeOptions.map((vibe) => (
              <SelectChip
                key={vibe}
                label={capitalize(vibe)}
                selected={vibes.includes(vibe)}
                onPress={() => toggleVibe(vibe)}
              />
            ))}
          </View>
        </FormField>

        {/* ── Primary fields: Budget + Time ── */}
        <View style={styles.row}>
          <FormField label="Budget" helper="Spend level">
            <View style={styles.chipWrap}>
              {["$", "$$", "$$$", "$$$$"].map((value) => (
                <SelectChip
                  key={value}
                  label={value}
                  selected={budget === value}
                  onPress={() => setBudget(value as GenerateFormRequest["budget"])}
                />
              ))}
            </View>
          </FormField>

          <FormField label="Time" helper="Best window">
            <View style={styles.chipWrap}>
              {timeWindows.map((value) => (
                <SelectChip
                  key={value}
                  label={value}
                  selected={timeWindow?.toLowerCase() === value.toLowerCase()}
                  onPress={() => setTimeWindow(value.toLowerCase())}
                />
              ))}
            </View>
          </FormField>
        </View>

        {/* ── Advanced toggle ── */}
        <SelectChip
          label={showAdvanced ? "Hide advanced options ▴" : "Advanced options ▾"}
          onPress={() => setShowAdvanced((v) => !v)}
          style={styles.advancedToggle}
        />

        {/* ── Advanced fields — only shown when expanded ── */}
        {showAdvanced ? (
          <View style={styles.advancedSection}>
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

            <FormField label="Transport" helper="How you'll move">
              <View style={styles.chipWrap}>
                {transportOptions.map((option) => (
                  <SelectChip
                    key={option.value}
                    label={option.label}
                    selected={transportMode === option.value}
                    onPress={() => setTransportMode(option.value)}
                  />
                ))}
              </View>
            </FormField>

            <FormField label="Number of ideas" helper="How many options">
              <TextInput
                style={styles.input}
                value={desiredIdeaCount}
                onChangeText={setDesiredIdeaCount}
                keyboardType="numeric"
                placeholder="4"
                placeholderTextColor={palette.textMuted}
              />
            </FormField>

            <FormField label="Dietary constraints" helper="Optional">
              <TextInput
                style={styles.input}
                value={dietaryConstraints}
                onChangeText={setDietaryConstraints}
                placeholder="Vegetarian, nut-free..."
                placeholderTextColor={palette.textMuted}
              />
            </FormField>

            <FormField label="Accessibility" helper="Optional">
              <TextInput
                style={styles.input}
                value={accessibilityConstraints}
                onChangeText={setAccessibilityConstraints}
                placeholder="Low walking, step-free..."
                placeholderTextColor={palette.textMuted}
              />
            </FormField>

            <FormField label="Extra notes" helper="Optional">
              <TextInput
                style={[styles.input, styles.notesInput]}
                value={notes}
                onChangeText={setNotes}
                placeholder="Anniversary, avoid loud bars..."
                placeholderTextColor={palette.textMuted}
                multiline
              />
            </FormField>
          </View>
        ) : null}

        <ActionButton label="Generate ideas →" onPress={handleSubmit} />
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

function capitalize(value: string) {
  return value.charAt(0).toUpperCase() + value.slice(1);
}

const styles = StyleSheet.create({
  container: {
    gap: 0,
  },
  card: {
    gap: 18,
  },
  row: {
    flexDirection: "row",
    gap: 12,
    flexWrap: "wrap",
  },
  field: {
    flex: 1,
    minWidth: 140,
    gap: 8,
  },
  fieldHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  label: {
    fontSize: 13,
    fontWeight: "700",
    color: palette.textSoft,
  },
  helper: {
    color: palette.textMuted,
    fontSize: 12,
  },
  input: {
    borderWidth: 1,
    borderColor: palette.border,
    backgroundColor: palette.panelSoft,
    borderRadius: 14,
    paddingHorizontal: 14,
    paddingVertical: 13,
    fontSize: 15,
    color: palette.text,
  },
  notesInput: {
    minHeight: 88,
    textAlignVertical: "top",
  },
  chipWrap: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
  },
  // Advanced toggle — looks like a subtle chip
  advancedToggle: {
    alignSelf: "flex-start",
  },
  advancedSection: {
    gap: 18,
    paddingTop: 4,
  },
});