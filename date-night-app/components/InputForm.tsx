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
  { label: "Public transport", value: "public_transport" },
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

const timeWindows = [
  "Morning",
  "Midday",
  "Afternoon",
  "Evening",
  "Night",
  "Flexible",
];

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

  useEffect(() => {
    if (!selectedTemplate) {
      return;
    }
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
      <View style={styles.heroWrap}>
        <Eyebrow tone="warm">Structured planner</Eyebrow>
        <Text style={styles.heroTitle}>Tell the planner exactly what to optimize for.</Text>
        <Text style={styles.heroSubtitle}>
          Use the form when you already know the city, mood, and practical constraints.
        </Text>
      </View>

      {selectedTemplate ? (
        <SurfaceCard style={styles.templateCard}>
          <Text style={styles.templateLabel}>Template selected</Text>
          <Text style={styles.templateTitle}>{selectedTemplate.title}</Text>
          <Text style={styles.templateText}>{selectedTemplate.description}</Text>
        </SurfaceCard>
      ) : null}

      <SurfaceCard style={styles.card}>
        <FormField label="Location" helper="Suburb, postcode, or city">
          <TextInput
            style={styles.input}
            value={location}
            onChangeText={setLocation}
            placeholder="Sydney CBD"
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

        <FormField label="Transport" helper="How you plan to move">
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

        <FormField label="Vibes" helper="Pick one or several">
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

          <FormField label="Time window" helper="Best fit">
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

        <View style={styles.row}>
          <FormField label="Idea count" helper="How many options">
            <TextInput
              style={styles.input}
              value={desiredIdeaCount}
              onChangeText={setDesiredIdeaCount}
              keyboardType="numeric"
              placeholder="4"
              placeholderTextColor={palette.textMuted}
            />
          </FormField>

          <FormField label="Template mode" helper="Optional backend hint">
            <View style={styles.templateHintBox}>
              <Text style={styles.templateHintText}>
                {selectedTemplate ? selectedTemplate.id : "No template selected"}
              </Text>
            </View>
          </FormField>
        </View>

        <FormField label="Dietary constraints" helper="Optional">
          <TextInput
            style={styles.input}
            value={dietaryConstraints}
            onChangeText={setDietaryConstraints}
            placeholder="Vegetarian, nut-free..."
            placeholderTextColor={palette.textMuted}
          />
        </FormField>

        <FormField label="Accessibility constraints" helper="Optional">
          <TextInput
            style={styles.input}
            value={accessibilityConstraints}
            onChangeText={setAccessibilityConstraints}
            placeholder="Low walking, step-free access..."
            placeholderTextColor={palette.textMuted}
          />
        </FormField>

        <FormField label="Notes for the planner" helper="Optional extra context">
          <TextInput
            style={[styles.input, styles.notesInput]}
            value={notes}
            onChangeText={setNotes}
            placeholder="Anniversary, want a surprise finish, avoid loud bars..."
            placeholderTextColor={palette.textMuted}
            multiline
          />
        </FormField>

        <ActionButton label="Generate ideas from form" onPress={handleSubmit} />
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
    gap: 16,
  },
  heroWrap: {
    gap: 10,
  },
  heroTitle: {
    fontSize: 34,
    lineHeight: 40,
    fontWeight: "900",
    color: palette.text,
  },
  heroSubtitle: {
    color: palette.textMuted,
    fontSize: 15,
    lineHeight: 23,
  },
  templateCard: {
    gap: 6,
    backgroundColor: "rgba(255, 122, 89, 0.14)",
  },
  templateLabel: {
    color: palette.accentWarm,
    fontSize: 12,
    fontWeight: "800",
    letterSpacing: 0.6,
    textTransform: "uppercase",
  },
  templateTitle: {
    color: palette.text,
    fontSize: 20,
    fontWeight: "800",
  },
  templateText: {
    color: palette.textSoft,
    lineHeight: 21,
  },
  card: {
    gap: 16,
  },
  row: {
    flexDirection: "row",
    gap: 12,
    flexWrap: "wrap",
  },
  field: {
    flex: 1,
    minWidth: 160,
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
    color: palette.textMuted,
    fontSize: 12,
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
    minHeight: 96,
    textAlignVertical: "top",
  },
  chipWrap: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 10,
  },
  templateHintBox: {
    minHeight: 48,
    borderRadius: 18,
    borderWidth: 1,
    borderColor: palette.border,
    backgroundColor: "rgba(255, 255, 255, 0.04)",
    justifyContent: "center",
    paddingHorizontal: 14,
  },
  templateHintText: {
    color: palette.text,
    fontWeight: "700",
  },
});
