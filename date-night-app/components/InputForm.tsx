import { useState } from "react";
import {
  View,
  Text,
  TextInput,
  Pressable,
  StyleSheet,
  ScrollView,
} from "react-native";
import { Budget, GenerateRequest, TransportMode } from "../lib/types";

type Props = {
  onSubmit: (payload: GenerateRequest) => void;
};

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
    <ScrollView contentContainerStyle={styles.container}>
      <Text style={styles.heroTitle}>Date Night</Text>
      <Text style={styles.heroSubtitle}>Plan your perfect date night</Text>

      <View style={styles.card}>
        <FormField label="Location">
          <TextInput
            style={styles.input}
            value={location}
            onChangeText={setLocation}
            placeholder="Enter suburb or postcode"
            placeholderTextColor="#94a3b8"
          />
        </FormField>

        <View style={styles.row}>
          <FormField label="Travel Radius">
            <TextInput
              style={styles.input}
              value={radiusKm}
              onChangeText={setRadiusKm}
              keyboardType="numeric"
              placeholder="10"
              placeholderTextColor="#94a3b8"
            />
          </FormField>

          <FormField label="Transport">
            <TextInput
              style={styles.input}
              value={transportMode}
              onChangeText={(text) => setTransportMode(text as TransportMode)}
              placeholder="walking"
              placeholderTextColor="#94a3b8"
            />
          </FormField>
        </View>

        <FormField label="Date Vibe">
          <TextInput
            style={styles.input}
            value={vibe}
            onChangeText={setVibe}
            placeholder="romantic"
            placeholderTextColor="#94a3b8"
          />
        </FormField>

        <View style={styles.row}>
          <FormField label="Budget">
            <TextInput
              style={styles.input}
              value={budget}
              onChangeText={(text) => setBudget(text as Budget)}
              placeholder="$$"
              placeholderTextColor="#94a3b8"
            />
          </FormField>

          <FormField label="Duration (mins)">
            <TextInput
              style={styles.input}
              value={durationMinutes}
              onChangeText={setDurationMinutes}
              keyboardType="numeric"
              placeholder="180"
              placeholderTextColor="#94a3b8"
            />
          </FormField>
        </View>

        <View style={styles.row}>
          <FormField label="Start Time">
            <TextInput
              style={styles.input}
              value={startTime}
              onChangeText={setStartTime}
              placeholder="18:00"
              placeholderTextColor="#94a3b8"
            />
          </FormField>

          <FormField label="Party Size">
            <TextInput
              style={styles.input}
              value={partySize}
              onChangeText={setPartySize}
              keyboardType="numeric"
              placeholder="2"
              placeholderTextColor="#94a3b8"
            />
          </FormField>
        </View>

        <FormField label="Special Notes">
          <TextInput
            style={[styles.input, styles.notesInput]}
            value={constraintsNote}
            onChangeText={setConstraintsNote}
            placeholder="Dietary restrictions, accessibility needs..."
            placeholderTextColor="#94a3b8"
            multiline
          />
        </FormField>

        <Pressable
          style={styles.primaryButton}
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
        >
          <Text style={styles.primaryButtonText}>Generate Date Ideas</Text>
        </Pressable>
      </View>
    </ScrollView>
  );
}

function FormField({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <View style={styles.field}>
      <Text style={styles.label}>{label}</Text>
      {children}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    padding: 20,
    backgroundColor: "#fff7f8",
  },
  heroTitle: {
    fontSize: 32,
    fontWeight: "700",
    textAlign: "center",
    color: "#be185d",
    marginTop: 20,
  },
  heroSubtitle: {
    fontSize: 16,
    textAlign: "center",
    color: "#64748b",
    marginTop: 6,
    marginBottom: 20,
  },
  card: {
    backgroundColor: "white",
    borderRadius: 24,
    padding: 18,
    shadowColor: "#000",
    shadowOpacity: 0.08,
    shadowRadius: 12,
    shadowOffset: { width: 0, height: 4 },
    elevation: 4,
    gap: 12,
  },
  row: {
    flexDirection: "row",
    gap: 12,
  },
  field: {
    flex: 1,
  },
  label: {
    fontSize: 14,
    fontWeight: "600",
    color: "#334155",
    marginBottom: 8,
  },
  input: {
    borderWidth: 1,
    borderColor: "#fbcfe8",
    backgroundColor: "#fff",
    borderRadius: 999,
    paddingHorizontal: 14,
    paddingVertical: 12,
    fontSize: 15,
    color: "#0f172a",
  },
  notesInput: {
    minHeight: 90,
    borderRadius: 20,
    textAlignVertical: "top",
  },
  primaryButton: {
    backgroundColor: "#ec4899",
    borderRadius: 999,
    paddingVertical: 15,
    marginTop: 8,
  },
  primaryButtonText: {
    color: "white",
    textAlign: "center",
    fontWeight: "700",
    fontSize: 16,
  },
});