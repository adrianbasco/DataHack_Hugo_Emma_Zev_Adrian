import { useState } from "react";
import { View, Text, TextInput, Button, StyleSheet } from "react-native";
import { GenerateRequest } from "../lib/types";

type Props = {
  onSubmit: (payload: GenerateRequest) => void;
};

export default function InputForm({ onSubmit }: Props) {
  const [location, setLocation] = useState("");
  const [radiusKm, setRadiusKm] = useState("5");
  const [transportMode, setTransportMode] = useState<"walking" | "transit" | "driving">("walking");
  const [vibe, setVibe] = useState("romantic");
  const [budget, setBudget] = useState<"$" | "$$" | "$$$" | "$$$$">("$$");
  const [startTime, setStartTime] = useState("Saturday 6pm");
  const [durationMinutes, setDurationMinutes] = useState("180");
  const [partySize, setPartySize] = useState("2");
  const [constraintsNote, setConstraintsNote] = useState("");

  return (
    <View style={styles.container}>
      <Text>Location</Text>
      <TextInput style={styles.input} value={location} onChangeText={setLocation} placeholder="e.g. Newtown" />

      <Text>Radius (km)</Text>
      <TextInput style={styles.input} value={radiusKm} onChangeText={setRadiusKm} keyboardType="numeric" />

      <Text>Transport mode</Text>
      <TextInput
        style={styles.input}
        value={transportMode}
        onChangeText={(text) => setTransportMode(text as "walking" | "transit" | "driving")}
        placeholder="walking / transit / driving"
      />

      <Text>Vibe</Text>
      <TextInput style={styles.input} value={vibe} onChangeText={setVibe} />

      <Text>Budget</Text>
      <TextInput style={styles.input} value={budget} onChangeText={(text) => setBudget(text as "$" | "$$" | "$$$" | "$$$$")} />

      <Text>Start time</Text>
      <TextInput style={styles.input} value={startTime} onChangeText={setStartTime} />

      <Text>Duration (minutes)</Text>
      <TextInput style={styles.input} value={durationMinutes} onChangeText={setDurationMinutes} keyboardType="numeric" />

      <Text>Party size</Text>
      <TextInput style={styles.input} value={partySize} onChangeText={setPartySize} keyboardType="numeric" />

      <Text>Dietary / accessibility notes</Text>
      <TextInput
        style={[styles.input, styles.largeInput]}
        value={constraintsNote}
        onChangeText={setConstraintsNote}
        multiline
      />

      <Button
        title="Generate Date Ideas"
        onPress={() =>
          onSubmit({
            location,
            radiusKm: Number(radiusKm),
            transportMode,
            vibe,
            budget,
            startTime,
            durationMinutes: Number(durationMinutes),
            partySize: Number(partySize),
            constraintsNote,
          })
        }
      />
    </View>
  );
}

const styles = StyleSheet.create({
  container: { gap: 8, padding: 16 },
  input: {
    borderWidth: 1,
    borderColor: "#ccc",
    borderRadius: 8,
    padding: 10,
  },
  largeInput: {
    minHeight: 80,
    textAlignVertical: "top",
  },
});