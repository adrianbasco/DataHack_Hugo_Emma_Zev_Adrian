import { StyleSheet, Text, View } from "react-native";
import { useRouter } from "expo-router";
import InputForm from "../components/InputForm";
import { ActionButton, ScreenShell, SurfaceCard, palette } from "../components/ui";
import { GenerateRequest } from "../lib/types";

export default function HomeScreen() {
  const router = useRouter();

  function handleSubmit(payload: GenerateRequest) {
    router.push({
      pathname: "/results",
      params: {
        payload: JSON.stringify(payload),
      },
    });
  }

  return (
    <ScreenShell scroll contentContainerStyle={styles.container}>
      <InputForm onSubmit={handleSubmit} />

      <SurfaceCard style={styles.savedCard}>
        <View style={styles.savedCopy}>
          <Text style={styles.savedTitle}>Already found a winner?</Text>
          <Text style={styles.savedText}>
            Jump back into your saved plans and revisit the best contenders.
          </Text>
        </View>

        <ActionButton
          label="Open saved dates"
          variant="secondary"
          onPress={() => router.push("/saved")}
        />
      </SurfaceCard>
    </ScreenShell>
  );
}

const styles = StyleSheet.create({
  container: {
    gap: 18,
  },
  savedCard: {
    gap: 14,
    marginBottom: 12,
  },
  savedCopy: {
    gap: 6,
  },
  savedTitle: {
    color: palette.text,
    fontSize: 20,
    fontWeight: "800",
  },
  savedText: {
    color: palette.textMuted,
    fontSize: 14,
    lineHeight: 20,
  },
});
