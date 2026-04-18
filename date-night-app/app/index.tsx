import { View, Pressable, Text, StyleSheet } from "react-native";
import { useRouter } from "expo-router";
import InputForm from "../components/InputForm";
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
    <View style={styles.container}>
      <InputForm onSubmit={handleSubmit} />

      <Pressable style={styles.savedLink} onPress={() => router.push("/saved")}>
        <Text style={styles.savedLinkText}>View Saved Dates</Text>
      </Pressable>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#fff7f8",
  },
  savedLink: {
    marginHorizontal: 20,
    marginBottom: 20,
    paddingVertical: 12,
  },
  savedLinkText: {
    textAlign: "center",
    color: "#be185d",
    fontWeight: "600",
  },
});