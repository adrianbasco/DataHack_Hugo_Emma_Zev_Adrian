import { View, Text } from "react-native";
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
    <View style={{ flex: 1 }}>
      <Text style={{ fontSize: 24, fontWeight: "600", padding: 16 }}>Plan a date night</Text>
      <InputForm onSubmit={handleSubmit} />
    </View>
  );
}