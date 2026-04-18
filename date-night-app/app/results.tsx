import { ScrollView, View, Text } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import { mockPlans } from "../lib/mockPlans";
import PlanCard from "../components/PlanCard";
import { savePlan } from "../lib/storage";
import { GenerateRequest } from "../lib/types";

export default function ResultsScreen() {
  const router = useRouter();
  const params = useLocalSearchParams();

  const payload = params.payload
    ? JSON.parse(params.payload as string)
    : null;

  const typedPayload = payload as GenerateRequest | null;

  return (
    <ScrollView contentContainerStyle={{ padding: 16 }}>
      <Text style={{ fontSize: 22, fontWeight: "600", marginBottom: 12 }}>
        Date ideas
      </Text>

      {typedPayload ? (
        <View style={{ marginBottom: 16 }}>
          <Text>Location: {typedPayload.location}</Text>
          <Text>Vibe: {typedPayload.vibe}</Text>
        </View>
      ) : null}

      {mockPlans.map((plan) => (
        <PlanCard
          key={plan.id}
          plan={plan}
          onView={() =>
            router.push({
              pathname: "/plan/[id]",
              params: { id: plan.id },
            })
          }
          onSave={() => savePlan(plan)}
        />
      ))}
    </ScrollView>
  );
}