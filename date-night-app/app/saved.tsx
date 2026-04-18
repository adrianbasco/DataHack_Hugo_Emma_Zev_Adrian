import { useEffect, useState } from "react";
import { ScrollView, Text } from "react-native";
import { useRouter } from "expo-router";
import PlanCard from "../components/PlanCard";
import { getSavedPlans } from "../lib/storage";
import { Plan } from "../lib/types";

export default function SavedScreen() {
  const router = useRouter();
  const [plans, setPlans] = useState<Plan[]>([]);

  useEffect(() => {
    getSavedPlans().then(setPlans);
  }, []);

  return (
    <ScrollView contentContainerStyle={{ padding: 16 }}>
      <Text style={{ fontSize: 22, fontWeight: "600", marginBottom: 12 }}>
        Saved dates
      </Text>

      {plans.length === 0 ? (
        <Text>No saved plans yet.</Text>
      ) : (
        plans.map((plan) => (
          <PlanCard
            key={plan.id}
            plan={plan}
            onView={() =>
              router.push({
                pathname: "/plan/[id]",
                params: { id: plan.id },
              })
            }
          />
        ))
      )}
    </ScrollView>
  );
}