import { useMemo } from "react";
import { ScrollView, Text, View, Image, Button, Linking, Share } from "react-native";
import { useLocalSearchParams } from "expo-router";
import { mockPlans } from "../../lib/mockPlans";

export default function PlanDetailScreen() {
  const { id } = useLocalSearchParams();

  const plan = useMemo(
    () => mockPlans.find((p) => p.id === id),
    [id]
  );

  if (!plan) {
    return (
      <View style={{ padding: 16 }}>
        <Text>Plan not found.</Text>
      </View>
    );
  }

  return (
    <ScrollView contentContainerStyle={{ padding: 16 }}>
      {plan.heroImageUrl ? (
        <Image
          source={{ uri: plan.heroImageUrl }}
          style={{ width: "100%", height: 220, borderRadius: 12, marginBottom: 16 }}
        />
      ) : null}

      <Text style={{ fontSize: 24, fontWeight: "700" }}>{plan.title}</Text>
      <Text style={{ color: "#666", marginBottom: 12 }}>{plan.vibeLine}</Text>
      <Text style={{ marginBottom: 16 }}>{plan.summary}</Text>

      <Text style={{ fontSize: 18, fontWeight: "600", marginBottom: 8 }}>Stops</Text>
      {plan.stops.map((stop, index) => (
        <View key={stop.id} style={{ marginBottom: 12 }}>
          <Text>{index + 1}. {stop.name}</Text>
          {stop.mapsUrl ? (
            <Button title="Open in Maps" onPress={() => Linking.openURL(stop.mapsUrl!)} />
          ) : null}
        </View>
      ))}

      <Text style={{ fontSize: 18, fontWeight: "600", marginBottom: 8 }}>Transport</Text>
      {plan.transportLegs?.map((leg, index) => (
        <Text key={index}>
          {leg.mode} · {leg.durationText}
        </Text>
      ))}

      <View style={{ marginTop: 20 }}>
        <Button
            title="Share plan"
            onPress={async () => {
                await Share.share({
                message: `${plan.title}\n${plan.vibeLine}\nStops: ${plan.stops.map((s) => s.name).join(" → ")}`,
                });
            }}
            />
      </View>
    </ScrollView>

    
  );
}