import { Stack } from "expo-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

const queryClient = new QueryClient();

export default function RootLayout() {
  return (
    <QueryClientProvider client={queryClient}>
      <Stack>
        <Stack.Screen name="index" options={{ title: "Date Night" }} />
        <Stack.Screen name="results" options={{ title: "Results" }} />
        <Stack.Screen name="saved" options={{ title: "Saved Dates" }} />
        <Stack.Screen name="plan/[id]" options={{ title: "Plan Details" }} />
      </Stack>
    </QueryClientProvider>
  );
}