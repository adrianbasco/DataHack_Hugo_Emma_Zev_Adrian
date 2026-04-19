import { Stack } from "expo-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { palette } from "../components/ui";

const queryClient = new QueryClient();

export default function RootLayout() {
  return (
    <QueryClientProvider client={queryClient}>
      <Stack
        screenOptions={{
          headerStyle: {
            backgroundColor: palette.bg,
          },
          headerShadowVisible: false,
          headerTintColor: palette.text,
          headerTitleStyle: {
            fontWeight: "800",
          },
          contentStyle: {
            backgroundColor: palette.bg,
          },
        }}
      >
        <Stack.Screen name="index" options={{ title: "Date Night", headerShown: false }} />
        <Stack.Screen name="templates" options={{ title: "Templates" }} />
        <Stack.Screen name="results" options={{ title: "Results" }} />
        <Stack.Screen name="saved" options={{ title: "Saved Dates" }} />
        <Stack.Screen name="plan/[id]" options={{ title: "Plan Details" }} />
        <Stack.Screen name="booking/request" options={{ title: "Booking Request" }} />
        <Stack.Screen name="booking/[status]" options={{ title: "Booking Status" }} />
      </Stack>
    </QueryClientProvider>
  );
}
