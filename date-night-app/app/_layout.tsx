import { Stack } from "expo-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { palette } from "../components/ui";

const queryClient = new QueryClient();

type ErrorUtilsShape = {
  getGlobalHandler?: () => (error: Error, isFatal?: boolean) => void;
  setGlobalHandler?: (handler: (error: Error, isFatal?: boolean) => void) => void;
};

const nativeErrorUtils = (globalThis as { ErrorUtils?: ErrorUtilsShape }).ErrorUtils;

export default function RootLayout() {

  return (
    <QueryClientProvider client={queryClient}>
      <Stack
        screenOptions={{
          headerStyle: { backgroundColor: palette.bg },
          headerShadowVisible: false,
          headerTintColor: palette.text,
          headerTitleStyle: { fontWeight: "800" },
          contentStyle: { backgroundColor: palette.bg },
        }}
      >
        {/* The (tabs) group renders its own tab bar internally */}
        <Stack.Screen name="(tabs)" options={{ headerShown: false }} />
        <Stack.Screen name="results" options={{ title: "Results", headerShown: false }} />
        <Stack.Screen name="plan/[id]" options={{ title: "Plan Details" }} />
        <Stack.Screen name="booking/request" options={{ title: "Booking Request" }} />
        <Stack.Screen name="booking/[status]" options={{ title: "Booking Status" }} />
      </Stack>
    </QueryClientProvider>
  );
}
