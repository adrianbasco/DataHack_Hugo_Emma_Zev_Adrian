import { Stack } from "expo-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { palette } from "../components/ui";
import { useEffect } from "react";
import { Platform } from "react-native";
import { reportClientError } from "../lib/clientErrors";

const queryClient = new QueryClient();

type ErrorUtilsShape = {
  getGlobalHandler?: () => (error: Error, isFatal?: boolean) => void;
  setGlobalHandler?: (handler: (error: Error, isFatal?: boolean) => void) => void;
};

const nativeErrorUtils = (globalThis as { ErrorUtils?: ErrorUtilsShape }).ErrorUtils;

export default function RootLayout() {
  useEffect(() => {
    if (!nativeErrorUtils?.setGlobalHandler) {
      return;
    }

    const previousHandler = nativeErrorUtils.getGlobalHandler?.();
    nativeErrorUtils.setGlobalHandler((error, isFatal) => {
      reportClientError({
        source: "global_error_handler",
        error,
        context: { isFatal: Boolean(isFatal) },
      });
      previousHandler?.(error, isFatal);
    });
  }, []);

  useEffect(() => {
    if (Platform.OS !== "web") return;

    const html = document.documentElement;
    const body = document.body;

    const prevHtmlOverscrollX = html.style.overscrollBehaviorX;
    const prevBodyOverscrollX = body.style.overscrollBehaviorX;

    html.style.overscrollBehaviorX = "none";
    body.style.overscrollBehaviorX = "none";

    return () => {
      html.style.overscrollBehaviorX = prevHtmlOverscrollX;
      body.style.overscrollBehaviorX = prevBodyOverscrollX;
    };
  }, []);
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
        <Stack.Screen name="index" options={{ title: "Date Night", headerShown: false }} />
        <Stack.Screen name="templates" options={{ title: "Templates" }} />
        <Stack.Screen name="results" options={{ title: "Results", headerShown: false }} />
        <Stack.Screen name="saved" options={{ title: "Saved Dates" }} />
        <Stack.Screen name="plan/[id]" options={{ title: "Plan Details" }} />
        <Stack.Screen name="booking/request" options={{ title: "Booking Request" }} />
        <Stack.Screen name="booking/[status]" options={{ title: "Booking Status" }} />
      </Stack>
    </QueryClientProvider>
  );
}
