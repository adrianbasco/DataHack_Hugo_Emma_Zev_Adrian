import Constants from "expo-constants";
import { Platform } from "react-native";

import { resolveApiBaseUrl } from "./apiConfig";

const env = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process
  ?.env;

const CLIENT_ERROR_API_BASE = resolveApiBaseUrl({
  env,
  expoDevServerHostUri: Constants.expoConfig?.hostUri,
  isDev: typeof __DEV__ === "boolean" ? __DEV__ : false,
  platformOS: Platform.OS,
});

type ClientErrorReport = {
  source: string;
  error: unknown;
  context?: Record<string, unknown>;
};

export function reportClientError({
  source,
  error,
  context = {},
}: ClientErrorReport): void {
  const payload = {
    source,
    message: errorToMessage(error),
    stack: error instanceof Error ? error.stack : undefined,
    platform: Platform.OS,
    context: {
      ...context,
      expoDevServerHostUri: Constants.expoConfig?.hostUri,
    },
  };

  console.error(`[client-error] ${source}: ${payload.message}`, error);

  if (!CLIENT_ERROR_API_BASE) {
    return;
  }

  void fetch(`${CLIENT_ERROR_API_BASE}/client-errors`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  }).catch((reportError) => {
    console.error("Failed to report client error to backend.", reportError);
  });
}

function errorToMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message;
  }
  if (typeof error === "string" && error.trim()) {
    return error;
  }
  try {
    return JSON.stringify(error);
  } catch {
    return "Unknown client error.";
  }
}
