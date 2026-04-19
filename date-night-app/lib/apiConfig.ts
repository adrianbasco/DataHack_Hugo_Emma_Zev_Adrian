const LOCAL_BACKEND_PORT = 8000;

type ResolveApiBaseUrlOptions = {
  env?: Record<string, string | undefined>;
  expoDevServerHostUri?: string | null;
  isDev: boolean;
  platformOS: string;
};

export function resolveApiBaseUrl({
  env,
  expoDevServerHostUri,
  isDev,
  platformOS,
}: ResolveApiBaseUrlOptions): string {
  const configured = (env?.EXPO_PUBLIC_API_BASE_URL || "").trim();
  if (configured) {
    return configured.replace(/\/+$/, "");
  }

  if (platformOS === "web") {
    return `http://127.0.0.1:${LOCAL_BACKEND_PORT}`;
  }

  if (isDev) {
    const host = parseReachableDevHost(expoDevServerHostUri);
    if (host) {
      return `http://${formatHostForUrl(host)}:${LOCAL_BACKEND_PORT}`;
    }
  }

  return "";
}

function parseReachableDevHost(hostUri?: string | null): string | null {
  const trimmed = (hostUri || "").trim();
  if (!trimmed) {
    return null;
  }

  const candidate = /^[a-z][a-z0-9+.-]*:\/\//i.test(trimmed)
    ? trimmed
    : `http://${trimmed}`;

  try {
    const host = new URL(candidate).hostname.replace(/^\[|\]$/g, "");
    return isReachableLocalDevHost(host) ? host : null;
  } catch (error) {
    console.error("Failed to parse Expo dev server host URI.", {
      error,
      hostUri,
    });
    return null;
  }
}

function isReachableLocalDevHost(host: string): boolean {
  if (!host) {
    return false;
  }

  const normalized = host.toLowerCase();
  if (normalized === "localhost" || normalized.endsWith(".localhost")) {
    return false;
  }

  if (normalized.endsWith(".local")) {
    return true;
  }

  const octets = normalized.split(".").map((part) => Number(part));
  if (
    octets.length === 4 &&
    octets.every((part) => Number.isInteger(part) && part >= 0 && part <= 255)
  ) {
    const [first, second] = octets;
    return (
      first === 10 ||
      (first === 169 && second === 254) ||
      (first === 172 && second >= 16 && second <= 31) ||
      (first === 192 && second === 168)
    );
  }

  if (normalized.includes(":")) {
    return (
      normalized.startsWith("fe80:") ||
      normalized.startsWith("fc") ||
      normalized.startsWith("fd")
    );
  }

  return false;
}

function formatHostForUrl(host: string): string {
  return host.includes(":") ? `[${host}]` : host;
}
