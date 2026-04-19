import assert from "node:assert/strict";
import test from "node:test";

import { resolveApiBaseUrl } from "../lib/apiConfig";

test("resolveApiBaseUrl uses an explicitly configured API URL first", () => {
  assert.equal(
    resolveApiBaseUrl({
      env: { EXPO_PUBLIC_API_BASE_URL: " http://192.168.1.5:9000/// " },
      expoDevServerHostUri: "192.168.1.10:8081",
      isDev: true,
      platformOS: "ios",
    }),
    "http://192.168.1.5:9000"
  );
});

test("resolveApiBaseUrl keeps the web localhost backend default", () => {
  assert.equal(
    resolveApiBaseUrl({
      env: {},
      expoDevServerHostUri: null,
      isDev: true,
      platformOS: "web",
    }),
    "http://127.0.0.1:8000"
  );
});

test("resolveApiBaseUrl derives a native dev backend URL from Expo LAN host", () => {
  assert.equal(
    resolveApiBaseUrl({
      env: {},
      expoDevServerHostUri: "192.168.1.10:8081",
      isDev: true,
      platformOS: "ios",
    }),
    "http://192.168.1.10:8000"
  );
});

test("resolveApiBaseUrl derives a native dev backend URL from Expo URI form", () => {
  assert.equal(
    resolveApiBaseUrl({
      env: {},
      expoDevServerHostUri: "exp://10.0.0.8:8081",
      isDev: true,
      platformOS: "android",
    }),
    "http://10.0.0.8:8000"
  );
});

test("resolveApiBaseUrl does not guess from Expo tunnel hosts", () => {
  assert.equal(
    resolveApiBaseUrl({
      env: {},
      expoDevServerHostUri: "https://example.exp.direct:443",
      isDev: true,
      platformOS: "ios",
    }),
    ""
  );
});

test("resolveApiBaseUrl does not infer native URLs from loopback hosts", () => {
  assert.equal(
    resolveApiBaseUrl({
      env: {},
      expoDevServerHostUri: "127.0.0.1:8081",
      isDev: true,
      platformOS: "ios",
    }),
    ""
  );
});

test("resolveApiBaseUrl does not infer native production URLs", () => {
  assert.equal(
    resolveApiBaseUrl({
      env: {},
      expoDevServerHostUri: "192.168.1.10:8081",
      isDev: false,
      platformOS: "ios",
    }),
    ""
  );
});
