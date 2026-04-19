import { chromium } from "@playwright/test";

const url = process.env.DATE_NIGHT_WEB_URL || "http://127.0.0.1:19006";
const query = process.env.DATE_NIGHT_SMOKE_QUERY || "find me a nice pub crawl to go on";

const errors = [];
const warnings = [];
const searchResponses = [];

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 390, height: 844 } });

page.on("console", (message) => {
  const text = message.text();
  if (message.type() === "error") {
    errors.push(text);
  }
  if (message.type() === "warning") {
    warnings.push(text);
  }
});

page.on("pageerror", (error) => {
  errors.push(error.stack || error.message);
});

page.on("response", async (response) => {
  if (response.url().includes("/dates/search")) {
    let body = "";
    try {
      body = await response.text();
    } catch (error) {
      body = `<<failed to read body: ${error}>>`;
    }
    searchResponses.push({
      status: response.status(),
      url: response.url(),
      body: body.slice(0, 1000),
    });
  }
});

try {
  await page.goto(url, {
    waitUntil: "domcontentloaded",
    timeout: 30_000,
  });
  await page.getByText("Plan your perfect date.").waitFor({ timeout: 30_000 });

  await page.getByPlaceholder("Describe the date you want.").fill(query);

  const searchResponsePromise = page.waitForResponse(
    (response) => response.url().includes("/dates/search"),
    { timeout: 60_000 }
  );
  await page.getByText("Generate ideas").click();
  await searchResponsePromise;

  await page
    .locator("text=Planner search failed")
    .or(page.locator("text=No cached matches found"))
    .or(page.getByText("Save plan"))
    .waitFor({ timeout: 60_000 });

  const bodyText = await page.locator("body").innerText();
  const state = bodyText.includes("Planner search failed")
    ? "planner_failed"
    : bodyText.includes("No cached matches found")
      ? "no_matches"
      : bodyText.includes("Save plan")
        ? "plans_rendered"
        : "unknown";

  const result = {
    state,
    url: page.url(),
    searchResponses,
    errors,
    warnings,
    bodyText: bodyText.slice(0, 2000),
  };

  console.log(JSON.stringify(result, null, 2));

  if (errors.length > 0 || state === "planner_failed") {
    process.exitCode = 1;
  }
} finally {
  await browser.close();
}
