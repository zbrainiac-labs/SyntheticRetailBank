import { expect, test } from "@playwright/test";

/** Settings + Ask AI ports (the_bank_app/pages/settings.py, ask_ai.py). */
test("settings shows the Snowflake account and role", async ({ page, request }) => {
  const health = await (await request.get("/api/health?deep=1")).json();
  expect(health.snowflake.ACCOUNT).toBe("ZS28104");
  await page.goto("/settings");
  await expect(page.getByText("Total Records")).toBeVisible({ timeout: 30_000 });
  await page.getByRole("tab", { name: "Settings" }).click();
  await expect(page.getByText("ZS28104").first()).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText(health.snowflake.ROLE, { exact: true }).first()).toBeVisible();
  await expect(page.getByText("DtI role")).toBeVisible();
});

test("Ask AI lists the bank agents", async ({ page, request }) => {
  const { agents } = await (await request.get("/api/agent")).json();
  expect(agents.length).toBeGreaterThanOrEqual(1);
  await page.goto("/ai/ask");
  const picker = page.getByLabel("Agent");
  await expect(picker).toBeVisible({ timeout: 30_000 });
  await expect(picker.locator("option")).toHaveCount(agents.length);
  await expect(page.getByRole("main").getByRole("link", { name: "Open Snowflake CoWork" })).toHaveAttribute("href", /ai\.snowflake\.com/);
});

test.describe("live agent", () => {
  // Cortex Agent orchestration occasionally stalls in "planning" server-side; allow one retry.
  test.describe.configure({ retries: 1 });

  test("liquidity agent answers the LCR question with a number", async ({ page }) => {
  test.setTimeout(360_000);
  await page.goto("/ai/ask");
  const picker = page.getByLabel("Agent");
  await expect(picker).toBeVisible({ timeout: 30_000 });
  await picker.selectOption("REP_AGG_V001.LIQUIDITY_RISK_AGENT");
  await page.getByLabel("Question").fill("What is the current LCR ratio?");
  await page.getByRole("button", { name: "Send" }).click();
  await expect(page.getByRole("button", { name: "Stop" })).toBeVisible();
  const answer = page.getByTestId("assistant-message").getByTestId("answer");
  await expect(answer).toBeVisible({ timeout: 300_000 });
  await expect(answer).toHaveText(/\d/);
  });
});
