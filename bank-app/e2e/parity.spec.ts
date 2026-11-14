import { expect, test } from "@playwright/test";

/** Feature parity with the_bank_app (docs/parity.md, "Resolution"). */
test("sidebar shows connection detail, last refresh and footer", async ({ page, request }) => {
  const health = await (await request.get("/api/health?deep=1")).json();
  expect(health.snowflake.DATABASE).toBeTruthy();
  await page.goto("/");
  const status = page.getByTestId("connection-status");
  await expect(status).toContainText("Connected to Snowflake", { timeout: 30_000 });
  await expect(status).toContainText(health.snowflake.DATABASE);
  await expect(page.getByText(/^Last refresh \d{2}:\d{2}:\d{2}$/)).toBeVisible();
  await expect(page.getByText("v2.0 | 2026 | Next.js + Snowflake")).toBeVisible();
});

test("home page links every area", async ({ page }) => {
  await page.goto("/");
  const main = page.getByRole("main");
  for (const name of ["Customer 360", "AML & Fraud", "Wealth Management", "DtI comparison", "LCR Monitoring", "Ask AI"]) {
    await expect(main.getByRole("link", { name: new RegExp(name) })).toBeVisible();
  }
});

test("customer list is no longer capped at 500", async ({ request }) => {
  const { rows } = await (await request.get("/api/data/customer360.list")).json();
  expect(rows.length).toBeGreaterThan(500);
  expect(rows).toHaveLength(Number(rows[0].MATCH_COUNT));
});

test("AML export holds the latest 1000 alerts with all columns", async ({ request }) => {
  const { rows } = await (await request.get("/api/data/aml.alert-export")).json();
  expect(rows).toHaveLength(1000);
  expect(Object.keys(rows[0]).length).toBeGreaterThan(7);
});

test("tables page instead of truncating", async ({ page }) => {
  await page.goto("/risk/lending");
  await page.getByRole("tab", { name: /Credit Risk/ }).click();
  const pager = page.getByText(/^Page 1 \/ \d+$/).first();
  await expect(pager).toBeVisible({ timeout: 30_000 });
  await page.getByRole("button", { name: "Next" }).first().click();
  await expect(page.getByText(/^Page 2 \/ \d+$/).first()).toBeVisible();
});

test("LCR drill-down: business-unit multiselect and counterparty treemap", async ({ page }) => {
  await page.goto("/liquidity/lcr");
  await page.getByRole("tab", { name: "Drill-down" }).click();
  await expect(page.getByText("Treemap: asset class > product group > counterparty")).toBeVisible({ timeout: 30_000 });
  const units = page.getByRole("group", { name: "Business unit" }).getByRole("button", { pressed: false });
  await expect(units.first()).toBeVisible();
  await units.nth(0).click();
  await units.nth(0).click(); // the first unpressed button is now the second unit
  await expect(page.getByRole("group", { name: "Business unit" }).getByRole("button", { pressed: true })).toHaveCount(2);
  await expect(page.locator(".recharts-treemap-depth-3").first()).toBeAttached();
});
