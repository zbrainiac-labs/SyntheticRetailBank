import { expect, test } from "@playwright/test";
import { bookDates } from "./book-dates";

/** LCR Monitoring port: parity with the Streamlit page (same loaders, same numbers). */
test("LCR KPIs match the regulatory BASE scenario of the latest date", async ({ page, request }) => {
  const { rows } = await (await request.get("/api/data/lcr.scenario-daily")).json();
  const base = rows.filter((r: { STRESS_SCENARIO: string }) => r.STRESS_SCENARIO === "BASE");
  const latest = base[base.length - 1];
  expect(base.length).toBeGreaterThanOrEqual(90);
  await page.goto("/liquidity/lcr");
  await expect(page.getByText(`${Math.round(Number(latest.LCR_RATIO))}%`).first()).toBeVisible();
  await expect(page.getByText(String(base.filter((r: { LCR_RATIO: number }) => Number(r.LCR_RATIO) < 100).length), { exact: true }).first()).toBeVisible();
});

test("all 11 LCR tabs render", async ({ page }) => {
  await page.goto("/liquidity/lcr");
  for (const [tab, text] of [
    ["HQLA & Caps", "HQLA composition and regulatory caps"],
    ["Flows & Inflow Cap", "Inflows vs. 75% inflow cap"],
    ["Behavioral", "Maturity ladder (BASE)"],
    ["Scenarios", "LCR by scenario"],
    ["Variance", "Deltas by asset class"],
    ["Drill-down", "Weighted LCR amounts: asset class > product group"],
    ["Currency", "Weighted LCR amounts by currency"],
    ["Data Quality", "Quarantined records"],
    ["Simulator", "What-if waterfall"],
    ["Reporting", "Monthly SNB summary"],
    ["Overview", "Regulatory LCR - 90 days"],
  ] as const) {
    await page.getByRole("tab", { name: tab }).click();
    await expect(page.getByText(text).first()).toBeVisible({ timeout: 30_000 });
  }
});

test("simulator: a CHF 500M withdrawal lowers the LCR", async ({ request }) => {
  const { asOf } = await bookDates(request);
  const q = (p: object) => `/api/data/lcr.simulate?date=${asOf}&params=${encodeURIComponent(JSON.stringify(p))}`;
  const base = (await (await request.get(q({}))).json()).rows;
  const hit = (await (await request.get(q({ withdrawal_chf: 500e6 }))).json()).rows;
  expect(Number(hit[hit.length - 1].LCR_RATIO)).toBeLessThan(Number(base[base.length - 1].LCR_RATIO));
});

test("loader API rejects unknown loaders and bad params", async ({ request }) => {
  expect((await request.get("/api/data/nope")).status()).toBe(404);
  expect((await request.get("/api/data/lcr.exceptions?date=x")).status()).toBe(400);
  expect((await request.get(`/api/data/lcr.simulate?date=${(await bookDates(request)).asOf}&params=notjson`)).status()).toBe(400);
});
