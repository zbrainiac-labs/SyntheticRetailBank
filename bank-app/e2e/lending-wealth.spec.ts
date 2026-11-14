import { expect, test } from "@playwright/test";

/** Lending & Wealth Management ports: headings, tabs and KPI parity with the loader API. */
const fmt = (n: number) => Math.round(n).toLocaleString("en-US");

test("lending KPIs match lending.portfolio", async ({ page, request }) => {
  const { rows } = await (await request.get("/api/data/lending.portfolio")).json();
  expect(rows.length).toBeGreaterThan(0);
  const total = rows.reduce((s: number, r: { LOAN_COUNT: unknown }) => s + Number(r.LOAN_COUNT), 0);
  const approved = rows
    .filter((r: { APPLICATION_STATUS: string }) => r.APPLICATION_STATUS === "APPROVED")
    .reduce((s: number, r: { LOAN_COUNT: unknown }) => s + Number(r.LOAN_COUNT), 0);
  await page.goto("/risk/lending");
  await expect(page.getByRole("heading", { name: "Lending & Loans" })).toBeVisible();
  const kpis = page.getByTestId("lending-kpis");
  await expect(kpis.getByText("Total Applications")).toBeVisible({ timeout: 30_000 });
  await expect(kpis.getByText(fmt(total), { exact: true }).first()).toBeVisible();
  // the page formats with formatNumber (max. 1 fraction digit, no trailing zeros)
  const rate = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 }).format(total ? (approved / total) * 100 : 0);
  await expect(kpis.getByText(`${rate}% approval rate`)).toBeVisible();
});

test("all 6 lending tabs render", async ({ page }) => {
  await page.goto("/risk/lending");
  for (const [tab, text] of [
    ["LTV Analysis", "Loan-to-Value (LTV) Distribution"],
    ["Application Funnel", "Application Funnel"],
    ["Affordability", "Affordability Assessment"],
    ["Compliance Screening", "Compliance & Risk Screening"],
    ["Credit Risk", "Credit Risk Distribution"],
    ["Portfolio Overview", "Portfolio by Country & Product"],
  ] as const) {
    await page.getByRole("tab", { name: tab }).click();
    await expect(page.getByText(text).first()).toBeVisible({ timeout: 30_000 });
  }
});

test("credit risk classes match lending.risk-classes", async ({ page, request }) => {
  const { rows } = await (await request.get("/api/data/lending.risk-classes")).json();
  await page.goto("/risk/lending");
  await page.getByRole("tab", { name: "Credit Risk" }).click();
  const list = page.getByTestId("risk-classes");
  await expect(list).toBeVisible({ timeout: 30_000 });
  await expect(list.getByText(fmt(Number(rows[0].CUSTOMERS)), { exact: true })).toBeVisible();
});

test("wealth KPIs match wealth.kpis", async ({ page, request }) => {
  const [k] = (await (await request.get("/api/data/wealth.kpis")).json()).rows;
  await page.goto("/risk/wealth");
  await expect(page.getByRole("heading", { name: "Wealth Management" })).toBeVisible();
  const kpis = page.getByTestId("wealth-kpis");
  await expect(kpis.getByText("Total Advisors")).toBeVisible({ timeout: 30_000 });
  await expect(kpis.getByText(fmt(Number(k.TOTAL_ADVISORS)), { exact: true }).first()).toBeVisible();
  await expect(kpis.getByText(fmt(Number(k.TOTAL_CLIENTS)), { exact: true })).toBeVisible();
  await expect(kpis.getByText(fmt(Number(k.TOTAL_SLOTS)), { exact: true })).toBeVisible();
  await expect(kpis.getByText(`${fmt(Number(k.AVG_UTILIZATION_PCT))}%`, { exact: true })).toBeVisible();
});

test("wealth advisor management tab shows team performance", async ({ page }) => {
  await page.goto("/risk/wealth");
  await page.getByRole("tab", { name: "Advisor Management" }).click();
  await expect(page.getByText("Available Capacity", { exact: true })).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText("Team Performance")).toBeVisible({ timeout: 30_000 });
});
