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

test("U14 lending portfolio summary is grouped and adds up to the status cards", async ({ page, request }) => {
  const { rows } = await (await request.get("/api/data/lending.portfolio")).json();
  const sum = (status?: string) =>
    rows
      .filter((r: { APPLICATION_STATUS: string }) => !status || r.APPLICATION_STATUS === status)
      .reduce((s: number, r: { LOAN_COUNT: unknown }) => s + Number(r.LOAN_COUNT), 0);
  // every application has one of the three card statuses
  expect(sum("APPROVED") + sum("UNDER_REVIEW") + sum("DECLINED")).toBe(sum());
  await page.goto("/risk/lending");
  await expect(page.getByText("Portfolio summary")).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText("One row per country / product / status; Loan count = number of applications in that group.")).toBeVisible();
});

test("U15 wealth AUM cards are not truncated", async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.goto("/risk/wealth");
  const kpis = page.getByTestId("wealth-kpis");
  await expect(kpis.getByText("Total Advisors")).toBeVisible({ timeout: 30_000 });
  for (const label of ["Total AUM", "Avg AUM/Client"]) {
    const value = kpis.getByText(label, { exact: true }).locator("xpath=ancestor::div[contains(@class,'p-5')]").locator("p.text-2xl");
    await expect(value).toHaveText(/^CHF -?[\d,]+[MK]$/);
    // truncate shows an ellipsis when the value is wider than the card
    expect(await value.evaluate((el) => el.scrollWidth <= el.clientWidth)).toBe(true);
  }
});

test("U16 wealth capacity chart uses a square-root clients axis", async ({ page }) => {
  await page.goto("/risk/wealth");
  await expect(page.getByText(/Clients \(square-root scale\) vs\. portfolio value/)).toBeVisible({ timeout: 30_000 });
  // sqrt-axis ticks (150 may be dropped when it collides with 200)
  const ticks = page.locator(".recharts-cartesian-axis-tick-value");
  for (const t of ["10", "25", "50", "100", "200"]) {
    await expect(ticks.filter({ hasText: new RegExp(`^${t}$`) }).first()).toBeVisible({ timeout: 30_000 });
  }
});
