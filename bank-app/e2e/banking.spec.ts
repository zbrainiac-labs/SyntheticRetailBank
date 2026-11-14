import { expect, test } from "@playwright/test";

/** Banking ports (Customer 360, Portfolio Analytics, Churn & Lifecycle): rendering + KPI parity with the loader API. */
const fmt = (n: number, d = 0) => n.toLocaleString("en-US", { maximumFractionDigits: d });
const first = async (request: import("@playwright/test").APIRequestContext, path: string) => {
  const res = await request.get(path);
  expect(res.ok()).toBeTruthy();
  return (await res.json()).rows[0];
};

test("Customer 360 renders KPIs, list and profile", async ({ page, request }) => {
  const k = await first(request, "/api/data/customer360.kpis");
  await page.goto("/banking/customer-360");
  await expect(page.getByRole("heading", { name: "Customer 360" })).toBeVisible();
  for (const label of ["Total Customers", "Critical / High Risk", "PEP Matches", "Avg Risk Score"]) {
    await expect(page.getByText(label, { exact: true }).first()).toBeVisible();
  }
  await expect(page.getByText(fmt(Number(k.TOTAL_CUSTOMERS)), { exact: true }).first()).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText(fmt(Number(k.PEP_MATCHES)), { exact: true }).first()).toBeVisible();
  await expect(page.getByText(`Found ${fmt(Number(k.TOTAL_CUSTOMERS))} customers matching your criteria`)).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText("Identity & Demographics")).toBeVisible({ timeout: 30_000 });
});

test("Customer 360 deep link and filters narrow the list", async ({ page, request }) => {
  const { rows } = await (await request.get("/api/data/customer360.list?tiers=GOLD%20PLATINUM")).json();
  const id = String(rows[0].CUSTOMER_ID);
  const detail = await first(request, `/api/data/customer360.detail?id=${id}`);
  expect(detail.CUSTOMER_ID).toBe(id);
  await page.goto(`/banking/customer-360?customer=${id}`);
  await expect(page.getByText("Found 1 customers matching your criteria")).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText(`Showing linked customer ${id}`)).toBeVisible();
  const single = await (await request.get(`/api/data/customer360.list?customer=${id}`)).json();
  expect(single.rows).toHaveLength(1);
});

test("Portfolio Analytics KPIs match the loader", async ({ page, request }) => {
  const k = await first(request, "/api/data/portfolio.kpis");
  await page.goto("/banking/portfolio");
  await expect(page.getByRole("heading", { name: "Portfolio Analytics" })).toBeVisible();
  await expect(page.getByText(fmt(Number(k.MULTI_CURRENCY)), { exact: true }).first()).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText(fmt(Number(k.INVESTMENT_HOLDERS)), { exact: true }).first()).toBeVisible();
  await expect(page.getByText(fmt(Number(k.AVG_ACCOUNTS), 1), { exact: true }).first()).toBeVisible();
  for (const t of ["Account Tier Distribution", "Geographic Distribution", "Account Type Holdings by Tier"]) {
    await expect(page.getByText(t).first()).toBeVisible();
  }
  const tiers = (await (await request.get("/api/data/portfolio.tier-distribution")).json()).rows;
  expect(tiers.reduce((s: number, r: { CUSTOMERS: number }) => s + Number(r.CUSTOMERS), 0)).toBe(Number(k.TOTAL_CUSTOMERS));
});

test("Churn & Lifecycle KPIs and sections match the loaders", async ({ page, request }) => {
  const k = await first(request, "/api/data/churn.kpis");
  const stages = (await (await request.get("/api/data/churn.stage-summary")).json()).rows;
  await page.goto("/banking/churn");
  await expect(page.getByRole("heading", { name: "Churn & Lifecycle" })).toBeVisible();
  await expect(page.getByText(fmt(Number(k.AT_RISK_CUSTOMERS)), { exact: true }).first()).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText(`CHF ${fmt(Number(k.TOTAL_REVENUE_AT_RISK))}K`)).toBeVisible();
  const s0 = stages[0];
  await expect(page.getByTestId(`stage-${s0.LIFECYCLE_STAGE}`)).toHaveText(fmt(Number(s0.CUSTOMER_COUNT)), { timeout: 30_000 });
  for (const t of ["Churn Probability Analysis", "Premium Customers at Risk (GOLD/PLATINUM)", "Dormant Accounts (>180 Days)", "Recommended Actions"]) {
    await expect(page.getByText(t).first()).toBeVisible();
  }
  const premium = (await (await request.get("/api/data/churn.premium-at-risk")).json()).rows;
  if (premium.length > 0) {
    await expect(page.getByText(`${fmt(premium.length)} premium customers at high risk of churning (>70%)`)).toBeVisible({ timeout: 30_000 });
  }
});

test("banking loaders reject bad params", async ({ request }) => {
  expect((await request.get("/api/data/customer360.detail")).status()).toBe(400);
  expect((await request.get("/api/data/customer360.list?name=%27%3B%20drop")).status()).toBe(400);
  expect((await request.get("/api/data/customer360.list?highRisk=yes")).status()).toBe(400);
});
