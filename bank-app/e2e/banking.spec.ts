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

const rowsOf = async (request: import("@playwright/test").APIRequestContext, path: string) => {
  const res = await request.get(path);
  expect(res.ok()).toBeTruthy();
  return (await res.json()).rows as Record<string, unknown>[];
};
const PICKER = "Select a customer to view profile";
const profileHref = (id: string) => `/banking/customer-360?customer=${id}`;

test("U1 Customer 360 address history shows zip codes as text", async ({ page, request }) => {
  // CUST_00001 has Norwegian zip codes with a leading zero and 5-digit French ones
  const history = await rowsOf(request, "/api/data/customer360.address-history?id=CUST_00001");
  const zips = history.map((r) => String(r.ZIPCODE ?? "")).filter((z) => /^0\d+$|^\d{5,}$/.test(z));
  expect(zips.length).toBeGreaterThan(0);
  await page.goto("/banking/customer-360?customer=CUST_00001");
  // the Address section is a collapsed <details>
  await page.locator("summary", { hasText: /^Address$/ }).click({ timeout: 30_000 });
  await expect(page.getByText("Address history")).toBeVisible({ timeout: 30_000 });
  for (const z of zips) {
    // raw string: no "133" for "0133", no "44,414" for "44414"
    await expect(page.getByRole("cell", { name: z, exact: true }).first()).toBeVisible({ timeout: 30_000 });
  }
});

test("U2 Customer ID in the list selects the profile", async ({ page, request }) => {
  const list = await rowsOf(request, "/api/data/customer360.list");
  const id = String(list[1].CUSTOMER_ID);
  await page.goto("/banking/customer-360");
  const select = page.getByLabel("Select a customer to view detailed profile");
  await expect(select).toHaveValue(String(list[0].CUSTOMER_ID), { timeout: 30_000 });
  const link = page.getByRole("link", { name: id, exact: true });
  await expect(link).toHaveAttribute("href", /#customer-profile$/);
  await link.click();
  await expect(select).toHaveValue(id);
  await expect(page.locator("#customer-profile")).toBeInViewport();
});

test("U3 Customer 360 KPI cards toggle the list filters", async ({ page, request }) => {
  const k = await first(request, "/api/data/customer360.kpis");
  await page.goto("/banking/customer-360");
  const total = `Found ${fmt(Number(k.TOTAL_CUSTOMERS))} customers matching your criteria`;
  await expect(page.getByText(total)).toBeVisible({ timeout: 30_000 });
  for (const [label, field] of [
    ["Critical / High Risk", "CRITICAL_HIGH"],
    ["PEP Matches", "PEP_MATCHES"],
  ] as const) {
    const card = page.getByRole("button", { name: `${label}: filter the list below` });
    await card.click();
    await expect(card).toHaveAttribute("aria-pressed", "true");
    await expect(page.getByRole("checkbox", { name: label, exact: true })).toBeChecked();
    await expect(page.getByText(`Found ${fmt(Number(k[field]))} customers matching your criteria`)).toBeVisible({ timeout: 30_000 });
    await card.click();
    await expect(card).toHaveAttribute("aria-pressed", "false");
    await expect(page.getByRole("checkbox", { name: label, exact: true })).not.toBeChecked();
    await expect(page.getByText(total)).toBeVisible({ timeout: 30_000 });
  }
});

test("U4 Churn premium list links to Customer 360, no profile picker", async ({ page, request }) => {
  const premium = await rowsOf(request, "/api/data/churn.premium-at-risk");
  await page.goto("/banking/churn");
  await expect(page.getByText("Premium Customers at Risk (GOLD/PLATINUM)")).toBeVisible({ timeout: 30_000 });
  if (premium.length > 0) {
    const id = String(premium[0].CUSTOMER_ID);
    await expect(page.getByRole("link", { name: id, exact: true }).first()).toHaveAttribute("href", profileHref(id), { timeout: 30_000 });
  }
  await expect(page.locator("option", { hasText: PICKER })).toHaveCount(0);
});

test("U5 Churn Dormant card uses the panel rule and shows the dormant list", async ({ page, request }) => {
  const k = await first(request, "/api/data/churn.kpis");
  const ds = await first(request, "/api/data/churn.dormant-stats");
  // card and panel count the same customers (no lifecycle-stage condition in the panel)
  expect(Number(k.DORMANT_180)).toBe(Number(ds.DORMANT_COUNT));
  await page.goto("/banking/churn");
  const panel = page.locator("#dormant-accounts");
  await expect(panel.getByText("same rule as the Dormant card")).toBeVisible({ timeout: 30_000 });
  const n = Number(ds.DORMANT_COUNT);
  if (n === 0) {
    await expect(panel.getByText("No dormant accounts")).toBeVisible();
    return;
  }
  const card = page.getByRole("button", { name: "Dormant (>180d): filter the list below" });
  await card.click();
  await expect(card).toHaveAttribute("aria-pressed", "true");
  await expect(panel.locator('a[href^="/banking/customer-360?customer="]')).toHaveCount(Math.min(n, 1000), { timeout: 30_000 });
  await panel.getByRole("button", { name: "show all" }).click();
  await expect(panel.locator('a[href^="/banking/customer-360?customer="]')).toHaveCount(0);
});

test("U6 lifecycle stages: DORMANT only after 180 days without a transaction", async ({ page, request }) => {
  const stages = await rowsOf(request, "/api/data/churn.stage-summary");
  for (const s of stages.filter((r) => r.LIFECYCLE_STAGE === "DORMANT")) {
    expect(Number(s.AVG_DAYS_INACTIVE)).toBeGreaterThanOrEqual(180);
  }
  await page.goto("/banking/churn");
  for (const s of stages) {
    await expect(page.getByTestId(`stage-${s.LIFECYCLE_STAGE}`)).toHaveText(fmt(Number(s.CUSTOMER_COUNT)), { timeout: 30_000 });
  }
});
