import { expect, test } from "@playwright/test";

const fmt = (n: number) => n.toLocaleString("en-US");
const one = async (request: import("@playwright/test").APIRequestContext, name: string) => {
  const res = await request.get(`/api/data/${name}`);
  expect(res.ok()).toBeTruthy();
  return (await res.json()).rows as Record<string, unknown>[];
};

/** Risk & Compliance port: parity with the Streamlit page. */
test("risk & compliance overview KPIs match the loader", async ({ page, request }) => {
  const [k] = await one(request, "risk.kpis");
  const hr = await one(request, "risk.high-risk-customers");
  await page.goto("/risk/compliance");
  await expect(page.getByRole("heading", { name: "Risk & Compliance" })).toBeVisible();
  for (const label of ["Critical/High Risk", "Requires PEP Review", "Requires Sanctions Review", "High Risk %"]) {
    await expect(page.getByText(label, { exact: true }).first()).toBeVisible({ timeout: 30_000 });
  }
  const pepCard = page.getByText("Requires PEP Review", { exact: true }).first().locator("xpath=ancestor::div[contains(@class,'p-5')]");
  await expect(pepCard).toContainText(fmt(Number(k.PEP_REVIEW)));
  await expect(page.getByText(`${Number(k.HIGH_RISK_PCT).toFixed(1)}%`).first()).toBeVisible();
  await expect(page.getByText("High-Risk Customers Requiring Action")).toBeVisible();
  await expect(page.getByText(`${hr.length} customers require immediate compliance review`)).toBeVisible();
});

test("risk & compliance sanctions and KYC tabs", async ({ page, request }) => {
  const sanctions = await one(request, "risk.sanctions-matches");
  const kyc = await one(request, "risk.kyc-completeness");
  await page.goto("/risk/compliance");
  await page.getByRole("tab", { name: "Sanctions Control" }).click();
  const card = page.getByText("Total Sanctions Matches", { exact: true }).locator("xpath=ancestor::div[contains(@class,'p-5')]");
  await expect(card).toContainText(fmt(sanctions.length), { timeout: 30_000 });
  await page.getByRole("tab", { name: "KYC Screening" }).click();
  await expect(page.getByText("KYC Data Completeness by Country")).toBeVisible();
  const total = kyc.reduce((s, r) => s + Number(r.TOTAL_CUSTOMERS), 0);
  const tc = page.getByText("Total Customers", { exact: true }).locator("xpath=ancestor::div[contains(@class,'p-5')]");
  await expect(tc).toContainText(fmt(total), { timeout: 30_000 });
});

/** AML & Fraud port. */
test("AML fraud KPIs match the loader", async ({ page, request }) => {
  const [k] = await one(request, "aml.fraud-kpis");
  await page.goto("/risk/aml");
  await expect(page.getByRole("heading", { name: "AML & Fraud Detection" })).toBeVisible();
  const card = page.getByText("Anomalous Customers", { exact: true }).locator("xpath=ancestor::div[contains(@class,'p-5')]");
  await expect(card).toContainText(fmt(Number(k.ANOMALOUS_CUSTOMERS)), { timeout: 30_000 });
  await expect(page.getByText("Anomaly Priority Queue")).toBeVisible();
  await expect(page.getByText(`${Number(k.ANOMALY_RATE_PCT).toFixed(1)}%`).first()).toBeVisible();
});

test("AML monitoring tab matches the 90-day metrics", async ({ page, request }) => {
  const [m] = await one(request, "aml.metrics");
  await page.goto("/risk/aml");
  await page.getByRole("tab", { name: "AML Monitoring" }).click();
  const card = page.getByText("Total Alerts (90d)", { exact: true }).locator("xpath=ancestor::div[contains(@class,'p-5')]");
  await expect(card).toContainText(fmt(Number(m.TOTAL_ALERTS)), { timeout: 30_000 });
  await expect(page.getByText("AML Alert Trend (Last 90 Days)")).toBeVisible();
  await expect(page.getByText("Latest AML alerts")).toBeVisible();
});

const PICKER = "Select a customer to view profile";
const LINK = 'a[href^="/banking/customer-360?customer="]';
const isTrue = (v: unknown) => v === true || v === "true" || v === 1 || v === "1";
const cardButton = (page: import("@playwright/test").Page, label: string) =>
  page.getByRole("button", { name: `${label}: filter the list below` });

/** Click a filter card, check aria-pressed and the filtered count, click again to clear. */
async function toggleCard(page: import("@playwright/test").Page, label: string, filtered: RegExp | string, cleared: RegExp | string) {
  const card = cardButton(page, label);
  await card.click();
  await expect(card).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByText(filtered).first()).toBeVisible({ timeout: 30_000 });
  await card.click();
  await expect(card).toHaveAttribute("aria-pressed", "false");
  await expect(page.getByText(cleared).first()).toBeVisible({ timeout: 30_000 });
}

test("U7/U13 compliance overview cards filter the high-risk list", async ({ page, request }) => {
  const [k] = await one(request, "risk.kpis");
  const hr = await one(request, "risk.high-risk-customers");
  await page.goto("/risk/compliance");
  const all = `${hr.length} customers require immediate compliance review`;
  await expect(page.getByText(all)).toBeVisible({ timeout: 30_000 });
  for (const [label, field] of [
    ["Critical/High Risk", "CRITICAL_HIGH"],
    ["Requires PEP Review", "PEP_REVIEW"],
    ["Requires Sanctions Review", "SANCTIONS_REVIEW"],
  ] as const) {
    await toggleCard(page, label, `${Number(k[field])} customers require immediate compliance review (${label})`, all);
  }
  // "show all" clears an active filter too
  await cardButton(page, "Requires PEP Review").click();
  await page.getByRole("button", { name: "show all" }).click();
  await expect(page.getByText(all)).toBeVisible();
});

test("U8 high-risk list holds every critical/high and review customer", async ({ request }) => {
  const [k] = await one(request, "risk.kpis");
  const hr = await one(request, "risk.high-risk-customers");
  expect(hr.filter((r) => isTrue(r.IS_CRITICAL_HIGH))).toHaveLength(Number(k.CRITICAL_HIGH));
  expect(hr.filter((r) => isTrue(r.REQUIRES_EXPOSED_PERSON_REVIEW))).toHaveLength(Number(k.PEP_REVIEW));
  expect(hr.filter((r) => isTrue(r.REQUIRES_SANCTIONS_REVIEW))).toHaveLength(Number(k.SANCTIONS_REVIEW));
});

test("U9 sanctions control cards filter the sanctions matches", async ({ page, request }) => {
  const rows = await one(request, "risk.sanctions-matches");
  const counts = {
    "Exact Matches": rows.filter((r) => r.SANCTIONS_MATCH_TYPE === "EXACT_MATCH").length,
    "Requires Review": rows.filter((r) => isTrue(r.REQUIRES_SANCTIONS_REVIEW)).length,
    "High Risk": rows.filter((r) => ["CRITICAL", "HIGH"].includes(String(r.OVERALL_SANCTIONS_RISK))).length,
  };
  await page.goto("/risk/compliance");
  await page.getByRole("tab", { name: "Sanctions Control" }).click();
  test.skip(rows.length === 0, "no sanctions matches in this data set");
  const all = new RegExp(`^${fmt(rows.length)} sanctions matches$`);
  await expect(page.getByText(all)).toBeVisible({ timeout: 30_000 });
  for (const [label, n] of Object.entries(counts)) {
    await toggleCard(page, label, `${fmt(n)} sanctions matches (${label})`, all);
  }
});

test("U10 KYC screening cards filter the PEP matches", async ({ page, request }) => {
  const rows = await one(request, "risk.pep-matches");
  const counts = {
    "Requires PEP Review": rows.filter((r) => isTrue(r.REQUIRES_EXPOSED_PERSON_REVIEW)).length,
    "Exact PEP Matches": rows.filter((r) => r.EXPOSED_PERSON_MATCH_TYPE === "EXACT_MATCH").length,
  };
  await page.goto("/risk/compliance");
  await page.getByRole("tab", { name: "KYC Screening" }).click();
  test.skip(rows.length === 0, "no PEP matches in this data set");
  const all = new RegExp(`^${fmt(rows.length)} PEP matches$`);
  await expect(page.getByText(all)).toBeVisible({ timeout: 30_000 });
  for (const [label, n] of Object.entries(counts)) {
    await toggleCard(page, label, `${fmt(n)} PEP matches (${label})`, all);
  }
});

test("U11 AML fraud cards filter the anomaly queue", async ({ page, request }) => {
  const [k] = await one(request, "aml.fraud-kpis");
  await page.goto("/risk/aml");
  const all = `${Number(k.ANOMALOUS_CUSTOMERS)} customers flagged with anomalous transaction patterns`;
  await expect(page.getByText(all)).toBeVisible({ timeout: 30_000 });
  await toggleCard(page, "High-Risk + Anomaly", `${Number(k.HIGH_RISK_ANOMALY)} customers flagged with anomalous transaction patterns (High-Risk + Anomaly)`, all);
  // every queue row is anomalous: the filter keeps all rows
  await toggleCard(page, "Anomalous Customers", `${Number(k.ANOMALOUS_CUSTOMERS)} customers flagged with anomalous transaction patterns (Anomalous Customers)`, all);
});

test("U12 risk lists link Customer IDs to Customer 360, no profile pickers", async ({ page }) => {
  const views: [string, string | null][] = [
    ["/risk/compliance", null],
    ["/risk/compliance", "Sanctions Control"],
    ["/risk/compliance", "KYC Screening"],
    ["/risk/aml", null],
    ["/risk/aml", "AML Monitoring"],
  ];
  for (const [path, tab] of views) {
    await page.goto(path);
    if (tab) await page.getByRole("tab", { name: tab }).click();
    const link = page.locator(LINK).first();
    await expect(link).toBeVisible({ timeout: 30_000 });
    await expect(link).toHaveAttribute("href", /^\/banking\/customer-360\?customer=CUST_\d+$/);
    await expect(page.locator("option", { hasText: PICKER })).toHaveCount(0);
  }
  // follow one link: Customer 360 opens the linked customer
  const id = (await page.locator(LINK).first().getAttribute("href"))!.split("=")[1];
  await page.locator(LINK).first().click();
  await expect(page.getByText(`Showing linked customer ${id}`)).toBeVisible({ timeout: 30_000 });
});

test("U12 read-only container: AML queue links render", async ({ page }) => {
  const RO = process.env.RO_URL;
  test.skip(!RO, "RO_URL not set (second container with SNOWFLAKE_ROLE=DTI_READ_ONLY)");
  await page.goto(`${RO}/risk/aml`);
  await expect(page.getByRole("heading", { name: "AML & Fraud Detection" })).toBeVisible({ timeout: 30_000 });
  await expect(page.locator("option", { hasText: PICKER })).toHaveCount(0);
  const links = page.locator(LINK);
  if ((await links.count()) > 0) {
    await expect(links.first()).toHaveAttribute("href", /^\/banking\/customer-360\?customer=CUST_\d+$/);
  }
});
