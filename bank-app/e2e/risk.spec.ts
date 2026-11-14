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
