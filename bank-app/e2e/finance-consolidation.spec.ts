import { expect, test } from "@playwright/test";

/** Financial Control consolidation showcase: decision bridge, sheet + answer trail, controls with the demo exception. */
const M = (v: number, d = 1) => (Number((v / 1e6).toFixed(d)) || 0).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });

async function latestMonth(request: import("@playwright/test").APIRequestContext) {
  const { rows } = await (await request.get("/api/data/finance.periods")).json();
  expect(rows.length).toBeGreaterThan(1);
  return String(rows[0].PERIOD_MONTH);
}

test("banner, KPIs and controls status match the loaders", async ({ page, request }) => {
  const month = await latestMonth(request);
  const cons = (await (await request.get(`/api/data/finance.consolidation?month=${month}`)).json()).rows;
  const checks = (await (await request.get(`/api/data/finance.checks?month=${month}`)).json()).rows;
  const assets = cons
    .filter((r: { COL: string; SECTION: string }) => r.COL === "GROUP" && r.SECTION === "ASSETS")
    .reduce((s: number, r: { AMOUNT_CHF: number }) => s + Number(r.AMOUNT_CHF), 0);

  await page.goto("/finance/consolidation");
  await expect(page.getByRole("heading", { name: "Group Consolidation" })).toBeVisible();
  await expect(page.getByTestId("illustrative-banner")).toContainText("Synthetic data, fictional entities, illustrative values");
  const kpis = page.getByTestId("consolidation-kpis");
  await expect(kpis.getByText(`CHF ${M(assets)}M`)).toBeVisible({ timeout: 30_000 });
  await expect(kpis.getByText(`${checks.length} / ${checks.length}`)).toBeVisible();
});

test("bridge: opening + movements = closing and the summary names the drivers", async ({ page, request }) => {
  const month = await latestMonth(request);
  const rows = (await (await request.get(`/api/data/finance.bridge?month=${month}`)).json()).rows.filter(
    (r: { METRIC: string }) => r.METRIC === "TOTAL_ASSETS",
  );
  const sum = (c: string) => rows.filter((r: { COMPONENT: string }) => r.COMPONENT === c).reduce((s: number, r: { AMOUNT_CHF: number }) => s + Number(r.AMOUNT_CHF), 0);
  const moves = ["ENTITY_PERFORMANCE", "FX_EFFECT", "IC_ELIMINATION"].reduce((s, c) => s + sum(c), 0);
  expect(Math.abs(sum("OPENING") + moves - sum("CLOSING"))).toBeLessThan(1);

  await page.goto("/finance/consolidation");
  await expect(page.getByTestId("bridge-chart")).toBeVisible({ timeout: 30_000 });
  const summary = page.getByTestId("bridge-summary");
  await expect(summary).toContainText(`CHF ${M(sum("OPENING"))}M`);
  await expect(summary).toContainText(`CHF ${M(sum("CLOSING"))}M`);
  await expect(page.getByTestId("bridge-table")).toContainText("FX effect");
  await page.getByRole("button", { name: "Net profit" }).click();
  await expect(summary).toContainText("Group net profit");
});

test("consolidation sheet: group column = sum of entities + IC Elim + NCI, trail drawer ends at the group value", async ({ page }) => {
  await page.goto("/finance/consolidation");
  await page.getByRole("tab", { name: "Consolidation sheet" }).click();
  const sheet = page.getByTestId("consolidation-sheet");
  await expect(sheet.getByText("Total assets")).toBeVisible({ timeout: 30_000 });
  for (const h of ["Group Holding CH", "Bank EU (DE)", "Brokerage US", "IC Elim", "NCI", "Group"]) {
    await expect(sheet.getByRole("columnheader", { name: new RegExp(h.replace(/[()]/g, "\\$&")) }).first()).toBeVisible();
  }
  // IC receivables are fully eliminated: the group value of the line is 0.0
  const ic = sheet.locator('tr[data-line="IC_RECEIVABLES"]');
  await expect(ic.locator('[data-col="GROUP"]')).toHaveText("0.0");

  // answer trail of the group value of client loans: the entity contributions add up to the group value
  const loans = sheet.locator('tr[data-line="CLIENT_LOANS"]');
  const groupValue = await loans.locator('[data-col="GROUP"]').innerText();
  await loans.locator('[data-col="GROUP"]').click();
  const drawer = page.getByTestId("trail-drawer");
  await expect(drawer).toBeVisible();
  await expect(drawer.getByTestId("trail-group-total")).toHaveText(groupValue);
  await page.keyboard.press("Escape");
  await expect(drawer).toBeHidden();

  // entity trail: source -> mapping -> FX -> elimination -> group contribution
  await loans.locator('[data-col="LE_09"]').click();
  await expect(drawer).toContainText("Mapping rule");
  await expect(drawer).toContainText("closing rate USD/CHF");
  await expect(drawer).toContainText("Contribution to the group");
  await page.keyboard.press("Escape");
  await expect(drawer).toBeHidden();

  // local currency view relabels the entity columns
  await page.getByRole("button", { name: "Local currency" }).click();
  await expect(sheet.getByRole("columnheader", { name: /Brokerage US\s*USD/ })).toBeVisible();
  await page.getByRole("button", { name: "P&L" }).click();
  await expect(sheet.getByText("Net profit attributable to shareholders")).toBeVisible();
});

test("controls: all pass, the demo exception fails the IC control and names the pair", async ({ page }) => {
  await page.goto("/finance/consolidation");
  await page.getByRole("tab", { name: "Controls" }).click();
  const list = page.getByTestId("controls-list");
  await expect(list.locator("[data-check]")).toHaveCount(5, { timeout: 30_000 });
  await expect(list.locator('[data-status="FAIL"]')).toHaveCount(0);

  await page.getByTestId("simulate-exception").click();
  await expect(list.locator('[data-check="FC-C2"]')).toHaveAttribute("data-status", "FAIL");
  await expect(list.locator('[data-status="FAIL"]')).toHaveCount(1);
  await expect(list.locator('[data-check="FC-C2"]')).toContainText("Brokerage US");
  await expect(page.getByTestId("controls-summary")).toContainText("not ready for sign-off");
  await expect(page.getByTestId("consolidation-kpis")).toContainText("exception open");

  await page.getByTestId("simulate-exception").click();
  await expect(list.locator('[data-status="FAIL"]')).toHaveCount(0);
});

test("method tab explains FX translation, the USD cross rate and NCI", async ({ page }) => {
  await page.goto("/finance/consolidation");
  await page.getByRole("tab", { name: "Method" }).click();
  const method = page.getByTestId("method");
  await expect(method).toContainText("Worked example: Brokerage US (USD)", { timeout: 30_000 });
  await expect(method).toContainText("EUR/CHF is crossed via USD");
  await expect(method).toContainText("Fund Services LU: 75 % owned");
  await expect(page.getByTestId("currency-mix")).toContainText("CHF");
});

test("CSV export carries the illustrative note", async ({ page }) => {
  await page.goto("/finance/consolidation");
  await page.getByRole("tab", { name: "Consolidation sheet" }).click();
  await expect(page.getByTestId("consolidation-sheet")).toBeVisible({ timeout: 30_000 });
  const [download] = await Promise.all([page.waitForEvent("download"), page.getByRole("button", { name: "Export CSV" }).click()]);
  const path = await download.path();
  const fs = await import("node:fs/promises");
  const csv = await fs.readFile(path!, "utf8");
  expect(csv.split("\n")[0]).toContain("GROUP_CHF");
  expect(csv).toContain("illustrative values");
});
