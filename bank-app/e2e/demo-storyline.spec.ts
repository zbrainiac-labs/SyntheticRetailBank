import { expect, test, type APIRequestContext, type Page } from "@playwright/test";
import { bookDates } from "./book-dates";

/**
 * Dress rehearsal of business_requirements/LIQUIDITY_DTI_LCR_DEMO_STORYLINE.md: walks acts 1-8 against the
 * running app and leaves one screenshot per act in test-results/demo/. Dates come from the loaded book, so
 * the storyline works for every regeneration. Act 7 (read-only) needs RO_URL (second container).
 */
const RO = process.env.RO_URL;
const SHOTS = "test-results/demo";

interface LcrEvent {
  EVENT_ID: string;
  EVENT_DATE: string;
  CONTRACT_ID: string | null;
}

async function lcrEvents(request: APIRequestContext): Promise<Record<string, LcrEvent>> {
  const rows = (await (await request.get("/api/data/lcr.events")).json()).rows as LcrEvent[];
  return Object.fromEntries(rows.map((r) => [r.EVENT_ID, r]));
}

async function shot(page: Page, act: number) {
  await page.screenshot({ path: `${SHOTS}/act-${act}.png`, fullPage: true });
}

async function openTab(page: Page, name: string, text: string) {
  await page.getByRole("tab", { name }).click();
  await expect(page.getByText(text).first()).toBeVisible({ timeout: 30_000 });
}

test.describe.configure({ mode: "serial" });

test("act 1 - one book, one run", async ({ page, request }) => {
  const { runId, asOf } = await bookDates(request);
  await page.goto("/liquidity/lcr");
  const badge = page.getByTestId("liquidity-book");
  await expect(badge).toHaveAttribute("data-run-id", runId);
  await expect(badge.getByText("LCR and DtI differ")).toHaveCount(0);
  await expect(page.getByLabel("Reporting date (list)")).toHaveValue(asOf);
  await shot(page, 1);
});

test("act 2 - LCR compliant today, the breach is visible in the history", async ({ page, request }) => {
  const ev = await lcrEvents(request);
  const daily = (await (await request.get("/api/liquidity/lcr-daily")).json()).rows as { AS_OF_DATE: string; LCR_RATIO: number }[];
  const latest = daily.at(-1)!;
  expect(Number(latest.LCR_RATIO)).toBeGreaterThanOrEqual(110);
  const breach = daily.filter((r) => Number(r.LCR_RATIO) < 100);
  expect(breach.length).toBeGreaterThanOrEqual(5);
  expect(breach.every((r) => r.AS_OF_DATE >= ev.EVT_02.EVENT_DATE && r.AS_OF_DATE < ev.EVT_04.EVENT_DATE)).toBe(true);
  await page.goto("/liquidity/lcr");
  for (const id of ["EVT_01", "EVT_02", "EVT_03", "EVT_04", "EVT_05"]) await expect(page.getByRole("cell", { name: id })).toBeVisible();
  await openTab(page, "HQLA & Caps", "Level 2 share of HQLA");
  await openTab(page, "Flows & Inflow Cap", "Cap utilisation");
  await shot(page, 2);
});

test("act 3 - root cause of the breach: one funding contract", async ({ page, request }) => {
  const ev = await lcrEvents(request);
  const q = (date: string) => `/api/data/lcr.variance-drivers?date=${date}&comparison=DOD&scenario=BASE`;
  const stress = (await (await request.get(q(ev.EVT_02.EVENT_DATE))).json()).rows as { POSITION_ID: string; CLIENT_IMPACT_PP: number }[];
  expect(stress[0].POSITION_ID).toBe(ev.EVT_02.CONTRACT_ID);
  expect(Number(stress[0].CLIENT_IMPACT_PP)).toBeLessThan(-20);
  // the recovery rolls the same contract out of the 30-day window in tranches
  const recovery = (await (await request.get(q(ev.EVT_03.EVENT_DATE))).json()).rows as { POSITION_ID: string }[];
  expect(recovery.slice(0, 3).map((r) => r.POSITION_ID)).toContain(ev.EVT_02.CONTRACT_ID);
  await page.goto("/liquidity/lcr");
  await openTab(page, "Scenarios", "LCR by scenario");
  await page.getByLabel("Reporting date (list)").selectOption(ev.EVT_02.EVENT_DATE);
  await openTab(page, "Variance", "Top-10 client drivers");
  await expect(page.getByText(ev.EVT_02.CONTRACT_ID!).first()).toBeVisible();
  await shot(page, 3);
});

test("act 4 - what-if simulator and SNB reporting", async ({ page }) => {
  await page.goto("/liquidity/lcr");
  await openTab(page, "Simulator", "Simulated LCR");
  await expect(page.getByText("Extra withdrawal to 100%")).toBeVisible();
  await openTab(page, "Reporting", "Monthly SNB summary");
  await expect(page.getByRole("cell", { name: "FAIL" }).first()).toBeVisible();
  await shot(page, 4);
});

test("act 5 - DtI per entity and the events of the book", async ({ page, request }) => {
  const { asOf } = await bookDates(request);
  await page.goto("/dti");
  await expect(page.getByText("DtI reference date")).toBeVisible();
  await expect(page.getByText(`vs.`).first()).toBeVisible();
  const card = page.getByTestId("dti-events");
  await expect(card.getByText("DtI history and events")).toBeVisible();
  for (const id of ["DTI_08", "DTI_09", "DTI_10"]) await expect(card.getByRole("cell", { name: new RegExp(id) })).toBeVisible();
  // the correction on the as-of date is part of today's comparison
  await expect(card.locator('[data-in-delta="true"]').first()).toBeVisible();
  const all = "LE_01,LE_02,LE_03,LE_04,LE_05,LE_06,LE_07,LE_08,LE_09";
  const s = await (await request.get(`/api/dti/summary?ref=${asOf}&comp=${asOf}&entities=${all}&valueType=STRESSED&prelim=1`)).json();
  for (const e of s.entities as { entity: string; ref: { days: number | null } }[]) expect(e.ref.days ?? 366).toBeGreaterThanOrEqual(20);
  await shot(page, 5);
  await page.goto("/dti/parameters");
  await expect(page.getByText("Legal entities")).toBeVisible();
});

test("act 6 - explain the delta: positions, free assets incl. central bank cash", async ({ page, request }) => {
  const { asOf } = await bookDates(request);
  await page.goto("/dti/positions");
  await expect(page.getByText("Control sums reconcile")).toBeVisible();
  const fa = await (await request.get(`/api/dti/free-assets?ref=${asOf}&entities=LE_01,LE_02,LE_03&basis=MV`)).json();
  expect((fa.assets as { wlk: string; value: number }[]).find((a) => a.wlk === "I")!.value).toBeGreaterThan(0);
  await page.goto("/dti/free-assets");
  await expect(page.getByText("Available assets in the collateral pool")).toBeVisible();
  await shot(page, 6);
});

test("act 7 - manual inputs: live correction round trip, never a negative CB balance", async ({ page, request }) => {
  const { asOf } = await bookDates(request);
  const list = await (await request.get(`/api/dti/corrections?ref=${asOf}`)).json();
  const corr1 = list.rows.find((r: { id: string }) => r.id === "CORR-0001");
  expect(corr1).toBeTruthy();
  expect(corr1.amount).toBeLessThan(0);
  const valueDate = new Date(Date.parse(`${asOf}T12:00:00Z`) + 2 * 86_400_000).toISOString().slice(0, 10);
  const body = { ref: asOf, entity: "LE_02", position: "OUT_DEP_CORPORATE", valueDate, amount: -5e6, comment: "demo rehearsal correction" };
  expect((await request.post("/api/dti/corrections", { data: body })).status()).toBe(200);
  const mine = (await (await request.get(`/api/dti/corrections?ref=${asOf}`)).json()).rows.find(
    (r: { comment: string }) => r.comment === body.comment,
  );
  await page.goto("/dti/manual-inputs");
  await expect(page.getByText("CORR-0001")).toBeVisible();
  await expect(page.getByText(body.comment)).toBeVisible();
  await shot(page, 7);
  expect((await request.delete(`/api/dti/corrections?id=${mine.id}`)).status()).toBe(200);
});

test("act 7b - audit view (read-only role)", async ({ page, request }) => {
  test.skip(!RO, "RO_URL not set (second container with SNOWFLAKE_ROLE=DTI_READ_ONLY)");
  const me = await (await request.get(`${RO}/api/dti/me`)).json();
  expect(me).toMatchObject({ canEdit: false, canExport: false });
  const ro = await (await request.get(`${RO}/api/liquidity/book`)).json();
  expect(ro.check).toMatchObject({ comparable: false, ok: true });
  await page.goto(`${RO}/dti/manual-inputs`);
  await expect(page.getByRole("button", { name: "Add correction" })).toHaveCount(0);
  await shot(page, 8);
});

test("act 8 - ask the data", async ({ page }) => {
  await page.goto("/ai/ask");
  await expect(page.getByLabel("Question")).toBeVisible();
  await shot(page, 9);
});
