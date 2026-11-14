import { expect, test } from "@playwright/test";
import { bookDates } from "./book-dates";

/**
 * /liquidity/lcr and /dti run on ONE liquidity book: same generator run, same default date and the
 * same free collateral pool (LCR HQLA securities before haircut = DtI pool base value).
 */
test("API: both engines end on the run as-of date and value the same pool", async ({ request }) => {
  const book = await (await request.get("/api/liquidity/book")).json();
  expect(book.runId).toMatch(/^LIQ-/);
  expect(book.lcrLatest).toBe(book.asOf);
  expect(book.dtiLatest).toBe(book.asOf);
  expect(book.lcrPoolChf).toBeGreaterThan(0);
  expect(Math.abs(book.lcrPoolChf - book.dtiPoolChf)).toBeLessThan(0.0005 * book.lcrPoolChf);
  expect(book.check).toEqual({ sameDate: true, samePool: true, ok: true, comparable: true });
});

test("both pages show the same run and open on the same date", async ({ page, request }) => {
  const { runId, asOf } = await bookDates(request);

  await page.goto("/liquidity/lcr");
  const lcrBadge = page.getByTestId("liquidity-book");
  await expect(lcrBadge).toHaveAttribute("data-run-id", runId);
  await expect(lcrBadge.getByText("LCR and DtI on the same book")).toBeVisible();
  await expect(page.getByLabel("Reporting date (list)")).toHaveValue(asOf);

  await lcrBadge.getByRole("link", { name: "Open DtI" }).click();
  await expect(page).toHaveURL(/\/dti$/);
  const dtiBadge = page.getByTestId("liquidity-book");
  await expect(dtiBadge).toHaveAttribute("data-run-id", runId);
  await expect(dtiBadge).toHaveAttribute("data-as-of", asOf);
  await expect(page.getByText(asOf).first()).toBeVisible();
});

test("LCR HQLA holdings and DtI free assets are the same securities", async ({ request }) => {
  const { asOf } = await bookDates(request);
  const holdings = (await (await request.get("/api/data/lcr.hqla-holdings")).json()).rows as { ASSET_TYPE: string; HOLDING_COUNT: number }[];
  const lcrSecurities = holdings.filter((h) => !h.ASSET_TYPE.startsWith("CASH")).reduce((s, h) => s + Number(h.HOLDING_COUNT), 0);
  const all = "LE_01,LE_02,LE_03,LE_04,LE_05,LE_06,LE_07,LE_08,LE_09";
  const q = `ref=${asOf}&comp=${asOf}&entities=${all}&valueType=STRESSED&view=BUCKETS&prelim=1`;
  const securities = new Set<string>();
  for (const wlk of ["II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "XIa", "XIb"]) {
    const body = await (await request.get(`/api/dti/securities?${q}&wlk=${wlk}`)).json();
    for (const r of body.rows as { id: string }[]) securities.add(r.id);
  }
  // every LCR security holding is a DtI security (eligible or not; credit claims, WLK X, are DtI collateral only)
  expect(lcrSecurities).toBeGreaterThan(100);
  expect(securities.size).toBe(lcrSecurities);
});
