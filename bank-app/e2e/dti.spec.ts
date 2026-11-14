import { expect, test } from "@playwright/test";
import { bookDates } from "./book-dates";

/**
 * DtI slice 1 against Snowflake DEV: the comparison API reproduces the
 * planted showcase events, and the pages render and drill down. All dates come
 * from the loaded book (book-dates.ts).
 */
const withdrawalQuery = async (request: Parameters<typeof bookDates>[0]) => {
  const d = await bookDates(request);
  return `comp=${d.beforeWithdrawal}&ref=${d.withdrawal}&entities=LE_02&valueType=STRESSED&view=BUCKETS&prelim=0`;
};

test("meta lists reporting dates and the LST tree", async ({ request }) => {
  const body = await (await request.get("/api/dti/meta")).json();
  expect(body.dates.length).toBeGreaterThan(1);
  expect(body.positions.length).toBeGreaterThan(30);
  expect(body.thresholds.GROUP).toBeGreaterThan(0);
});

test("withdrawal of LE_02 is the largest delta (central bank balance, W1)", async ({ request }) => {
  const { rows } = await (await request.get(`/api/dti/compare?${await withdrawalQuery(request)}`)).json();
  const top = [...rows].sort((a, b) => Math.abs(b.delta ?? 0) - Math.abs(a.delta ?? 0))[0];
  expect(top.position).toBe("POOL_CB_BALANCE");
  expect(top.period).toBe("W1");
  expect(top.delta).toBeLessThan(-40e6);
});

test("invalid parameters are rejected", async ({ request }) => {
  const d = await bookDates(request);
  expect((await request.get(`/api/dti/compare?ref=x&comp=${d.prev}&entities=LE_02`)).status()).toBe(400);
  expect((await request.get(`/api/dti/summary?ref=${d.asOf}&comp=${d.prev}&entities=LE_99`)).status()).toBe(400);
});

test("summary shows the DtI of both dates", async ({ page }) => {
  await page.goto("/dti");
  await expect(page.getByText("DtI reference date")).toBeVisible();
  await expect(page.getByText(/\d+ days|> 365 days/).first()).toBeVisible();
});

test("LST positions reconcile and drill down to deals", async ({ page }) => {
  await page.goto("/dti/positions");
  await expect(page.getByText("Control sums reconcile")).toBeVisible();
  await page.locator("tbody button.w-full").first().click();
  await expect(page).toHaveURL(/\/dti\/deals\?/);
  await expect(page.getByText("Control sum")).toBeVisible();
});

test("product drill path: cluster breakdown reconciles with its deals", async ({ request }) => {
  const q = await withdrawalQuery(request);
  const { rows } = await (await request.get(`/api/dti/compare?${q}&dims=product,side&cluster=PC_CORP_DEP`)).json();
  const w1 = rows.filter((r: { period: string; info: boolean }) => r.period === "W1" && !r.info);
  const cellDelta = w1.reduce((s: number, r: { delta: number }) => s + r.delta, 0);
  const col = w1[0];
  const deals = await (
    await request.get(`/api/dti/deals?${q}&cluster=PC_CORP_DEP&period=W1&refFrom=${col.refFrom}&refTo=${col.refTo}&compFrom=${col.compFrom}&compTo=${col.compTo}`)
  ).json();
  const dealDelta = deals.rows.reduce((s: number, r: { delta: number }) => s + r.delta, 0);
  expect(Math.abs(cellDelta - dealDelta)).toBeLessThan(1);
});

for (const [path, text] of [
  ["/dti/applications", "Application Level"],
  ["/dti/product-groups", "Product Groups"],
  ["/dti/products?cluster=PC_RETAIL_DEP", "Cluster sum (unfiltered)"],
  ["/dti/securities?wlk=II", "Master data"],
  ["/dti/free-assets", "Mobilisation status"],
  ["/dti/parameters", "Position assignment rules"],
] as const) {
  test(`slice 2 page ${path} renders`, async ({ page }) => {
    await page.goto(path);
    await expect(page.getByText(text).first()).toBeVisible();
  });
}

// ---- step 6: richer data (business days + month-ends, securities book, transactions)
const ALL = "LE_01,LE_02,LE_03,LE_04,LE_05,LE_06,LE_07,LE_08,LE_09";

test("reporting dates are business days with month-end history", async ({ request }) => {
  const meta = await (await request.get("/api/dti/meta")).json();
  const weekday = (d: string) => new Date(`${d}T12:00:00Z`).getUTCDay();
  expect(meta.dates.length).toBeGreaterThanOrEqual(75);
  expect(meta.dates.every((d: string) => weekday(d) >= 1 && weekday(d) <= 5)).toBe(true);
  expect(meta.monthEnds.length).toBeGreaterThanOrEqual(15);
  expect(meta.dates[0]).toBe((await bookDates(request)).asOf);
});

test("WLK securities are repo capacity on the next day (first bucket)", async ({ request }) => {
  const d = await bookDates(request);
  const q = `ref=${d.asOf}&comp=${d.prev}&entities=${ALL}&valueType=STRESSED&view=BUCKETS&prelim=1`;
  const body = await (await request.get(`/api/dti/securities?${q}&wlk=II`)).json();
  const periods = new Set(body.points.filter((p: { ref: number }) => p.ref !== 0).map((p: { period: string }) => p.period));
  expect([...periods]).toEqual(["W1"]);
  expect(body.rows.length).toBeGreaterThan(20);
});

test("corporate deposits move on the default date pair", async ({ request }) => {
  const d = await bookDates(request);
  const q = `ref=${d.asOf}&comp=${d.prev}&entities=${ALL}&valueType=STRESSED&view=BUCKETS&prelim=1`;
  const { rows } = await (await request.get(`/api/dti/compare?${q}&dims=product,side&cluster=PC_CORP_DEP`)).json();
  const moved = rows.filter((r: { info: boolean; delta: number }) => !r.info && Math.abs(r.delta) > 1e5);
  expect(moved.length).toBeGreaterThan(0);
});

test("amount filter hides rows but keeps the control sums", async ({ page }) => {
  await page.goto("/dti/positions");
  await expect(page.getByText("Control sums reconcile")).toBeVisible();
  const total = () => page.locator("tfoot tr").first().innerText();
  const rows = () => page.locator("tbody tr").count();
  const before = await total();
  const n = await rows();
  await page.getByLabel("Hide rows with all absolute values below (CHF M)").fill("50");
  await expect.poll(rows).toBeLessThan(n);
  expect(await total()).toBe(before);
});

test("summary shows entity slots and the group line", async ({ page }) => {
  await page.goto("/dti");
  await expect(page.getByText("DtI per entity")).toBeVisible();
  await expect(page.getByLabel("Entity slot 1")).toHaveValue("LE_01");
  await expect(page.getByLabel("Entity slot 5")).toBeVisible();
  await expect(page.getByRole("row", { name: /Group \(3 entities\)/ })).toBeVisible();
});

test("back from the deal level returns to the clicked cell", async ({ page }) => {
  await page.goto("/dti/positions");
  await expect(page.getByText("Control sums reconcile")).toBeVisible();
  await page.locator("tbody button.w-full").first().click();
  await expect(page).toHaveURL(/\/dti\/deals\?/);
  await page.getByRole("link", { name: "Back" }).click();
  await expect(page).toHaveURL(/\/dti\/positions\?focus=/);
  await expect(page.locator("button[data-focused]")).toBeVisible();
});
