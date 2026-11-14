import { expect, test } from "@playwright/test";
import { bookDates } from "./book-dates";

/**
 * DtI governance (step 7). The default app (BASE_URL) runs as CICD, which
 * inherits DTI_OWNER; a second container runs with SNOWFLAKE_ROLE=DTI_READ_ONLY
 * (RO_URL, e.g. http://localhost:8089). Tests against RO_URL are skipped when
 * it is not set.
 */
const RO = process.env.RO_URL;
const query = async (request: Parameters<typeof bookDates>[0], entities = "LE_01,LE_02,LE_03,LE_04,LE_05") => {
  const d = await bookDates(request);
  return `ref=${d.asOf}&comp=${d.prev}&entities=${entities}&valueType=STRESSED&view=BUCKETS&prelim=1`;
};

test("owner: capabilities, export and manual inputs editor", async ({ request, page }) => {
  const Q = await query(request);
  const me = await (await request.get("/api/dti/me")).json();
  expect(me).toMatchObject({ dtiRole: "DTI_OWNER", canExport: true, canEdit: true });
  const csv = await request.get(`/api/dti/export?${Q}`);
  expect(csv.status()).toBe(200);
  expect(csv.headers()["content-type"]).toContain("text/csv");
  expect((await csv.text()).split("\n")[1]).toContain("POSITION_CODE");
  await page.goto("/dti/manual-inputs");
  await expect(page.getByRole("button", { name: "Add correction" })).toBeVisible();
  await expect(page.getByText("CORR-0001")).toBeVisible();
});

test("owner: correction round trip (create, list, delete)", async ({ request }) => {
  const { asOf } = await bookDates(request);
  const valueDate = new Date(Date.parse(`${asOf}T12:00:00Z`) + 3 * 86_400_000).toISOString().slice(0, 10);
  const body = { ref: asOf, entity: "LE_09", position: "OUT_DEP_RETAIL", valueDate, amount: -1234567, comment: "e2e test correction" };
  expect((await request.post("/api/dti/corrections", { data: body })).status()).toBe(200);
  const list = await (await request.get(`/api/dti/corrections?ref=${asOf}`)).json();
  const mine = list.rows.find((r: { comment: string; entity: string }) => r.comment === body.comment && r.entity === "LE_09");
  expect(mine).toBeTruthy();
  expect(mine.inEngine).toBe(false);
  expect((await request.delete(`/api/dti/corrections?id=${mine.id}`)).status()).toBe(200);
  expect((await request.post("/api/dti/corrections", { data: { ...body, position: "OUT_DEP" } })).status()).toBe(400);
});

test.describe("read-only role", () => {
  test.skip(!RO, "RO_URL not set (second container with SNOWFLAKE_ROLE=DTI_READ_ONLY)");

  test("sees only mapped entities, no export, no editor", async ({ request, page }) => {
    const Q = await query(request);
    const me = await (await request.get(`${RO}/api/dti/me`)).json();
    expect(me).toMatchObject({ role: "DTI_READ_ONLY", canExport: false, canEdit: false });
    const { rows } = await (await request.get(`${RO}/api/dti/compare?${Q}&dims=app`)).json();
    expect(rows.length).toBeGreaterThan(0);
    const all = await (await request.get(`${RO}/api/dti/summary?${Q}`)).json();
    expect(all.entities.map((e: { entity: string }) => e.entity).sort()).toEqual(["LE_01", "LE_02", "LE_03"]);
    expect((await request.get(`${RO}/api/dti/export?${Q}`)).status()).toBe(403);
    expect((await request.post(`${RO}/api/dti/corrections`, { data: {} })).status()).toBe(403);
    expect((await request.put(`${RO}/api/dti/limits`, { data: { entity: "LE_01", threshold: 1 } })).status()).toBe(403);
    await page.goto(`${RO}/dti/manual-inputs`);
    await expect(page.getByText(/read-only \(DTI_READ_ONLY\)/)).toBeVisible();
    await expect(page.getByRole("button", { name: "Add correction" })).toHaveCount(0);
    await page.goto(`${RO}/dti/positions`);
    await expect(page.getByText("Control sums reconcile")).toBeVisible();
    await expect(page.getByRole("link", { name: "Export CSV" })).toHaveCount(0);
  });

  test("entities outside the mapping return no data", async ({ request }) => {
    const q = await query(request, "LE_04,LE_05");
    const { rows } = await (await request.get(`${RO}/api/dti/compare?${q}`)).json();
    expect(rows.length).toBe(0);
  });

  test("liquidity book: same run, LCR not visible is no mismatch", async ({ request, page }) => {
    const owner = await (await request.get("/api/liquidity/book")).json();
    const ro = await (await request.get(`${RO}/api/liquidity/book`)).json();
    expect(ro).toMatchObject({ runId: owner.runId, asOf: owner.asOf, dtiLatest: owner.asOf, lcrLatest: null });
    expect(ro.check).toMatchObject({ comparable: false, ok: true });
    await page.goto(`${RO}/dti`);
    await expect(page.getByTestId("liquidity-book").getByText("LCR not visible for this role")).toBeVisible();
  });
});
