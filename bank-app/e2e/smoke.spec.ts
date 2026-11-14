import { expect, test } from "@playwright/test";

test("health reports runtime", async ({ request }) => {
  const res = await request.get("/api/health");
  expect(res.ok()).toBeTruthy();
  expect((await res.json()).status).toBe("ok");
});

test("deep health reaches Snowflake", async ({ request }) => {
  const body = await (await request.get("/api/health?deep=1")).json();
  expect(body.status).toBe("ok");
  expect(body.snowflake.ACCOUNT).toBe("ZS28104");
});

test("LCR daily returns the reporting window", async ({ request }) => {
  const { rows } = await (await request.get("/api/liquidity/lcr-daily")).json();
  expect(rows.length).toBeGreaterThanOrEqual(90);
  expect(rows[0]).toHaveProperty("LCR_RATIO");
});

test("home page renders", async ({ request }) => {
  const res = await request.get("/");
  expect(res.ok()).toBeTruthy();
  expect(await res.text()).toContain("Synthetic Retail Bank");
});
