import { describe, expect, it } from "vitest";
import { CASH_CLUSTER, withCentralBankCash, type FreeAsset } from "@/lib/dti/free-assets";

const assets: FreeAsset[] = [
  { wlk: "I", name: "Central bank balances and cash", hqla: "L1", haircut: 0, value: 0 },
  { wlk: "II", name: "Swiss Confederation bonds", hqla: "L1", haircut: 0.02, value: 600e6 },
];

describe("free assets", () => {
  it("adds central bank and vault cash to WLK I only", () => {
    const out = withCentralBankCash(assets, 44e6);
    expect(out.find((a) => a.wlk === CASH_CLUSTER)?.value).toBe(44e6);
    expect(out.find((a) => a.wlk === "II")?.value).toBe(600e6);
    expect(assets[0].value).toBe(0); // input untouched
  });

  it("no cash leaves the clusters unchanged", () => {
    expect(withCentralBankCash(assets, 0)).toEqual(assets);
  });
});
