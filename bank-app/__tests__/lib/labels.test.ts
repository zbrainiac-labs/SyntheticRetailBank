import { errorStatus, respond } from "@/lib/dti/api";
import { ForbiddenError } from "@/lib/dti/access";
import { ParamError } from "@/lib/dti/params";
import { MEASURE_LABEL, SIDE_LABEL, valueOf } from "@/lib/dti/pivot";
import { scopeLabel, valueTypeLabel } from "@/lib/filters/model";
import { describe, expect, it } from "vitest";

describe("page labels", () => {
  it("scope is the entity name for one entity, else the group", () => {
    expect(scopeLabel(["LE_03"])).toBe("Legal Entity 3");
    expect(scopeLabel(["LE_01", "LE_02"])).toBe("Group (2 entities)");
  });

  it("value type reads stressed / base", () => {
    expect(valueTypeLabel("STRESSED")).toBe("stressed");
    expect(valueTypeLabel("BASE")).toBe("base");
  });

  it("pivot side and measure labels", () => {
    expect(SIDE_LABEL).toEqual({ N: "Netto", A: "Aktiva", P: "Passiva" });
    expect(MEASURE_LABEL.delta).toBe("Δ");
    const c = { ref: 5, comp: 3, delta: 2 };
    expect([valueOf(c, "delta"), valueOf(c, "ref"), valueOf(c, "comp")]).toEqual([2, 5, 3]);
  });
});

describe("API error status", () => {
  it("maps parameter errors to 400, missing privilege to 403, else 500", () => {
    expect(errorStatus(new ParamError("bad date"))).toBe(400);
    expect(errorStatus(new ForbiddenError("no export"))).toBe(403);
    expect(errorStatus(new Error("boom"))).toBe(500);
  });

  it("respond() returns the JSON result or the mapped error", async () => {
    const ok = await respond(async () => ({ a: 1 }));
    expect(ok.status).toBe(200);
    expect(await ok.json()).toEqual({ a: 1 });
    const bad = await respond(async () => {
      throw new ParamError("bad date");
    });
    expect(bad.status).toBe(400);
    expect(await bad.json()).toEqual({ error: "bad date" });
  });
});
