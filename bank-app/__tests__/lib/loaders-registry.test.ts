import { describe, expect, it } from "vitest";
import { DTI_PAGES, NAV_ITEMS } from "@/lib/constants";
import { LOADERS } from "@/lib/loaders";
import { bindParams, LoaderParamError, type LoaderDef } from "@/lib/loaders/types";

describe("loader registry", () => {
  const entries = Object.entries(LOADERS);

  it("names every loader <page>.<loader> and gives it read-only SQL", () => {
    expect(entries.length).toBeGreaterThan(20);
    for (const [name, def] of entries) {
      expect(name).toMatch(/^[a-z0-9-]+\.[a-z0-9-]+$/);
      // queries, or a call of a reporting procedure (LCR what-if simulator)
      expect(def.sql.trim()).toMatch(/^(SELECT|WITH)\b|^CALL AAA_DEV_SYNTHETIC_BANK\.REP_AGG_V001\.REPP_AGG_SP_/i);
    }
  });

  it("has one bind placeholder per declared parameter", () => {
    for (const [name, def] of entries) {
      const binds = (def.sql.replace(/'[^']*'/g, "").match(/\?/g) ?? []).length;
      expect({ name, binds }).toEqual({ name, binds: def.params?.length ?? 0 });
    }
  });
});

describe("loader parameters", () => {
  const def: LoaderDef = {
    sql: "SELECT ?, ?, ?, ?",
    params: [
      { name: "d", type: "date" },
      { name: "c", type: "code", default: "ALL" },
      { name: "n", type: "int" },
      { name: "j", type: "json" },
    ],
  };
  const sp = (o: Record<string, string>) => new URLSearchParams(o);

  it("binds in declaration order, applies defaults and converts ints", () => {
    expect(bindParams(def, sp({ d: "2026-09-14", n: "-5", j: '{"a":1}' }))).toEqual(["2026-09-14", "ALL", -5, '{"a":1}']);
  });

  it.each([
    [{ n: "1", j: "{}" }, "missing parameter d"],
    [{ d: "14.09.2026", n: "1", j: "{}" }, "invalid parameter d"],
    [{ d: "2026-09-14", c: "x'; DROP TABLE t; --", n: "1", j: "{}" }, "invalid parameter c"],
    [{ d: "2026-09-14", n: "1.5", j: "{}" }, "invalid parameter n"],
    [{ d: "2026-09-14", n: "1", j: "{oops}" }, "invalid JSON in j"],
  ])("rejects %j", (params, message) => {
    expect(() => bindParams(def, sp(params))).toThrow(new LoaderParamError(message));
  });
});

describe("navigation", () => {
  it("lists every DtI page in the command palette", () => {
    expect(DTI_PAGES[0]).toEqual({ label: "Summary", href: "/dti" });
    for (const p of DTI_PAGES) expect(NAV_ITEMS).toContainEqual({ label: `DtI ${p.label}`, href: p.href });
  });
});
