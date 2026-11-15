import { describe, expect, it } from "vitest";
import {
  activeOverrides,
  defaultFilters,
  effectiveFilters,
  entityLabel,
  restoreSaved,
  setOverride,
  toSearchParams,
} from "@/lib/filters/model";

const defaults = defaultFilters("2026-09-30", "2026-08-31");

describe("filter model", () => {
  it("defaults follow UC2: stressed values, time buckets, 3 entities", () => {
    expect(defaults).toMatchObject({
      entities: ["LE_01", "LE_02", "LE_03"],
      valueType: "STRESSED",
      presentation: "BUCKETS",
      includePreliminary: false,
    });
    expect(entityLabel("LE_07")).toBe("Legal Entity 7");
  });

  it("effective = override ?? default, only for applicable filters", () => {
    const o = setOverride(undefined, 0, "comparisonDate", "2026-09-29");
    const withValue = setOverride(o, 0, "valueType", "BASE");
    const eff = effectiveFilters(defaults, activeOverrides(withValue, 0), ["comparisonDate"]);
    expect(eff.comparisonDate).toBe("2026-09-29");
    expect(eff.valueType).toBe("STRESSED"); // not applicable on this page
  });

  it("an empty entity override falls back to the default", () => {
    const o = setOverride(undefined, 0, "entities", []);
    expect(effectiveFilters(defaults, activeOverrides(o, 0), ["entities"]).entities).toEqual(defaults.entities);
  });

  it("a Filter Page change (new version) invalidates page overrides", () => {
    const o = setOverride(undefined, 3, "presentation", "DAYS");
    expect(activeOverrides(o, 3)).toEqual({ presentation: "DAYS" });
    expect(activeOverrides(o, 4)).toEqual({});
  });

  it("clearing an override removes the key", () => {
    const o = setOverride(setOverride(undefined, 0, "presentation", "DAYS"), 0, "presentation", undefined);
    expect(o.values).toEqual({});
  });

  it("search params drop empty values", () => {
    expect(toSearchParams({ cluster: "PC_10", bucket: "W2", product: undefined, x: "" })).toBe("cluster=PC_10&bucket=W2");
  });
});

describe("restore saved filters", () => {
  const dates = ["2026-08-31", "2026-09-14", "2026-09-29", "2026-09-30"];
  const saved = {
    global: { ...defaults, referenceDate: "2026-09-14", comparisonDate: "2026-08-31", valueType: "BASE" as const },
    version: 2,
    pages: { positions: { version: 2, values: { comparisonDate: "2026-09-29", presentation: "DAYS" as const } } },
  };

  it("keeps dates and filters while the book is the same", () => {
    const r = restoreSaved(defaults, { ...saved, asOf: "2026-09-30" }, dates, "2026-09-30");
    expect(r.global).toMatchObject({ referenceDate: "2026-09-14", comparisonDate: "2026-08-31", valueType: "BASE" });
    expect(r.pages.positions.values).toEqual({ comparisonDate: "2026-09-29", presentation: "DAYS" });
    expect(r.version).toBe(2);
  });

  it("a new book (other as-of date) opens on the new defaults but keeps the other filters", () => {
    const r = restoreSaved(defaults, { ...saved, asOf: "2026-08-31" }, dates, "2026-09-30");
    expect(r.global).toMatchObject({ referenceDate: "2026-09-30", comparisonDate: "2026-08-31", valueType: "BASE" });
    expect(r.global.referenceDate).toBe(defaults.referenceDate);
    expect(r.pages.positions.values).toEqual({ presentation: "DAYS" });
    expect(r.asOf).toBe("2026-09-30");
  });

  it("state saved before asOf existed resets the dates", () => {
    const r = restoreSaved(defaults, saved, dates, "2026-09-30");
    expect(r.global.referenceDate).toBe("2026-09-30");
  });

  it("dates that are no longer reporting dates fall back to the defaults", () => {
    const r = restoreSaved(defaults, { ...saved, asOf: "2026-09-30" }, ["2026-08-31", "2026-09-30"], "2026-09-30");
    expect(r.global.referenceDate).toBe("2026-09-30");
    expect(r.pages.positions.values).toEqual({ presentation: "DAYS" });
  });

  it("missing parts take the defaults", () => {
    expect(restoreSaved(defaults, {}, dates, "2026-09-30")).toEqual({ global: defaults, version: 0, pages: {}, asOf: "2026-09-30" });
  });
});
