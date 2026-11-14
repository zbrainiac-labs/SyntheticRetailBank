import { describe, expect, it } from "vitest";
import {
  activeOverrides,
  defaultFilters,
  effectiveFilters,
  entityLabel,
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
