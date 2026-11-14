import { aggregate, cellOf, sideKeys, totalOf } from "@/lib/dti/pivot";
import type { ComparePoint } from "@/lib/dti/types";
import { describe, expect, it } from "vitest";

const pt = (application: string | null, side: string | null, period: string, ref: number, comp: number, info = false): ComparePoint => ({
  position: "DET_FIX_LOANS_CORP",
  application,
  side,
  period,
  sort: 1,
  date: null,
  bucketRef: period,
  bucketComp: period,
  refFrom: null,
  refTo: null,
  compFrom: null,
  compTo: null,
  ref,
  comp,
  delta: info ? null : ref - comp,
  info,
});

describe("dti pivot", () => {
  const pivot = aggregate(
    [pt("KRE", "A", "W1", 10, 4), pt("KRE", "P", "W1", -3, -1), pt("KRE", "A", "M01", 5, 5), pt(null, null, "W1", -15, 0), pt("KRE", "A", "BEFORE_REF", 0, 9, true)],
    (p) => sideKeys(p.application ?? "CORR", p.side ?? null),
  );

  it("net equals Aktiva plus Passiva", () => {
    const n = cellOf(pivot, "KRE|N", "W1");
    expect(n.delta).toBe(cellOf(pivot, "KRE|A", "W1").delta + cellOf(pivot, "KRE|P", "W1").delta);
    expect(n).toEqual({ ref: 7, comp: 3, delta: 4 });
  });

  it("rows without side contribute to the net only, info rows not at all", () => {
    expect(cellOf(pivot, "CORR|N", "W1").delta).toBe(-15);
    expect(pivot.has("CORR|A")).toBe(false);
    expect(pivot.get("KRE|A")?.has("BEFORE_REF")).toBe(false);
  });

  it("control sum over periods", () => {
    expect(totalOf(pivot, "KRE|N", "ref")).toBe(12);
    expect(totalOf(pivot, "KRE|N", "delta")).toBe(4);
  });
});
