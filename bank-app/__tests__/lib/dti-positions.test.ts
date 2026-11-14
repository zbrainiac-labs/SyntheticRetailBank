import { buildTree, reconciliationBreaks } from "@/lib/delta-tree";
import { indexPositions, pathOf, periodColumns, thresholdFor, toDeltaRows } from "@/lib/dti/positions";
import type { ComparePoint, PositionRow } from "@/lib/dti/types";
import { describe, expect, it } from "vitest";

const pos = (c: string, n: string, parent: string | null, level: number, sort: number): PositionRow => ({
  POSITION_CODE: c,
  POSITION_NAME: n,
  PARENT_CODE: parent,
  LEVEL_NO: level,
  SORT_ORDER: sort,
  IS_LEAF: level === 3,
});

const POSITIONS = [
  pos("OUT", "Outflows", null, 1, 4),
  pos("POOL", "Collateral Pool", null, 1, 1),
  pos("OUT_DEP", "Deposit run-off", "OUT", 2, 1),
  pos("OUT_DEP_RETAIL", "Retail", "OUT_DEP", 3, 1),
  pos("OUT_DEP_CORP", "Corporate", "OUT_DEP", 3, 2),
  pos("POOL_CB", "Central bank", "POOL", 2, 1),
  pos("POOL_CB_BALANCE", "SNB balance", "POOL_CB", 3, 1),
];

const pt = (position: string, period: string, sort: number, ref: number, comp: number, info = false): ComparePoint => ({
  position,
  period,
  sort,
  date: null,
  bucketRef: period,
  bucketComp: period,
  refFrom: "2026-10-25",
  refTo: "2026-11-01",
  compFrom: "2026-10-24",
  compTo: "2026-10-31",
  ref,
  comp,
  delta: info ? null : ref - comp,
  info,
});

describe("dti positions", () => {
  const idx = indexPositions(POSITIONS);

  it("resolves the root-first path and the code behind a grid node", () => {
    expect(pathOf("OUT_DEP_CORP", idx.byCode).map((p) => p.POSITION_CODE)).toEqual(["OUT", "OUT_DEP", "OUT_DEP_CORP"]);
    expect(idx.codeById.get("Outflows > Deposit run-off")).toBe("OUT_DEP");
  });

  it("orders rows by the position tree and excludes info rows", () => {
    const rows = toDeltaRows(
      [pt("OUT_DEP_CORP", "W1", 1, -10, -4), pt("POOL_CB_BALANCE", "W1", 1, 100, 150), pt("OUT_DEP_RETAIL", "BEFORE_REF", -1, 0, 7, true)],
      idx,
      "delta",
    );
    expect(rows.map((r) => r.path[0])).toEqual(["Collateral Pool", "Outflows"]);
    expect(rows[0].values.W1).toBe(-50);
    expect(rows.some((r) => "BEFORE_REF" in r.values)).toBe(false);
  });

  it("parent rows reconcile with their leaves", () => {
    const nodes = buildTree(toDeltaRows([pt("OUT_DEP_CORP", "W1", 1, -10, -4), pt("OUT_DEP_RETAIL", "W1", 1, -3, -1)], idx, "ref"));
    expect(nodes[0].values.W1).toBe(-13);
    expect(reconciliationBreaks(nodes, ["W1"])).toEqual([]);
  });

  it("builds period columns in sort order with the reference range as sublabel", () => {
    const cols = periodColumns([pt("OUT_DEP_CORP", "M01", 4, 1, 1), pt("OUT_DEP_CORP", "W1", 1, 1, 1)], "BUCKETS");
    expect(cols.map((c) => c.key)).toEqual(["W1", "M01"]);
    expect(cols[0].sublabel).toBe("25.10. - 01.11.");
  });

  it("uses the entity threshold for one entity and the group threshold otherwise", () => {
    const t = { GROUP: 75e6, LE_02: 25e6 };
    expect(thresholdFor(["LE_02"], t)).toBe(25e6);
    expect(thresholdFor(["LE_01", "LE_02"], t)).toBe(75e6);
  });
});
