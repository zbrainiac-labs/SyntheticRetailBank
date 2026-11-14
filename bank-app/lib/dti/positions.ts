/**
 * LST position tree helpers (pure): name paths for DeltaGrid rows, the code
 * behind a grid node, and the period columns of a comparison result.
 */
import type { FlatDeltaRow } from "@/lib/delta-tree";
import type { ComparePoint, PositionRow } from "@/lib/dti/types";

export interface PositionIndex {
  byCode: Map<string, PositionRow>;
  /** DeltaGrid node id ("Outflows > Deposit run-off") -> position code */
  codeById: Map<string, string>;
}

export function indexPositions(positions: PositionRow[]): PositionIndex {
  const byCode = new Map(positions.map((p) => [p.POSITION_CODE, p]));
  const codeById = new Map<string, string>();
  for (const p of positions) codeById.set(pathOf(p.POSITION_CODE, byCode).map((x) => x.POSITION_NAME).join(" > "), p.POSITION_CODE);
  return { byCode, codeById };
}

/** Root-first chain of positions down to `code`. */
export function pathOf(code: string, byCode: Map<string, PositionRow>): PositionRow[] {
  const out: PositionRow[] = [];
  let cur = byCode.get(code);
  while (cur) {
    out.unshift(cur);
    cur = cur.PARENT_CODE ? byCode.get(cur.PARENT_CODE) : undefined;
  }
  return out;
}

/** Level I name of a position (Collateral Pool, Deterministic Cash Flows, Inflows, Outflows). */
export function levelOneName(code: string, idx: PositionIndex): string {
  return pathOf(code, idx.byCode)[0]?.POSITION_NAME ?? code;
}

export type Measure = "delta" | "ref" | "comp";

function measureValue(pt: ComparePoint, measure: Measure): number {
  if (measure === "delta") return pt.delta ?? 0;
  return measure === "ref" ? pt.ref : pt.comp;
}

/** ISO yyyy-mm-dd -> "DD.MM." */
function dayMonth(iso: string): string {
  return `${iso.slice(8, 10)}.${iso.slice(5, 7)}.`;
}

/** Second header line: the business day in DAYS view, else the reference date range of the bucket. */
function periodSublabel(pt: ComparePoint, view: "BUCKETS" | "DAYS"): string | undefined {
  if (view === "DAYS") return pt.bucketRef ?? undefined;
  if (!pt.refFrom || !pt.refTo) return undefined;
  return `${dayMonth(pt.refFrom)} - ${dayMonth(pt.refTo)}`;
}

/**
 * Grid rows in position-tree order: one row per leaf with its values per
 * period. Info rows (comparison flows before the reference date) are excluded.
 */
export function toDeltaRows(points: ComparePoint[], idx: PositionIndex, measure: Measure): FlatDeltaRow[] {
  const byLeaf = new Map<string, Record<string, number>>();
  for (const pt of points) {
    if (pt.info) continue;
    const v = measureValue(pt, measure);
    const values = byLeaf.get(pt.position) ?? {};
    values[pt.period] = (values[pt.period] ?? 0) + v;
    byLeaf.set(pt.position, values);
  }
  const sortKey = (code: string) =>
    pathOf(code, idx.byCode)
      .map((p) => String(p.SORT_ORDER).padStart(3, "0"))
      .join(".");
  return [...byLeaf.entries()]
    .sort(([a], [b]) => sortKey(a).localeCompare(sortKey(b)))
    .map(([code, values]) => ({ path: pathOf(code, idx.byCode).map((p) => p.POSITION_NAME), values }));
}

export interface PeriodColumn {
  key: string;
  label: string;
  sublabel?: string;
  refFrom: string | null;
  refTo: string | null;
  compFrom: string | null;
  compTo: string | null;
  bucketRef: string | null;
  bucketComp: string | null;
}

/** Distinct periods in display order; the ranges are what the deal level drills into. */
export function periodColumns(points: ComparePoint[], view: "BUCKETS" | "DAYS"): PeriodColumn[] {
  const seen = new Map<string, ComparePoint & { sortKey: number }>();
  for (const pt of points) if (!pt.info && !seen.has(pt.period)) seen.set(pt.period, { ...pt, sortKey: pt.sort });
  return [...seen.values()]
    .sort((a, b) => a.sortKey - b.sortKey)
    .map((pt) => ({
      key: pt.period,
      label: view === "DAYS" ? pt.period.slice(5) : pt.period,
      sublabel: periodSublabel(pt, view),
      refFrom: pt.refFrom,
      refTo: pt.refTo,
      compFrom: pt.compFrom,
      compTo: pt.compTo,
      bucketRef: pt.bucketRef,
      bucketComp: pt.bucketComp,
    }));
}

/** Comment threshold: entity limit for one entity, group limit for several. */
export function thresholdFor(entities: string[], thresholds: Record<string, number>): number | undefined {
  return entities.length === 1 ? thresholds[entities[0]] : thresholds.GROUP;
}
