/**
 * Pure aggregation for the column-pivot pages (Application, Product Groups,
 * Single Products): rows = periods, columns = groups such as applications,
 * product clusters or single products, each optionally split into A/P.
 */
import type { ComparePoint } from "@/lib/dti/types";

export interface Cell {
  ref: number;
  comp: number;
  delta: number;
}

export type Measure = "delta" | "ref" | "comp";

export const ZERO: Cell = { ref: 0, comp: 0, delta: 0 };

/** key -> period -> cell. `keysOf` returns every column key a point contributes to (e.g. leaf and its parents). */
export type Pivot = Map<string, Map<string, Cell>>;

export function aggregate(points: ComparePoint[], keysOf: (p: ComparePoint) => string[]): Pivot {
  const out: Pivot = new Map();
  for (const p of points) {
    if (p.info) continue;
    for (const key of keysOf(p)) {
      const byPeriod = out.get(key) ?? new Map<string, Cell>();
      const c = byPeriod.get(p.period) ?? { ...ZERO };
      c.ref += p.ref;
      c.comp += p.comp;
      c.delta += p.delta ?? 0;
      byPeriod.set(p.period, c);
      out.set(key, byPeriod);
    }
  }
  return out;
}

export function cellOf(pivot: Pivot, key: string, period: string): Cell {
  return pivot.get(key)?.get(period) ?? ZERO;
}

export function valueOf(c: Cell, m: Measure): number {
  if (m === "delta") return c.delta;
  return m === "ref" ? c.ref : c.comp;
}

/** Column labels of the pivot pages: balance side and measure. */
export const SIDE_LABEL = { N: "Netto", A: "Aktiva", P: "Passiva" } as const;
export const MEASURE_LABEL: Record<Measure, string> = { delta: "Δ", ref: "Reference", comp: "Comparison" };

/** Column total over all periods (control sum). */
export function totalOf(pivot: Pivot, key: string, m: Measure): number {
  let s = 0;
  for (const c of pivot.get(key)?.values() ?? []) s += valueOf(c, m);
  return s;
}

/** Side key helper: "<group>|N" net, "<group>|A" assets, "<group>|P" liabilities. */
export function sideKeys(group: string, side: string | null): string[] {
  return side === "A" || side === "P" ? [`${group}|N`, `${group}|${side}`] : [`${group}|N`];
}
