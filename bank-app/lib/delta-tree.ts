/**
 * Pure helpers for DeltaGrid: build the LST/product hierarchy from flat
 * rows and check that children sum to their parent (UC2 control values).
 */

export interface FlatDeltaRow {
  /** Path from Level I down to the leaf, e.g. ["Outflows", "Deposits", "Retail"] */
  path: string[];
  /** Column key (time bucket code or ISO date) -> value in CHF */
  values: Record<string, number>;
  drillable?: boolean;
}

export interface DeltaNode {
  id: string;
  label: string;
  level: number;
  values: Record<string, number>;
  drillable: boolean;
  children?: DeltaNode[];
}

export function buildTree(rows: FlatDeltaRow[]): DeltaNode[] {
  const roots: DeltaNode[] = [];
  const index = new Map<string, DeltaNode>();
  for (const row of rows) {
    let siblings = roots;
    row.path.forEach((label, depth) => {
      const id = row.path.slice(0, depth + 1).join(" > ");
      let node = index.get(id);
      if (!node) {
        node = { id, label, level: depth + 1, values: {}, drillable: true };
        index.set(id, node);
        siblings.push(node);
      }
      for (const [col, v] of Object.entries(row.values)) node.values[col] = (node.values[col] ?? 0) + v;
      if (row.drillable === false) node.drillable = false;
      if (depth < row.path.length - 1) {
        node.children ??= [];
        siblings = node.children;
      }
    });
  }
  return roots;
}

export function columnTotals(nodes: DeltaNode[], columns: string[]): Record<string, number> {
  const out: Record<string, number> = {};
  for (const c of columns) out[c] = nodes.reduce((s, n) => s + (n.values[c] ?? 0), 0);
  return out;
}

/** True when any cell reaches the amount filter (|value| >= min, CHF). */
export function passesAmount(values: Record<string, number>, min: number): boolean {
  return min <= 0 || Object.values(values).some((v) => Math.abs(v) >= min);
}

/**
 * Global amount filter (prototype FilterBetrag): hides rows whose cells are all
 * below `min`. Values are not recomputed, so parents and control sums stay
 * unfiltered and still reconcile with the hidden rows.
 */
export function pruneByAmount(nodes: DeltaNode[], min: number): DeltaNode[] {
  if (Number.isNaN(min) || min <= 0) return nodes; // NaN from a missing filter value = no filter
  return nodes
    .filter((n) => passesAmount(n.values, min))
    .map((n) => (n.children ? { ...n, children: pruneByAmount(n.children, min) } : n));
}

/** Nodes whose children do not add up to them (tolerance in CHF). */
export function reconciliationBreaks(nodes: DeltaNode[], columns: string[], tol = 0.5): string[] {
  const breaks: string[] = [];
  const walk = (n: DeltaNode) => {
    if (!n.children?.length) return;
    const sums = columnTotals(n.children, columns);
    for (const c of columns) if (Math.abs((n.values[c] ?? 0) - sums[c]) > tol) breaks.push(`${n.id} @ ${c}`);
    n.children.forEach(walk);
  };
  nodes.forEach(walk);
  return breaks;
}
