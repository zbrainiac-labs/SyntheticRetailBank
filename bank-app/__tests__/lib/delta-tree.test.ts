import { describe, expect, it } from "vitest";
import { buildTree, columnTotals, passesAmount, pruneByAmount, reconciliationBreaks, type FlatDeltaRow } from "@/lib/delta-tree";

const rows: FlatDeltaRow[] = [
  { path: ["Outflows", "Deposits", "Retail"], values: { W1: -10, W2: -5 } },
  { path: ["Outflows", "Deposits", "Corporate"], values: { W1: -50, W2: 0 } },
  { path: ["Outflows", "Facilities", "Credit"], values: { W1: -2, W2: -2 } },
  { path: ["Deterministic Cash Flows", "Planning", "New lending"], values: { W1: 7 }, drillable: false },
];

describe("delta tree", () => {
  const tree = buildTree(rows);

  it("aggregates leaves into every parent level", () => {
    const outflows = tree.find((n) => n.label === "Outflows")!;
    expect(outflows.values).toEqual({ W1: -62, W2: -7 });
    expect(outflows.children!.find((n) => n.label === "Deposits")!.values).toEqual({ W1: -60, W2: -5 });
  });

  it("control sum over Level I equals the sum of all leaves", () => {
    expect(columnTotals(tree, ["W1", "W2"])).toEqual({ W1: -55, W2: -7 });
  });

  it("children reconcile to their parent", () => {
    expect(reconciliationBreaks(tree, ["W1", "W2"])).toEqual([]);
  });

  it("detects a broken parent", () => {
    tree[0].values.W1 += 1;
    expect(reconciliationBreaks(tree, ["W1"])).toEqual(["Outflows @ W1"]);
  });

  it("non-drillable positions propagate up", () => {
    expect(tree.find((n) => n.label === "Deterministic Cash Flows")!.drillable).toBe(false);
  });
});

describe("amount filter (prototype FilterBetrag)", () => {
  const tree = buildTree(rows);
  const labels = (ns: ReturnType<typeof buildTree>): string[] => ns.flatMap((n) => [n.label, ...labels(n.children ?? [])]);

  it("hides rows whose cells are all below the amount", () => {
    const shown = labels(pruneByAmount(tree, 6));
    expect(shown).toContain("Corporate");
    expect(shown).toContain("Retail");
    expect(shown).not.toContain("Credit");
    expect(shown).toContain("New lending");
  });

  it("keeps parent values and control sums unfiltered", () => {
    const shown = pruneByAmount(tree, 6);
    const out = shown.find((n) => n.label === "Outflows")!;
    expect(out.values.W1).toBe(-62);
    expect(columnTotals(tree, ["W1", "W2"])).toEqual({ W1: -55, W2: -7 });
  });

  it("is a no-op at zero", () => {
    expect(pruneByAmount(tree, 0)).toBe(tree);
    expect(passesAmount({ W1: 0 }, 0)).toBe(true);
    expect(passesAmount({ W1: -3, W2: 1 }, 3)).toBe(true);
    expect(passesAmount({ W1: -2.9 }, 3)).toBe(false);
  });
});
