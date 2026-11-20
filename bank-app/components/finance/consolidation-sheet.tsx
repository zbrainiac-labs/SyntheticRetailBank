"use client";

import { Panel } from "@/components/data/data-table";
import { fmtM, fmtRate, ILLUSTRATIVE_NOTE } from "@/components/finance/common";
import { CsvButton } from "@/components/risk/common";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { num, type LoaderRow } from "@/lib/loaders/client";
import { formatNumber } from "@/lib/format";
import { cn } from "@/lib/utils";
import { useMemo, useState } from "react";

export type Statement = "BS" | "PL";
export type CcyView = "CHF" | "LCY";

const GROUP_COLS = [
  { id: "SUM", label: "Σ Entities" },
  { id: "IC_ELIM", label: "IC Elim" },
  { id: "NCI", label: "NCI" },
  { id: "GROUP", label: "Group" },
] as const;

interface Line {
  key: string;
  label: string;
  fsLines: string[];
  subtotal?: boolean;
}

/** Statement layout: FS lines in sort order with section subtotals. */
function layout(rows: LoaderRow[], statement: Statement): Line[] {
  const lines = [...new Map(rows.filter((r) => r.STATEMENT === statement).map((r) => [String(r.FS_LINE), r])).values()].sort(
    (a, b) => num(a.SORT_ORDER) - num(b.SORT_ORDER),
  );
  const of = (sec: string, test: (r: LoaderRow) => boolean = () => true) =>
    lines.filter((r) => r.SECTION === sec && test(r)).map((r) => ({ key: String(r.FS_LINE), label: String(r.LABEL), fsLines: [String(r.FS_LINE)] }));
  const keys = (ls: Line[]) => ls.flatMap((l) => l.fsLines);
  if (statement === "BS") {
    const a = of("ASSETS");
    const l = of("LIABILITIES");
    const e = of("EQUITY");
    return [
      ...a, { key: "T_A", label: "Total assets", fsLines: keys(a), subtotal: true },
      ...l, { key: "T_L", label: "Total liabilities", fsLines: keys(l), subtotal: true },
      ...e, { key: "T_E", label: "Total equity", fsLines: keys(e), subtotal: true },
      { key: "T_LE", label: "Total liabilities & equity", fsLines: [...keys(l), ...keys(e)], subtotal: true },
    ];
  }
  const p = of("PNL", (r) => r.FS_LINE !== "NCI_SHARE");
  const nci = of("PNL", (r) => r.FS_LINE === "NCI_SHARE");
  return [
    ...p, { key: "T_NP", label: "Net profit before NCI", fsLines: keys(p), subtotal: true },
    ...nci, { key: "T_NPA", label: "Net profit attributable to shareholders", fsLines: [...keys(p), ...keys(nci)], subtotal: true },
  ];
}

export interface CellRef {
  line: Line;
  col: string;
}

export function ConsolidationSheet({
  rows,
  trail,
  entities,
  statement,
  ccy,
  period,
}: Readonly<{ rows: LoaderRow[]; trail: LoaderRow[]; entities: LoaderRow[]; statement: Statement; ccy: CcyView; period: string }>) {
  const [cell, setCell] = useState<CellRef | null>(null);
  const lines = useMemo(() => layout(rows, statement), [rows, statement]);
  const ents = entities.map((e) => ({ id: String(e.ENTITY_ID), name: String(e.DISPLAY_NAME), ccy: String(e.FUNCTIONAL_CCY) }));

  const chf = (fs: string[], col: string) =>
    rows.reduce((s, r) => {
      if (!fs.includes(String(r.FS_LINE))) return s;
      const c = String(r.COL);
      const hit = col === "SUM" ? c.startsWith("LE_") : c === col;
      return hit ? s + num(r.AMOUNT_CHF) : s;
    }, 0);
  const lcy = (fs: string[], entity: string) =>
    trail.reduce((s, r) => (r.ENTITY_ID === entity && fs.includes(String(r.FS_LINE)) ? s + num(r.AMOUNT_LCY) : s), 0);
  const value = (l: Line, col: string) => (ccy === "LCY" && col.startsWith("LE_") ? lcy(l.fsLines, col) : chf(l.fsLines, col));

  const csv = lines.map((l) => ({
    LINE: l.label,
    ...Object.fromEntries(ents.map((e) => [`${e.id}_${ccy === "LCY" ? e.ccy : "CHF"}`, value(l, e.id).toFixed(2)])),
    ...Object.fromEntries(GROUP_COLS.map((g) => [`${g.id}_CHF`, value(l, g.id).toFixed(2)])),
    NOTE: ILLUSTRATIVE_NOTE,
  }));

  return (
    <Panel
      title={statement === "BS" ? "Consolidated balance sheet" : "Consolidated P&L (month)"}
      description={
        ccy === "LCY"
          ? "Entity columns in functional currency (CHF, EUR, USD); Σ, eliminations, NCI and group in CHF million. Click any figure for its answer trail."
          : "CHF million. Group = Σ entities + IC eliminations + NCI. Click any figure for its answer trail."
      }
      actions={<CsvButton rows={csv} label="Export CSV" filename={`consolidation_${statement}_${period}`} />}
    >
      <div className="overflow-x-auto" data-testid="consolidation-sheet">
        <table className="w-full min-w-[1100px] text-sm">
          <thead>
            <tr className="border-b text-xs text-muted-foreground">
              <th className="py-2 pr-3 text-left font-medium">Line</th>
              {ents.map((e) => (
                <th key={e.id} className="px-2 py-2 text-right font-medium" title={e.id}>
                  <div>{e.name}</div>
                  <div className="font-normal">{ccy === "LCY" ? e.ccy : "CHF"}</div>
                </th>
              ))}
              {GROUP_COLS.map((g) => (
                <th key={g.id} className={cn("px-2 py-2 text-right font-medium", g.id === "GROUP" && "text-foreground")}>
                  <div>{g.label}</div>
                  <div className="font-normal">CHF</div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {lines.map((l) => (
              <tr key={l.key} className={cn("border-b last:border-0", l.subtotal && "bg-muted/40 font-medium")} data-line={l.key}>
                <td className={cn("py-1.5 pr-3", !l.subtotal && "pl-2")}>{l.label}</td>
                {[...ents.map((e) => e.id), ...GROUP_COLS.map((g) => g.id)].map((col) => {
                  const v = value(l, col);
                  return (
                    <td key={col} className={cn("px-2 py-1.5 text-right tabular-nums", col === "GROUP" && "font-semibold")}>
                      {l.subtotal ? (
                        fmtM(v)
                      ) : (
                        <button
                          type="button"
                          className="rounded px-1 hover:bg-accent hover:underline"
                          data-col={col}
                          onClick={() => setCell({ line: l, col })}
                        >
                          {fmtM(v)}
                        </button>
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <TrailDrawer cell={cell} onClose={() => setCell(null)} trail={trail} rows={rows} ents={ents} />
    </Panel>
  );
}

function Step({ n, title, children }: Readonly<{ n: number; title: string; children: React.ReactNode }>) {
  return (
    <li className="flex gap-3">
      <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-primary/10 text-xs font-semibold text-primary">{n}</span>
      <div className="space-y-0.5 pb-3">
        <p className="text-sm font-medium">{title}</p>
        <div className="text-sm text-muted-foreground">{children}</div>
      </div>
    </li>
  );
}

/** Answer trail: from the source positions over mapping, FX and eliminations to the group value. */
function TrailDrawer({
  cell,
  onClose,
  trail,
  rows,
  ents,
}: Readonly<{ cell: CellRef | null; onClose: () => void; trail: LoaderRow[]; rows: LoaderRow[]; ents: { id: string; name: string; ccy: string }[] }>) {
  const fs = cell?.line.fsLines[0] ?? "";
  const lineRows = trail.filter((r) => r.FS_LINE === fs);
  const group = rows.filter((r) => r.FS_LINE === fs && r.COL === "GROUP").reduce((s, r) => s + num(r.AMOUNT_CHF), 0);
  const entity = cell?.col.startsWith("LE_") ? lineRows.find((r) => r.ENTITY_ID === cell.col) : undefined;
  const name = ents.find((e) => e.id === cell?.col)?.name ?? cell?.col;
  return (
    <Sheet open={!!cell} onOpenChange={(o) => !o && onClose()}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-xl" data-testid="trail-drawer">
        <SheetHeader>
          <SheetTitle>Answer trail: {cell?.line.label}</SheetTitle>
          <SheetDescription>
            {entity ? `${name}, from source positions to the group value.` : `How the group value of CHF ${fmtM(group)}M is built from the entities.`}
          </SheetDescription>
        </SheetHeader>
        {entity ? (
          <ol className="mt-6">
            <Step n={1} title="Source">
              {num(entity.SOURCE_POSITIONS) > 0
                ? `${formatNumber(num(entity.SOURCE_POSITIONS), 0)} liquidity book positions held in ${entity.HOLDING_CCYS}`
                : "Derived line (no source positions)"}
            </Step>
            <Step n={2} title={num(entity.SOURCE_POSITIONS) > 0 ? "Mapping rule" : "Derivation rule"}>
              {String(entity.SOURCE_PRODUCTS ?? "")} → {cell?.line.label}
            </Step>
            <Step n={3} title="Entity amount (functional currency)">
              {String(entity.FUNCTIONAL_CCY)} {fmtM(entity.AMOUNT_LCY)}M
            </Step>
            <Step n={4} title="FX translation">
              {entity.FUNCTIONAL_CCY === "CHF"
                ? "CHF entity: no translation"
                : entity.RATE_TYPE === "RESIDUAL"
                  ? "Residual: net assets at closing rate − capital at historical rate − reserves at average rate"
                  : `${String(entity.RATE_TYPE).toLowerCase()} rate ${String(entity.FUNCTIONAL_CCY)}/CHF ${fmtRate(entity.TRANSLATION_RATE)}`}
              {" "}→ CHF {fmtM(entity.ENTITY_CHF)}M
            </Step>
            <Step n={5} title="Intercompany elimination">
              {num(entity.IC_ELIM_CHF) === 0 ? "None on this line" : `CHF ${fmtM(entity.IC_ELIM_CHF)}M eliminated against the counterparty entity`}
            </Step>
            <Step n={6} title="Non-controlling interest">
              {num(entity.NCI_CHF) === 0 ? "None (100 % owned or line not affected)" : `CHF ${fmtM(entity.NCI_CHF)}M reclassified to NCI`}
            </Step>
            <Step n={7} title="Contribution to the group">
              CHF {fmtM(entity.GROUP_CONTRIBUTION_CHF)}M of the group value CHF {fmtM(group)}M
            </Step>
          </ol>
        ) : (
          <table className="mt-6 w-full text-sm" data-testid="trail-group">
            <thead>
              <tr className="border-b text-xs text-muted-foreground">
                <th className="py-1 text-left font-medium">Entity</th>
                <th className="py-1 text-right font-medium">Local</th>
                <th className="py-1 text-right font-medium">CHF</th>
                <th className="py-1 text-right font-medium">IC Elim</th>
                <th className="py-1 text-right font-medium">NCI</th>
                <th className="py-1 text-right font-medium">To group</th>
              </tr>
            </thead>
            <tbody>
              {lineRows.map((r) => (
                <tr key={String(r.ENTITY_ID)} className="border-b">
                  <td className="py-1">{String(r.DISPLAY_NAME)}</td>
                  <td className="py-1 text-right tabular-nums">{r.AMOUNT_LCY === null ? "—" : `${String(r.FUNCTIONAL_CCY)} ${fmtM(r.AMOUNT_LCY)}`}</td>
                  <td className="py-1 text-right tabular-nums">{fmtM(r.ENTITY_CHF)}</td>
                  <td className="py-1 text-right tabular-nums">{fmtM(r.IC_ELIM_CHF)}</td>
                  <td className="py-1 text-right tabular-nums">{fmtM(r.NCI_CHF)}</td>
                  <td className="py-1 text-right tabular-nums">{fmtM(r.GROUP_CONTRIBUTION_CHF)}</td>
                </tr>
              ))}
              <tr className="font-semibold">
                <td className="py-1">Group</td>
                <td />
                <td colSpan={4} className="py-1 text-right tabular-nums" data-testid="trail-group-total">
                  {fmtM(lineRows.reduce((s, r) => s + num(r.GROUP_CONTRIBUTION_CHF), 0))}
                </td>
              </tr>
            </tbody>
          </table>
        )}
        <p className="mt-6 text-xs text-muted-foreground">{ILLUSTRATIVE_NOTE}</p>
      </SheetContent>
    </Sheet>
  );
}
