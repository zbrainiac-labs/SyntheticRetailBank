"use client";

import { CHART_THEME, formatTick } from "@/components/chart-utils";
import { DataTable, Panel } from "@/components/data/data-table";
import { fmtDeltaM, fmtM, monthLabel } from "@/components/finance/common";
import { num, type LoaderRow } from "@/lib/loaders/client";
import { Bar, BarChart, CartesianGrid, Cell, LabelList, ResponsiveContainer, XAxis, YAxis } from "recharts";

export type Metric = "NET_PROFIT" | "TOTAL_ASSETS";
const STEP_LABEL: Record<string, string> = {
  OPENING: "Last month",
  ENTITY_PERFORMANCE: "Entity performance",
  FX_EFFECT: "FX effect",
  IC_ELIMINATION: "IC eliminations",
  NCI: "NCI share",
  CLOSING: "This month",
};
/** short x-axis labels so the bars fit side by side */
const AXIS_LABEL: Record<string, string> = {
  OPENING: "Last month",
  ENTITY_PERFORMANCE: "Entities",
  FX_EFFECT: "FX",
  IC_ELIMINATION: "IC elim.",
  NCI: "NCI",
  CLOSING: "This month",
};
/** P&L lines of the profit composition, in statement order (intercompany lines net to 0 in the group) */
const PNL_STEPS: { fs: string; label: string; axis: string }[] = [
  { fs: "INTEREST_INCOME", label: "Interest income", axis: "Int. income" },
  { fs: "INTEREST_EXPENSE", label: "Interest expense", axis: "Int. expense" },
  { fs: "FEE_INCOME", label: "Fee & commission income", axis: "Fees" },
  { fs: "PERSONNEL", label: "Personnel expenses", axis: "Personnel" },
  { fs: "OTHER_OPEX", label: "Other operating expenses", axis: "Other opex" },
  { fs: "TAX", label: "Income taxes", axis: "Tax" },
];

interface Step {
  key: string;
  label: string;
  axis: string;
  amount: number;
  total?: boolean;
}

/** Bridge components of one metric, entity rows summed: OPENING + movements = CLOSING. */
export function bridgeSteps(rows: LoaderRow[], metric: Metric) {
  const m = rows.filter((r) => r.METRIC === metric);
  const total = (c: string) => m.filter((r) => r.COMPONENT === c).reduce((s, r) => s + num(r.AMOUNT_CHF), 0);
  const order = ["OPENING", "ENTITY_PERFORMANCE", "FX_EFFECT", "IC_ELIMINATION", ...(metric === "NET_PROFIT" ? ["NCI"] : []), "CLOSING"];
  return order.map((c) => ({ component: c, label: STEP_LABEL[c], amount: total(c) }));
}

/** Round tick step (1 / 2 / 5 x 10^n) for a value span: about five ticks. */
function niceStep(span: number): number {
  const raw = Math.max(span, 1) / 5;
  const mag = 10 ** Math.floor(Math.log10(raw));
  return ([1, 2, 5, 10].find((m) => m * mag >= raw) ?? 10) * mag;
}

/** Waterfall as range bars: totals from 0, movements from the running total; the axis zooms onto the movements. */
function Waterfall({ steps, digits, zoom, testId }: Readonly<{ steps: Step[]; digits: number; zoom: boolean; testId: string }>) {
  let run = 0;
  const chart = steps.map((s) => {
    const lo = s.total ? Math.min(0, s.amount) : Math.min(run, run + s.amount);
    const hi = s.total ? Math.max(0, s.amount) : Math.max(run, run + s.amount);
    run = s.total ? s.amount : run + s.amount;
    const fill = s.total ? "var(--chart-1)" : s.amount >= 0 ? "var(--success)" : "var(--destructive)";
    return { name: s.axis, range: [lo, hi], fill, value: s.amount, label: s.total ? fmtM(s.amount, digits) : fmtDeltaM(s.amount, digits) };
  });
  // zoom for the asset bridge: totals are cut at the bottom (a waterfall from 0 hides small changes);
  // bounds snap to a round step so the ticks read 6,000 / 6,100 / ... instead of 6,147.9
  const tops = chart.map((c) => c.range[1]);
  const lows = chart.filter((_, i) => !steps[i].total).map((c) => c.range[0]);
  const span = Math.max(...tops) - Math.min(...lows, ...tops);
  const step = niceStep(span);
  const domain: [number, number] | ["auto", "auto"] = zoom
    ? [Math.floor((Math.min(...lows, ...tops) - span * 0.5) / step) * step, Math.ceil((Math.max(...tops) + span * 0.25) / step) * step]
    : ["auto", "auto"];
  return (
    <div data-testid={testId}>
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={chart} margin={{ top: 24, right: 10, left: 0, bottom: 0 }}>
          <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" vertical={false} />
          <XAxis dataKey="name" tick={{ ...CHART_THEME.tick, fontSize: 11 }} interval={0} />
          <YAxis tick={CHART_THEME.tick} tickFormatter={formatTick} width={70} domain={domain} allowDataOverflow={zoom} />
          {/* values are printed on the bars; a tooltip would show the [low, high] range of the range bar */}
          <Bar dataKey="range" name="CHF" radius={[3, 3, 0, 0]}>
            {chart.map((c) => (
              <Cell key={c.name} fill={c.fill} />
            ))}
            <LabelList dataKey="label" position="top" className="fill-foreground text-[11px] tabular-nums" />
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function StepTable({ steps, digits, testId }: Readonly<{ steps: Step[]; digits: number; testId: string }>) {
  return (
    <table className="w-full text-sm" data-testid={testId}>
      <tbody>
        {steps.map((s) => (
          <tr key={s.key} className="border-b last:border-0">
            <td className={s.total ? "py-2 font-medium" : "py-2 pl-3"}>{s.label}</td>
            <td className="py-2 text-right tabular-nums">{s.total ? fmtM(s.amount, digits) : fmtDeltaM(s.amount, digits)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** "Entity performance" -> "entity performance", keeps "FX" / "IC" / "NCI" */
const lowerFirst = (s: string) => s.replace(/^[A-Z][a-z]/, (w) => w.toLowerCase());

export function Bridge({ rows, names, pnl }: Readonly<{ rows: LoaderRow[]; names: Record<string, string>; pnl: LoaderRow[] }>) {
  if (!rows.length) {
    return <p className="text-sm text-muted-foreground">No comparison month: this is the first reporting month.</p>;
  }
  const prev = monthLabel(rows[0]?.PREV_PERIOD_MONTH);

  // 1) asset bridge: last month -> entities -> FX -> IC eliminations -> this month
  const assets: Step[] = bridgeSteps(rows, "TOTAL_ASSETS").map((s) => ({
    key: s.component,
    label: s.label,
    axis: AXIS_LABEL[s.component],
    amount: s.amount,
    total: s.component === "OPENING" || s.component === "CLOSING",
  }));
  const opening = assets[0].amount;
  const closing = assets.at(-1)?.amount ?? 0;
  const moves = assets.filter((s) => !s.total && Math.abs(s.amount) >= Math.max(1e3, Math.abs(opening) * 0.001));

  // 2) profit composition of the month: P&L lines -> profit before NCI -> NCI -> profit to shareholders
  const line = (fs: string) => pnl.filter((r) => r.FS_LINE === fs).reduce((s, r) => s + num(r.AMOUNT_CHF), 0);
  const lines: Step[] = PNL_STEPS.map((p) => ({ key: p.fs, label: p.label, axis: p.axis, amount: line(p.fs) }));
  const beforeNci = lines.reduce((s, l) => s + l.amount, 0) + line("IC_INTEREST") + line("IC_SERVICE_FEE");
  const nci = line("NCI_SHARE");
  const profit: Step[] = [
    ...lines,
    { key: "BEFORE_NCI", label: "Net profit before NCI", axis: "Before NCI", amount: beforeNci, total: true },
    { key: "NCI", label: "NCI share", axis: "NCI", amount: nci },
    { key: "ATTRIB", label: "Profit to shareholders", axis: "Shareholders", amount: beforeNci + nci, total: true },
  ];
  const np = bridgeSteps(rows, "NET_PROFIT");
  const npDelta = (np.at(-1)?.amount ?? 0) - np[0].amount;

  const perEntity = Object.values(
    rows
      .filter((r) => r.METRIC === "TOTAL_ASSETS" && (r.COMPONENT === "ENTITY_PERFORMANCE" || r.COMPONENT === "FX_EFFECT"))
      .reduce<Record<string, LoaderRow>>((acc, r) => {
        const id = String(r.ENTITY_ID);
        const e = (acc[id] ??= { ENTITY: names[id] ?? id, ENTITY_ID: id, PERFORMANCE_CHF_M: 0, FX_EFFECT_CHF_M: 0, TOTAL_CHF_M: 0 });
        const v = num(r.AMOUNT_CHF) / 1e6;
        if (r.COMPONENT === "ENTITY_PERFORMANCE") e.PERFORMANCE_CHF_M = num(e.PERFORMANCE_CHF_M) + v;
        else e.FX_EFFECT_CHF_M = num(e.FX_EFFECT_CHF_M) + v;
        e.TOTAL_CHF_M = num(e.PERFORMANCE_CHF_M) + num(e.FX_EFFECT_CHF_M);
        return acc;
      }, {}),
  ).sort((a, b) => Math.abs(num(b.TOTAL_CHF_M)) - Math.abs(num(a.TOTAL_CHF_M)));

  return (
    <div className="space-y-4">
      <p className="text-sm text-muted-foreground" data-testid="bridge-summary">
        Group total assets moved from CHF {fmtM(opening)}M ({prev}) to CHF {fmtM(closing)}M ({fmtDeltaM(closing - opening)}M):{" "}
        {moves.length ? moves.map((s) => `${lowerFirst(s.label)} ${fmtDeltaM(s.amount)}M`).join(", ") : "no material movements"}.
      </p>
      <div className="grid gap-4 lg:grid-cols-5">
        <Panel className="lg:col-span-3" title={`Group total assets: ${prev} to this month`} description="CHF million; green = increase, red = decrease">
          <Waterfall steps={assets} digits={1} zoom testId="bridge-chart" />
        </Panel>
        <Panel className="lg:col-span-2" title="Bridge components" description="Opening + movements = closing">
          <StepTable steps={assets} digits={1} testId="bridge-table" />
        </Panel>
      </div>
      <Panel title="Contribution by entity" description="Change of total assets at constant FX rates and FX effect per entity (CHF million), largest first">
        <DataTable
          rows={perEntity}
          columns={[
            { key: "ENTITY", label: "Entity" },
            { key: "PERFORMANCE_CHF_M", label: "Performance", render: (r) => fmtDeltaM(num(r.PERFORMANCE_CHF_M) * 1e6) },
            { key: "FX_EFFECT_CHF_M", label: "FX effect", render: (r) => fmtDeltaM(num(r.FX_EFFECT_CHF_M) * 1e6) },
            { key: "TOTAL_CHF_M", label: "Total", render: (r) => fmtDeltaM(num(r.TOTAL_CHF_M) * 1e6) },
          ]}
        />
      </Panel>
      <p className="text-sm text-muted-foreground" data-testid="profit-summary">
        Profit to shareholders this month: CHF {fmtM(beforeNci + nci, 2)}M (net profit before NCI CHF {fmtM(beforeNci, 2)}M, NCI share CHF{" "}
        {fmtM(nci, 2)}M), {fmtDeltaM(npDelta, 2)}M vs {prev}.
      </p>
      <div className="grid gap-4 lg:grid-cols-5">
        <Panel className="lg:col-span-3" title="What makes up this month's profit" description="Group P&L of the month, CHF million; intercompany interest and fees net to zero">
          <Waterfall steps={profit} digits={2} zoom={false} testId="profit-chart" />
        </Panel>
        <Panel className="lg:col-span-2" title="Profit composition" description="P&L lines → before NCI → to shareholders">
          <StepTable steps={profit} digits={2} testId="profit-table" />
        </Panel>
      </div>
    </div>
  );
}
