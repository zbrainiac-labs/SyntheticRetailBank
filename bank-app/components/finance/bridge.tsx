"use client";

import { ChartTooltip, CHART_THEME, formatTick } from "@/components/chart-utils";
import { DataTable, Panel } from "@/components/data/data-table";
import { fmtDeltaM, fmtM, monthLabel, Segmented } from "@/components/finance/common";
import { num, type LoaderRow } from "@/lib/loaders/client";
import { useMemo, useState } from "react";
import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

export type Metric = "NET_PROFIT" | "TOTAL_ASSETS";
const METRICS: { v: Metric; label: string }[] = [
  { v: "NET_PROFIT", label: "Net profit" },
  { v: "TOTAL_ASSETS", label: "Total assets" },
];
const STEP_LABEL: Record<string, string> = {
  OPENING: "Last month",
  ENTITY_PERFORMANCE: "Entity performance",
  FX_EFFECT: "FX effect",
  IC_ELIMINATION: "IC eliminations",
  NCI: "NCI share",
  CLOSING: "This month",
};
/** short x-axis labels so six bars fit side by side */
const AXIS_LABEL: Record<string, string> = {
  OPENING: "Last month",
  ENTITY_PERFORMANCE: "Entities",
  FX_EFFECT: "FX",
  IC_ELIMINATION: "IC elim.",
  NCI: "NCI",
  CLOSING: "This month",
};

/** Bridge components of one metric, entity rows summed: OPENING + movements = CLOSING. */
export function bridgeSteps(rows: LoaderRow[], metric: Metric) {
  const m = rows.filter((r) => r.METRIC === metric);
  const total = (c: string) => m.filter((r) => r.COMPONENT === c).reduce((s, r) => s + num(r.AMOUNT_CHF), 0);
  const order = ["OPENING", "ENTITY_PERFORMANCE", "FX_EFFECT", "IC_ELIMINATION", ...(metric === "NET_PROFIT" ? ["NCI"] : []), "CLOSING"];
  return order.map((c) => ({ component: c, label: STEP_LABEL[c], amount: total(c) }));
}

export function Bridge({ rows, names }: Readonly<{ rows: LoaderRow[]; names: Record<string, string> }>) {
  const [metric, setMetric] = useState<Metric>("TOTAL_ASSETS");
  const steps = useMemo(() => bridgeSteps(rows, metric), [rows, metric]);
  const prev = monthLabel(rows[0]?.PREV_PERIOD_MONTH);
  if (!rows.length) {
    return <p className="text-sm text-muted-foreground">No comparison month: this is the first reporting month.</p>;
  }

  // waterfall as range bars: totals from 0, movements from the running total
  let run = 0;
  const chart = steps.map((s) => {
    const isTotal = s.component === "OPENING" || s.component === "CLOSING";
    const lo = isTotal ? Math.min(0, s.amount) : Math.min(run, run + s.amount);
    const hi = isTotal ? Math.max(0, s.amount) : Math.max(run, run + s.amount);
    run = isTotal ? s.amount : run + s.amount;
    const fill = isTotal ? "var(--chart-1)" : s.amount >= 0 ? "var(--success)" : "var(--destructive)";
    return { name: AXIS_LABEL[s.component], range: [lo, hi], fill, value: s.amount };
  });

  const opening = steps[0].amount;
  const closing = steps.at(-1)?.amount ?? 0;
  // zoom the axis onto the movements: totals are cut at the bottom (a waterfall from 0 hides small changes)
  const tops = chart.map((c) => c.range[1]);
  const lows = chart.filter((_, i) => i > 0 && i < chart.length - 1).map((c) => c.range[0]);
  const span = Math.max(...tops) - Math.min(...lows, ...tops);
  const domain: [number, number] = [Math.min(...lows, ...tops) - span * 0.6, Math.max(...tops) + span * 0.2];
  const digits = metric === "NET_PROFIT" ? 2 : 1;
  const moves = steps.slice(1, -1).filter((s) => Math.abs(s.amount) >= Math.max(1e3, Math.abs(opening) * 0.001));
  const metricLabel = metric === "NET_PROFIT" ? "Group net profit (attributable)" : "Group total assets";

  const perEntity = Object.values(
    rows
      .filter((r) => r.METRIC === metric && (r.COMPONENT === "ENTITY_PERFORMANCE" || r.COMPONENT === "FX_EFFECT"))
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
      <div className="flex flex-wrap items-center justify-between gap-3">
        <Segmented label="Bridge metric" options={METRICS} value={metric} onChange={setMetric} />
        <p className="text-sm text-muted-foreground" data-testid="bridge-summary">
          {metricLabel} moved from CHF {fmtM(opening, digits)}M ({prev}) to CHF {fmtM(closing, digits)}M ({fmtDeltaM(closing - opening, digits)}M):{" "}
          {moves.length
            ? moves.map((s) => `${s.label.replace(/^[A-Z][a-z]/, (w) => w.toLowerCase())} ${fmtDeltaM(s.amount, digits)}M`).join(", ")
            : "no material movements"}.
        </p>
      </div>
      <div className="grid gap-4 lg:grid-cols-5">
        <Panel className="lg:col-span-3" title={`${metricLabel}: ${prev} to this month`} description="CHF million; green = increase, red = decrease">
          <div data-testid="bridge-chart">
            <ResponsiveContainer width="100%" height={340}>
              <BarChart data={chart} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="name" tick={{ ...CHART_THEME.tick, fontSize: 11 }} interval={0} />
                <YAxis tick={CHART_THEME.tick} tickFormatter={formatTick} width={70} domain={domain} allowDataOverflow />
                <Tooltip content={<ChartTooltip />} formatter={(_v, _n, p) => `CHF ${fmtDeltaM(p.payload.value, digits)}M`} />
                <Bar dataKey="range" name="CHF" radius={[3, 3, 0, 0]}>
                  {chart.map((c) => (
                    <Cell key={c.name} fill={c.fill} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Panel>
        <Panel className="lg:col-span-2" title="Bridge components" description="Opening + movements = closing">
          <table className="w-full text-sm" data-testid="bridge-table">
            <tbody>
              {steps.map((s) => (
                <tr key={s.component} className="border-b last:border-0">
                  <td className={s.component === "OPENING" || s.component === "CLOSING" ? "py-2 font-medium" : "py-2 pl-3"}>{s.label}</td>
                  <td className="py-2 text-right tabular-nums">
                    {s.component === "OPENING" || s.component === "CLOSING" ? fmtM(s.amount, digits) : fmtDeltaM(s.amount, digits)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Panel>
      </div>
      <Panel title="Contribution by entity" description="Change at constant FX rates and FX effect per entity (CHF million), largest first">
        <DataTable
          rows={perEntity}
          columns={[
            { key: "ENTITY", label: "Entity" },
            { key: "PERFORMANCE_CHF_M", label: "Performance", render: (r) => fmtDeltaM(num(r.PERFORMANCE_CHF_M) * 1e6, digits) },
            { key: "FX_EFFECT_CHF_M", label: "FX effect", render: (r) => fmtDeltaM(num(r.FX_EFFECT_CHF_M) * 1e6, digits) },
            { key: "TOTAL_CHF_M", label: "Total", render: (r) => fmtDeltaM(num(r.TOTAL_CHF_M) * 1e6, digits) },
          ]}
        />
      </Panel>
    </div>
  );
}
