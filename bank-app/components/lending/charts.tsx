"use client";

import { ChartTooltip, CHART_COLORS, CHART_THEME, formatTick } from "@/components/chart-utils";
import { num, type LoaderRow } from "@/lib/loaders/client";
import { formatNumber } from "@/lib/format";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Pie,
  PieChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

/** Semantic colours for loan / risk statuses (Streamlit used a fixed colour map). */
export const STATUS_COLOR: Record<string, string> = {
  APPROVED: "var(--success)",
  UNDER_REVIEW: "var(--warning)",
  DECLINED: "var(--destructive)",
  LOW: "var(--success)",
  MEDIUM: "var(--warning)",
  HIGH: "var(--chart-4)",
  CRITICAL: "var(--destructive)",
};

const colorOf = (key: string, i: number) => STATUS_COLOR[key] ?? CHART_COLORS[i % CHART_COLORS.length];

/** Sum `value` per (x, series) and pivot into one row per x for stacked/grouped Recharts bars. */
export function pivot(rows: LoaderRow[], x: string, series: string, value: string, scale = 1) {
  const keys: string[] = [];
  const byX = new Map<string, Record<string, string | number>>();
  for (const r of rows) {
    const xv = String(r[x] ?? "n/a");
    const sv = String(r[series] ?? "n/a");
    if (!keys.includes(sv)) keys.push(sv);
    const row = byX.get(xv) ?? { [x]: xv };
    row[sv] = num(row[sv]) + num(r[value]) / scale;
    byX.set(xv, row);
  }
  return { data: [...byX.values()], keys };
}

/** Mean of `value` per group (pandas groupby().mean()). */
export function meanBy(rows: LoaderRow[], by: string, value: string) {
  const acc = new Map<string, { s: number; n: number }>();
  for (const r of rows) {
    const k = String(r[by] ?? "n/a");
    const a = acc.get(k) ?? { s: 0, n: 0 };
    a.s += num(r[value]);
    a.n += 1;
    acc.set(k, a);
  }
  return [...acc.entries()].map(([k, a]) => ({ [by]: k, [value]: a.n ? a.s / a.n : 0 }));
}

export function StackedBars({
  data,
  x,
  keys,
  stacked = true,
  height = 300,
  labels = {},
}: {
  data: Record<string, unknown>[];
  x: string;
  keys: string[];
  stacked?: boolean;
  height?: number;
  labels?: Record<string, string>;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data}>
        <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" vertical={false} />
        <XAxis dataKey={x} tick={CHART_THEME.tick} />
        <YAxis tick={CHART_THEME.tick} tickFormatter={formatTick} width={56} />
        <Tooltip content={<ChartTooltip />} />
        <Legend wrapperStyle={{ fontSize: 12 }} />
        {keys.map((k, i) => (
          <Bar key={k} dataKey={k} name={labels[k] ?? k} stackId={stacked ? "s" : undefined} fill={colorOf(labels[k] ?? k, i)} />
        ))}
      </BarChart>
    </ResponsiveContainer>
  );
}

/** Single-series bar; per-bar colour via `color(row)` (replaces plotly continuous colour scales). */
export function SimpleBars({
  data,
  x,
  y,
  name,
  color,
  height = 300,
  threshold,
  yFormat = formatTick,
}: {
  data: Record<string, unknown>[];
  x: string;
  y: string;
  name: string;
  color?: (row: Record<string, unknown>, i: number) => string;
  height?: number;
  threshold?: { y: number; label: string; color: string };
  yFormat?: (v: number) => string;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data}>
        <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" vertical={false} />
        <XAxis dataKey={x} tick={CHART_THEME.tick} />
        <YAxis
          tick={CHART_THEME.tick}
          tickFormatter={yFormat}
          width={56}
          domain={threshold ? [0, (max: number) => Math.ceil(Math.max(max, threshold.y) * 1.1)] : undefined}
        />
        <Tooltip content={<ChartTooltip />} />
        <Bar dataKey={y} name={name} fill="var(--chart-1)">
          {color && data.map((r, i) => <Cell key={i} fill={color(r, i)} />)}
        </Bar>
        {threshold && (
          <ReferenceLine
            y={threshold.y}
            stroke={threshold.color}
            strokeDasharray="4 4"
            label={{ value: threshold.label, position: "insideTopRight", fontSize: 11, fill: "var(--muted-foreground)" }}
          />
        )}
      </BarChart>
    </ResponsiveContainer>
  );
}

export function Donut({
  data,
  label,
  value,
  height = 300,
  hole = 0.5,
}: {
  data: Record<string, unknown>[];
  label: string;
  value: string;
  height?: number;
  hole?: number;
}) {
  const total = data.reduce((s, r) => s + num(r[value]), 0);
  const slices = data.map((r) => ({ name: String(r[label] ?? "n/a"), value: num(r[value]) }));
  return (
    <ResponsiveContainer width="100%" height={height}>
      <PieChart>
        <Pie
          data={slices}
          dataKey="value"
          nameKey="name"
          innerRadius={`${Math.round(hole * 80)}%`}
          outerRadius="80%"
          label={({ name, value: v }) => `${name} ${formatNumber((100 * num(v)) / (total || 1), 1)}%`}
          labelLine={false}
          fontSize={11}
        >
          {slices.map((s, i) => (
            <Cell key={s.name} fill={colorOf(s.name, i)} />
          ))}
        </Pie>
        <Tooltip content={<ChartTooltip />} />
        <Legend wrapperStyle={{ fontSize: 12 }} />
      </PieChart>
    </ResponsiveContainer>
  );
}

/** Sequential colour for a value in [min, max]: mixes chart-1 with the background (plotly "Blues"/"Greens"). */
export function seqColor(v: number, min: number, max: number, base = "var(--chart-1)") {
  const t = max > min ? (v - min) / (max - min) : 1;
  return `color-mix(in oklab, ${base} ${Math.round(35 + 65 * t)}%, transparent)`;
}

/** Diverging green -> red for "lower is better" values (plotly RdYlGn_r). */
export function riskColor(v: number, min: number, max: number) {
  const t = max > min ? (v - min) / (max - min) : 0;
  return `color-mix(in oklab, var(--destructive) ${Math.round(100 * t)}%, var(--success))`;
}
