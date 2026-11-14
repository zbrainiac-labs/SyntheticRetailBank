"use client";

import { ChartTooltip, CHART_THEME, formatTick } from "@/components/chart-utils";
import { num, type LoaderRow } from "@/lib/loaders/client";
import { cn } from "@/lib/utils";
import type React from "react";
import { Bar, BarChart, CartesianGrid, LabelList, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

/** Fixed-height ResponsiveContainer. */
export function Chart({ height = 360, children }: { height?: number; children: React.ReactElement }) {
  return (
    <div style={{ height }}>
      <ResponsiveContainer width="100%" height="100%">
        {children}
      </ResponsiveContainer>
    </div>
  );
}

/** Ranked horizontal bar chart (plotly px.bar orientation='h' with value labels). */
export function HBarChart({
  rows,
  category,
  value,
  name,
  color = "var(--chart-1)",
  height,
}: {
  rows: LoaderRow[];
  category: string;
  value: string;
  name: string;
  color?: string;
  height?: number;
}) {
  const data = rows.map((r) => ({ cat: String(r[category] ?? "(none)"), v: num(r[value]) }));
  return (
    <Chart height={height ?? Math.max(240, data.length * 30 + 40)}>
      <BarChart data={data} layout="vertical" margin={{ left: 8, right: 48 }}>
        <CartesianGrid stroke={CHART_THEME.grid} horizontal={false} />
        <XAxis type="number" tick={CHART_THEME.tick} tickFormatter={formatTick} />
        <YAxis type="category" dataKey="cat" tick={CHART_THEME.tick} width={110} />
        <Tooltip content={<ChartTooltip />} cursor={{ fill: "var(--muted)" }} />
        <Bar dataKey="v" name={name} fill={color} radius={[0, 4, 4, 0]}>
          <LabelList dataKey="v" position="right" fontSize={11} fill="var(--muted-foreground)" />
        </Bar>
      </BarChart>
    </Chart>
  );
}

/** Label / value pair of a profile section (st.write("**Label:**", value)). */
export function Field({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="min-w-0">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="truncate text-sm font-medium">{value === null || value === undefined || value === "" ? "—" : value}</dd>
    </div>
  );
}

/** Coloured callout (st.info / st.warning / st.error / st.success). */
export function Callout({
  tone,
  title,
  children,
}: {
  tone: "info" | "warning" | "error" | "success";
  title?: React.ReactNode;
  children?: React.ReactNode;
}) {
  const color = { info: "var(--chart-1)", warning: "var(--warning)", error: "var(--destructive)", success: "var(--success)" }[tone];
  // errors interrupt (alert); other tones are announced politely without a status role
  const live = tone === "error" ? { role: "alert" as const } : { "aria-live": "polite" as const };
  return (
    <div
      {...live}
      className={cn("rounded-md border-l-4 bg-muted/40 px-4 py-3 text-sm")}
      style={{ borderLeftColor: color }}
    >
      {title && <p className="font-medium">{title}</p>}
      {children && <div className="text-muted-foreground">{children}</div>}
    </div>
  );
}

export const SELECT_CLS = "h-9 rounded-md border bg-background px-2 text-sm";
