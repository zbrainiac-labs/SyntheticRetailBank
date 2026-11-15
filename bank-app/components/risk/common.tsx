"use client";

import { ChartTooltip, CHART_THEME } from "@/components/chart-utils";
import { StatCard } from "@/components/stat-card";
import { Button } from "@/components/ui/button";
import { num, type LoaderRow } from "@/lib/loaders/client";
import { formatNumber } from "@/lib/format";
import Link from "next/link";
import type React from "react";
import { useState } from "react";
import { Bar, BarChart, CartesianGrid, Cell, Legend, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

/** CUSTOMER_ID column that deep-links into the Customer 360 profile. */
export const CUSTOMER_LINK_COLUMN = {
  key: "CUSTOMER_ID",
  render: (r: LoaderRow) => (
    <Link
      href={`/banking/customer-360?customer=${encodeURIComponent(String(r.CUSTOMER_ID))}`}
      className="text-primary underline-offset-2 hover:underline"
    >
      {String(r.CUSTOMER_ID)}
    </Link>
  ),
};

/** KPI card that toggles a list filter (click again to clear). */
export function FilterCard({
  label,
  value,
  active,
  onToggle,
}: Readonly<{ label: string; value: React.ReactNode; active: boolean; onToggle: () => void }>) {
  return (
    <button
      type="button"
      aria-pressed={active}
      aria-label={`${label}: filter the list below`}
      onClick={onToggle}
      className="rounded-xl text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]"
    >
      <StatCard
        label={label}
        value={value}
        hint={active ? "Filtering list - click to clear" : "Click to filter list"}
        className={active ? "h-full ring-2 ring-[var(--primary)]" : "h-full hover:bg-muted/40"}
      />
    </button>
  );
}

/** "N <noun> (filter label) show all" line above a filtered list. */
export function FilterNote({ count, noun, filter, onClear }: Readonly<{ count: number; noun: string; filter?: string; onClear: () => void }>) {
  return (
    <p className="text-sm text-muted-foreground">
      <strong className="text-foreground">{formatNumber(count, 0)}</strong> {noun}
      {filter && (
        <>
          {" "}({filter}){" "}
          <button type="button" onClick={onClear} className="underline">
            show all
          </button>
        </>
      )}
    </p>
  );
}

/**
 * Card-driven filter over a row set: `defs` maps a filter id to its label and row predicate.
 * Returns the filtered rows plus card/note props for the active filter.
 */
export function useCardFilter<K extends string>(defs: Record<K, { label: string; test: (r: LoaderRow) => boolean }>) {
  const [active, setActive] = useState<K | null>(null);
  return {
    active,
    clear: () => setActive(null),
    label: active ? defs[active].label : undefined,
    apply: (rows: LoaderRow[]) => (active ? rows.filter(defs[active].test) : rows),
    card: (k: K, value: React.ReactNode) => (
      <FilterCard key={k} label={defs[k].label} value={value} active={active === k} onToggle={() => setActive((c) => (c === k ? null : k))} />
    ),
  };
}

/** Risk rating colours (Streamlit COLORS map, mapped to theme tokens). */
export const RISK_COLOR: Record<string, string> = {
  CRITICAL: "var(--destructive)",
  HIGH: "var(--warning)",
  MEDIUM: "var(--chart-3)",
  LOW: "var(--success)",
  NO_RISK: "var(--chart-1)",
};

/** Traffic-light level of a category label (match types, risk levels, anomaly classes). */
export function levelOf(v: string): "red" | "orange" | "yellow" | "green" {
  const s = v.toUpperCase();
  if (s === "EXACT_MATCH" || s.startsWith("CRITICAL")) return "red";
  if (s.startsWith("HIGH")) return "orange";
  if (s === "FUZZY_MATCH" || s.startsWith("MEDIUM") || s.startsWith("MODERATE")) return "yellow";
  return "green";
}
const LEVEL_COLOR = { red: "var(--destructive)", orange: "var(--warning)", yellow: "var(--chart-3)", green: "var(--success)" };

/** Replacement for the "icon label: **count**" st.write lists. */
export function CountList({ items, label, value }: { items: LoaderRow[]; label: string; value: string }) {
  return (
    <ul className="space-y-1.5 text-sm">
      {items.map((r) => (
        <li key={String(r[label])} className="flex items-center gap-2">
          <span className="size-2.5 rounded-full" style={{ background: LEVEL_COLOR[levelOf(String(r[label]))] }} />
          <span>{String(r[label])}</span>
          <span className="ml-auto font-semibold tabular-nums">{formatNumber(num(r[value]), 0)}</span>
        </li>
      ))}
    </ul>
  );
}

/** value_counts() over already-loaded rows */
export function countBy(rows: LoaderRow[], key: string): LoaderRow[] {
  const m = new Map<string, number>();
  for (const r of rows) m.set(String(r[key] ?? "—"), (m.get(String(r[key] ?? "—")) ?? 0) + 1);
  return [...m.entries()].sort((a, b) => b[1] - a[1]).map(([k, n]) => ({ [key]: k, COUNT: n }));
}

export function isTrue(v: unknown) {
  return v === true || v === "true" || v === 1 || v === "1";
}

/** Donut chart (px.pie hole=0.3) of risk ratings. */
export function RiskDonut({ rows }: { rows: LoaderRow[] }) {
  const data = rows.map((r) => ({ name: String(r.OVERALL_RISK_RATING), value: num(r.CUSTOMERS) }));
  return (
    <ResponsiveContainer width="100%" height={300}>
      <PieChart>
        <Pie data={data} dataKey="value" nameKey="name" innerRadius="35%" outerRadius="80%" paddingAngle={1}>
          {data.map((d) => (
            <Cell key={d.name} fill={RISK_COLOR[d.name] ?? "var(--muted-foreground)"} />
          ))}
        </Pie>
        <Tooltip content={<ChartTooltip />} />
        <Legend wrapperStyle={{ fontSize: 12 }} />
      </PieChart>
    </ResponsiveContainer>
  );
}

/** Horizontal bar of match-type counts (plot_sanctions/pep_screening_results). */
export function MatchTypeBars({ rows, keyName, color }: { rows: LoaderRow[]; keyName: string; color: string }) {
  const data = countBy(rows, keyName).map((r) => ({ name: String(r[keyName]), Matches: num(r.COUNT) }));
  return (
    <ResponsiveContainer width="100%" height={Math.max(160, data.length * 50 + 60)}>
      <BarChart data={data} layout="vertical" margin={{ left: 8, right: 16 }}>
        <CartesianGrid stroke={CHART_THEME.grid} horizontal={false} />
        <XAxis type="number" allowDecimals={false} tick={CHART_THEME.tick} />
        <YAxis type="category" dataKey="name" width={110} tick={CHART_THEME.tick} />
        <Tooltip content={<ChartTooltip />} />
        <Bar dataKey="Matches" fill={color} radius={[0, 4, 4, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}

/** st.download_button for a CSV export. */
export function CsvButton({ rows, label, filename }: { rows: LoaderRow[]; label: string; filename: string }) {
  const onClick = () => {
    const cols = Object.keys(rows[0] ?? {});
    const esc = (v: unknown) => {
      const s = v === null || v === undefined ? "" : String(v);
      return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    };
    const csv = [cols.join(","), ...rows.map((r) => cols.map((c) => esc(r[c])).join(","))].join("\n");
    const url = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
    const a = document.createElement("a");
    a.href = url;
    a.download = `${filename}_${new Date().toISOString().slice(0, 10).replace(/-/g, "")}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };
  return (
    <Button variant="outline" size="sm" onClick={onClick} disabled={!rows.length}>
      {label}
    </Button>
  );
}
