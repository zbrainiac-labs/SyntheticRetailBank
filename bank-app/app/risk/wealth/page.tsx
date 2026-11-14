"use client";

import { ChartTooltip, CHART_COLORS, CHART_THEME, formatTick } from "@/components/chart-utils";
import { DataTable, LoaderState, Panel, Tabs } from "@/components/data/data-table";
import { seqColor } from "@/components/lending/charts";
import { PageHeader } from "@/components/page-header";
import { StatCard } from "@/components/stat-card";
import { num, useLoader, type LoaderRow } from "@/lib/loaders/client";
import { formatNumber } from "@/lib/format";
import { useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";

const TABS = [
  { v: "portfolio", label: "Portfolio Overview" },
  { v: "advisor", label: "Advisor Management" },
] as const;
type Tab = (typeof TABS)[number]["v"];

const WORKLOAD_COLOR: Record<string, string> = {
  AVAILABLE: "var(--success)",
  LOW_LOAD: "var(--chart-1)",
  NORMAL: "var(--chart-2)",
  HIGH_LOAD: "var(--warning)",
  AT_CAPACITY: "var(--destructive)",
};

/** Capacity KPIs shared by both tabs (Streamlit computed them from load_advisor_capacity). */
/** Capacity scatter hover: advisor name, clients and portfolio value (Streamlit hover_name=ADVISOR_NAME). */
function AdvisorTooltip({ active, payload }: Readonly<{ active?: boolean; payload?: { payload: { advisor: string; x: number; y: number } }[] }>) {
  const p = active ? payload?.[0]?.payload : undefined;
  if (!p) return null;
  return (
    <div className="rounded-md border bg-popover px-3 py-2 text-xs shadow-sm">
      <p className="font-medium">{p.advisor}</p>
      <p>Clients: {formatNumber(p.x, 0)}</p>
      <p>Portfolio value: CHF {formatNumber(p.y, 0)}</p>
    </div>
  );
}

function capacityKpis(rows: LoaderRow[]) {
  const n = rows.length;
  return [
    { label: "Available", value: formatNumber(rows.filter((r) => r.WORKLOAD_STATUS === "AVAILABLE").length, 0) },
    { label: "At Capacity", value: formatNumber(rows.filter((r) => r.WORKLOAD_STATUS === "AT_CAPACITY").length, 0) },
    { label: "Total Slots", value: formatNumber(rows.reduce((s, r) => s + num(r.AVAILABLE_CAPACITY), 0), 0) },
    { label: "Avg Util.", value: `${formatNumber(n ? rows.reduce((s, r) => s + num(r.CAPACITY_UTILIZATION_PCT), 0) / n : 0, 0)}%` },
  ];
}

export default function WealthPage() {
  const [tab, setTab] = useState<Tab>("portfolio");
  const advisors = useLoader("wealth.advisors");
  return (
    <div className="space-y-6">
      <PageHeader eyebrow="Risk" title="Wealth Management" description="Advisors, client portfolios and capacity" />
      <Tabs tabs={[...TABS]} value={tab} onChange={setTab} />
      <LoaderState q={advisors} empty="Advisor data not available. Check EMPA_AGG_VW_ADVISOR_PERFORMANCE_ENRICHED.">
        {(rows) => (tab === "portfolio" ? <Portfolio rows={rows} /> : <Advisors rows={rows} />)}
      </LoaderState>
    </div>
  );
}

function Portfolio({ rows }: { rows: LoaderRow[] }) {
  const aum = rows.reduce((s, r) => s + num(r.TOTAL_PORTFOLIO_VALUE), 0);
  const clients = rows.reduce((s, r) => s + num(r.TOTAL_CLIENTS), 0);
  const kpis = [
    { label: "Total Advisors", value: formatNumber(rows.length, 0) },
    { label: "Total AUM", value: `CHF ${formatNumber(aum / 1e6, 0)}M` },
    { label: "Total Clients", value: formatNumber(clients, 0) },
    { label: "Avg AUM/Client", value: `CHF ${formatNumber(aum / Math.max(clients, 1) / 1e3, 0)}K` },
    ...capacityKpis(rows),
  ];
  const top = [...rows]
    .sort((a, b) => num(b.TOTAL_PORTFOLIO_VALUE) - num(a.TOTAL_PORTFOLIO_VALUE))
    .slice(0, 20)
    .map((r) => ({ name: `${r.ADVISOR_NAME} (${r.EMPLOYEE_ID})`, AUM: num(r.TOTAL_PORTFOLIO_VALUE) }));
  const [lo, hi] = [Math.min(...top.map((t) => t.AUM)), Math.max(...top.map((t) => t.AUM))];
  const statuses = [...new Set(rows.map((r) => String(r.WORKLOAD_STATUS ?? "n/a")))];
  return (
    <div className="space-y-4">
      <div className="grid gap-4 sm:grid-cols-4 xl:grid-cols-8" data-testid="wealth-kpis">
        {kpis.map((k) => (
          <StatCard key={k.label} label={k.label} value={k.value} />
        ))}
      </div>
      <div className="grid gap-4 lg:grid-cols-2">
        <Panel title="Top 20 Advisors by AUM" description="Total portfolio value (CHF)">
          <ResponsiveContainer width="100%" height={500}>
            <BarChart data={top} layout="vertical" margin={{ left: 20 }}>
              <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" horizontal={false} />
              <XAxis type="number" tick={CHART_THEME.tick} tickFormatter={formatTick} />
              <YAxis type="category" dataKey="name" tick={{ ...CHART_THEME.tick, fontSize: 10 }} width={130} interval={0} />
              <Tooltip content={<ChartTooltip />} />
              <Bar dataKey="AUM" name="AUM (CHF)">
                {top.map((t) => (
                  <Cell key={t.name} fill={seqColor(t.AUM, lo, hi, "var(--success)")} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </Panel>
        <Panel title="Advisor Capacity Analysis" description="Clients vs. portfolio value; bubble size = |AUM|, colour = workload status">
          <ResponsiveContainer width="100%" height={500}>
            <ScatterChart>
              <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" />
              <XAxis type="number" dataKey="x" name="Clients" tick={CHART_THEME.tick} />
              <YAxis type="number" dataKey="y" name="Portfolio value" tick={CHART_THEME.tick} tickFormatter={formatTick} width={60} />
              <ZAxis type="number" dataKey="z" range={[40, 600]} name="|AUM|" />
              <Tooltip content={<AdvisorTooltip />} />
              <Legend wrapperStyle={{ fontSize: 12 }} />
              {statuses.map((s, i) => (
                <Scatter
                  key={s}
                  name={s}
                  fill={WORKLOAD_COLOR[s] ?? CHART_COLORS[i % CHART_COLORS.length]}
                  fillOpacity={0.7}
                  data={rows
                    .filter((r) => String(r.WORKLOAD_STATUS ?? "n/a") === s)
                    .map((r) => ({
                      x: num(r.TOTAL_CLIENTS),
                      y: num(r.TOTAL_PORTFOLIO_VALUE),
                      z: Math.abs(num(r.TOTAL_PORTFOLIO_VALUE)),
                      advisor: String(r.ADVISOR_NAME ?? r.EMPLOYEE_ID),
                    }))}
                />
              ))}
            </ScatterChart>
          </ResponsiveContainer>
        </Panel>
      </div>
      <Panel title="Advisors">
        <DataTable rows={rows} columns={["EMPLOYEE_ID", "ADVISOR_NAME", "TOTAL_CLIENTS", "TOTAL_PORTFOLIO_VALUE", "PERFORMANCE_RATING", "REGION"]} />
      </Panel>
    </div>
  );
}

function Advisors({ rows }: { rows: LoaderRow[] }) {
  const team = useLoader("wealth.team");
  const kpis = [{ label: "Total Advisors", value: formatNumber(rows.length, 0) }, ...capacityKpis(rows)].map((k) =>
    k.label === "Available" ? { ...k, label: "Available Capacity" } : k,
  );
  return (
    <div className="space-y-4">
      <div className="grid gap-4 sm:grid-cols-3 lg:grid-cols-5">
        {kpis.map((k) => (
          <StatCard key={k.label} label={k.label} value={k.value} />
        ))}
      </div>
      <Panel title="Advisor capacity">
        <DataTable
          rows={rows}
          columns={[
            "EMPLOYEE_ID",
            "ADVISOR_NAME",
            "TOTAL_CLIENTS",
            "AVAILABLE_CAPACITY",
            "WORKLOAD_STATUS",
            "CAPACITY_UTILIZATION_PCT",
            "TOTAL_PORTFOLIO_VALUE",
            "REGION",
            "COUNTRY",
            "HIGH_RISK_CLIENTS",
            "PERFORMANCE_RATING",
          ]}
        />
      </Panel>
      {(team.data?.rows.length ?? 0) > 0 && (
        <Panel title="Team Performance">
          <DataTable rows={team.data!.rows} />
        </Panel>
      )}
    </div>
  );
}
