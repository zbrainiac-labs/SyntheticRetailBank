"use client";

import { Callout, Chart, SELECT_CLS } from "@/components/banking/charts";
import { ChartTooltip, CHART_THEME } from "@/components/chart-utils";
import { DataTable, LoaderState, Panel } from "@/components/data/data-table";
import { EmptyState } from "@/components/empty-state";
import { QueryView } from "@/components/query-view";
import { PageHeader } from "@/components/page-header";
import { StatCard, StatCardSkeleton } from "@/components/stat-card";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { num, useLoader, type LoaderRow } from "@/lib/loaders/client";
import { formatNumber } from "@/lib/format";
import Link from "next/link";
import { useState } from "react";
import { Bar, BarChart, CartesianGrid, Cell, LabelList, Legend, Pie, PieChart, ReferenceLine, Tooltip, XAxis, YAxis } from "recharts";

const STAGE_COLOR: Record<string, string> = {
  NEW: "var(--success)",
  ACTIVE: "var(--chart-1)",
  MATURE: "var(--chart-3)",
  DECLINING: "var(--warning)",
  DORMANT: "var(--muted-foreground)",
  CHURNED: "var(--destructive)",
};

const PREMIUM_COLS = [
  "CUSTOMER_ID",
  "FIRST_NAME",
  "FAMILY_NAME",
  "ACCOUNT_TIER",
  "COUNTRY",
  "CHURN_PROBABILITY",
  "DAYS_SINCE_LAST_TRANSACTION",
  "EMAIL",
  "PHONE",
];

function downloadCsv(rows: LoaderRow[], file: string) {
  const cols = Object.keys(rows[0] ?? {});
  const esc = (v: unknown) => {
    const s = v === null || v === undefined ? "" : String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const csv = [cols.join(","), ...rows.map((r) => cols.map((c) => esc(r[c])).join(","))].join("\n");
  const url = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
  const a = document.createElement("a");
  a.href = url;
  a.download = file;
  a.click();
  URL.revokeObjectURL(url);
}

/** show_profile_picker: pick a customer and open it on Customer 360. */
function ProfilePicker({ rows }: { rows: LoaderRow[] }) {
  const [id, setId] = useState("");
  return (
    <div className="flex flex-wrap items-center gap-2">
      <select aria-label="Customer profile" value={id} onChange={(e) => setId(e.target.value)} className={`${SELECT_CLS} min-w-72`}>
        <option value="">Select a customer to view profile...</option>
        {rows.map((r) => (
          <option key={String(r.CUSTOMER_ID)} value={String(r.CUSTOMER_ID)}>
            {String(r.CUSTOMER_ID)}
          </option>
        ))}
      </select>
      {id ? (
        <Button asChild size="sm">
          <Link href={`/banking/customer-360?customer=${encodeURIComponent(id)}`}>View profile</Link>
        </Button>
      ) : (
        <Button size="sm" disabled>
          View profile
        </Button>
      )}
    </div>
  );
}

function Histogram({ rows, width, color, lines, xLabel }: { rows: LoaderRow[]; width: number; color: string; lines: { x: number; label: string; color: string }[]; xLabel: string }) {
  const data = rows.map((r) => ({ x: num(r.BIN_START) + width / 2, v: num(r.CUSTOMERS) }));
  const max = Math.max(...data.map((d) => d.x + width / 2), ...lines.map((l) => l.x));
  const min = Math.min(...data.map((d) => d.x - width / 2), ...lines.map((l) => l.x));
  return (
    <Chart height={360}>
      <BarChart data={data} barCategoryGap={1}>
        <CartesianGrid stroke={CHART_THEME.grid} vertical={false} />
        <XAxis type="number" dataKey="x" domain={[min, max]} tick={CHART_THEME.tick} label={{ value: xLabel, position: "insideBottom", offset: -2, fontSize: 11 }} height={40} />
        <YAxis tick={CHART_THEME.tick} width={48} />
        <Tooltip content={<ChartTooltip />} labelFormatter={(x) => `${formatNumber(Number(x) - width / 2, 0)} - ${formatNumber(Number(x) + width / 2, 0)}`} />
        <Bar dataKey="v" name="Customers" fill={color} />
        {lines.map((l) => (
          <ReferenceLine key={l.x} x={l.x} stroke={l.color} strokeDasharray="4 3" label={{ value: l.label, position: "top", fontSize: 11, fill: l.color }} />
        ))}
      </BarChart>
    </Chart>
  );
}

function ChurnKpis({ k }: Readonly<{ k: Record<string, unknown> | undefined }>) {
  if (!k) return [0, 1, 2, 3].map((i) => <StatCardSkeleton key={i} />);
  return (
    <>
      <StatCard label="Total Customers" value={formatNumber(num(k.TOTAL_CUSTOMERS))} />
      <StatCard label="At-Risk (>70%)" value={formatNumber(num(k.AT_RISK_CUSTOMERS))} hint={`${formatNumber(num(k.CRITICAL_CUSTOMERS))} critical (>90%)`} />
      <StatCard label="Revenue at Risk" value={`CHF ${formatNumber(num(k.TOTAL_REVENUE_AT_RISK), 0)}K`} hint="Total balance of at-risk customers" />
      <StatCard label="Dormant (>180d)" value={formatNumber(num(k.DORMANT_180))} />
    </>
  );
}

/** Churn & Lifecycle - port of the_bank_app/pages/churn_lifecycle.py. */
export default function ChurnLifecyclePage() {
  const kpis = useLoader("churn.kpis");
  const stages = useLoader("churn.stage-summary");
  const hist = useLoader("churn.probability-histogram");
  const byTier = useLoader("churn.by-tier");
  const premium = useLoader("churn.premium-at-risk");
  const dormStats = useLoader("churn.dormant-stats");
  const dormHist = useLoader("churn.dormant-histogram");
  const k = kpis.data?.rows[0];

  if (kpis.isError) return <EmptyState variant="error" title="Error loading lifecycle data" description={kpis.error.message} />;
  if (k && num(k.TOTAL_CUSTOMERS) === 0) {
    return <EmptyState title="Lifecycle data not available" description="Ensure CRMA_AGG_DT_CUSTOMER_LIFECYCLE is deployed." />;
  }
  const ds = dormStats.data?.rows[0];
  const dormantCount = ds ? num(ds.DORMANT_COUNT) : 0;

  return (
    <div className="space-y-6">
      <PageHeader eyebrow="Banking" title="Churn & Lifecycle" description="Customer lifecycle, dormancy and churn-risk signals" />
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <ChurnKpis k={k} />
      </div>

      <Panel title="Customer Lifecycle Stage Distribution">
        <LoaderState q={stages}>
          {(rows) => (
            <div className="grid gap-4 lg:grid-cols-3">
              <div className="lg:col-span-2">
                <Chart height={400}>
                  <PieChart>
                    <Pie
                      data={rows.map((r) => ({ name: String(r.LIFECYCLE_STAGE), value: num(r.CUSTOMER_COUNT) }))}
                      dataKey="value"
                      nameKey="name"
                      innerRadius="45%"
                      outerRadius="80%"
                      label={({ name, percent }) => `${name} ${((percent ?? 0) * 100).toFixed(1)}%`}
                    >
                      {rows.map((r) => (
                        <Cell key={String(r.LIFECYCLE_STAGE)} fill={STAGE_COLOR[String(r.LIFECYCLE_STAGE)] ?? "var(--chart-5)"} />
                      ))}
                    </Pie>
                    <Tooltip content={<ChartTooltip />} />
                    <Legend layout="vertical" align="right" verticalAlign="middle" />
                  </PieChart>
                </Chart>
              </div>
              <div className="space-y-2">
                <p className="text-sm font-medium">Stage Summary</p>
                <ul className="space-y-1 text-sm">
                  {rows.map((r) => (
                    <li key={String(r.LIFECYCLE_STAGE)} className="flex justify-between gap-4 border-b py-1">
                      <span className="font-medium">{String(r.LIFECYCLE_STAGE)}</span>
                      <span className="tabular-nums" data-testid={`stage-${String(r.LIFECYCLE_STAGE)}`}>
                        {formatNumber(num(r.CUSTOMER_COUNT))}
                      </span>
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          )}
        </LoaderState>
      </Panel>

      <h2 className="text-lg font-semibold">Churn Probability Analysis</h2>
      <div className="grid gap-4 lg:grid-cols-2">
        <Panel title="Churn Probability Distribution" description="Number of customers per 5 pp churn-probability bin">
          <LoaderState q={hist}>
            {(rows) => (
              <Histogram
                rows={rows}
                width={5}
                color="var(--destructive)"
                xLabel="Churn Probability (%)"
                lines={[
                  { x: 70, label: "High Risk (70%)", color: "var(--warning)" },
                  { x: 90, label: "Critical Risk (90%)", color: "var(--destructive)" },
                ]}
              />
            )}
          </LoaderState>
        </Panel>
        <Panel title="Average Churn Probability by Account Tier">
          <LoaderState q={byTier}>
            {(rows) => {
              const data = rows.map((r) => ({ tier: String(r.ACCOUNT_TIER ?? "(none)"), v: num(r.AVG_CHURN_PROBABILITY) }));
              const max = Math.max(...data.map((d) => d.v), 1);
              return (
                <Chart height={360}>
                  <BarChart data={data} margin={{ top: 20 }}>
                    <CartesianGrid stroke={CHART_THEME.grid} vertical={false} />
                    <XAxis dataKey="tier" tick={CHART_THEME.tick} />
                    <YAxis tick={CHART_THEME.tick} tickFormatter={(v) => `${v}%`} width={48} />
                    <Tooltip content={<ChartTooltip />} />
                    <Bar dataKey="v" name="Avg churn probability (%)" radius={[4, 4, 0, 0]}>
                      {data.map((d) => (
                        <Cell key={d.tier} fill="var(--destructive)" fillOpacity={0.35 + 0.65 * (d.v / max)} />
                      ))}
                      <LabelList dataKey="v" position="top" fontSize={11} formatter={(v) => `${Number(v).toFixed(1)}%`} />
                    </Bar>
                  </BarChart>
                </Chart>
              );
            }}
          </LoaderState>
        </Panel>
      </div>

      <Panel title="Premium Customers at Risk (GOLD/PLATINUM)">
        <QueryView q={premium} skeleton={<Skeleton className="h-64 w-full" />} errorTitle="Query failed">
          {({ rows: premiumRows }) =>
            premiumRows.length === 0 ? (
          <Callout tone="success" title="No premium customers currently at high risk" />
        ) : (
          <div className="space-y-4">
            <Callout tone="error" title={`${formatNumber(premiumRows.length)} premium customers at high risk of churning (>70%)`} />
            <DataTable rows={premiumRows} columns={PREMIUM_COLS} maxRows={1000} />
            <div className="flex flex-wrap items-center justify-between gap-2">
              <ProfilePicker rows={premiumRows} />
              <Button
                variant="outline"
                size="sm"
                onClick={() => downloadCsv(premiumRows, `premium_at_risk_${new Date().toISOString().slice(0, 10).replace(/-/g, "")}.csv`)}
              >
                Export Premium At-Risk (CSV)
              </Button>
            </div>
          </div>
            )
          }
        </QueryView>
      </Panel>

      <Panel title="Dormant Accounts (>180 Days)" description="DORMANT / DECLINING customers inactive for more than 180 days">
        <QueryView q={dormStats} skeleton={<Skeleton className="h-32 w-full" />} errorTitle="Query failed">
          {() =>
            dormantCount === 0 ? (
          <Callout tone="success" title="No dormant accounts" />
        ) : (
          <div className="space-y-4">
            <Callout tone="warning" title={`${formatNumber(dormantCount)} customers inactive for more than 180 days`} />
            <div className="grid gap-4 lg:grid-cols-2">
              <LoaderState q={dormHist}>
                {(rows) => {
                  const lo = num(rows[0].BIN_START);
                  const w = rows.length > 1 ? num(rows[1].BIN_START) - lo : 1;
                  return (
                    <Histogram
                      rows={rows}
                      width={Math.max(w, 1)}
                      color="var(--muted-foreground)"
                      xLabel="Days Since Last Transaction"
                      lines={[{ x: 180, label: "Dormant (180 days)", color: "var(--destructive)" }]}
                    />
                  );
                }}
              </LoaderState>
              <dl className="space-y-2 text-sm">
                <div><dt className="inline font-medium">Avg Days Inactive: </dt><dd className="inline">{formatNumber(num(ds?.AVG_DAYS_INACTIVE), 0)}</dd></div>
                <div><dt className="inline font-medium">Longest Inactive: </dt><dd className="inline">{formatNumber(num(ds?.MAX_DAYS_INACTIVE), 0)}</dd></div>
                <div><dt className="inline font-medium">Premium Dormant: </dt><dd className="inline">{formatNumber(num(ds?.PREMIUM_DORMANT))}</dd></div>
              </dl>
            </div>
          </div>
            )
          }
        </QueryView>
      </Panel>

      <h2 className="text-lg font-semibold">Recommended Actions</h2>
      <div className="grid gap-4 md:grid-cols-3">
        <Callout tone="info" title="High Churn Risk (>70%)">
          <ul className="list-disc pl-4"><li>Immediate outreach</li><li>Personalized retention offers</li><li>Account review within 7 days</li></ul>
        </Callout>
        <Callout tone="warning" title="Dormant Accounts (>180d)">
          <ul className="list-disc pl-4"><li>Reactivation campaign</li><li>Special promotions</li><li>Product recommendations</li></ul>
        </Callout>
        <Callout tone="success" title="Lifecycle Optimization">
          <ul className="list-disc pl-4"><li>Move NEW to ACTIVE faster</li><li>Prevent ACTIVE to DECLINING</li><li>Reactivate DORMANT</li></ul>
        </Callout>
      </div>
    </div>
  );
}
