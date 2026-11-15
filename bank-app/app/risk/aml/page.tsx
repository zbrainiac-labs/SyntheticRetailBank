"use client";

import { ChartTooltip, CHART_THEME } from "@/components/chart-utils";
import { DataTable, LoaderState, Panel, Tabs } from "@/components/data/data-table";
import { PageHeader } from "@/components/page-header";
import { CountList, CsvButton, CUSTOMER_LINK_COLUMN, isTrue, useCardFilter } from "@/components/risk/common";
import { StatCard, StatCardSkeleton } from "@/components/stat-card";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { num, useLoader, type LoaderRow } from "@/lib/loaders/client";
import { formatNumber } from "@/lib/format";
import { useState } from "react";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

const TABS = [
  { v: "fraud", label: "Fraud Detection" },
  { v: "aml", label: "AML Monitoring" },
] as const;
type Tab = (typeof TABS)[number]["v"];

function KpiGrid({ loading, children }: { loading: boolean; children: React.ReactNode }) {
  return (
    <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
      {loading ? Array.from({ length: 4 }, (_, i) => <StatCardSkeleton key={i} />) : children}
    </div>
  );
}

/** Fraud KPI cards that filter the anomaly queue (every queue row is anomalous). */
const QUEUE_FILTERS = {
  anomalous: { label: "Anomalous Customers", test: () => true },
  highRisk: { label: "High-Risk + Anomaly", test: (r: LoaderRow) => isTrue(r.HIGH_RISK_CUSTOMER) },
};

function Fraud() {
  const kpis = useLoader("aml.fraud-kpis");
  const queue = useLoader("aml.anomaly-queue");
  const f = useCardFilter(QUEUE_FILTERS);
  const k = kpis.data?.rows[0];
  return (
    <div className="space-y-6">
      <KpiGrid loading={kpis.isPending}>
        <StatCard label="Total Customers" value={formatNumber(num(k?.TOTAL_CUSTOMERS), 0)} />
        {f.card("anomalous", formatNumber(num(k?.ANOMALOUS_CUSTOMERS), 0))}
        {f.card("highRisk", formatNumber(num(k?.HIGH_RISK_ANOMALY), 0))}
        <StatCard label="Anomaly Rate" value={`${num(k?.ANOMALY_RATE_PCT).toFixed(1)}%`} />
      </KpiGrid>
      <Panel
        title="Anomaly Priority Queue"
        actions={queue.data?.rows.length ? <CsvButton rows={queue.data.rows} label="Export Anomaly Report (CSV)" filename="anomaly_report" /> : null}
      >
        <LoaderState q={queue} empty="No anomalous transaction patterns detected">
          {(all) => {
            const rows = f.apply(all);
            return (
              <div className="space-y-3">
                <Alert className="border-[var(--warning)]">
                  <AlertDescription>
                    <strong>{rows.length}</strong> customers flagged with anomalous transaction patterns
                    {f.label && (
                      <>
                        {" "}({f.label}){" "}
                        <button type="button" onClick={f.clear} className="underline">
                          show all
                        </button>
                      </>
                    )}
                  </AlertDescription>
                </Alert>
                <DataTable
                  rows={rows}
                  columns={[CUSTOMER_LINK_COLUMN, "FULL_NAME", "ACCOUNT_TIER", "COUNTRY", "OVERALL_RISK_RATING", "OVERALL_RISK_SCORE"]}
                />
              </div>
            );
          }}
        </LoaderState>
      </Panel>
    </div>
  );
}

function Aml() {
  const metrics = useLoader("aml.metrics");
  const trend = useLoader("aml.alert-trend");
  const summary = useLoader("aml.alert-summary");
  const alerts = useLoader("aml.alert-rows");
  const alertExport = useLoader("aml.alert-export");
  const m = metrics.data?.rows[0];
  const total = num(m?.TOTAL_ALERTS);
  const sRows = summary.data?.rows ?? [];
  return (
    <div className="space-y-6">
      <KpiGrid loading={metrics.isPending}>
        <StatCard label="Total Alerts (90d)" value={formatNumber(total, 0)} />
        <StatCard label="Unique Customers" value={formatNumber(num(m?.UNIQUE_CUSTOMERS), 0)} />
        <StatCard label="Anomalous Transactions" value={formatNumber(num(m?.ANOMALOUS_TRANSACTIONS), 0)} />
        <StatCard label="Anomaly Rate" value={total > 0 ? `${((num(m?.ANOMALOUS_TRANSACTIONS) / total) * 100).toFixed(1)}%` : "—"} />
      </KpiGrid>
      <div className="grid gap-6 lg:grid-cols-2">
        <Panel title="AML Alert Trend (Last 90 Days)" description="Alerts per booking day, latest 1,000 alert records">
          <LoaderState q={trend} empty="No AML alert data available">
            {(rows) => (
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={rows.map((r) => ({ date: String(r.ALERT_DATE), Alerts: num(r.ALERT_COUNT) }))}>
                  <CartesianGrid stroke={CHART_THEME.grid} vertical={false} />
                  <XAxis dataKey="date" tick={CHART_THEME.tick} />
                  <YAxis allowDecimals={false} tick={CHART_THEME.tick} width={48} />
                  <Tooltip content={<ChartTooltip />} />
                  <Line dataKey="Alerts" stroke="var(--chart-1)" strokeWidth={2} dot={rows.length < 3} />
                </LineChart>
              </ResponsiveContainer>
            )}
          </LoaderState>
        </Panel>
        <Panel title="Alert classification">
          <LoaderState q={summary} height="h-32">
            {() => (
              <div className="space-y-3 text-sm">
                <p>
                  <strong>Total Alerts:</strong> {formatNumber(sRows.reduce((s, r) => s + num(r.ALERTS), 0), 0)}
                </p>
                <CountList items={sRows} label="OVERALL_ANOMALY_CLASSIFICATION" value="ALERTS" />
                <p>
                  <strong>Immediate Review Required:</strong> {formatNumber(sRows.reduce((s, r) => s + num(r.IMMEDIATE_REVIEW), 0), 0)}
                </p>
              </div>
            )}
          </LoaderState>
        </Panel>
      </div>
      <Panel
        title="Latest AML alerts"
        description="100 most recent records; the export holds the latest 1000 with all columns"
        actions={alertExport.data?.rows.length ? <CsvButton rows={alertExport.data.rows} label="Export AML Alerts (CSV)" filename="aml_alerts" /> : null}
      >
        <LoaderState q={alerts} empty="No AML alerts found">
          {(rows) => (
            <div className="space-y-3">
              <DataTable
                rows={rows}
                columns={Object.keys(rows[0] ?? {}).map((k) => (k === "CUSTOMER_ID" ? CUSTOMER_LINK_COLUMN : k))}
              />
            </div>
          )}
        </LoaderState>
      </Panel>
    </div>
  );
}

export default function AmlFraudPage() {
  const [tab, setTab] = useState<Tab>("fraud");
  return (
    <div className="space-y-6">
      <PageHeader title="AML & Fraud Detection" description="Transaction monitoring, alerts and suspicious-activity patterns" />
      <Tabs tabs={[...TABS]} value={tab} onChange={setTab} />
      {tab === "fraud" ? <Fraud /> : <Aml />}
    </div>
  );
}
