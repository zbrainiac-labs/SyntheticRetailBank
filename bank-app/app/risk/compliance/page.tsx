"use client";

import { ChartTooltip, CHART_THEME } from "@/components/chart-utils";
import { DataTable, LoaderState, Panel, Tabs } from "@/components/data/data-table";
import { PageHeader } from "@/components/page-header";
import { CountList, countBy, CsvButton, isTrue, MatchTypeBars, ProfilePicker, RiskDonut } from "@/components/risk/common";
import { StatCard, StatCardSkeleton } from "@/components/stat-card";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { num, useLoader, type LoaderRow } from "@/lib/loaders/client";
import { formatNumber } from "@/lib/format";
import { useState } from "react";
import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

const TABS = [
  { v: "overview", label: "Overview" },
  { v: "sanctions", label: "Sanctions Control" },
  { v: "kyc", label: "KYC Screening" },
] as const;
type Tab = (typeof TABS)[number]["v"];

function KpiGrid({ loading, children }: { loading: boolean; children: React.ReactNode }) {
  return (
    <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
      {loading ? Array.from({ length: 4 }, (_, i) => <StatCardSkeleton key={i} />) : children}
    </div>
  );
}

/** Green (low) to red (high) like RdYlGn_r. */
function scoreColor(v: number, min: number, max: number) {
  const t = max > min ? (v - min) / (max - min) : 0;
  if (t > 0.66) return "var(--destructive)";
  return t > 0.33 ? "var(--warning)" : "var(--success)";
}

function CountryRiskChart({ rows }: { rows: LoaderRow[] }) {
  const data = rows.map((r) => ({ name: String(r.COUNTRY), "Avg risk score": num(r.AVG_RISK_SCORE), customers: num(r.CUSTOMERS) }));
  const vals = data.map((d) => d["Avg risk score"]);
  const [min, max] = [Math.min(...vals), Math.max(...vals)];
  return (
    <ResponsiveContainer width="100%" height={Math.max(300, data.length * 22 + 40)}>
      <BarChart data={data} layout="vertical" margin={{ left: 8, right: 16 }}>
        <CartesianGrid stroke={CHART_THEME.grid} horizontal={false} />
        <XAxis type="number" tick={CHART_THEME.tick} />
        <YAxis type="category" dataKey="name" width={110} tick={CHART_THEME.tick} interval={0} />
        <Tooltip content={<ChartTooltip />} />
        <Bar dataKey="Avg risk score" radius={[0, 4, 4, 0]}>
          {data.map((d) => (
            <Cell key={d.name} fill={scoreColor(d["Avg risk score"], min, max)} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}

function Overview() {
  const kpis = useLoader("risk.kpis");
  const dist = useLoader("risk.rating-distribution");
  const screening = useLoader("risk.screening-status");
  const hr = useLoader("risk.high-risk-customers");
  const country = useLoader("risk.country-risk");
  const k = kpis.data?.rows[0];
  const screen = (s: string) => (screening.data?.rows ?? []).filter((r) => r.SCREENING === s);

  return (
    <div className="space-y-6">
      <KpiGrid loading={kpis.isPending}>
        <StatCard label="Critical/High Risk" value={formatNumber(num(k?.CRITICAL_HIGH), 0)} />
        <StatCard label="Requires PEP Review" value={formatNumber(num(k?.PEP_REVIEW), 0)} />
        <StatCard label="Requires Sanctions Review" value={formatNumber(num(k?.SANCTIONS_REVIEW), 0)} />
        <StatCard label="High Risk %" value={`${num(k?.HIGH_RISK_PCT).toFixed(1)}%`} hint={`${formatNumber(num(k?.HIGH_RISK_COUNT), 0)} of ${formatNumber(num(k?.TOTAL_CUSTOMERS), 0)} customers`} />
      </KpiGrid>

      <div className="grid gap-6 lg:grid-cols-2">
        <Panel title="Customer Risk Distribution (All)">
          <LoaderState q={dist}>{(rows) => <RiskDonut rows={rows} />}</LoaderState>
        </Panel>
        <Panel title="Customer Risk Distribution (Excluding No Risk)">
          <LoaderState q={dist} empty="No customers with risk ratings">
            {(rows) => {
              const r = rows.filter((x) => x.OVERALL_RISK_RATING !== "NO_RISK");
              return r.length ? <RiskDonut rows={r} /> : <p className="text-sm text-muted-foreground">No customers with risk ratings</p>;
            }}
          </LoaderState>
        </Panel>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <Panel title="PEP Screening Status">
          <LoaderState q={screening} height="h-24">{() => <CountList items={screen("PEP")} label="MATCH_TYPE" value="CUSTOMERS" />}</LoaderState>
        </Panel>
        <Panel title="Sanctions Screening Status">
          <LoaderState q={screening} height="h-24">{() => <CountList items={screen("SANCTIONS")} label="MATCH_TYPE" value="CUSTOMERS" />}</LoaderState>
        </Panel>
      </div>

      <Panel
        title="High-Risk Customers Requiring Action"
        actions={hr.data?.rows.length ? <CsvButton rows={hr.data.rows} label="Export High-Risk List (CSV)" filename="high_risk" /> : null}
      >
        <LoaderState q={hr} empty="No high-risk customers requiring immediate action">
          {(rows) => (
            <div className="space-y-3">
              <Alert className="border-[var(--warning)]">
                <AlertDescription>
                  <strong>{rows.length}</strong> customers require immediate compliance review
                </AlertDescription>
              </Alert>
              <DataTable rows={rows} />
              <ProfilePicker rows={rows} />
            </div>
          )}
        </LoaderState>
      </Panel>

      <div className="grid gap-6 lg:grid-cols-2">
        <Panel title="Customer Risk Distribution (All)" description="Compliance risk overview">
          <LoaderState q={dist}>{(rows) => <RiskDonut rows={rows} />}</LoaderState>
        </Panel>
        <Panel title="Average Risk Score by Country (Top 20)">
          <LoaderState q={country}>{(rows) => <CountryRiskChart rows={rows} />}</LoaderState>
        </Panel>
      </div>
    </div>
  );
}

function Sanctions() {
  const q = useLoader("risk.sanctions-matches");
  const rows = q.data?.rows ?? [];
  return (
    <div className="space-y-6">
      <KpiGrid loading={q.isPending}>
        <StatCard label="Total Sanctions Matches" value={formatNumber(rows.length, 0)} />
        <StatCard label="Exact Matches" value={formatNumber(rows.filter((r) => r.SANCTIONS_MATCH_TYPE === "EXACT_MATCH").length, 0)} />
        <StatCard label="Requires Review" value={formatNumber(rows.filter((r) => isTrue(r.REQUIRES_SANCTIONS_REVIEW)).length, 0)} />
        <StatCard label="High Risk" value={formatNumber(rows.filter((r) => ["CRITICAL", "HIGH"].includes(String(r.OVERALL_SANCTIONS_RISK))).length, 0)} />
      </KpiGrid>
      <LoaderState q={q} empty="No sanctions matches found">
        {(rows) => (
          <>
            <div className="grid gap-6 lg:grid-cols-2">
              <Panel title="Sanctions Screening Results">
                <MatchTypeBars rows={rows} keyName="SANCTIONS_MATCH_TYPE" color="var(--destructive)" />
              </Panel>
              <Panel title="Overall sanctions risk">
                <CountList items={countBy(rows, "OVERALL_SANCTIONS_RISK")} label="OVERALL_SANCTIONS_RISK" value="COUNT" />
              </Panel>
            </div>
            <Panel title="Sanctions matches">
              <div className="space-y-3">
                <DataTable rows={rows} columns={["CUSTOMER_ID", "FULL_NAME", "COUNTRY", "SANCTIONS_MATCH_TYPE", "SANCTIONS_MATCH_ACCURACY_PERCENT", "OVERALL_SANCTIONS_RISK"]} />
                <ProfilePicker rows={rows} />
              </div>
            </Panel>
          </>
        )}
      </LoaderState>
    </div>
  );
}

function Kyc() {
  const pep = useLoader("risk.pep-matches");
  const kyc = useLoader("risk.kyc-completeness");
  const rows = pep.data?.rows ?? [];
  const totalCustomers = (kyc.data?.rows ?? []).reduce((s, r) => s + num(r.TOTAL_CUSTOMERS), 0);
  return (
    <div className="space-y-6">
      <KpiGrid loading={pep.isPending || kyc.isPending}>
        <StatCard label="PEP Matches" value={formatNumber(rows.length, 0)} />
        <StatCard label="Requires PEP Review" value={formatNumber(rows.filter((r) => isTrue(r.REQUIRES_EXPOSED_PERSON_REVIEW)).length, 0)} />
        <StatCard label="Exact PEP Matches" value={formatNumber(rows.filter((r) => r.EXPOSED_PERSON_MATCH_TYPE === "EXACT_MATCH").length, 0)} />
        <StatCard label="Total Customers" value={formatNumber(totalCustomers, 0)} />
      </KpiGrid>
      <LoaderState q={pep} empty="No PEP matches found">
        {(rows) => (
          <>
            <div className="grid gap-6 lg:grid-cols-2">
              <Panel title="PEP Screening Results">
                <MatchTypeBars rows={rows} keyName="EXPOSED_PERSON_MATCH_TYPE" color="var(--warning)" />
              </Panel>
              <Panel title="Overall PEP risk">
                <CountList items={countBy(rows, "OVERALL_EXPOSED_PERSON_RISK")} label="OVERALL_EXPOSED_PERSON_RISK" value="COUNT" />
              </Panel>
            </div>
            <Panel title="PEP matches">
              <div className="space-y-3">
                <DataTable rows={rows} columns={["CUSTOMER_ID", "FULL_NAME", "COUNTRY", "EXPOSED_PERSON_MATCH_TYPE", "EXPOSED_PERSON_MATCH_ACCURACY_PERCENT", "OVERALL_EXPOSED_PERSON_RISK"]} />
                <ProfilePicker rows={rows} />
              </div>
            </Panel>
          </>
        )}
      </LoaderState>
      <Panel title="KYC Data Completeness by Country" description="Contact fields are masked for this role; counts reflect non-null values.">
        <LoaderState q={kyc}>{(rows) => <DataTable rows={rows} />}</LoaderState>
      </Panel>
    </div>
  );
}

export default function RiskCompliancePage() {
  const [tab, setTab] = useState<Tab>("overview");
  return (
    <div className="space-y-6">
      <PageHeader title="Risk & Compliance" description="Credit risk, regulatory capital and compliance monitoring" />
      <Tabs tabs={[...TABS]} value={tab} onChange={setTab} />
      {tab === "overview" && <Overview />}
      {tab === "sanctions" && <Sanctions />}
      {tab === "kyc" && <Kyc />}
    </div>
  );
}
