"use client";

import { DataTable, LoaderState, Panel, Tabs } from "@/components/data/data-table";
import { StateView } from "@/components/query-view";
import { Donut, meanBy, pivot, riskColor, seqColor, SimpleBars, StackedBars } from "@/components/lending/charts";
import { PageHeader } from "@/components/page-header";
import { StatCard, StatCardSkeleton } from "@/components/stat-card";
import { num, useLoader, type LoaderRow } from "@/lib/loaders/client";
import { formatNumber, formatValue } from "@/lib/format";
import Link from "next/link";
import { useMemo, useState } from "react";

const TABS = [
  { v: "overview", label: "Portfolio Overview" },
  { v: "ltv", label: "LTV Analysis" },
  { v: "funnel", label: "Application Funnel" },
  { v: "afford", label: "Affordability" },
  { v: "compliance", label: "Compliance Screening" },
  { v: "credit", label: "Credit Risk" },
] as const;
type Tab = (typeof TABS)[number]["v"];

const sum = (rows: LoaderRow[], k: string, where?: (r: LoaderRow) => boolean) =>
  rows.filter((r) => !where || where(r)).reduce((s, r) => s + num(r[k]), 0);
const range = (rows: Record<string, unknown>[], k: string) => {
  const v = rows.map((r) => num(r[k]));
  return [Math.min(...v), Math.max(...v)] as const;
};

export default function LendingPage() {
  const [tab, setTab] = useState<Tab>("overview");
  const portfolio = useLoader("lending.portfolio");
  const rows = portfolio.data?.rows ?? [];

  const kpi = useMemo(() => {
    const total = sum(rows, "LOAN_COUNT");
    const approved = sum(rows, "LOAN_COUNT", (r) => r.APPLICATION_STATUS === "APPROVED");
    return {
      total,
      amountM: sum(rows, "TOTAL_REQUESTED_AMOUNT") / 1e6,
      approved,
      review: sum(rows, "LOAN_COUNT", (r) => r.APPLICATION_STATUS === "UNDER_REVIEW"),
      declined: sum(rows, "LOAN_COUNT", (r) => r.APPLICATION_STATUS === "DECLINED"),
      rate: total > 0 ? (approved / total) * 100 : 0,
    };
  }, [rows]);

  const asOf = rows[0]?.AS_OF_DATE ? ` · as of ${formatValue(rows[0].AS_OF_DATE)}` : "";

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="Risk"
        title="Lending & Loans"
        description={`Retail Loans & Mortgages Portfolio Analysis${asOf}`}
      />
      <StateView
        pending={portfolio.isPending}
        error={portfolio.error}
        errorTitle="Query failed"
        skeleton={
          <div className="grid gap-4 sm:grid-cols-3 lg:grid-cols-5">
            {["k1", "k2", "k3", "k4", "k5"].map((k) => (
              <StatCardSkeleton key={k} />
            ))}
          </div>
        }
        empty={rows.length === 0}
        emptyTitle="No loan portfolio data available. Check data pipeline."
      >
        {() => (
        <>
          <div className="grid gap-4 sm:grid-cols-3 lg:grid-cols-5" data-testid="lending-kpis">
            <StatCard label="Total Applications" value={formatNumber(kpi.total, 0)} />
            <StatCard label="Total Amount" value={`CHF ${formatNumber(kpi.amountM, 0)}M`} />
            <StatCard label="Approved" value={formatNumber(kpi.approved, 0)} hint={`${formatNumber(kpi.rate, 1)}% approval rate`} />
            <StatCard label="Under Review" value={formatNumber(kpi.review, 0)} />
            <StatCard label="Declined" value={formatNumber(kpi.declined, 0)} />
          </div>
          <Tabs tabs={[...TABS]} value={tab} onChange={setTab} />
          {tab === "overview" && <Overview rows={rows} />}
          {tab === "ltv" && <Ltv />}
          {tab === "funnel" && <Funnel />}
          {tab === "afford" && <Affordability />}
          {tab === "compliance" && <Compliance />}
          {tab === "credit" && <CreditRisk />}
        </>
        )}
      </StateView>
    </div>
  );
}

function Overview({ rows }: { rows: LoaderRow[] }) {
  const apps = pivot(rows, "COUNTRY", "PRODUCT_TYPE", "LOAN_COUNT");
  const amt = pivot(rows, "COUNTRY", "PRODUCT_TYPE", "TOTAL_REQUESTED_AMOUNT", 1e6);
  const status = pivot(rows, "COUNTRY", "APPLICATION_STATUS", "LOAN_COUNT");
  const avg = meanBy(rows, "COUNTRY", "AVG_REQUESTED_AMOUNT");
  const [lo, hi] = range(avg, "AVG_REQUESTED_AMOUNT");
  return (
    <div className="space-y-4">
      <h2 className="text-lg font-semibold">Portfolio by Country & Product</h2>
      <div className="grid gap-4 lg:grid-cols-2">
        <Panel title="Applications by Country">
          <StackedBars data={apps.data} x="COUNTRY" keys={apps.keys} />
        </Panel>
        <Panel title="Total Requested Amount (M CHF)">
          <StackedBars data={amt.data} x="COUNTRY" keys={amt.keys} />
        </Panel>
        <Panel title="Status by Country">
          <StackedBars data={status.data} x="COUNTRY" keys={status.keys} />
        </Panel>
        <Panel title="Avg Amount by Country">
          <SimpleBars
            data={avg}
            x="COUNTRY"
            y="AVG_REQUESTED_AMOUNT"
            name="Avg requested amount"
            color={(r) => seqColor(num(r.AVG_REQUESTED_AMOUNT), lo, hi)}
          />
        </Panel>
      </div>
      <Panel title="Portfolio summary">
        <DataTable rows={rows} />
      </Panel>
    </div>
  );
}

function Ltv() {
  const q = useLoader("lending.ltv");
  return (
    <div className="space-y-4">
      <h2 className="text-lg font-semibold">Loan-to-Value (LTV) Distribution</h2>
      <LoaderState q={q} empty="No LTV data available">
        {(rows) => {
          const [lo, hi] = range(rows, "AVG_LTV_PCT");
          const amt = rows.map((r) => ({ ...r, AMT_M: num(r.TOTAL_LOAN_AMOUNT) / 1e6 }));
          const high = rows.filter((r) => r.LTV_BUCKET === "80-90%" || r.LTV_BUCKET === ">90%");
          return (
            <>
              <Panel title="Loans by LTV Bucket" description="Bar colour: average LTV (green low, red high)">
                <SimpleBars data={rows} x="LTV_BUCKET" y="LOAN_COUNT" name="Loans" color={(r) => riskColor(num(r.AVG_LTV_PCT), lo, hi)} />
              </Panel>
              <div className="grid gap-4 lg:grid-cols-2">
                <Panel title="Amount by LTV (M CHF)">
                  <SimpleBars
                    data={amt}
                    x="LTV_BUCKET"
                    y="AMT_M"
                    name="Amount (M CHF)"
                    color={(_, i) => seqColor(i, 0, amt.length - 1, "var(--destructive)")}
                  />
                </Panel>
                <Panel title="High-Risk LTV (>80%)">
                  {high.length ? (
                    <Donut data={high} label="LTV_BUCKET" value="LOAN_COUNT" hole={0} />
                  ) : (
                    <p className="text-sm text-[var(--success)]">No high-risk LTV applications (&gt;80%)</p>
                  )}
                </Panel>
              </div>
              <Panel title="LTV distribution">
                <DataTable rows={rows} />
              </Panel>
            </>
          );
        }}
      </LoaderState>
    </div>
  );
}

function Funnel() {
  const q = useLoader("lending.funnel");
  return (
    <div className="space-y-4">
      <h2 className="text-lg font-semibold">Application Funnel</h2>
      <LoaderState q={q} empty="No funnel data available">
        {(rows) => {
          const funnel = [
            { Stage: "Submitted", Count: sum(rows, "TOTAL_APPLICATIONS") },
            { Stage: "Under Review", Count: sum(rows, "UNDER_REVIEW_COUNT") },
            { Stage: "Approved", Count: sum(rows, "APPROVED_COUNT") },
          ];
          const byCountry = new Map<string, Record<string, string | number>>();
          for (const r of rows) {
            const c = String(r.COUNTRY);
            const a = byCountry.get(c) ?? { COUNTRY: c, APPROVED: 0, UNDER_REVIEW: 0, DECLINED: 0 };
            a.APPROVED = num(a.APPROVED) + num(r.APPROVED_COUNT);
            a.UNDER_REVIEW = num(a.UNDER_REVIEW) + num(r.UNDER_REVIEW_COUNT);
            a.DECLINED = num(a.DECLINED) + num(r.DECLINED_COUNT);
            byCountry.set(c, a);
          }
          const rate = meanBy(rows, "COUNTRY", "APPROVAL_RATE_PCT");
          const [lo, hi] = range(rate, "APPROVAL_RATE_PCT");
          return (
            <>
              <Panel title="Application Funnel">
                <div className="space-y-2" data-testid="lending-funnel">
                  {funnel.map((f) => (
                    <div key={f.Stage} className="flex items-center gap-3 text-sm">
                      <span className="w-28 shrink-0 text-muted-foreground">{f.Stage}</span>
                      <div className="flex flex-1 justify-center">
                        <div
                          className="flex h-8 items-center justify-center rounded bg-[var(--chart-1)] text-xs font-medium text-primary-foreground"
                          style={{ width: `${Math.max(4, (100 * f.Count) / (funnel[0].Count || 1))}%` }}
                        >
                          {formatNumber(f.Count, 0)}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </Panel>
              <div className="grid gap-4 lg:grid-cols-2">
                <Panel title="Status by Country">
                  <StackedBars data={[...byCountry.values()]} x="COUNTRY" keys={["APPROVED", "UNDER_REVIEW", "DECLINED"]} />
                </Panel>
                <Panel title="Approval Rate by Country (%)">
                  <SimpleBars
                    data={rate}
                    x="COUNTRY"
                    y="APPROVAL_RATE_PCT"
                    name="Approval rate %"
                    yFormat={(v) => `${v}%`}
                    color={(r) => seqColor(num(r.APPROVAL_RATE_PCT), lo, hi, "var(--success)")}
                  />
                </Panel>
              </div>
              <Panel title="Funnel by product, country and channel">
                <DataTable rows={rows} />
              </Panel>
            </>
          );
        }}
      </LoaderState>
    </div>
  );
}

function Affordability() {
  const q = useLoader("lending.affordability");
  return (
    <div className="space-y-4">
      <h2 className="text-lg font-semibold">Affordability Assessment</h2>
      <LoaderState q={q} empty="No affordability data available">
        {(rows) => {
          const dti = meanBy(rows, "COUNTRY", "AVG_DTI_RATIO_PCT");
          const dsti = meanBy(rows, "COUNTRY", "AVG_DSTI_RATIO_PCT");
          const [dl, dh] = range(dti, "AVG_DTI_RATIO_PCT");
          const [sl, sh] = range(dsti, "AVG_DSTI_RATIO_PCT");
          return (
            <>
              <div className="grid gap-4 lg:grid-cols-2">
                <Panel title="Avg DTI by Country (%)">
                  <SimpleBars
                    data={dti}
                    x="COUNTRY"
                    y="AVG_DTI_RATIO_PCT"
                    name="Avg DTI %"
                    yFormat={(v) => `${v}%`}
                    color={(r) => riskColor(num(r.AVG_DTI_RATIO_PCT), dl, dh)}
                    threshold={{ y: 45, label: "45% Threshold", color: "var(--destructive)" }}
                  />
                </Panel>
                <Panel title="Avg DSTI by Country (%)">
                  <SimpleBars
                    data={dsti}
                    x="COUNTRY"
                    y="AVG_DSTI_RATIO_PCT"
                    name="Avg DSTI %"
                    yFormat={(v) => `${v}%`}
                    color={(r) => riskColor(num(r.AVG_DSTI_RATIO_PCT), sl, sh)}
                    threshold={{ y: 33.33, label: "Swiss 33% Threshold", color: "var(--warning)" }}
                  />
                </Panel>
              </div>
              <Panel title="Affordability by country and result">
                <DataTable rows={rows} />
              </Panel>
            </>
          );
        }}
      </LoaderState>
    </div>
  );
}

function Compliance() {
  const q = useLoader("lending.compliance");
  return (
    <div className="space-y-4">
      <h2 className="text-lg font-semibold">Compliance & Risk Screening</h2>
      <LoaderState q={q} empty="No applications flagged for compliance review">
        {(rows) => {
          const flag = (k: string) => rows.filter((r) => r[k] === true || r[k] === "true" || num(r[k]) === 1).length;
          const count = (k: string) => {
            const m = new Map<string, number>();
            for (const r of rows) m.set(String(r[k] ?? "n/a"), (m.get(String(r[k] ?? "n/a")) ?? 0) + 1);
            return [...m.entries()].sort((a, b) => b[1] - a[1]).map(([name, Count]) => ({ [k]: name, Count }));
          };
          return (
            <>
              <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
                <StatCard label="Total Flagged" value={formatNumber(rows.length, 0)} hint="top 100 by review priority" />
                <StatCard label="Sanctions Review" value={formatNumber(flag("REQUIRES_SANCTIONS_REVIEW"), 0)} />
                <StatCard label="PEP Review" value={formatNumber(flag("REQUIRES_EXPOSED_PERSON_REVIEW"), 0)} />
                <StatCard label="Vulnerable" value={formatNumber(flag("VULNERABLE_CUSTOMER_FLAG"), 0)} />
              </div>
              <div className="grid gap-4 lg:grid-cols-2">
                <Panel title="By Compliance Status">
                  <Donut data={count("COMPLIANCE_STATUS")} label="COMPLIANCE_STATUS" value="Count" hole={0} />
                </Panel>
                <Panel title="By Risk Rating">
                  <SimpleBars
                    data={count("OVERALL_RISK_RATING")}
                    x="OVERALL_RISK_RATING"
                    y="Count"
                    name="Applications"
                    color={(r) =>
                      ({ CRITICAL: "var(--destructive)", HIGH: "var(--chart-4)", MEDIUM: "var(--warning)", LOW: "var(--success)" })[
                        String(r.OVERALL_RISK_RATING)
                      ] ?? "var(--chart-1)"
                    }
                  />
                </Panel>
              </div>
              <Panel title="Flagged applications">
                <DataTable rows={rows} />
              </Panel>
            </>
          );
        }}
      </LoaderState>
    </div>
  );
}

function CreditRisk() {
  const bands = useLoader("lending.credit-bands");
  const classes = useLoader("lending.risk-classes");
  const customers = useLoader("lending.customers");
  return (
    <div className="space-y-4">
      <h2 className="text-lg font-semibold">Credit Risk Overview</h2>
      <div className="grid gap-4 lg:grid-cols-2">
        <Panel title="Credit Risk Distribution" description="Customers by credit score band">
          <LoaderState q={bands} empty="No lending portfolio data available">
            {(rows) => <Donut data={rows} label="CREDIT_SCORE_BAND" value="CUSTOMERS" hole={0.3} height={360} />}
          </LoaderState>
        </Panel>
        <Panel title="Risk classification" description="Customers per risk class">
          <LoaderState q={classes}>
            {(rows) => (
              <ul className="space-y-2 text-sm" data-testid="risk-classes">
                {rows.map((r) => (
                  <li key={String(r.RISK_CLASSIFICATION)} className="flex justify-between border-b pb-1">
                    <span className="font-semibold">{String(r.RISK_CLASSIFICATION)}</span>
                    <span className="tabular-nums">{formatNumber(r.CUSTOMERS, 0)}</span>
                  </li>
                ))}
              </ul>
            )}
          </LoaderState>
        </Panel>
      </div>
      <Panel title="Customers" description="First 1,000 customers by ID; open a customer in Customer 360">
        <LoaderState q={customers} empty="No lending portfolio data available">
          {(rows) => (
            <DataTable
              rows={rows}
              columns={[
                {
                  key: "CUSTOMER_ID",
                  render: (r) => (
                    <Link className="text-primary hover:underline" href={`/banking/customer-360?customer=${encodeURIComponent(String(r.CUSTOMER_ID))}`}>
                      {String(r.CUSTOMER_ID)}
                    </Link>
                  ),
                },
                "FULL_NAME",
                "COUNTRY",
                "CREDIT_SCORE_BAND",
                "RISK_CLASSIFICATION",
                "ACCOUNT_TIER",
              ]}
            />
          )}
        </LoaderState>
      </Panel>
    </div>
  );
}
