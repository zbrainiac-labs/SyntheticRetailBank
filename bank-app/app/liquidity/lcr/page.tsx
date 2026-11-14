"use client";

import { ChartTooltip, CHART_THEME, formatTick } from "@/components/chart-utils";
import { DataTable, LoaderState, Panel, Tabs } from "@/components/data/data-table";
import { Segmented } from "@/components/dti/common";
import { EmptyState } from "@/components/empty-state";
import { BookBadge, useLiquidityBook } from "@/components/liquidity/book-badge";
import { StateView } from "@/components/query-view";
import { PageHeader } from "@/components/page-header";
import { StatCard } from "@/components/stat-card";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { num, useLoader, type LoaderRow } from "@/lib/loaders/client";
import { formatNumber } from "@/lib/format";
import { defaultDate } from "@/lib/liquidity/book";
import { useEffect, useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ComposedChart,
  Legend,
  Line,
  LineChart,
  ReferenceArea,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  Treemap,
  XAxis,
  YAxis,
} from "recharts";

const FAIL_LIMIT = 100;
const PASS_LIMIT = 110;
const STATUS = { PASS: "var(--success)", WARNING: "var(--warning)", FAIL: "var(--destructive)" } as const;
const SCENARIOS = [
  { v: "BASE", label: "Regulatory (BASE)" },
  { v: "BANK_STRESS", label: "Bank stress" },
  { v: "MARKET_STRESS", label: "Market stress" },
  { v: "COMBINED", label: "Combined stress" },
] as const;
type Scenario = (typeof SCENARIOS)[number]["v"];
const SCEN_LABEL = Object.fromEntries(SCENARIOS.map((s) => [s.v, s.label])) as Record<string, string>;
const SCEN_COLOR: Record<string, string> = { BASE: "var(--chart-1)", BANK_STRESS: "var(--chart-2)", MARKET_STRESS: "var(--chart-3)", COMBINED: "var(--chart-4)" };

const TABS = [
  { v: "overview", label: "Overview" },
  { v: "hqla", label: "HQLA & Caps" },
  { v: "flows", label: "Flows & Inflow Cap" },
  { v: "behavioral", label: "Behavioral" },
  { v: "scenarios", label: "Scenarios" },
  { v: "variance", label: "Variance" },
  { v: "drilldown", label: "Drill-down" },
  { v: "currency", label: "Currency" },
  { v: "dq", label: "Data Quality" },
  { v: "simulator", label: "Simulator" },
  { v: "reporting", label: "Reporting" },
] as const;
type Tab = (typeof TABS)[number]["v"];

const chfM = (v: unknown) => `CHF ${formatNumber(num(v) / 1e6, 0)}M`;
function statusOf(lcr: number): "FAIL" | "WARNING" | "PASS" {
  if (lcr < FAIL_LIMIT) return "FAIL";
  return lcr < PASS_LIMIT ? "WARNING" : "PASS";
}
const TREATMENT_COLOR: Record<string, string> = { HQLA: "var(--chart-1)", OUTFLOW: "var(--warning)" };

/** FAIL (<100%) and WARNING (100-110%) bands plus the two limit lines. */
function StatusBands({ yMax }: { yMax: number }) {
  return (
    <>
      <ReferenceArea y1={0} y2={FAIL_LIMIT} fill={STATUS.FAIL} fillOpacity={0.07} ifOverflow="hidden" />
      <ReferenceArea y1={FAIL_LIMIT} y2={PASS_LIMIT} fill={STATUS.WARNING} fillOpacity={0.08} ifOverflow="hidden" />
      <ReferenceLine y={FAIL_LIMIT} stroke={STATUS.FAIL} strokeDasharray="3 3" label={{ value: "100% minimum", position: "insideBottomRight", fontSize: 11 }} />
      <ReferenceLine y={PASS_LIMIT} stroke={STATUS.WARNING} strokeDasharray="3 3" label={{ value: "110% target", position: "insideTopRight", fontSize: 11 }} />
      <YAxis domain={[Math.max(0, Math.min(80, yMax - 100)), Math.ceil(yMax + 10)]} tickFormatter={(v) => `${v}%`} tick={CHART_THEME.tick} width={48} />
    </>
  );
}

function EventLines({ events, labels = true }: { events: LoaderRow[]; labels?: boolean }) {
  return (
    <>
      {events.map((e) => (
        <ReferenceLine
          key={String(e.EVENT_ID)}
          x={String(e.EVENT_DATE)}
          stroke="var(--muted-foreground)"
          strokeDasharray="4 3"
          label={labels ? { value: String(e.EVENT_ID), position: "top", fontSize: 10, fill: "var(--muted-foreground)" } : undefined}
        />
      ))}
    </>
  );
}

function Chart({ height = 360, children }: { height?: number; children: React.ReactElement }) {
  return (
    <div style={{ height }}>
      <ResponsiveContainer width="100%" height="100%">
        {children}
      </ResponsiveContainer>
    </div>
  );
}

/**
 * LCR Monitoring - port of the_bank_app/pages/lcr_monitoring.py (FINMA LiqV /
 * Circ. 2015/2): regulatory LCR, caps, flows, behavioral ladder, stress
 * scenarios, variance drivers, drill-down, currency, data quality, what-if
 * simulator and SNB reporting.
 */
export default function LcrMonitoringPage() {
  const scen = useLoader("lcr.scenario-daily");
  const events = useLoader("lcr.events");
  const base = useMemo(() => (scen.data?.rows ?? []).filter((r) => r.STRESS_SCENARIO === "BASE"), [scen.data]);
  const dates = useMemo(() => base.map((r) => String(r.AS_OF_DATE)), [base]);
  const [asOf, setAsOf] = useState<string | null>(null);
  const [tab, setTab] = useState<Tab>("overview");
  // open on the as-of date of the shared liquidity book (same default date as /dti)
  const book = useLiquidityBook();
  const bookReady = !book.isPending;
  useEffect(() => {
    if (!asOf && dates.length && bookReady) setAsOf(defaultDate(dates, book.data?.asOf));
  }, [asOf, dates, bookReady, book.data?.asOf]);

  if (scen.isPending) return <Skeleton className="h-96 w-full" />;
  if (scen.isError) return <EmptyState variant="error" title="LCR data failed" description={scen.error.message} />;
  if (base.length === 0 || !asOf) {
    return <EmptyState title="No LCR data" description="Run the LCR engine refresh (operation/execute_all_tasks_and_refresh_dts.sql)." />;
  }

  const idx = Math.max(0, dates.indexOf(asOf));
  const cur = base[idx];
  const prev = idx > 0 ? base[idx - 1] : null;
  const lcr = num(cur.LCR_RATIO);
  const dod = prev ? lcr - num(prev.LCR_RATIO) : 0;
  const status = statusOf(lcr);
  const ev = events.data?.rows ?? [];

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="Liquidity"
        title="LCR Monitoring"
        description="FINMA Liquidity Coverage Ratio (LiqV / Circ. 2015/2) - regulatory LCR, stress scenarios, root-cause analysis and what-if simulation"
      />
      <BookBadge other="dti" />
      <div className="flex flex-wrap items-center gap-3 rounded-lg border bg-card p-3">
        <label className="flex items-center gap-2 text-sm text-muted-foreground">
          <span>Reporting date</span>
          <input
            type="range"
            aria-label="Reporting date"
            min={0}
            max={dates.length - 1}
            value={idx}
            onChange={(e) => setAsOf(dates[Number(e.target.value)])}
            className="w-64 accent-[var(--primary)]"
          />
        </label>
        <select aria-label="Reporting date (list)" className="h-8 rounded-md border bg-background px-2 text-sm" value={asOf} onChange={(e) => setAsOf(e.target.value)}>
          {[...dates].reverse().map((d) => (
            <option key={d} value={d}>
              {d}
            </option>
          ))}
        </select>
        <span className="text-xs text-muted-foreground">All tabs show the selected cut-off date; trends show the full history.</span>
      </div>

      <div className="grid gap-4 sm:grid-cols-3 lg:grid-cols-6">
        <StatCard label="LCR" value={`${formatNumber(lcr, 0)}%`} hint={`${dod >= 0 ? "+" : ""}${formatNumber(dod, 1)}pp vs previous day`} />
        <StatCard label="Status" value={<span style={{ color: STATUS[status] }}>{status}</span>} />
        <StatCard label="HQLA (CHF)" value={`${formatNumber(num(cur.HQLA_TOTAL) / 1e6, 0)}M`} />
        <StatCard label="Net outflows (CHF)" value={`${formatNumber(num(cur.NET_OUTFLOW) / 1e6, 0)}M`} />
        <StatCard label="Buffer (CHF)" value={`${formatNumber(num(cur.LCR_BUFFER_CHF) / 1e6, 0)}M`} />
        <StatCard label="Days < 100%" value={String(base.filter((r) => num(r.LCR_RATIO) < FAIL_LIMIT).length)} hint={`of ${base.length} days`} />
      </div>
      {status === "FAIL" && (
        <Alert variant="destructive">
          <AlertTitle>LCR below the 100% regulatory minimum</AlertTitle>
          <AlertDescription>FINMA notification and remediation plan required.</AlertDescription>
        </Alert>
      )}
      {status === "WARNING" && (
        <Alert>
          <AlertTitle>LCR between 100% and 110%</AlertTitle>
          <AlertDescription>Below the internal buffer target.</AlertDescription>
        </Alert>
      )}

      <Tabs tabs={[...TABS]} value={tab} onChange={setTab} />
      {tab === "overview" && <Overview base={base} events={ev} />}
      {tab === "hqla" && <HqlaTab cur={cur} />}
      {tab === "flows" && <FlowsTab cur={cur} base={base} />}
      {tab === "behavioral" && <BehavioralTab asOf={asOf} />}
      {tab === "scenarios" && <ScenariosTab rows={scen.data.rows} asOf={asOf} events={ev} />}
      {tab === "variance" && <VarianceTab asOf={asOf} />}
      {tab === "drilldown" && <DrilldownTab asOf={asOf} />}
      {tab === "currency" && <CurrencyTab asOf={asOf} />}
      {tab === "dq" && <DataQualityTab asOf={asOf} />}
      {tab === "simulator" && <SimulatorTab asOf={asOf} />}
      {tab === "reporting" && <ReportingTab />}
    </div>
  );
}

function Overview({ base, events }: { base: LoaderRow[]; events: LoaderRow[] }) {
  const data = base.map((r) => ({ d: String(r.AS_OF_DATE), lcr: num(r.LCR_RATIO), breach: num(r.LCR_RATIO) < FAIL_LIMIT ? num(r.LCR_RATIO) : null }));
  const yMax = Math.max(...data.map((d) => d.lcr));
  return (
    <div className="space-y-4">
      <Panel title="Regulatory LCR - 90 days" description="Status bands (FAIL < 100%, WARNING 100-110%) and the liquidity events">
        <Chart height={420}>
          <ComposedChart data={data} margin={{ top: 20, right: 16, bottom: 0, left: 0 }}>
            <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" />
            <XAxis dataKey="d" tick={CHART_THEME.tick} minTickGap={40} />
            <StatusBands yMax={yMax} />
            <EventLines events={events} />
            <Tooltip content={<ChartTooltip />} />
            <Legend />
            <Line type="monotone" dataKey="lcr" name="LCR %" stroke="var(--chart-1)" strokeWidth={2.5} dot={false} />
            <Line dataKey="breach" name="Breach" stroke="none" dot={{ r: 4, fill: STATUS.FAIL }} isAnimationActive={false} />
          </ComposedChart>
        </Chart>
      </Panel>
      {events.length > 0 && (
        <Panel title="Liquidity events">
          <DataTable
            rows={events}
            columns={["EVENT_ID", "EVENT_DATE", "EVENT_TYPE", "DESCRIPTION", { key: "AMOUNT_CHF", label: "Amount", render: (r) => chfM(r.AMOUNT_CHF), numeric: true }]}
          />
        </Panel>
      )}
    </div>
  );
}

function HqlaTab({ cur }: { cur: LoaderRow }) {
  const l1 = num(cur.L1_TOTAL);
  const l2a = num(cur.L2A_TOTAL);
  const l2b = num(cur.L2B_TOTAL);
  const l2bCapped = num(cur.L2B_CAPPED);
  const l2Capped = num(cur.L2_CAPPED);
  const hqla = num(cur.HQLA_TOTAL);
  const discarded = l2b - l2bCapped + (l2a + l2bCapped - l2Capped);
  const data = [{ name: "HQLA stock", l1: l1 / 1e6, l2a: l2a / 1e6, l2b: l2bCapped / 1e6, disc: discarded / 1e6 }];
  const holdings = useLoader("lcr.hqla-holdings");
  return (
    <div className="space-y-4">
      <div className="grid gap-4 lg:grid-cols-2">
        <Panel title="HQLA composition and regulatory caps" description="CHF M">
          <Chart height={380}>
            <BarChart data={data}>
              <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" />
              <XAxis dataKey="name" tick={CHART_THEME.tick} />
              <YAxis tick={CHART_THEME.tick} tickFormatter={formatTick} />
              <Tooltip content={<ChartTooltip />} />
              <Legend />
              <Bar dataKey="l1" stackId="h" name="Level 1" fill="var(--chart-1)" />
              <Bar dataKey="l2a" stackId="h" name="Level 2A" fill="var(--chart-2)" />
              <Bar dataKey="l2b" stackId="h" name="Level 2B (after 15% cap)" fill="var(--chart-3)" />
              <Bar dataKey="disc" stackId="h" name="Discarded by caps" fill="var(--muted)" />
              <ReferenceLine y={(l1 + (2 / 3) * l1) / 1e6} stroke="var(--muted-foreground)" strokeDasharray="3 3" label={{ value: "L2 40% cap", fontSize: 11, position: "insideTopRight" }} />
            </BarChart>
          </Chart>
        </Panel>
        <div className="grid content-start gap-4 sm:grid-cols-2 lg:grid-cols-1">
          <StatCard label="Level 2 share of HQLA" value={`${formatNumber(hqla ? (l2Capped / hqla) * 100 : 0, 1)}%`} hint="max 40%" />
          <StatCard label="Headroom to L2B 15% cap" value={chfM((15 / 85) * (l1 + l2a) - l2b)} />
          <StatCard label="Headroom to L2 40% cap" value={chfM((2 / 3) * l1 - (l2a + l2bCapped))} hint="L2B capped at 15/85 of (L1 + L2A); L2 capped at 2/3 of L1 (LiqV Annex 2)" />
        </div>
      </div>
      <Panel title="HQLA holdings by asset type" description="Latest reporting date">
        <LoaderState q={holdings}>{(rows) => <DataTable rows={rows} />}</LoaderState>
      </Panel>
    </div>
  );
}

function FlowsTab({ cur, base }: { cur: LoaderRow; base: LoaderRow[] }) {
  const seg = [
    { s: "Retail", v: num(cur.OUTFLOW_RETAIL) },
    { s: "Corporate", v: num(cur.OUTFLOW_CORP) },
    { s: "Financial institutions", v: num(cur.OUTFLOW_FI) },
    { s: "Committed facilities", v: num(cur.OUTFLOW_FACILITIES) },
  ];
  const trend = base.map((r) => ({ d: String(r.AS_OF_DATE), inflow: num(r.INFLOW_TOTAL) / 1e6, cap: (0.75 * num(r.OUTFLOW_TOTAL)) / 1e6 }));
  const out = num(cur.OUTFLOW_TOTAL);
  return (
    <div className="space-y-4">
      <div className="grid gap-4 lg:grid-cols-2">
        <Panel title="Stressed 30-day outflows by segment" description="CHF">
          <Chart>
            <BarChart data={seg} layout="vertical" margin={{ left: 40 }}>
              <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" />
              <XAxis type="number" tick={CHART_THEME.tick} tickFormatter={formatTick} />
              <YAxis type="category" dataKey="s" tick={CHART_THEME.tick} width={140} />
              <Tooltip content={<ChartTooltip />} />
              <Bar dataKey="v" name="Outflow" fill="var(--warning)" />
            </BarChart>
          </Chart>
        </Panel>
        <Panel title="Inflows vs. 75% inflow cap" description="CHF M">
          <Chart>
            <LineChart data={trend}>
              <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" />
              <XAxis dataKey="d" tick={CHART_THEME.tick} minTickGap={40} />
              <YAxis tick={CHART_THEME.tick} tickFormatter={formatTick} />
              <Tooltip content={<ChartTooltip />} />
              <Legend />
              <Line dataKey="inflow" name="Weighted inflows" stroke="var(--success)" dot={false} />
              <Line dataKey="cap" name="75% inflow cap" stroke="var(--destructive)" strokeDasharray="4 3" dot={false} />
            </LineChart>
          </Chart>
        </Panel>
      </div>
      <div className="grid gap-4 sm:grid-cols-4">
        <StatCard label="Gross outflows" value={chfM(out)} />
        <StatCard label="Inflows (weighted)" value={chfM(cur.INFLOW_TOTAL)} />
        <StatCard label="Inflows after cap" value={chfM(cur.INFLOW_CAPPED)} />
        <StatCard label="Cap utilisation" value={`${formatNumber(out ? (num(cur.INFLOW_TOTAL) / (0.75 * out)) * 100 : 0, 0)}%`} />
      </div>
    </div>
  );
}

function BehavioralTab({ asOf }: { asOf: string }) {
  const gap = useLoader("lcr.liquidity-gap", { date: asOf });
  return (
    <LoaderState q={gap} empty="No liquidity gap data for this date.">
      {(rows) => {
        const buckets = [...new Map(rows.map((r) => [String(r.BUCKET_NAME), num(r.SORT_ORDER)])).entries()].sort((a, b) => a[1] - b[1]).map(([b]) => b);
        const scens = SCENARIOS.map((s) => s.v).filter((s) => rows.some((r) => r.STRESS_SCENARIO === s));
        const cell = (s: string, b: string) => num(rows.find((r) => r.STRESS_SCENARIO === s && r.BUCKET_NAME === b)?.BEHAVIORAL_OUTFLOW_CHF) / 1e6;
        const max = Math.max(1, ...scens.flatMap((s) => buckets.map((b) => Math.abs(cell(s, b)))));
        const ladder = rows
          .filter((r) => r.STRESS_SCENARIO === "BASE")
          .sort((a, b) => num(a.SORT_ORDER) - num(b.SORT_ORDER))
          .map((r) => ({ b: String(r.BUCKET_NAME), gap: num(r.NET_GAP_CHF) / 1e6, cum: num(r.CUMULATIVE_GAP_CHF) / 1e6 }));
        return (
          <div className="space-y-4">
            <Panel title="Behavioral deposit run-down by maturity bucket" description="CHF M per scenario">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr>
                      <th className="p-2 text-left font-medium text-muted-foreground">Scenario</th>
                      {buckets.map((b) => (
                        <th key={b} className="p-2 text-right font-medium text-muted-foreground">
                          {b}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {scens.map((s) => (
                      <tr key={s}>
                        <td className="p-2">{SCEN_LABEL[s]}</td>
                        {buckets.map((b) => {
                          const v = cell(s, b);
                          return (
                            <td
                              key={b}
                              className="p-2 text-right tabular-nums"
                              style={{ background: `color-mix(in oklab, var(--warning) ${Math.round((Math.abs(v) / max) * 60)}%, transparent)` }}
                            >
                              {formatNumber(v, 0)}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Panel>
            <Panel title="Maturity ladder (BASE)" description="Contractual + behavioral, CHF M">
              <Chart>
                <ComposedChart data={ladder}>
                  <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" />
                  <XAxis dataKey="b" tick={CHART_THEME.tick} />
                  <YAxis tick={CHART_THEME.tick} tickFormatter={formatTick} />
                  <Tooltip content={<ChartTooltip />} />
                  <Legend />
                  <Bar dataKey="gap" name="Net gap">
                    {ladder.map((r) => (
                      <Cell key={r.b} fill={r.gap >= 0 ? "var(--success)" : "var(--destructive)"} />
                    ))}
                  </Bar>
                  <Line dataKey="cum" name="Cumulative gap incl. HQLA" stroke="var(--chart-1)" />
                </ComposedChart>
              </Chart>
            </Panel>
          </div>
        );
      }}
    </LoaderState>
  );
}

function ScenariosTab({ rows, asOf, events }: { rows: LoaderRow[]; asOf: string; events: LoaderRow[] }) {
  const byDate = new Map<string, Record<string, number | string>>();
  for (const r of rows) {
    const d = String(r.AS_OF_DATE);
    const o = byDate.get(d) ?? { d };
    o[String(r.STRESS_SCENARIO)] = num(r.LCR_RATIO);
    byDate.set(d, o);
  }
  const data = [...byDate.values()];
  const yMax = Math.max(...rows.map((r) => num(r.LCR_RATIO)));
  const today = rows
    .filter((r) => r.AS_OF_DATE === asOf)
    .map((r) => ({ Scenario: SCEN_LABEL[String(r.STRESS_SCENARIO)] ?? r.STRESS_SCENARIO, LCR_RATIO: r.LCR_RATIO, LCR_STATUS: r.LCR_STATUS, HQLA_TOTAL: r.HQLA_TOTAL, NET_OUTFLOW: r.NET_OUTFLOW, LCR_BUFFER_CHF: r.LCR_BUFFER_CHF }));
  return (
    <div className="space-y-4">
      <Panel title="LCR by scenario">
        <Chart height={420}>
          <LineChart data={data} margin={{ top: 20, right: 16 }}>
            <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" />
            <XAxis dataKey="d" tick={CHART_THEME.tick} minTickGap={40} />
            <StatusBands yMax={yMax} />
            <EventLines events={events} labels={false} />
            <Tooltip content={<ChartTooltip />} />
            <Legend />
            {SCENARIOS.map((s) => (
              <Line key={s.v} dataKey={s.v} name={s.label} stroke={SCEN_COLOR[s.v]} dot={false} strokeWidth={s.v === "BASE" ? 2.5 : 1.5} />
            ))}
          </LineChart>
        </Chart>
      </Panel>
      <Panel title={`Scenarios on ${asOf}`}>
        <DataTable rows={today} />
      </Panel>
    </div>
  );
}

function VarianceTab({ asOf }: { asOf: string }) {
  const [cmp, setCmp] = useState<"DOD" | "WOW" | "MOM">("DOD");
  const deltas = useLoader("lcr.cutoff-deltas", { date: asOf, comparison: cmp });
  const drivers = useLoader("lcr.variance-drivers", { date: asOf, comparison: cmp, scenario: "BASE" });
  return (
    <div className="space-y-4">
      <Segmented
        label="Comparison"
        value={cmp}
        onChange={setCmp}
        options={[
          { v: "DOD", label: "Day over day" },
          { v: "WOW", label: "Week over week" },
          { v: "MOM", label: "Month over month" },
        ]}
      />
      <LoaderState q={deltas} empty="No prior date available for this comparison.">
        {(rows) => {
          const total = rows.find((r) => r.ASSET_CLASS === "TOTAL");
          const drv = (drivers.data?.rows ?? [])
            .map((r) => ({ name: `${r.COUNTERPARTY_NAME} - ${r.POSITION_ID}`, v: num(r.CLIENT_IMPACT_PP) }))
            .sort((a, b) => a.v - b.v);
          return (
            <div className="space-y-4">
              {total && (
                <div className="grid gap-4 sm:grid-cols-3">
                  <StatCard label="LCR prior" value={`${formatNumber(num(total.LCR_PRIOR), 1)}%`} hint={String(total.PRIOR_DATE)} />
                  <StatCard label="LCR now" value={`${formatNumber(num(total.LCR_RATIO), 1)}%`} />
                  <StatCard label="Change" value={`${num(total.LCR_DELTA_PP) >= 0 ? "+" : ""}${formatNumber(num(total.LCR_DELTA_PP), 2)}pp`} />
                </div>
              )}
              {drv.length > 0 && (
                <Panel title="Top-10 client drivers" description="Funding-adjusted impact in percentage points">
                  <Chart height={420}>
                    <BarChart data={drv} layout="vertical" margin={{ left: 40 }}>
                      <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" />
                      <XAxis type="number" tick={CHART_THEME.tick} />
                      <YAxis type="category" dataKey="name" tick={CHART_THEME.tick} width={260} />
                      <Tooltip content={<ChartTooltip />} />
                      <Bar dataKey="v" name="Impact (pp)">
                        {drv.map((d) => (
                          <Cell key={d.name} fill={d.v < 0 ? STATUS.FAIL : STATUS.PASS} />
                        ))}
                      </Bar>
                    </BarChart>
                  </Chart>
                  <DataTable rows={drivers.data?.rows ?? []} />
                </Panel>
              )}
              <Panel title="Deltas by asset class">
                <DataTable rows={rows.filter((r) => r.ASSET_CLASS !== "TOTAL")} />
              </Panel>
            </div>
          );
        }}
      </LoaderState>
    </div>
  );
}

/** Streamlit px.treemap path ASSET_CLASS > PRODUCT_GROUP > COUNTERPARTY_NAME, values = WEIGHTED_CHF. */
const TOP_COUNTERPARTIES = 15;
type TreeLeaf = {
  name: string;
  size: number;
  treatment: string;
};
type TreeNode = {
  name: string;
  children: (TreeNode | TreeLeaf)[];
};

function buildTreemap(rows: LoaderRow[]): TreeNode[] {
  const tree = new Map<string, Map<string, Map<string, TreeLeaf>>>();
  for (const r of rows) {
    const ac = String(r.ASSET_CLASS);
    const pg = String(r.PRODUCT_GROUP);
    const cp = typeof r.COUNTERPARTY_NAME === "string" && r.COUNTERPARTY_NAME ? r.COUNTERPARTY_NAME : "n/a";
    const pgs = tree.get(ac) ?? new Map<string, Map<string, TreeLeaf>>();
    const cps = pgs.get(pg) ?? new Map<string, TreeLeaf>();
    const leaf = cps.get(cp) ?? { name: cp, size: 0, treatment: String(r.LCR_TREATMENT) };
    leaf.size += num(r.WEIGHTED_CHF);
    cps.set(cp, leaf);
    pgs.set(pg, cps);
    tree.set(ac, pgs);
  }
  return [...tree].map(([ac, pgs]) => ({
    name: ac,
    children: [...pgs].map(([pg, cps]) => {
      const leaves = [...cps.values()].sort((a, b) => b.size - a.size);
      const rest = leaves.slice(TOP_COUNTERPARTIES);
      const other = rest.length
        ? [{ name: `Other (${rest.length})`, size: rest.reduce((s, l) => s + l.size, 0), treatment: rest[0].treatment }]
        : [];
      return { name: pg, children: [...leaves.slice(0, TOP_COUNTERPARTIES), ...other] };
    }),
  }));
}

interface TreemapCellProps {
  x?: number;
  y?: number;
  width?: number;
  height?: number;
  depth?: number;
  name?: string;
  treatment?: string;
}

/** Treemap rectangle: leaves coloured by LCR treatment and labelled where they fit; asset class and
 *  product group show as thicker borders (labels of all levels would overlap in the corners). */
function TreemapCell({ x = 0, y = 0, width = 0, height = 0, depth = 0, name = "", treatment = "" }: Readonly<TreemapCellProps>) {
  const leaf = depth === 3;
  const showLabel = leaf && width > 60 && height > 18;
  return (
    <g>
      <rect
        x={x}
        y={y}
        width={width}
        height={height}
        fill={leaf ? (TREATMENT_COLOR[treatment] ?? "var(--success)") : "none"}
        fillOpacity={leaf ? 0.75 : 0}
        stroke="var(--background)"
        strokeWidth={leaf ? 1 : 3 - depth}
      />
      {showLabel && (
        <text x={x + 4} y={y + 14} fontSize={10} fill="var(--foreground)">
          {name.length > width / 7 ? `${name.slice(0, Math.max(3, Math.floor(width / 7) - 1))}…` : name}
        </text>
      )}
    </g>
  );
}

function DrilldownTab({ asOf }: { asOf: string }) {
  const [scenario, setScenario] = useState<Scenario>("BASE");
  const [bus, setBus] = useState<string[]>([]);
  const [assetClass, setAssetClass] = useState<string | null>(null);
  const dd = useLoader("lcr.drilldown", { date: asOf, scenario });
  const rows = dd.data?.rows ?? [];
  const units = [...new Set(rows.map((r) => (typeof r.BUSINESS_UNIT === "string" ? r.BUSINESS_UNIT : "")).filter(Boolean))].sort((a, b) => a.localeCompare(b));
  // multiselect as in Streamlit; nothing selected = all business units
  const view = rows.filter((r) => (bus.length === 0 || bus.includes(String(r.BUSINESS_UNIT))) && num(r.WEIGHTED_CHF) > 0);
  const toggleBu = (u: string) => setBus((s) => (s.includes(u) ? s.filter((x) => x !== u) : [...s, u]));
  const classes = [...new Set(view.map((r) => String(r.ASSET_CLASS)))].sort((a, b) => a.localeCompare(b));
  const ac = assetClass && classes.includes(assetClass) ? assetClass : (classes[0] ?? null);
  const positions = useLoader("lcr.drilldown-positions", { date: asOf, scenario, assetClass: ac });
  // asset class > product group (the treemap of the Streamlit page as a ranked breakdown)
  const groups = new Map<string, { ac: string; pg: string; treatment: string; v: number; n: number }>();
  for (const r of view) {
    const k = `${r.ASSET_CLASS}|${r.PRODUCT_GROUP}`;
    const g = groups.get(k) ?? { ac: String(r.ASSET_CLASS), pg: String(r.PRODUCT_GROUP), treatment: String(r.LCR_TREATMENT), v: 0, n: 0 };
    g.v += num(r.WEIGHTED_CHF);
    g.n += num(r.POSITIONS);
    groups.set(k, g);
  }
  const ranked = [...groups.values()].sort((a, b) => b.v - a.v);
  const top = ranked[0]?.v ?? 1;
  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-3">
        <select aria-label="Scenario" className="h-8 rounded-md border bg-background px-2 text-sm" value={scenario} onChange={(e) => setScenario(e.target.value as Scenario)}>
          {SCENARIOS.map((s) => (
            <option key={s.v} value={s.v}>
              {s.label}
            </option>
          ))}
        </select>
        <fieldset className="m-0 flex min-w-0 flex-wrap items-center gap-1 border-0 p-0" aria-label="Business unit">
          <span className="mr-1 text-sm text-muted-foreground">Business units:</span>
          {units.map((u) => (
            <button
              key={u}
              type="button"
              aria-pressed={bus.includes(u)}
              onClick={() => toggleBu(u)}
              className={`rounded-md border px-2 py-1 text-xs ${bus.includes(u) ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:bg-muted"}`}
            >
              {u}
            </button>
          ))}
          {bus.length > 0 && (
            <button type="button" onClick={() => setBus([])} className="px-1 text-xs text-primary hover:underline">
              all
            </button>
          )}
        </fieldset>
      </div>
      <LoaderState q={dd} empty="No drill-down data.">
        {() => (
          <Panel
            title="Treemap: asset class > product group > counterparty"
            description="Area = weighted CHF, colour = LCR treatment (top 15 counterparties per product group)"
          >
            <ResponsiveContainer width="100%" height={420}>
              <Treemap data={buildTreemap(view)} dataKey="size" nameKey="name" isAnimationActive={false} content={<TreemapCell />}>
                <Tooltip content={<ChartTooltip />} />
              </Treemap>
            </ResponsiveContainer>
          </Panel>
        )}
      </LoaderState>
      <LoaderState q={dd} empty="No drill-down data.">
        {() => (
          <Panel
            title="Weighted LCR amounts: asset class > product group"
            description={`${formatNumber(rows.reduce((s, r) => s + num(r.POSITIONS), 0), 0)} positions aggregated in ${dd.data?.ms ?? 0} ms`}
          >
            <div className="space-y-1.5">
              {ranked.map((g) => (
                <button
                  key={`${g.ac}|${g.pg}`}
                  type="button"
                  onClick={() => setAssetClass(g.ac)}
                  className="grid w-full grid-cols-[14rem_1fr_7rem] items-center gap-3 text-left text-sm hover:text-foreground"
                >
                  <span className="truncate">
                    <span className={g.ac === ac ? "font-semibold" : undefined}>{g.ac}</span>
                    <span className="text-muted-foreground"> › {g.pg}</span>
                  </span>
                  <span className="h-3 rounded-sm" style={{ width: `${Math.max(1, (g.v / top) * 100)}%`, background: TREATMENT_COLOR[g.treatment] ?? "var(--success)" }} />
                  <span className="text-right tabular-nums">{chfM(g.v)}</span>
                </button>
              ))}
            </div>
          </Panel>
        )}
      </LoaderState>
      {ac && (
        <Panel title={`Contract level: ${ac}`} description="Top 500 positions by weighted amount">
          <LoaderState q={positions}>{(r) => <DataTable rows={r} />}</LoaderState>
        </Panel>
      )}
    </div>
  );
}

function CurrencyTab({ asOf }: { asOf: string }) {
  const dd = useLoader("lcr.drilldown", { date: asOf, scenario: "BASE" });
  return (
    <LoaderState q={dd}>
      {(rows) => {
        const treatments = [...new Set(rows.map((r) => String(r.LCR_TREATMENT)))].sort((a, b) => a.localeCompare(b));
        const by = new Map<string, Record<string, number | string>>();
        for (const r of rows) {
          const o = by.get(String(r.CURRENCY)) ?? { ccy: String(r.CURRENCY) };
          o[String(r.LCR_TREATMENT)] = num(o[String(r.LCR_TREATMENT)]) + num(r.WEIGHTED_CHF);
          by.set(String(r.CURRENCY), o);
        }
        const data = [...by.values()];
        const table = data.map((o) => ({
          ...o,
          HQLA_PER_OUTFLOW: num(o.OUTFLOW) > 0 ? Math.round((num(o.HQLA) / num(o.OUTFLOW)) * 100) / 100 : null,
        }));
        return (
          <div className="space-y-4">
            <Panel title="Weighted LCR amounts by currency" description="CHF, end-of-day FX">
              <Chart height={400}>
                <BarChart data={data}>
                  <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" />
                  <XAxis dataKey="ccy" tick={CHART_THEME.tick} />
                  <YAxis tick={CHART_THEME.tick} tickFormatter={formatTick} />
                  <Tooltip content={<ChartTooltip />} />
                  <Legend />
                  {treatments.map((t, i) => (
                    <Bar key={t} dataKey={t} name={t} fill={`var(--chart-${(i % 5) + 1})`} />
                  ))}
                </BarChart>
              </Chart>
            </Panel>
            <Panel title="By currency">
              <DataTable rows={table} />
            </Panel>
          </div>
        );
      }}
    </LoaderState>
  );
}

function DataQualityTab({ asOf }: { asOf: string }) {
  const exc = useLoader("lcr.exceptions", { date: asOf });
  if (exc.isPending) return <Skeleton className="h-48 w-full" />;
  if (exc.isError) return <EmptyState variant="error" title="Exceptions failed" description={exc.error.message} />;
  const rows = exc.data.rows;
  if (rows.length === 0) return <EmptyState title="No quarantined records for this date." />;
  const byCode = new Map<string, number>();
  for (const r of rows) byCode.set(String(r.ERROR_CODE), (byCode.get(String(r.ERROR_CODE)) ?? 0) + 1);
  return (
    <div className="space-y-4">
      <div className="grid gap-4 sm:grid-cols-4">
        <StatCard label="Quarantined records" value={String(rows.length)} />
        {[...byCode.entries()].map(([c, n]) => (
          <StatCard key={c} label={c} value={String(n)} />
        ))}
      </div>
      <Panel title="Quarantined records" description="Invalid records are excluded from all LCR figures and listed here for remediation (REQ-DQ-01).">
        <DataTable rows={rows} />
      </Panel>
    </div>
  );
}

const LEVERS: { key: string; label: string; min: number; max: number; step: number; def: number; group: string }[] = [
  { key: "retail_stable_mult", label: "Retail stable ×", min: 0.5, max: 3, step: 0.1, def: 1, group: "Deposit run-off" },
  { key: "retail_less_stable_mult", label: "Retail less stable ×", min: 0.5, max: 3, step: 0.1, def: 1, group: "Deposit run-off" },
  { key: "corp_operational_mult", label: "Corporate operational ×", min: 0.5, max: 3, step: 0.1, def: 1, group: "Deposit run-off" },
  { key: "corp_non_operational_mult", label: "Corporate non-operational ×", min: 0.5, max: 2.5, step: 0.1, def: 1, group: "Deposit run-off" },
  { key: "fi_mult", label: "Financial institutions ×", min: 0.5, max: 1, step: 0.05, def: 1, group: "Funding & flows" },
  { key: "facilities_mult", label: "Facility drawdowns ×", min: 0.5, max: 4, step: 0.1, def: 1, group: "Funding & flows" },
  { key: "inflow_mult", label: "Inflows ×", min: 0, max: 1.5, step: 0.05, def: 1, group: "Funding & flows" },
  { key: "inflow_cap_pct", label: "Inflow cap", min: 0, max: 0.75, step: 0.05, def: 0.75, group: "Funding & flows" },
  { key: "haircut_addon_l1_pp", label: "L1 haircut add-on (pp)", min: 0, max: 20, step: 1, def: 0, group: "HQLA & shocks" },
  { key: "haircut_addon_l2a_pp", label: "L2A haircut add-on (pp)", min: 0, max: 40, step: 1, def: 0, group: "HQLA & shocks" },
  { key: "haircut_addon_l2b_pp", label: "L2B haircut add-on (pp)", min: 0, max: 40, step: 1, def: 0, group: "HQLA & shocks" },
  { key: "price_shock_pct", label: "Market price shock (%)", min: 0, max: 30, step: 1, def: 0, group: "HQLA & shocks" },
  { key: "withdrawal_m", label: "Corporate withdrawal (CHF M)", min: 0, max: 2000, step: 10, def: 0, group: "HQLA & shocks" },
];

function SimulatorTab({ asOf }: { asOf: string }) {
  const [preset, setPreset] = useState<Scenario>("BASE");
  const [levers, setLevers] = useState<Record<string, number>>(() => Object.fromEntries(LEVERS.map((l) => [l.key, l.def])));
  // debounce slider changes, then run the simulator procedure
  const [applied, setApplied] = useState(levers);
  useEffect(() => {
    const t = setTimeout(() => setApplied(levers), 400);
    return () => clearTimeout(t);
  }, [levers]);
  const params = useMemo(() => {
    const { withdrawal_m, ...rest } = applied;
    const keys = { preset, ...rest, withdrawal_chf: withdrawal_m * 1e6 } as Record<string, unknown>;
    return JSON.stringify(Object.fromEntries(Object.entries(keys).sort(([a], [b]) => a.localeCompare(b))));
  }, [applied, preset]);
  const sim = useLoader("lcr.simulate", { date: asOf, params }, { staleTime: 10 * 60_000 });
  const rows = (sim.data?.rows ?? []).map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k.replace(/"/g, "").toUpperCase(), v])));
  const first = rows[0];
  const last = rows[rows.length - 1];
  // waterfall: invisible base + signed step, so each bar floats from the previous level
  let level = num(first?.LCR_RATIO);
  const steps = rows.filter((r) => num(r.STEP_ORDER) > 0);
  const wf = [
    { name: "Baseline", base: 0, up: level, down: 0, total: true },
    ...steps.map((s) => {
      const d = num(s.DELTA_PP);
      const from = level;
      level += d;
      return { name: String(s.STEP_NAME).replace(/^'?\+ /, ""), base: Math.min(from, level), up: d > 0 ? d : 0, down: d < 0 ? -d : 0, total: false };
    }),
    { name: "Simulated", base: 0, up: num(last?.LCR_RATIO), down: 0, total: true },
  ];
  const groups = [...new Set(LEVERS.map((l) => l.group))];
  return (
    <div className="space-y-4">
      <p className="text-sm text-muted-foreground">
        Recalculates the regulatory LCR with the official formula (L2B 15% cap, L2 40% cap, 75% inflow cap). Levers apply on top of the selected preset.
      </p>
      <Segmented label="Preset" value={preset} onChange={setPreset} options={SCENARIOS.map((s) => ({ v: s.v, label: s.label }))} />
      <div className="grid gap-4 md:grid-cols-3">
        {groups.map((g) => (
          <Panel key={g} title={g}>
            <div className="space-y-3">
              {LEVERS.filter((l) => l.group === g).map((l) => (
                <label key={l.key} className="block text-sm">
                  <span className="flex justify-between text-muted-foreground">
                    {l.label}
                    <span className="tabular-nums text-foreground">{formatNumber(levers[l.key], 2)}</span>
                  </span>
                  <input
                    type="range"
                    aria-label={l.label}
                    min={l.min}
                    max={l.max}
                    step={l.step}
                    value={levers[l.key]}
                    onChange={(e) => setLevers({ ...levers, [l.key]: Number(e.target.value) })}
                    className="w-full accent-[var(--primary)]"
                  />
                </label>
              ))}
            </div>
          </Panel>
        ))}
      </div>
      <StateView
        pending={!first && !sim.isError}
        error={sim.error}
        errorTitle="Simulation failed"
        skeleton={<Skeleton className="h-64 w-full" />}
      >
        {() => first && (
        <>
          <div className="grid gap-4 sm:grid-cols-4">
            <StatCard label="Baseline LCR" value={`${formatNumber(num(first.LCR_RATIO), 1)}%`} />
            <StatCard
              label="Simulated LCR"
              value={`${formatNumber(num(last.LCR_RATIO), 1)}%`}
              hint={`${signedPp(num(last.LCR_RATIO) - num(first.LCR_RATIO))}pp`}
            />
            <StatCard label="Status" value={<span style={{ color: STATUS[statusOf(num(last.LCR_RATIO))] }}>{String(last.LCR_STATUS)}</span>} />
            <StatCard label="Extra withdrawal to 100%" value={last.BREAKEVEN_WITHDRAWAL_CHF === null || last.BREAKEVEN_WITHDRAWAL_CHF === undefined ? "n/a" : chfM(last.BREAKEVEN_WITHDRAWAL_CHF)} />
          </div>
          <Panel title="What-if waterfall" description="LCR in %">
            <Chart height={420}>
              <BarChart data={wf}>
                <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" />
                <XAxis dataKey="name" tick={CHART_THEME.tick} interval={0} angle={-20} textAnchor="end" height={70} />
                <YAxis tick={CHART_THEME.tick} tickFormatter={(v) => `${v}%`} />
                <Tooltip content={<ChartTooltip />} />
                <ReferenceLine y={FAIL_LIMIT} stroke={STATUS.FAIL} strokeDasharray="3 3" />
                <Bar dataKey="base" stackId="w" fill="transparent" isAnimationActive={false} />
                <Bar dataKey="up" stackId="w" name="Increase">
                  {wf.map((w) => (
                    <Cell key={w.name} fill={w.total ? "var(--chart-1)" : STATUS.PASS} />
                  ))}
                </Bar>
                <Bar dataKey="down" stackId="w" name="Decrease" fill={STATUS.FAIL} />
              </BarChart>
            </Chart>
          </Panel>
        </>
        )}
      </StateView>
    </div>
  );
}

/** "+1.2" / "-0.7" percentage points, one decimal. */
function signedPp(d: number): string {
  return `${d >= 0 ? "+" : ""}${formatNumber(d, 1)}`;
}

function ReportingTab() {
  const monthly = useLoader("lcr.monthly-summary");
  const alerts = useLoader("lcr.alerts");
  return (
    <div className="space-y-4">
      <Panel title="Monthly SNB summary">
        <LoaderState q={monthly}>{(rows) => <DataTable rows={rows} />}</LoaderState>
      </Panel>
      <Panel title="Alerts">
        {alerts.data?.rows.length === 0 ? <EmptyState title="No active alerts." /> : <LoaderState q={alerts}>{(rows) => <DataTable rows={rows} />}</LoaderState>}
      </Panel>
    </div>
  );
}
