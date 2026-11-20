"use client";

import { LoaderState, Tabs } from "@/components/data/data-table";
import { Bridge, bridgeSteps } from "@/components/finance/bridge";
import { fmtDeltaM, fmtM, ILLUSTRATIVE_NOTE, monthLabel, Segmented, entityNames, sumWhere } from "@/components/finance/common";
import { ConsolidationSheet, type CcyView, type Statement } from "@/components/finance/consolidation-sheet";
import { checkView, Controls } from "@/components/finance/controls";
import { Method } from "@/components/finance/method";
import { PageHeader } from "@/components/page-header";
import { StatCard, StatCardSkeleton } from "@/components/stat-card";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { formatDateTime } from "@/lib/format";
import { useLoader, type LoaderRow } from "@/lib/loaders/client";
import { Info } from "lucide-react";
import { useState } from "react";

const TABS = [
  { v: "bridge", label: "Bridge" },
  { v: "sheet", label: "Consolidation sheet" },
  { v: "controls", label: "Controls" },
  { v: "method", label: "Method" },
] as const;
type Tab = (typeof TABS)[number]["v"];

export default function ConsolidationPage() {
  const periods = useLoader("finance.periods");
  const [month, setMonth] = useState<string | null>(null);
  const current = month ?? (periods.data?.rows[0]?.PERIOD_MONTH as string | undefined) ?? null;
  const periodDate = periods.data?.rows.find((r) => r.PERIOD_MONTH === current)?.PERIOD_DATE as string | undefined;

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="Financial Control"
        title="Group Consolidation"
        description="What drove the change in group profit and assets versus last month, and how much came from entity performance, FX and intercompany eliminations?"
        actions={
          <label className="flex items-center gap-2 text-sm">
            <span className="text-muted-foreground">Month</span>
            <select
              aria-label="Reporting month"
              className="h-8 rounded-md border bg-background px-2 text-sm"
              value={current ?? ""}
              onChange={(e) => setMonth(e.target.value)}
            >
              {(periods.data?.rows ?? []).map((p) => (
                <option key={String(p.PERIOD_MONTH)} value={String(p.PERIOD_MONTH)}>
                  {monthLabel(p.PERIOD_MONTH)}
                </option>
              ))}
            </select>
          </label>
        }
      />
      <Banner periodDate={periodDate} />
      {current ? <Content month={current} /> : <StatCardSkeleton />}
    </div>
  );
}

function Banner({ periodDate }: Readonly<{ periodDate?: string }>) {
  const fresh = useLoader("finance.freshness");
  const refreshed = fresh.data?.rows[0]?.REFRESHED_AT;
  return (
    <Alert className="border-warning/50 bg-warning/10" data-testid="illustrative-banner">
      <Info className="h-4 w-4" />
      <AlertDescription className="flex flex-wrap justify-between gap-2 text-sm">
        <span className="font-medium">{ILLUSTRATIVE_NOTE}</span>
        <span className="text-muted-foreground">
          Reporting date {periodDate ?? "…"}
          {refreshed ? ` · data refreshed ${formatDateTime(refreshed)}` : ""}
        </span>
      </AlertDescription>
    </Alert>
  );
}

function Content({ month }: Readonly<{ month: string }>) {
  const [tab, setTab] = useState<Tab>("bridge");
  const [statement, setStatement] = useState<Statement>("BS");
  const [ccy, setCcy] = useState<CcyView>("CHF");
  const [simulate, setSimulate] = useState(false);
  const p = { month };
  const entities = useLoader("finance.entities");
  const cons = useLoader("finance.consolidation", p);
  const trail = useLoader("finance.trail", p);
  const bridge = useLoader("finance.bridge", p);
  const checks = useLoader("finance.checks", p);
  const ic = useLoader("finance.ic", p);
  const translation = useLoader("finance.translation", p);
  const mix = useLoader("finance.currency-mix", p);
  const names = entityNames(entities.data?.rows ?? []);

  return (
    <div className="space-y-6">
      <Kpis cons={cons.data?.rows} bridge={bridge.data?.rows ?? []} checks={checks.data?.rows} simulate={simulate} />
      <Tabs tabs={[...TABS]} value={tab} onChange={setTab} />
      {tab === "bridge" && (
        <LoaderState q={bridge} empty="No comparison month: this is the first reporting month.">
          {(rows) => <Bridge rows={rows} names={names} />}
        </LoaderState>
      )}
      {tab === "sheet" && (
        <div className="space-y-3">
          <div className="flex flex-wrap gap-3">
            <Segmented
              label="Statement"
              options={[
                { v: "BS", label: "Balance sheet" },
                { v: "PL", label: "P&L" },
              ]}
              value={statement}
              onChange={setStatement}
            />
            <Segmented
              label="Currency view"
              options={[
                { v: "CHF", label: "CHF" },
                { v: "LCY", label: "Local currency" },
              ]}
              value={ccy}
              onChange={setCcy}
            />
          </div>
          <LoaderState q={cons} empty="No consolidation data for this month.">
            {(rows) => (
              <ConsolidationSheet
                rows={rows}
                trail={trail.data?.rows ?? []}
                entities={entities.data?.rows ?? []}
                statement={statement}
                ccy={ccy}
                period={month}
              />
            )}
          </LoaderState>
        </div>
      )}
      {tab === "controls" && (
        <LoaderState q={checks} empty="No controls for this month.">
          {(rows) => <Controls checks={rows} ic={ic.data?.rows ?? []} simulate={simulate} onSimulate={setSimulate} />}
        </LoaderState>
      )}
      {tab === "method" && (
        <LoaderState q={translation} empty="No translation data for this month.">
          {(rows) => <Method translation={rows} mix={mix.data?.rows ?? []} names={names} entities={entities.data?.rows ?? []} />}
        </LoaderState>
      )}
      {tab === "sheet" && (
        <p className="text-xs text-muted-foreground">
          Group = Σ entities + IC eliminations + NCI. Entities are fully consolidated; Wealth CH (80 %) and Fund Services LU (75 %) carry NCI.
        </p>
      )}
    </div>
  );
}

function Kpis({
  cons,
  bridge,
  checks,
  simulate,
}: Readonly<{ cons?: LoaderRow[]; bridge: LoaderRow[]; checks?: LoaderRow[]; simulate: boolean }>) {
  if (!cons || !checks) {
    return (
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
        {Array.from({ length: 6 }, (_, i) => (
          <StatCardSkeleton key={i} />
        ))}
      </div>
    );
  }
  const group = (t: (r: LoaderRow) => boolean) => sumWhere(cons, (r) => r.COL === "GROUP" && t(r));
  const profit = group((r) => r.STATEMENT === "PL");
  const assets = group((r) => r.SECTION === "ASSETS");
  const cta = group((r) => r.FS_LINE === "CTA");
  const icElim = Math.abs(sumWhere(cons, (r) => r.COL === "IC_ELIM" && r.FS_LINE === "IC_RECEIVABLES"));
  const delta = (m: "NET_PROFIT" | "TOTAL_ASSETS") => {
    const s = bridgeSteps(bridge, m);
    return bridge.length ? (s.at(-1)?.amount ?? 0) - s[0].amount : null;
  };
  const fx = bridgeSteps(bridge, "TOTAL_ASSETS").find((s) => s.component === "FX_EFFECT")?.amount ?? 0;
  const passed = checks.filter((r) => checkView(r, simulate).status === "PASS").length;
  const dp = delta("NET_PROFIT");
  const da = delta("TOTAL_ASSETS");
  const kpis = [
    { label: "Group net profit", value: `CHF ${fmtM(profit, 2)}M`, hint: dp === null ? "month, attributable" : `${fmtDeltaM(dp, 2)}M vs last month` },
    { label: "Group total assets", value: `CHF ${fmtM(assets)}M`, hint: da === null ? "after eliminations" : `${fmtDeltaM(da)}M vs last month` },
    { label: "FX effect on assets", value: `CHF ${fmtDeltaM(fx)}M`, hint: "rate change vs last month" },
    { label: "IC eliminated", value: `CHF ${fmtM(icElim)}M`, hint: "receivables = payables" },
    { label: "Translation reserve", value: `CHF ${fmtM(cta, 2)}M`, hint: "CTA, group equity" },
    { label: "Controls passed", value: `${passed} / ${checks.length}`, hint: passed === checks.length ? "ready for sign-off" : "exception open" },
  ];
  return (
    <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6" data-testid="consolidation-kpis">
      {kpis.map((k) => (
        <StatCard key={k.label} label={k.label} value={k.value} hint={k.hint} className={k.label === "Controls passed" && passed < checks.length ? "border-destructive" : undefined} />
      ))}
    </div>
  );
}
