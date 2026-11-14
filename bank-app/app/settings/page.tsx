"use client";

import { DataTable, LoaderState, Panel, Tabs } from "@/components/data/data-table";
import { EmptyState } from "@/components/empty-state";
import { PageHeader } from "@/components/page-header";
import { StatCard } from "@/components/stat-card";
import { ThemeToggle } from "@/components/theme-toggle";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { formatNumber } from "@/lib/format";
import { num, useLoader } from "@/lib/loaders/client";
import { useQuery, useQueryClient, type UseQueryResult } from "@tanstack/react-query";
import { useState } from "react";

interface Health {
  status: string;
  runtime: string;
  snowflake?: { ACCOUNT: string; ROLE: string; WAREHOUSE: string };
  error?: string;
}
interface DtiMe {
  role?: string;
  dtiRole?: string;
  canExport?: boolean;
  canEdit?: boolean;
}

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url, { cache: "no-store" });
  const body = await res.json().catch(() => ({}));
  if (!res.ok && !body.status) throw new Error(body.error ?? `HTTP ${res.status}`);
  return body as T;
}

const TABS = [
  { v: "quality", label: "Data Quality" },
  { v: "settings", label: "Settings" },
] as const;
type Tab = (typeof TABS)[number]["v"];

/** Settings & Data Quality - port of the_bank_app/pages/settings.py. */
export default function SettingsPage() {
  const [tab, setTab] = useState<Tab>("quality");
  return (
    <div className="space-y-6">
      <PageHeader eyebrow="Settings" title="Settings & Data Quality" description="Connection, configuration and data quality" />
      <Tabs tabs={[...TABS]} value={tab} onChange={setTab} />
      {tab === "quality" ? <QualityTab /> : <SettingsTab />}
    </div>
  );
}

function QualityTab() {
  const q = useLoader("settings.data-quality");
  return (
    <LoaderState q={q} empty="No data quality metrics available">
      {(rows) => {
        const m = rows[0];
        const fields = [
          ["Email", "EMAIL_COMPLETENESS", "MISSING_EMAIL"],
          ["Phone", "PHONE_COMPLETENESS", "MISSING_PHONE"],
          ["Date of birth", "DOB_COMPLETENESS", "MISSING_DOB"],
          ["Address", "ADDRESS_COMPLETENESS", "MISSING_ADDRESS"],
        ] as const;
        const tone = (p: number) => {
          if (p > 95) return "var(--success)";
          return p >= 80 ? "var(--warning)" : "var(--destructive)";
        };
        return (
          <div className="space-y-6">
            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
              <StatCard label="Total Records" value={formatNumber(num(m.TOTAL_RECORDS), 0)} />
              <StatCard label="Unique Customers" value={formatNumber(num(m.UNIQUE_CUSTOMERS), 0)} />
              <StatCard label="Email Completeness" value={`${formatNumber(num(m.EMAIL_COMPLETENESS), 1)}%`} />
              <StatCard label="Phone Completeness" value={`${formatNumber(num(m.PHONE_COMPLETENESS), 1)}%`} />
            </div>
            <Panel title="Field completeness" description="Share of customer 360 records with a value (CRMA_AGG_VW_CUSTOMER_360_ENRICHED)">
              <div className="space-y-3">
                {fields.map(([label, pct]) => {
                  const p = num(m[pct]);
                  return (
                    <div key={label} className="space-y-1">
                      <div className="flex justify-between text-sm">
                        <span>{label}</span>
                        <span className="tabular-nums">{formatNumber(p, 2)}%</span>
                      </div>
                      <div className="h-2 rounded bg-muted">
                        <div className="h-2 rounded" style={{ width: `${Math.min(100, p)}%`, background: tone(p) }} />
                      </div>
                    </div>
                  );
                })}
              </div>
            </Panel>
            <Panel title="Missing Data Analysis">
              <DataTable
                rows={fields.map(([label, pct, miss]) => ({ FIELD: label, MISSING_RECORDS: num(m[miss]), COMPLETENESS_PCT: num(m[pct]) }))}
              />
            </Panel>
            <Alert>
              <AlertTitle>Quality Thresholds</AlertTitle>
              <AlertDescription>
                Excellent: &gt;95% completeness · Acceptable: 80-95% completeness · Poor: &lt;80% completeness (requires remediation)
              </AlertDescription>
            </Alert>
          </div>
        );
      }}
    </LoaderState>
  );
}

function Row({ k, v }: { k: string; v: React.ReactNode }) {
  return (
    <div className="flex justify-between gap-4 border-b py-1.5 text-sm last:border-0">
      <span className="text-muted-foreground">{k}</span>
      <span className="font-medium">{v ?? "-"}</span>
    </div>
  );
}

function ConnectionBody({
  health,
  s,
}: Readonly<{ health: UseQueryResult<Health>; s: Record<string, unknown> | undefined }>) {
  if (health.isPending) return <Skeleton className="h-40 w-full" />;
  if (health.isError || health.data?.status !== "ok") {
    return <EmptyState variant="error" title="Not connected to Snowflake" description={health.error?.message ?? health.data?.error} />;
  }
  const sf = health.data.snowflake;
  return (
    <>
      <div className="mb-2"><Badge>Connected to Snowflake</Badge></div>
      <Row k="Account" v={sf?.ACCOUNT} />
      <Row k="Role" v={sf?.ROLE} />
      <Row k="Warehouse" v={sf?.WAREHOUSE} />
      <Row k="Runtime" v={health.data.runtime === "spcs" ? "SPCS (container service)" : "Local"} />
      {s && (
        <>
          <Row k="User" v={String(s.USER_NAME)} />
          <Row k="Database" v={String(s.DATABASE_NAME)} />
          <Row k="Schema" v={String(s.SCHEMA_NAME ?? "-")} />
          <Row k="Snowflake version" v={String(s.SNOWFLAKE_VERSION)} />
        </>
      )}
    </>
  );
}

function AccessBody({ me }: Readonly<{ me: UseQueryResult<DtiMe> }>) {
  if (me.isPending) return <Skeleton className="h-24 w-full" />;
  if (me.isError) return <EmptyState variant="error" title="Access info unavailable" description={me.error.message} />;
  return (
    <>
      <Row k="Viewer role" v={me.data.role} />
      <Row k="DtI role" v={me.data.dtiRole} />
      <Row k="Can export" v={me.data.canExport ? "Yes" : "No"} />
      <Row k="Can edit" v={me.data.canEdit ? "Yes" : "No"} />
    </>
  );
}

function SettingsTab() {
  const qc = useQueryClient();
  const [cleared, setCleared] = useState<string | null>(null);
  const health = useQuery({ queryKey: ["settings", "health"], queryFn: () => getJson<Health>("/api/health?deep=1"), staleTime: 0 });
  const me = useQuery({ queryKey: ["settings", "dti-me"], queryFn: () => getJson<DtiMe>("/api/dti/me") });
  const session = useLoader("settings.session", {}, { staleTime: 0 });
  const s = session.data?.rows[0];

  return (
    <div className="grid gap-6 lg:grid-cols-2">
      <Panel title="Data Freshness" description="Loader results are cached client-side (TanStack Query) for 10 minutes">
        <Row k="Last Updated" v={new Date().toLocaleString("sv-SE")} />
        {s && <Row k="Snowflake server time" v={String(s.SERVER_TIME)} />}
        <div className="mt-4 flex flex-wrap gap-2">
          <Button
            onClick={() => {
              qc.clear();
              setCleared("Cache cleared - data reloads on next view.");
            }}
          >
            Refresh All Data
          </Button>
          <Button
            variant="outline"
            onClick={() => {
              qc.removeQueries({ predicate: (q) => q.state.status === "error" });
              void qc.invalidateQueries();
              setCleared("Error cache cleared.");
            }}
          >
            Clear Error Cache
          </Button>
        </div>
        {cleared && <p className="mt-2 text-sm text-[var(--success)]">{cleared}</p>}
      </Panel>

      <Panel
        title="Snowflake Connection"
        actions={
          <Button variant="outline" size="sm" onClick={() => { void health.refetch(); void session.refetch(); }} disabled={health.isFetching}>
            {health.isFetching ? "Testing..." : "Test Connection"}
          </Button>
        }
      >
        <ConnectionBody health={health} s={s} />
      </Panel>

      <Panel title="Access" description="Viewer role and Delta-to-Inventory capabilities (/api/dti/me)">
        <AccessBody me={me} />
      </Panel>

      <Panel title="Display Preferences">
        <div className="flex items-center justify-between text-sm">
          <span>Theme (light / dark)</span>
          <ThemeToggle />
        </div>
        <p className="mt-4 text-xs text-muted-foreground">
          Not applicable in the new app: the Streamlit &quot;Rows per page&quot;, &quot;Date format&quot; and &quot;Currency&quot; selectors were
          never wired to any page. Tables are sortable and paged, dates are ISO (YYYY-MM-DD) and amounts are shown in CHF
          as reported. Connection secrets (secrets.toml) are replaced by the server-side Snowflake connection / SPCS OAuth token.
        </p>
      </Panel>
    </div>
  );
}
