"use client";

import { Callout, Field, SELECT_CLS } from "@/components/banking/charts";
import { DataTable, LoaderState, Panel } from "@/components/data/data-table";
import { EmptyState } from "@/components/empty-state";
import { StateView } from "@/components/query-view";
import { PageHeader } from "@/components/page-header";
import { StatCard, StatCardSkeleton } from "@/components/stat-card";
import { Skeleton } from "@/components/ui/skeleton";
import { num, useLoader, type LoaderRow } from "@/lib/loaders/client";
import { formatNumber } from "@/lib/format";
import { useRouter, useSearchParams } from "next/navigation";
import type React from "react";
import { Suspense, useDeferredValue, useEffect, useMemo, useState } from "react";

const ANY = "_";
/** Same character class as the loader "code" param rule; anything else is dropped. */
const clean = (s: string) => s.replace(/[^A-Za-z0-9_ .&/-]/g, "").trim().slice(0, 100);
const pct = (v: unknown) => (v === null || v === undefined ? "—" : `${formatNumber(num(v), 0)}%`);

function Section({ title, open, children }: { title: string; open?: boolean; children: React.ReactNode }) {
  return (
    <details open={open} className="rounded-md border px-4 py-3 [&_summary]:cursor-pointer">
      <summary className="text-sm font-medium">{title}</summary>
      <div className="mt-3 space-y-3">{children}</div>
    </details>
  );
}

function Grid({ cols = 4, children }: { cols?: 3 | 4; children: React.ReactNode }) {
  return <dl className={cols === 4 ? "grid gap-3 sm:grid-cols-2 lg:grid-cols-4" : "grid gap-3 sm:grid-cols-3"}>{children}</dl>;
}

function Profile({ c }: { c: LoaderRow }) {
  return (
    <div className="space-y-3">
      <Section title="Identity & Demographics" open>
        <Grid>
          <Field label="Customer ID" value={String(c.CUSTOMER_ID)} />
          <Field label="Full Name" value={c.FULL_NAME as string} />
          <Field label="First Name" value={c.FIRST_NAME as string} />
          <Field label="Family Name" value={c.FAMILY_NAME as string} />
          <Field label="Date of Birth" value={c.DATE_OF_BIRTH as string} />
          <Field label="Onboarding Date" value={c.ONBOARDING_DATE as string} />
          <Field label="Currency" value={c.REPORTING_CURRENCY as string} />
          <Field label="Status" value={c.CURRENT_STATUS as string} />
        </Grid>
      </Section>
      <Section title="Contact Information">
        <Grid cols={3}>
          <Field label="Email" value={c.EMAIL as string} />
          <Field label="Phone" value={c.PHONE as string} />
          <Field label="Preferred Method" value={c.PREFERRED_CONTACT_METHOD as string} />
        </Grid>
      </Section>
      <Section title="Employment & Financial Profile">
        <Grid>
          <Field label="Employer" value={c.EMPLOYER as string} />
          <Field label="Position" value={c.POSITION as string} />
          <Field label="Employment Type" value={c.EMPLOYMENT_TYPE as string} />
          <Field label="Income Range" value={c.INCOME_RANGE as string} />
          <Field label="Account Tier" value={c.ACCOUNT_TIER as string} />
          <Field label="Credit Score Band" value={c.CREDIT_SCORE_BAND as string} />
          <Field label="Risk Classification" value={c.RISK_CLASSIFICATION as string} />
        </Grid>
      </Section>
      <Section title="Address">
        <Grid cols={3}>
          <Field label="Street" value={c.STREET_ADDRESS as string} />
          <Field label="City" value={c.CITY as string} />
          <Field label="State" value={c.STATE as string} />
          <Field label="Zipcode" value={c.ZIPCODE as string} />
          <Field label="Country" value={c.COUNTRY as string} />
        </Grid>
        <AddressHistory id={c.CUSTOMER_ID as string} />
      </Section>
      <Section title="Account Portfolio">
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <StatCard label="Total Accounts" value={formatNumber(c.TOTAL_ACCOUNTS)} />
          <StatCard label="Checking" value={formatNumber(c.CHECKING_ACCOUNTS)} />
          <StatCard label="Savings" value={formatNumber(c.SAVINGS_ACCOUNTS)} />
          <StatCard label="Investment" value={formatNumber(c.INVESTMENT_ACCOUNTS)} />
        </div>
        <Grid cols={3}>
          <Field label="Account Types" value={c.ACCOUNT_TYPES as string} />
          <Field label="Currencies" value={c.CURRENCIES as string} />
        </Grid>
      </Section>
      <Section title="Risk & Compliance">
        <div className="grid gap-3 sm:grid-cols-3">
          <div className="space-y-3">
            <StatCard label="Overall Risk Rating" value={String(c.OVERALL_RISK_RATING ?? "—")} />
            <StatCard label="Risk Score" value={formatNumber(num(c.OVERALL_RISK_SCORE), 1)} />
          </div>
          <dl className="space-y-3">
            <Field label="PEP Match Type" value={c.EXPOSED_PERSON_MATCH_TYPE as string} />
            <Field label="PEP Accuracy" value={pct(c.EXPOSED_PERSON_MATCH_ACCURACY_PERCENT)} />
            <Field label="PEP Risk" value={c.OVERALL_EXPOSED_PERSON_RISK as string} />
          </dl>
          <dl className="space-y-3">
            <Field label="Sanctions Match" value={c.SANCTIONS_MATCH_TYPE as string} />
            <Field label="Sanctions Accuracy" value={pct(c.SANCTIONS_MATCH_ACCURACY_PERCENT)} />
            <Field label="Sanctions Risk" value={c.OVERALL_SANCTIONS_RISK as string} />
          </dl>
        </div>
        {c.HIGH_RISK_CUSTOMER === true && <Callout tone="error" title="HIGH RISK CUSTOMER - Enhanced monitoring required" />}
        {c.REQUIRES_EXPOSED_PERSON_REVIEW === true && <Callout tone="warning" title="Requires PEP Review" />}
        {c.REQUIRES_SANCTIONS_REVIEW === true && <Callout tone="warning" title="Requires Sanctions Review" />}
        {c.HAS_ANOMALY === true && <Callout tone="warning" title="Anomalous transaction pattern detected" />}
      </Section>
    </div>
  );
}

/** All address versions of the customer (SCD Type 2), newest first; masked like the current address. */
function AddressHistory({ id }: Readonly<{ id: string }>) {
  const history = useLoader("customer360.address-history", { id });
  // roles without read access on the history (e.g. DTI_READ_ONLY) get a note instead of the SQL error
  const noAccess = history.isError && /not authorized|does not exist/i.test(history.error.message);
  return (
    <div className="mt-4 space-y-2">
      <p className="text-sm font-medium text-muted-foreground">Address history</p>
      {noAccess ? (
        <p className="text-sm text-muted-foreground">Not available for this role.</p>
      ) : (
        <LoaderState q={history} empty="No address history.">
          {(rows) => <DataTable rows={rows} />}
        </LoaderState>
      )}
    </div>
  );
}

function Check({ label, checked, onChange }: { label: string; checked: boolean; onChange: (v: boolean) => void }) {
  return (
    <label className="flex items-center gap-2 text-sm">
      <input type="checkbox" checked={checked} onChange={(e) => onChange(e.target.checked)} className="size-4 accent-[var(--primary)]" />
      {label}
    </label>
  );
}

type Flags = { highRisk: boolean; pep: boolean; review: boolean; anomaly: boolean };

const orAny = (v: string, all = "All") => (v === all || !v ? ANY : v);

/** customer360.list parameters from the filter state (ANY = no filter). */
function listParams(linked: string, name: string, country: string, tiers: string[], risk: string, flags: Flags) {
  return {
    customer: linked || ANY,
    name: name || ANY,
    country: orAny(country),
    tiers: tiers.length ? tiers.join(" ") : ANY,
    risk: orAny(risk),
    highRisk: flags.highRisk ? 1 : 0,
    pep: flags.pep ? 1 : 0,
    review: flags.review ? 1 : 0,
    anomaly: flags.anomaly ? 1 : 0,
  };
}

function Kpis() {
  const kpis = useLoader("customer360.kpis");
  const k = kpis.data?.rows[0];
  if (kpis.isError) {
    return <EmptyState variant="error" title="KPIs failed" description={kpis.error.message} className="sm:col-span-4" />;
  }
  if (!k) return [0, 1, 2, 3].map((i) => <StatCardSkeleton key={i} />);
  return (
    <>
      <StatCard label="Total Customers" value={formatNumber(num(k.TOTAL_CUSTOMERS))} />
      <StatCard label="Critical / High Risk" value={formatNumber(num(k.CRITICAL_HIGH))} />
      <StatCard label="PEP Matches" value={formatNumber(num(k.PEP_MATCHES))} />
      <StatCard label="Avg Risk Score" value={formatNumber(num(k.AVG_RISK_SCORE), 1)} />
    </>
  );
}

function Customer360() {
  const router = useRouter();
  const linked = clean(useSearchParams().get("customer") ?? "");
  const opts = useLoader("customer360.filter-options");
  const [name, setName] = useState("");
  const [country, setCountry] = useState("All");
  const [tiers, setTiers] = useState<string[]>([]);
  const [risk, setRisk] = useState("All");
  const [flags, setFlags] = useState<Flags>({ highRisk: false, pep: false, review: false, anomaly: false });
  const deferredName = useDeferredValue(clean(name));

  const options = useMemo(() => {
    const by = (kind: string) => (opts.data?.rows ?? []).filter((r) => r.KIND === kind).map((r) => String(r.VALUE));
    return { countries: by("COUNTRY"), tiers: by("ACCOUNT_TIER"), risks: by("RISK") };
  }, [opts.data]);

  const list = useLoader("customer360.list", listParams(linked, deferredName, country, tiers, risk, flags));
  const rows = list.data?.rows ?? [];
  const matches = rows.length ? num(rows[0].MATCH_COUNT) : 0;

  const [selected, setSelected] = useState<string | null>(null);
  useEffect(() => {
    const ids = rows.map((r) => String(r.CUSTOMER_ID));
    if (!selected || !ids.includes(selected)) setSelected(ids[0] ?? null);
  }, [rows, selected]);
  const detail = useLoader("customer360.detail", { id: selected }, { enabled: !!selected });

  return (
    <div className="space-y-6">
      <PageHeader eyebrow="Banking" title="Customer 360" description="Complete view of a customer: profile, accounts, activity and risk" />
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <Kpis />
      </div>

      <Panel title="Search & Filter">
        <div className="space-y-4">
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <label className="space-y-1 text-sm">
              <span className="text-muted-foreground">Search by Name</span>
              <input
                type="search"
                aria-label="Search by Name"
                placeholder="Enter customer name..."
                value={name}
                onChange={(e) => setName(e.target.value)}
                className={`${SELECT_CLS} w-full`}
              />
            </label>
            <label className="space-y-1 text-sm">
              <span className="text-muted-foreground">Country</span>
              <select aria-label="Country" value={country} onChange={(e) => setCountry(e.target.value)} className={`${SELECT_CLS} w-full`}>
                <option>All</option>
                {options.countries.map((c) => (
                  <option key={c}>{c}</option>
                ))}
              </select>
            </label>
            <div className="space-y-1 text-sm">
              <span className="text-muted-foreground">Account Tier</span>
              <fieldset className="m-0 flex min-w-0 flex-wrap gap-1 border-0 p-0" aria-label="Account Tier">
                {options.tiers.map((t) => {
                  const on = tiers.includes(t);
                  return (
                    <button
                      key={t}
                      type="button"
                      aria-pressed={on}
                      onClick={() => setTiers((s) => (on ? s.filter((x) => x !== t) : [...s, t]))}
                      className={`rounded-md border px-2 py-1 text-xs ${on ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:bg-muted"}`}
                    >
                      {t}
                    </button>
                  );
                })}
              </fieldset>
            </div>
            <label className="space-y-1 text-sm">
              <span className="text-muted-foreground">Risk Level</span>
              <select aria-label="Risk Level" value={risk} onChange={(e) => setRisk(e.target.value)} className={`${SELECT_CLS} w-full`}>
                <option>All</option>
                {options.risks.map((r) => (
                  <option key={r}>{r}</option>
                ))}
              </select>
            </label>
          </div>
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <Check label="Critical / High Risk" checked={flags.highRisk} onChange={(v) => setFlags((f) => ({ ...f, highRisk: v }))} />
            <Check label="PEP Matches" checked={flags.pep} onChange={(v) => setFlags((f) => ({ ...f, pep: v }))} />
            <Check label="Requires Review" checked={flags.review} onChange={(v) => setFlags((f) => ({ ...f, review: v }))} />
            <Check label="Anomaly Flagged" checked={flags.anomaly} onChange={(v) => setFlags((f) => ({ ...f, anomaly: v }))} />
          </div>
          {linked && (
            <Callout tone="info" title={`Showing linked customer ${linked}`}>
              <button type="button" className="underline" onClick={() => router.replace("/banking/customer-360")}>
                Show all customers
              </button>
            </Callout>
          )}
        </div>
      </Panel>

      <StateView
        pending={list.isPending}
        error={list.error}
        errorTitle="Customer query failed"
        skeleton={<Skeleton className="h-96 w-full" />}
      >
        {() =>
          rows.length === 0 ? (
        <Callout tone="warning" title="No customers found matching your criteria." />
      ) : (
        <>
          <Callout tone="info" title={`Found ${formatNumber(matches)} customers matching your criteria`}>
            {matches > rows.length ? `Table and picker show the first ${rows.length} by customer ID.` : null}
          </Callout>
          <Panel title="Customers">
            <DataTable
              rows={rows}
              columns={[
                "CUSTOMER_ID",
                "FULL_NAME",
                "COUNTRY",
                "ACCOUNT_TIER",
                "OVERALL_RISK_RATING",
                "OVERALL_RISK_SCORE",
                "EXPOSED_PERSON_MATCH_TYPE",
                "SANCTIONS_MATCH_TYPE",
              ]}
              maxRows={500}
            />
          </Panel>
          <Panel title="Customer Profile">
            <div className="space-y-4">
              <label className="flex flex-wrap items-center gap-2 text-sm">
                <span className="text-muted-foreground">Select a customer to view detailed profile:</span>
                <select
                  aria-label="Select a customer to view detailed profile"
                  value={selected ?? ""}
                  onChange={(e) => setSelected(e.target.value)}
                  className={`${SELECT_CLS} min-w-72`}
                >
                  {rows.map((r) => (
                    <option key={String(r.CUSTOMER_ID)} value={String(r.CUSTOMER_ID)}>
                      {String(r.CUSTOMER_ID)} - {String(r.FULL_NAME ?? "")}
                    </option>
                  ))}
                </select>
              </label>
              <LoaderState q={detail} empty="Customer not found">
                {(d) => <Profile c={d[0]} />}
              </LoaderState>
            </div>
          </Panel>
        </>
          )
        }
      </StateView>
    </div>
  );
}

/** Customer 360 - port of the_bank_app/pages/customer_360.py (?customer=<id> deep link). */
export default function Customer360Page() {
  return (
    <Suspense fallback={<Skeleton className="h-96 w-full" />}>
      <Customer360 />
    </Suspense>
  );
}
