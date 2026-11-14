"use client";

import { Button } from "@/components/ui/button";
import { dtiQuery, useDtiAccess, useDtiMeta } from "@/lib/dti/client";
import { usePageFilters } from "@/lib/filters/context";
import { ENTITIES, entityLabel, type DtiFilters, type FilterKey } from "@/lib/filters/model";
import { cn } from "@/lib/utils";
import { Download, RotateCcw } from "lucide-react";
import type React from "react";

const SELECT =
  "h-8 rounded-md border bg-background px-2 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring";

function Field({ label, overridden, children }: { label: string; overridden?: boolean; children: React.ReactNode }) {
  return (
    <label className="flex flex-col gap-1 text-xs text-muted-foreground">
      <span className={cn(overridden && "font-medium text-primary")}>
        {label}
        {overridden && " (local)"}
      </span>
      {children}
    </label>
  );
}

function Toggle<T extends string>({ value, options, onChange }: { value: T; options: { v: T; label: string }[]; onChange: (v: T) => void }) {
  return (
    <div className="inline-flex h-8 rounded-md border p-0.5">
      {options.map((o) => (
        <button
          key={o.v}
          type="button"
          onClick={() => onChange(o.v)}
          aria-pressed={value === o.v}
          className={cn("rounded px-2.5 text-sm", value === o.v ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:bg-muted")}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

export interface FilterControlsProps {
  values: DtiFilters;
  onChange: <K extends FilterKey>(key: K, value: DtiFilters[K]) => void;
  show: FilterKey[];
  overridden?: Partial<Record<FilterKey, boolean>>;
}

/** The six UC2 filters as compact controls; used by the Filter Page and the local page filters. */
export function FilterControls({ values, onChange, show, overridden = {} }: FilterControlsProps) {
  const meta = useDtiMeta();
  const dates = meta.data?.dates ?? [];
  const monthEnds = new Set(meta.data?.monthEnds ?? []);
  const dateLabel = (d: string) => (monthEnds.has(d) ? `${d} · month-end` : d);
  const has = (k: FilterKey) => show.includes(k);
  const toggleEntity = (e: string) => {
    const next = values.entities.includes(e) ? values.entities.filter((x) => x !== e) : [...values.entities, e].sort((a, b) => a.localeCompare(b));
    if (next.length > 0) onChange("entities", next);
  };

  return (
    <div className="flex flex-wrap items-end gap-4">
      {has("entities") && (
        <Field label="Legal entities" overridden={overridden.entities}>
          <div className="flex flex-wrap gap-1">
            {ENTITIES.map((e) => (
              <button
                key={e}
                type="button"
                title={entityLabel(e)}
                aria-pressed={values.entities.includes(e)}
                onClick={() => toggleEntity(e)}
                className={cn(
                  "h-8 rounded-md border px-2 text-sm tabular-nums",
                  values.entities.includes(e) ? "border-primary bg-primary/10 text-foreground" : "text-muted-foreground hover:bg-muted",
                )}
              >
                {e.slice(3)}
              </button>
            ))}
            <button
              type="button"
              onClick={() => onChange("entities", values.entities.length === ENTITIES.length ? ["LE_01"] : [...ENTITIES])}
              className="h-8 rounded-md px-2 text-sm text-muted-foreground hover:bg-muted"
            >
              {values.entities.length === ENTITIES.length ? "None" : "All"}
            </button>
          </div>
        </Field>
      )}
      {has("referenceDate") && (
        <Field label="Reference date" overridden={overridden.referenceDate}>
          <select className={SELECT} value={values.referenceDate} onChange={(e) => onChange("referenceDate", e.target.value)}>
            {dates.map((d) => (
              <option key={d} value={d}>
                {dateLabel(d)}
              </option>
            ))}
          </select>
        </Field>
      )}
      {has("comparisonDate") && (
        <Field label="Comparison date" overridden={overridden.comparisonDate}>
          <select className={SELECT} value={values.comparisonDate} onChange={(e) => onChange("comparisonDate", e.target.value)}>
            {dates.map((d) => (
              <option key={d} value={d}>
                {dateLabel(d)}
              </option>
            ))}
          </select>
        </Field>
      )}
      {has("valueType") && (
        <Field label="Values" overridden={overridden.valueType}>
          <Toggle
            value={values.valueType}
            onChange={(v) => onChange("valueType", v)}
            options={[
              { v: "STRESSED", label: "Stressed" },
              { v: "BASE", label: "Base" },
            ]}
          />
        </Field>
      )}
      {has("presentation") && (
        <Field label="Presentation" overridden={overridden.presentation}>
          <Toggle
            value={values.presentation}
            onChange={(v) => onChange("presentation", v)}
            options={[
              { v: "BUCKETS", label: "Buckets" },
              { v: "DAYS", label: "Days" },
            ]}
          />
        </Field>
      )}
      {has("includePreliminary") && (
        <Field label="Data status" overridden={overridden.includePreliminary}>
          <span className="flex h-8 items-center gap-2 text-sm text-foreground">
            <input
              type="checkbox"
              checked={values.includePreliminary}
              onChange={(e) => onChange("includePreliminary", e.target.checked)}
              className="size-4 accent-[var(--primary)]"
            />
            <span>Include preliminary</span>
          </span>
        </Field>
      )}
      {has("minAbsDelta") && (
        <Field label="Amount filter (CHF M)" overridden={overridden.minAbsDelta}>
          <input
            type="number"
            min={0}
            step={0.5}
            value={values.minAbsDelta}
            onChange={(e) => onChange("minAbsDelta", Math.max(0, Number(e.target.value) || 0))}
            aria-label="Hide rows with all absolute values below (CHF M)"
            className={cn(SELECT, "w-24 text-right tabular-nums")}
          />
        </Field>
      )}
    </div>
  );
}

/** Local filters of a detail page: defaults from the Filter Page, overridable per page. */
export function PageFilterBar({ page, applicable }: { page: string; applicable: FilterKey[] }) {
  const { effective, overrides, set, reset } = usePageFilters(page, applicable);
  const access = useDtiAccess();
  const overridden = Object.fromEntries(applicable.map((k) => [k, overrides[k] !== undefined])) as Partial<Record<FilterKey, boolean>>;
  const anyOverride = Object.values(overridden).some(Boolean);
  // export only for roles with export rights (DTI_LRC_READ, DTI_OWNER); the route checks again
  const canExport = access.data?.canExport && applicable.includes("comparisonDate");
  return (
    <div className="flex flex-wrap items-end justify-between gap-3 rounded-lg border bg-card p-3">
      <FilterControls values={effective} show={applicable} overridden={overridden} onChange={(k, v) => set(k, v)} />
      <div className="flex gap-2">
        {anyOverride && (
          <Button variant="ghost" size="sm" onClick={reset}>
            <RotateCcw className="size-4" /> Filter Page defaults
          </Button>
        )}
        {canExport && (
          <Button asChild variant="outline" size="sm">
            <a href={`/api/dti/export?${dtiQuery(effective)}`} download>
              <Download className="size-4" /> Export CSV
            </a>
          </Button>
        )}
      </div>
    </div>
  );
}
