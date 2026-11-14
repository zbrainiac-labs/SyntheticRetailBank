"use client";

import { Button } from "@/components/ui/button";
import { scopeLabel, valueTypeLabel, type DtiFilters } from "@/lib/filters/model";
import { cn } from "@/lib/utils";
import { ChevronDown, ChevronRight } from "lucide-react";
import { useState, type ReactNode } from "react";

/** Small segmented control (measure, valuation basis, ...). */
export function Segmented<T extends string>({
  value,
  options,
  onChange,
  label,
}: {
  value: T;
  options: { v: T; label: string }[];
  onChange: (v: T) => void;
  label?: string;
}) {
  return (
    <fieldset aria-label={label} className="m-0 inline-flex h-8 min-w-0 rounded-md border p-0.5">
      {options.map((o) => (
        <button
          key={o.v}
          type="button"
          aria-pressed={value === o.v}
          onClick={() => onChange(o.v)}
          className={cn("rounded px-2.5 text-sm", value === o.v ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:bg-muted")}
        >
          {o.label}
        </button>
      ))}
    </fieldset>
  );
}

export function scopeText(f: DtiFilters, withValues = true): string {
  const values = withValues ? ` · ${valueTypeLabel(f.valueType)} values` : "";
  return `${scopeLabel(f.entities)}${values} · ${f.referenceDate} vs. ${f.comparisonDate}`;
}

/** UC2 "Element C": forecast tables of both dates, hidden by default and opened by a click. */
export function ForecastSection({ referenceDate, comparisonDate, render }: { referenceDate: string; comparisonDate: string; render: (m: "ref" | "comp") => ReactNode }) {
  const [open, setOpen] = useState(false);
  return (
    <section className="space-y-3">
      <Button variant="outline" size="sm" onClick={() => setOpen((o) => !o)} aria-expanded={open}>
        {open ? <ChevronDown className="size-4" /> : <ChevronRight className="size-4" />}
        Forecast tables (absolute values of both dates)
      </Button>
      {open && (
        <div className="space-y-6">
          <div className="space-y-2">
            <h3 className="text-sm font-medium">Liquidity flows as of reference date {referenceDate}</h3>
            {render("ref")}
          </div>
          <div className="space-y-2">
            <h3 className="text-sm font-medium">Liquidity flows as of comparison date {comparisonDate}</h3>
            {render("comp")}
          </div>
        </div>
      )}
    </section>
  );
}
