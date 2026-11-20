"use client";

import { Button } from "@/components/ui/button";
import { num, type LoaderRow } from "@/lib/loaders/client";
import { cn } from "@/lib/utils";

/** Note carried on every screen and export of the showcase (feedback: illustrative status unmistakable). */
export const ILLUSTRATIVE_NOTE =
  "Synthetic data, fictional entities, illustrative values. Management consolidation, not an IFRS statement.";

/** CHF millions with one decimal: 6_123_456_789 -> "6,123.5" */
export function fmtM(v: unknown, digits = 1): string {
  const m = Number((num(v) / 1e6).toFixed(digits)) || 0;
  return m.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

/** Signed CHF millions for movements: "+12.3" / "-4.0" */
export function fmtDeltaM(v: unknown, digits = 1): string {
  const s = fmtM(v, digits);
  return num(v) > 0 && s !== "0.0" ? `+${s}` : s;
}

export function fmtRate(v: unknown): string {
  return num(v).toLocaleString("en-US", { minimumFractionDigits: 4, maximumFractionDigits: 4 });
}

/** "2026-09-01" -> "Sep 2026" */
export function monthLabel(iso: unknown): string {
  if (!iso) return "";
  const d = new Date(`${String(iso).slice(0, 10)}T00:00:00Z`);
  return d.toLocaleString("en-US", { month: "short", year: "numeric", timeZone: "UTC" });
}

/** Small segmented control (BS / P&L, Local / CHF, metric switch). */
export function Segmented<T extends string>({
  options,
  value,
  onChange,
  label,
}: Readonly<{ options: { v: T; label: string }[]; value: T; onChange: (v: T) => void; label: string }>) {
  return (
    <div role="group" aria-label={label} className="inline-flex rounded-md border p-0.5">
      {options.map((o) => (
        <Button
          key={o.v}
          type="button"
          size="sm"
          variant={value === o.v ? "default" : "ghost"}
          aria-pressed={value === o.v}
          className={cn("h-7 px-3 text-xs")}
          onClick={() => onChange(o.v)}
        >
          {o.label}
        </Button>
      ))}
    </div>
  );
}

/** Sum of AMOUNT_CHF over consolidation rows matching a predicate. */
export function sumWhere(rows: LoaderRow[], test: (r: LoaderRow) => boolean, key = "AMOUNT_CHF"): number {
  return rows.reduce((s, r) => (test(r) ? s + num(r[key]) : s), 0);
}

/** Display name lookup from finance.entities. */
export function entityNames(entities: LoaderRow[]): Record<string, string> {
  return Object.fromEntries(entities.map((e) => [String(e.ENTITY_ID), String(e.DISPLAY_NAME)]));
}
