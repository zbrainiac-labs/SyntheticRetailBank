"use client";

import { Table, TableBody, TableCell, TableFooter, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import type { PeriodColumn } from "@/lib/dti/positions";
import { formatChfM } from "@/lib/format";
import { cn } from "@/lib/utils";
import type React from "react";

export interface PivotColumn {
  id: string;
  label: string;
  /** value per period key */
  value: (period: string) => number;
  onClick?: (period: PeriodColumn) => void;
  /** threshold highlight applies (delta columns only) */
  highlight?: boolean;
  emphasis?: boolean;
}

export interface PivotGroup {
  id: string;
  header: React.ReactNode;
  columns: PivotColumn[];
}

/**
 * Period x column-group table (UC2 6.4-6.6): one row per bucket or business
 * day, grouped column headers, clickable cells and a control-sum footer.
 * `minAbs` (CHF) hides periods whose cells are all smaller; the control sum
 * still covers every period.
 */
export function PivotTable({
  periods,
  groups,
  threshold,
  minAbs = 0,
}: {
  periods: PeriodColumn[];
  groups: PivotGroup[];
  threshold?: number;
  minAbs?: number;
}) {
  const visible =
    minAbs <= 0 ? periods : periods.filter((p) => groups.some((g) => g.columns.some((c) => Math.abs(c.value(p.key)) >= minAbs)));
  return (
    <Table bordered>
      <TableHeader>
        <TableRow>
          <TableHead rowSpan={2} className="sticky left-0 z-10 min-w-36 bg-muted align-bottom">
            Period
          </TableHead>
          {groups.map((g) => (
            <TableHead key={g.id} colSpan={g.columns.length} className="border-l text-center normal-case tracking-normal">
              {g.header}
            </TableHead>
          ))}
        </TableRow>
        <TableRow>
          {groups.map((g) =>
            g.columns.map((c, i) => (
              <TableHead key={c.id} numeric className={cn("normal-case", i === 0 && "border-l")}>
                {c.label}
              </TableHead>
            )),
          )}
        </TableRow>
      </TableHeader>
      <TableBody>
        {visible.map((p) => (
          <TableRow key={p.key}>
            <TableCell className="sticky left-0 z-10 bg-card py-2">
              <div className="font-medium">{p.label}</div>
              {p.sublabel && <div className="text-xs text-muted-foreground">{p.sublabel}</div>}
            </TableCell>
            {groups.map((g) =>
              g.columns.map((c, i) => {
                const v = c.value(p.key);
                const hot = c.highlight && threshold !== undefined && Math.abs(v) >= threshold;
                const text = formatChfM(v);
                return (
                  <TableCell key={c.id} numeric className={cn("py-2", i === 0 && "border-l", c.emphasis && "font-medium")}>
                    {c.onClick && v !== 0 ? (
                      <button
                        type="button"
                        onClick={() => c.onClick?.(p)}
                        className={cn("w-full rounded px-1 text-right underline-offset-2 hover:underline", hot && "bg-warning/25")}
                      >
                        {text}
                      </button>
                    ) : (
                      <span className={cn("block px-1", hot && "rounded bg-warning/25")}>{text}</span>
                    )}
                  </TableCell>
                );
              }),
            )}
          </TableRow>
        ))}
      </TableBody>
      <TableFooter>
        <TableRow>
          <TableCell className="sticky left-0 z-10 bg-muted py-2">Control sum</TableCell>
          {groups.map((g) =>
            g.columns.map((c, i) => (
              <TableCell key={c.id} numeric className={cn("py-2", i === 0 && "border-l")}>
                {formatChfM(periods.reduce((s, p) => s + c.value(p.key), 0))}
              </TableCell>
            )),
          )}
        </TableRow>
      </TableFooter>
    </Table>
  );
}
