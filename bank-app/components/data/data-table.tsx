"use client";

import { EmptyState } from "@/components/empty-state";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import type { LoaderRow } from "@/lib/loaders/client";
import { formatNumber, formatValue, isNumeric } from "@/lib/format";
import { cn } from "@/lib/utils";
import { ArrowDown, ArrowUp } from "lucide-react";
import { useEffect, useMemo, useState, type ReactNode } from "react";

/** "OUTFLOW_TOTAL_CHF" -> "Outflow total CHF" */
export function columnLabel(c: string): string {
  const words = c.split("_").map((w) => (["CHF", "LCR", "HQLA", "ID", "PP", "KYC", "AML", "PEP", "LTV", "DTI", "FX", "SNB"].includes(w) ? w : w.toLowerCase()));
  const s = words.join(" ");
  return s.charAt(0).toUpperCase() + s.slice(1);
}

export interface DataColumn {
  key: string;
  label?: string;
  /** custom cell rendering; default formats numbers and dates */
  render?: (row: LoaderRow) => ReactNode;
  numeric?: boolean;
}

/** Cell: custom render, else numbers formatted (booleans excluded), else the formatted value. */
function cellContent(c: DataColumn, r: LoaderRow): ReactNode {
  if (c.render) return c.render(r);
  const v = r[c.key];
  return isNumeric(v) && typeof v !== "boolean" ? formatNumber(Number(v)) : formatValue(v);
}

/**
 * Generic result table (replacement for st.dataframe): sortable columns,
 * numbers right-aligned, row cap with a note. Columns default to all keys of
 * the first row.
 */
export function DataTable({
  rows,
  columns,
  maxRows = 200,
  className,
  caption,
}: {
  rows: LoaderRow[];
  columns?: (string | DataColumn)[];
  maxRows?: number;
  className?: string;
  caption?: ReactNode;
}) {
  const cols: DataColumn[] = useMemo(
    () => (columns ?? Object.keys(rows[0] ?? {})).map((c) => (typeof c === "string" ? { key: c } : c)),
    [columns, rows],
  );
  const [sort, setSort] = useState<{ key: string; dir: 1 | -1 } | null>(null);
  // rows keep their original index as a stable React key across re-sorts
  const sorted = useMemo(() => {
    const indexed = rows.map((r, id) => ({ r, id }));
    if (!sort) return indexed;
    return indexed.sort(({ r: a }, { r: b }) => {
      const x = a[sort.key];
      const y = b[sort.key];
      if (isNumeric(x) && isNumeric(y)) return (Number(x) - Number(y)) * sort.dir;
      return String(x ?? "").localeCompare(String(y ?? "")) * sort.dir;
    });
  }, [rows, sort]);
  const numericCol = (c: DataColumn) => c.numeric ?? rows.slice(0, 20).some((r) => isNumeric(r[c.key]) && typeof r[c.key] !== "boolean");
  // paging instead of a hard cap (st.dataframe showed every row); back to page 1 on new data or sort
  const [page, setPage] = useState(0);
  useEffect(() => setPage(0), [rows, sort]);
  const pages = Math.max(1, Math.ceil(rows.length / maxRows));
  const from = page * maxRows;
  const pageRows = sorted.slice(from, from + maxRows);

  if (rows.length === 0) return <EmptyState title="No rows" />;
  return (
    <div className={cn("space-y-1", className)}>
      <div className="max-h-[32rem] overflow-auto rounded-md border">
        <Table>
          <TableHeader className="sticky top-0 z-10 bg-muted">
            <TableRow>
              {cols.map((c) => (
                <TableHead key={c.key} numeric={numericCol(c)}>
                  <button
                    type="button"
                    className="inline-flex items-center gap-1 hover:text-foreground"
                    onClick={() => setSort((s) => (s?.key === c.key ? { key: c.key, dir: s.dir === 1 ? -1 : 1 } : { key: c.key, dir: -1 }))}
                  >
                    {c.label ?? columnLabel(c.key)}
                    {sort?.key === c.key && (sort.dir === 1 ? <ArrowUp className="size-3" /> : <ArrowDown className="size-3" />)}
                  </button>
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {pageRows.map(({ r, id }) => (
              <TableRow key={id}>
                {cols.map((c) => (
                  <TableCell key={c.key} numeric={numericCol(c)} className="whitespace-nowrap">
                    {cellContent(c, r)}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
      <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
        <span>
          {pages > 1 ? `Rows ${from + 1}–${from + pageRows.length} of ${rows.length}` : `${rows.length} rows`}
          {caption ? <> · {caption}</> : null}
        </span>
        {pages > 1 && (
          <span className="ml-auto flex items-center gap-1">
            <button type="button" className="rounded border px-2 py-0.5 hover:bg-muted disabled:opacity-40" disabled={page === 0} onClick={() => setPage(page - 1)}>
              Previous
            </button>
            <span>
              Page {page + 1} / {pages}
            </span>
            <button type="button" className="rounded border px-2 py-0.5 hover:bg-muted disabled:opacity-40" disabled={page >= pages - 1} onClick={() => setPage(page + 1)}>
              Next
            </button>
          </span>
        )}
      </div>
    </div>
  );
}

/** Loading / error / empty wrapper for one loader result. */
export function LoaderState({
  q,
  children,
  empty = "No data",
  height = "h-64",
}: {
  q: { isPending: boolean; isError: boolean; error: Error | null; data?: { rows: LoaderRow[] } };
  children: (rows: LoaderRow[]) => ReactNode;
  empty?: string;
  height?: string;
}) {
  if (q.isPending) return <Skeleton className={cn("w-full", height)} />;
  if (q.isError) return <EmptyState variant="error" title="Query failed" description={q.error?.message} />;
  if (!q.data || q.data.rows.length === 0) return <EmptyState title={empty} />;
  return <>{children(q.data.rows)}</>;
}

/** Card with title and optional description around a chart or table. */
export function Panel({
  title,
  description,
  children,
  className,
  actions,
}: {
  title: ReactNode;
  description?: ReactNode;
  children: ReactNode;
  className?: string;
  actions?: ReactNode;
}) {
  return (
    <Card className={className}>
      <CardHeader className="flex flex-row items-start justify-between gap-4 space-y-0">
        <div className="space-y-1">
          <CardTitle>{title}</CardTitle>
          {description && <CardDescription>{description}</CardDescription>}
        </div>
        {actions}
      </CardHeader>
      <CardContent>{children}</CardContent>
    </Card>
  );
}

/** Tab bar (replacement for st.tabs); controlled, keeps the URL out of it. */
export function Tabs<T extends string>({
  tabs,
  value,
  onChange,
}: {
  tabs: { v: T; label: string }[];
  value: T;
  onChange: (v: T) => void;
}) {
  return (
    <div role="tablist" className="flex flex-wrap gap-1 border-b">
      {tabs.map((t) => (
        <button
          key={t.v}
          role="tab"
          type="button"
          aria-selected={value === t.v}
          onClick={() => onChange(t.v)}
          className={cn(
            "-mb-px border-b-2 px-3 py-2 text-sm",
            value === t.v ? "border-primary font-medium text-foreground" : "border-transparent text-muted-foreground hover:text-foreground",
          )}
        >
          {t.label}
        </button>
      ))}
    </div>
  );
}
