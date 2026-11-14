"use client";

import { ForecastSection, scopeText } from "@/components/dti/common";
import { PageFilterBar } from "@/components/dti/filter-bar";
import { StateView } from "@/components/query-view";
import { PageHeader } from "@/components/page-header";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useParameters, useSecurities } from "@/lib/dti/client";
import type { SecurityRow } from "@/lib/dti/types";
import { usePageFilters } from "@/lib/filters/context";
import type { FilterKey } from "@/lib/filters/model";
import { formatChfM, formatPercent } from "@/lib/format";
import { cn } from "@/lib/utils";
import Link from "next/link";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { Suspense, useMemo } from "react";

const PAGE = "securities";
const FILTERS: FilterKey[] = ["entities", "referenceDate", "comparisonDate", "valueType", "presentation", "includePreliminary", "minAbsDelta"];

type Measure = "delta" | "ref" | "comp";

/** UC2 6.8 Securities per WLK cluster: securities in rows, periods in columns, plus master data. */
function Securities() {
  const router = useRouter();
  const pathname = usePathname();
  const sp = useSearchParams();
  const wlk = sp.get("wlk") ?? "II";
  const from = sp.get("from");
  const { effective: f } = usePageFilters(PAGE, FILTERS);
  const params = useParameters();
  const sec = useSecurities(f, wlk);
  const clusters = (params.data?.wlk ?? []).map((w) => ({ id: String(w.WLK_CLUSTER), name: String(w.CLUSTER_NAME), haircut: Number(w.HAIRCUT) }));
  const info = clusters.find((c) => c.id === wlk);

  // security -> period -> {ref, comp}; periods in display order
  const { grid, periods, rows } = useMemo(() => {
    const grid = new Map<string, Map<string, { ref: number; comp: number }>>();
    const sorts = new Map<string, number>();
    for (const pt of sec.data?.points ?? []) {
      const g = grid.get(pt.id) ?? new Map();
      g.set(pt.period, { ref: pt.ref, comp: pt.comp });
      grid.set(pt.id, g);
      sorts.set(pt.period, pt.sort);
    }
    const periods = [...sorts.entries()].sort((a, b) => a[1] - b[1]).map(([k]) => k);
    const absDelta = (id: string) => [...(grid.get(id)?.entries() ?? [])].reduce((s, [p, c]) => s + (p === "BEFORE_REF" ? 0 : Math.abs(c.ref - c.comp)), 0);
    const rows = [...(sec.data?.rows ?? [])].sort((a, b) => absDelta(b.id) - absDelta(a.id));
    return { grid, periods, rows };
  }, [sec.data]);

  const cell = (r: SecurityRow, period: string, m: Measure): number => {
    const c = grid.get(r.id)?.get(period);
    if (!c) return 0;
    if (m === "ref") return c.ref;
    if (m === "comp") return c.comp;
    return period === "BEFORE_REF" ? 0 : c.ref - c.comp;
  };

  const table = (m: Measure) => {
    if (!sec.data) return null;
    const ps = m === "comp" ? periods : periods.filter((p) => p !== "BEFORE_REF");
    const sum = (p: string) => rows.reduce((s, r) => s + cell(r, p, m), 0);
    const min = f.minAbsDelta * 1e6;
    const visible = min > 0 ? rows.filter((r) => ps.some((p) => Math.abs(cell(r, p, m)) >= min)) : rows;
    const label = (p: string) => {
      if (p === "BEFORE_REF") return "Before ref. date";
      return f.presentation === "DAYS" ? p.slice(5) : p;
    };
    return (
      <Table bordered>
        <TableHeader>
          <TableRow>
            <TableHead className="sticky left-0 z-10 bg-muted">ISIN</TableHead>
            <TableHead>Name</TableHead>
            <TableHead>Issuer</TableHead>
            {ps.map((p) => (
              <TableHead key={p} numeric>
                {label(p)}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          <TableRow className="font-semibold">
            <TableCell className="sticky left-0 z-10 bg-card" colSpan={3}>
              Cluster sum (unfiltered)
            </TableCell>
            {ps.map((p) => (
              <TableCell key={p} numeric>
                {formatChfM(sum(p))}
              </TableCell>
            ))}
          </TableRow>
          {visible.map((r) => (
            <TableRow key={r.id}>
              <TableCell className="sticky left-0 z-10 bg-card font-mono text-xs">{r.isin ?? r.id}</TableCell>
              <TableCell>{r.name}</TableCell>
              <TableCell className="text-muted-foreground">{r.issuer ?? "—"}</TableCell>
              {ps.map((p) => {
                const v = cell(r, p, m);
                return (
                  <TableCell key={p} numeric className={cn(m === "delta" && v !== 0 && "font-medium", m === "delta" && v < 0 && "text-destructive")}>
                    {formatChfM(v)}
                  </TableCell>
                );
              })}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    );
  };

  const clusterName = info ? ` (${info.name})` : "";
  const valueBasis = f.valueType === "STRESSED" ? "Stressed = value after haircut" : "Base = market value";

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="DtI comparison"
        title={`Collateral Pool – WLK cluster ${wlk}${clusterName}`}
        description={`${valueBasis}, CHF M · ${scopeText(f)}`}
        actions={
          <div className="flex gap-3 text-sm">
            {from && from.startsWith("/dti") && (
              <Link href={from} className="text-muted-foreground hover:text-foreground">
                ← Back
              </Link>
            )}
            <Link href="/dti/positions" className="text-muted-foreground hover:text-foreground">
              LST Positions
            </Link>
            <Link href="/dti/free-assets" className="text-muted-foreground hover:text-foreground">
              Free Assets
            </Link>
          </div>
        }
      />
      <PageFilterBar page={PAGE} applicable={FILTERS} />
      <fieldset className="m-0 flex min-w-0 flex-wrap gap-1 border-0 p-0" aria-label="WLK cluster">
        {clusters.map((c) => (
          <button
            key={c.id}
            type="button"
            title={c.name}
            aria-pressed={c.id === wlk}
            onClick={() => {
              const next = new URLSearchParams(sp.toString());
              next.set("wlk", c.id);
              router.replace(`${pathname}?${next.toString()}`);
            }}
            className={cn("h-8 rounded-md border px-2.5 text-sm", c.id === wlk ? "border-primary bg-primary/10" : "text-muted-foreground hover:bg-muted")}
          >
            {c.id}
          </button>
        ))}
      </fieldset>
      <StateView
        pending={sec.isPending}
        error={sec.error}
        errorTitle="Securities query failed"
        skeleton={<Skeleton className="h-64 w-full" />}
        empty={rows.length === 0}
        emptyTitle="No securities"
        emptyDescription={`WLK cluster ${wlk} holds no pool securities for this selection.`}
      >
        {() => (
        <>
          <div className="space-y-2">
            <h2 className="text-sm font-medium">Δ reference ./. comparison – liquidity value per cashflow date, sorted by |Δ|</h2>
            {table("delta")}
          </div>
          <ForecastSection referenceDate={f.referenceDate} comparisonDate={f.comparisonDate} render={(m) => table(m)} />
          <Card>
            <CardHeader>
              <CardTitle>Master data</CardTitle>
              <CardDescription>Securities in WLK cluster {wlk} as of {f.referenceDate}; maturity bucket relative to the reference date.</CardDescription>
            </CardHeader>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>ISIN</TableHead>
                    <TableHead>Name</TableHead>
                    <TableHead>Issuer</TableHead>
                    <TableHead numeric>Nominal (M)</TableHead>
                    <TableHead>Maturity</TableHead>
                    <TableHead numeric>Haircut</TableHead>
                    <TableHead numeric>Liquidity value base (M)</TableHead>
                    <TableHead numeric>Liquidity value stressed (M)</TableHead>
                    <TableHead>Maturity bucket</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {rows.map((r) => (
                    <TableRow key={r.id}>
                      <TableCell className="font-mono text-xs">{r.isin ?? r.id}</TableCell>
                      <TableCell>
                        {r.name}
                        {r.encumbered && (
                          <Badge variant="secondary" className="ml-2">
                            encumbered
                          </Badge>
                        )}
                        {r.onlyComparison && (
                          <Badge variant="outline" className="ml-2">
                            comparison date only
                          </Badge>
                        )}
                      </TableCell>
                      <TableCell className="text-muted-foreground">{r.issuer ?? "—"}</TableCell>
                      <TableCell numeric>{formatChfM(r.nominal)}</TableCell>
                      <TableCell>{r.maturity ?? "—"}</TableCell>
                      <TableCell numeric>{formatPercent(r.haircut)}</TableCell>
                      <TableCell numeric>{formatChfM(r.valueBase)}</TableCell>
                      <TableCell numeric>{formatChfM(r.valueStressed)}</TableCell>
                      <TableCell>{r.maturityBucket ?? "—"}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </>
        )}
      </StateView>
    </div>
  );
}

export default function DtiSecuritiesPage() {
  return (
    <Suspense fallback={<Skeleton className="h-64 w-full" />}>
      <Securities />
    </Suspense>
  );
}
