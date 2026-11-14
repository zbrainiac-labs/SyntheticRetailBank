"use client";

import { EmptyState } from "@/components/empty-state";
import { StateView } from "@/components/query-view";
import { PageHeader } from "@/components/page-header";
import { StatCard } from "@/components/stat-card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableFooter, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useDeals, useDtiMeta } from "@/lib/dti/client";
import { indexPositions, pathOf } from "@/lib/dti/positions";
import { usePageFilters } from "@/lib/filters/context";
import { scopeLabel, valueTypeLabel, type DtiFilters } from "@/lib/filters/model";
import { formatNumber } from "@/lib/format";
import { ArrowLeft } from "lucide-react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { Suspense, useMemo } from "react";

const CELL_KEYS = ["position", "cluster", "product", "period", "refFrom", "refTo", "compFrom", "compTo"] as const;

/** Selection handed over by the drill-down (filters + cell); null when opened directly. */
function useDrillSelection(): { filters: DtiFilters; cell: Record<string, string | null>; from: string } | null {
  const sp = useSearchParams();
  if (!(sp.get("position") || sp.get("cluster") || sp.get("product")) || !sp.get("ref") || !sp.get("comp") || !sp.get("compFrom")) return null;
  return {
    filters: {
      entities: (sp.get("entities") ?? "").split(",").filter(Boolean),
      referenceDate: sp.get("ref") ?? "",
      comparisonDate: sp.get("comp") ?? "",
      valueType: sp.get("valueType") === "BASE" ? "BASE" : "STRESSED",
      presentation: sp.get("view") === "DAYS" ? "DAYS" : "BUCKETS",
      includePreliminary: sp.get("prelim") === "1",
      minAbsDelta: 0,
    },
    cell: Object.fromEntries(CELL_KEYS.map((k) => [k, sp.get(k)])),
    from: sp.get("from")?.startsWith("/dti") ? (sp.get("from") as string) : "/dti/positions",
  };
}

function chf(v: number): string {
  return formatNumber(Math.round(v), 0);
}

function DealLevel() {
  const sel = useDrillSelection();
  const meta = useDtiMeta();
  const deals = useDeals(sel?.filters ?? ({} as DtiFilters), sel?.cell ?? null);
  const idx = useMemo(() => indexPositions(meta.data?.positions ?? []), [meta.data]);
  // global amount filter from the Filter Page; the control sum stays unfiltered
  const minAbs = usePageFilters("deals", ["minAbsDelta"]).effective.minAbsDelta * 1e6;

  if (!sel) {
    return (
      <div className="space-y-6">
        <PageHeader eyebrow="DtI comparison" title="Deal Level" />
        <EmptyState
          title="No cell selected"
          description="Open LST Positions and click a value to see the deals behind it."
          action={
            <Button asChild variant="outline" size="sm">
              <Link href="/dti/positions">Go to LST Positions</Link>
            </Button>
          }
        />
      </div>
    );
  }

  const { filters: f, cell, from } = sel;
  const path =
    [cell.position ? pathOf(cell.position, idx.byCode).map((p) => p.POSITION_NAME).join(" › ") || cell.position : null, cell.cluster, cell.product]
      .filter(Boolean)
      .join(" › ");
  const rows = deals.data ?? [];
  const visible = minAbs > 0 ? rows.filter((r) => Math.abs(r.delta) >= minAbs) : rows;
  const sum = (k: "ref" | "comp" | "delta") => rows.reduce((s, r) => s + r[k], 0);
  const scope = scopeLabel(f.entities);
  const period = cell.period === "BEFORE_REF" ? "before reference date" : cell.period;

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="DtI comparison"
        title="Deal Level"
        description={`${path} · ${period} · ${scope} · ${valueTypeLabel(f.valueType)} values in CHF`}
        actions={
          <div className="flex gap-2">
            <Button asChild variant="outline" size="sm">
              <Link href={from}>
                <ArrowLeft className="size-4" /> Back
              </Link>
            </Button>
            <Button asChild variant="ghost" size="sm">
              <Link href="/dti">Start</Link>
            </Button>
          </div>
        }
      />
      <div className="grid gap-4 sm:grid-cols-3">
        <StatCard label={`Reference ${f.referenceDate}`} value={chf(sum("ref"))} hint={cell.refFrom ? `${cell.refFrom} – ${cell.refTo}` : "not in reference date"} />
        <StatCard label={`Comparison ${f.comparisonDate}`} value={chf(sum("comp"))} hint={`${cell.compFrom} – ${cell.compTo}`} />
        <StatCard
          label="Delta"
          value={chf(sum("delta"))}
          hint={dealsHint(rows.length, visible.length)}
        />
      </div>
      <StateView
        pending={deals.isPending}
        error={deals.error}
        errorTitle="Deal query failed"
        skeleton={<Skeleton className="h-96 w-full" />}
        empty={rows.length === 0}
        emptyTitle="No deals"
        emptyDescription="No flows in this cell."
      >
        {() => (
        <Card>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Deal</TableHead>
                  <TableHead>Entity</TableHead>
                  <TableHead>Position</TableHead>
                  <TableHead>Product</TableHead>
                  <TableHead>App</TableHead>
                  <TableHead>A/P</TableHead>
                  <TableHead>Ccy</TableHead>
                  <TableHead numeric>Reference</TableHead>
                  <TableHead numeric>Comparison</TableHead>
                  <TableHead numeric>Delta</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {visible.map((r) => (
                  <TableRow key={`${r.deal}-${r.entity}-${r.position}-${r.product}-${r.application}-${r.currency}-${r.flowKind}`}>
                    <TableCell className="font-mono text-xs">
                      {r.deal}
                      {r.correction && (
                        <Badge variant="warning" className="ml-2">
                          correction
                        </Badge>
                      )}
                    </TableCell>
                    <TableCell>{r.entity.slice(3)}</TableCell>
                    <TableCell className="text-xs">{idx.byCode.get(r.position)?.POSITION_NAME ?? r.position}</TableCell>
                    <TableCell className="text-xs">{r.product}</TableCell>
                    <TableCell>{r.application ?? "—"}</TableCell>
                    <TableCell>{r.side ?? "—"}</TableCell>
                    <TableCell>{r.currency}</TableCell>
                    <TableCell numeric>{chf(r.ref)}</TableCell>
                    <TableCell numeric>{chf(r.comp)}</TableCell>
                    <TableCell numeric className={r.delta < 0 ? "text-destructive" : undefined}>
                      {chf(r.delta)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
              <TableFooter>
                <TableRow>
                  <TableCell colSpan={7}>Control sum</TableCell>
                  <TableCell numeric>{chf(sum("ref"))}</TableCell>
                  <TableCell numeric>{chf(sum("comp"))}</TableCell>
                  <TableCell numeric>{chf(sum("delta"))}</TableCell>
                </TableRow>
              </TableFooter>
            </Table>
          </CardContent>
        </Card>
        )}
      </StateView>
    </div>
  );
}

/** "523+ (top 500) deals / products · 40 shown (amount filter)" */
function dealsHint(total: number, shown: number): string {
  const capped = total === 500 ? "+ (top 500)" : "";
  const filtered = shown < total ? ` · ${shown} shown (amount filter)` : "";
  return `${total}${capped} deals / products${filtered}`;
}

/** UC2 6.7 Deal Level: deals behind one LST position x period, reference vs. comparison. */
export default function DtiDealsPage() {
  return (
    <Suspense fallback={<Skeleton className="h-96 w-full" />}>
      <DealLevel />
    </Suspense>
  );
}
