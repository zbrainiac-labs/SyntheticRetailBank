"use client";

import { DeltaGrid, type DeltaCellClick } from "@/components/delta-grid";
import { PageFilterBar } from "@/components/dti/filter-bar";
import { StateView } from "@/components/query-view";
import { PageHeader } from "@/components/page-header";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { buildTree, pruneByAmount, reconciliationBreaks } from "@/lib/delta-tree";
import { dtiQuery, useCompare, useDtiMeta, useParameters } from "@/lib/dti/client";
import { indexPositions, periodColumns, thresholdFor, toDeltaRows, type Measure } from "@/lib/dti/positions";
import { LST_LEVEL_COLORS } from "@/lib/constants";
import { usePageFilters } from "@/lib/filters/context";
import { scopeLabel, valueTypeLabel, type DtiFilters, type FilterKey } from "@/lib/filters/model";
import { formatChfM } from "@/lib/format";
import { cn } from "@/lib/utils";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import Link from "next/link";
import { Suspense, useMemo, useState } from "react";

const PAGE = "positions";
const FILTERS: FilterKey[] = ["entities", "referenceDate", "comparisonDate", "valueType", "presentation", "includePreliminary", "minAbsDelta"];
const MEASURES: { v: Measure; label: string }[] = [
  { v: "delta", label: "Delta" },
  { v: "ref", label: "Reference" },
  { v: "comp", label: "Comparison" },
];

type PeriodColumn = ReturnType<typeof periodColumns>[number];

/** Deal-level URL for a clicked cell; back navigation returns to that cell (prototype NavZurueck). */
function dealsHref(f: DtiFilters, code: string, col: PeriodColumn, pathname: string, c: DeltaCellClick): string {
  return `/dti/deals?${dtiQuery(f, {
    position: code,
    period: col.key,
    refFrom: col.refFrom,
    refTo: col.refTo,
    compFrom: col.compFrom,
    compTo: col.compTo,
    from: `${pathname}?${new URLSearchParams({ focus: c.node.id, col: c.column }).toString()}`,
  })}`;
}

/** UC2 6.3 LST Positions: Level I-IV x bucket (or business day), reference vs. comparison date. */
function Positions() {
  const router = useRouter();
  const pathname = usePathname();
  const sp = useSearchParams();
  const { effective: f } = usePageFilters(PAGE, FILTERS);
  const meta = useDtiMeta();
  const params = useParameters();
  const compare = useCompare(f);
  const [measure, setMeasure] = useState<Measure>("delta");

  const idx = useMemo(() => indexPositions(meta.data?.positions ?? []), [meta.data]);
  const columns = useMemo(() => periodColumns(compare.data ?? [], f.presentation), [compare.data, f.presentation]);
  const nodes = useMemo(() => buildTree(toDeltaRows(compare.data ?? [], idx, measure)), [compare.data, idx, measure]);
  const shown = useMemo(() => pruneByAmount(nodes, f.minAbsDelta * 1e6), [nodes, f.minAbsDelta]);
  const breaks = useMemo(() => reconciliationBreaks(nodes, columns.map((c) => c.key)), [nodes, columns]);
  const beforeRef = useMemo(() => (compare.data ?? []).filter((p) => p.info).reduce((s, p) => s + p.comp, 0), [compare.data]);
  const threshold = measure === "delta" ? thresholdFor(f.entities, meta.data?.thresholds ?? {}) : undefined;
  const scope = scopeLabel(f.entities);

  const drill = (c: DeltaCellClick) => {
    const col = columns.find((x) => x.key === c.column);
    const code = idx.codeById.get(c.node.id);
    if (!col || !code) return;
    router.push(dealsHref(f, code, col, pathname, c));
  };

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="DtI comparison"
        title="LST Positions"
        description={`${scope} · ${valueTypeLabel(f.valueType)} values in CHF M · ${f.referenceDate} vs. ${f.comparisonDate} · buckets relative to each date`}
        actions={
          <div className="inline-flex h-8 rounded-md border p-0.5">
            {MEASURES.map((m) => (
              <button
                key={m.v}
                type="button"
                aria-pressed={measure === m.v}
                onClick={() => setMeasure(m.v)}
                className={cn("rounded px-2.5 text-sm", measure === m.v ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:bg-muted")}
              >
                {m.label}
              </button>
            ))}
          </div>
        }
      />
      <PageFilterBar page={PAGE} applicable={FILTERS} />

      <StateView
        pending={compare.isPending || meta.isPending}
        error={compare.error}
        errorTitle="Comparison failed"
        skeleton={<Skeleton className="h-96 w-full" />}
        empty={nodes.length === 0}
        emptyTitle="No flows"
        emptyDescription="No LST flows for this selection. Check the dates, entities or the preliminary filter."
      >
        {() => (
        <>
          <div className="flex flex-wrap items-center gap-2 text-sm">
            <Badge variant={breaks.length === 0 ? "success" : "destructive"}>
              {breaks.length === 0 ? "Control sums reconcile" : `${breaks.length} reconciliation breaks`}
            </Badge>
            {threshold !== undefined && (
              <Badge variant="warning">Highlighted: |delta| ≥ CHF {formatChfM(threshold, 0)}M (comment required)</Badge>
            )}
            {beforeRef !== 0 && (
              <span className="text-muted-foreground">
                Comparison flows dated before the reference date (info, not in delta): CHF {formatChfM(beforeRef)}M
              </span>
            )}
            <span className="text-muted-foreground">Click a value to open the deals behind it.</span>
          </div>
          <div className="flex flex-wrap items-center gap-1 text-sm">
            <span className="mr-1 text-muted-foreground">Collateral Pool by WLK cluster:</span>
            {(params.data?.wlk ?? []).map((w) => (
              <Link
                key={String(w.WLK_CLUSTER)}
                href={`/dti/securities?wlk=${String(w.WLK_CLUSTER)}&from=/dti/positions`}
                title={String(w.CLUSTER_NAME)}
                className="rounded-md border px-2 py-0.5 hover:border-primary hover:text-primary"
              >
                ▸ {String(w.WLK_CLUSTER)}
              </Link>
            ))}
            <span className="ml-3 text-muted-foreground">Fixed cash flows in detail:</span>
            <Link href="/dti/applications" className="text-primary hover:underline">
              Application
            </Link>
            <Link href="/dti/product-groups" className="text-primary hover:underline">
              Product Groups
            </Link>
          </div>
          <DeltaGrid
            key={`${dtiQuery(f)}-${measure}-${f.minAbsDelta}`}
            nodes={shown}
            totalNodes={nodes}
            columns={columns}
            threshold={threshold}
            levelColors={LST_LEVEL_COLORS}
            onCellClick={drill}
            totalLabel="Total liquidity balance"
            focusId={sp.get("focus") ?? undefined}
            focusColumn={sp.get("col") ?? undefined}
          />
        </>
        )}
      </StateView>
    </div>
  );
}

export default function DtiPositionsPage() {
  return (
    <Suspense fallback={<Skeleton className="h-96 w-full" />}>
      <Positions />
    </Suspense>
  );
}
