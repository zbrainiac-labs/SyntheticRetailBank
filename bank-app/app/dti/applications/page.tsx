"use client";

import { ForecastSection, scopeText } from "@/components/dti/common";
import { PageFilterBar } from "@/components/dti/filter-bar";
import { PivotTable, type PivotGroup } from "@/components/dti/pivot-table";
import { StateView } from "@/components/query-view";
import { PageHeader } from "@/components/page-header";
import { Skeleton } from "@/components/ui/skeleton";
import { useCompare, useDtiMeta, useParameters } from "@/lib/dti/client";
import { aggregate, cellOf, sideKeys, valueOf, type Measure, type Pivot } from "@/lib/dti/pivot";
import { indexPositions, pathOf, periodColumns, thresholdFor } from "@/lib/dti/positions";
import { usePageFilters } from "@/lib/filters/context";
import type { FilterKey } from "@/lib/filters/model";
import { ChevronDown, ChevronRight } from "lucide-react";
import Link from "next/link";
import { useMemo, useState } from "react";

const PAGE = "applications";
// Wertefilter has no effect here: fixed cash flows are deterministic (base = stressed).
const FILTERS: FilterKey[] = ["entities", "referenceDate", "comparisonDate", "presentation", "includePreliminary", "minAbsDelta"];
const NO_APP = "CORR";

/** Set with `id` added, or removed if it was present (new Set, for React state). */
function toggled(s: Set<string>, id: string): Set<string> {
  const next = new Set(s);
  if (next.has(id)) next.delete(id);
  else next.add(id);
  return next;
}

/** UC2 6.4 Application Level: fixed cash flows per application (Saldo, Aktiva, Passiva). */
export default function DtiApplicationsPage() {
  const { effective } = usePageFilters(PAGE, FILTERS);
  const f = { ...effective, valueType: "BASE" as const };
  const meta = useDtiMeta();
  const params = useParameters();
  const compare = useCompare(f, { dims: "app,side" });
  const [open, setOpen] = useState<Set<string>>(new Set());

  const idx = useMemo(() => indexPositions(meta.data?.positions ?? []), [meta.data]);
  const det = useMemo(
    () => (compare.data ?? []).filter((p) => pathOf(p.position, idx.byCode)[0]?.POSITION_CODE === "DET"),
    [compare.data, idx],
  );
  const pivot = useMemo(() => aggregate(det, (p) => sideKeys(p.application ?? NO_APP, p.side ?? null)), [det]);
  const periods = useMemo(() => periodColumns(det, f.presentation), [det, f.presentation]);
  const apps = useMemo(() => {
    const list = (params.data?.applications ?? []).map((a) => ({ id: String(a.APPLICATION_ID), name: String(a.APPLICATION_NAME) }));
    return [...list, { id: NO_APP, name: "Corrections" }].filter((a) => pivot.has(`${a.id}|N`));
  }, [params.data, pivot]);
  const threshold = thresholdFor(f.entities, meta.data?.thresholds ?? {});

  const groups = (m: Measure, p: Pivot): PivotGroup[] =>
    apps.map((a) => {
      const expanded = open.has(a.id);
      const col = (side: "N" | "A" | "P", label: string) => ({
        id: `${a.id}|${side}`,
        label,
        value: (period: string) => valueOf(cellOf(p, `${a.id}|${side}`, period), m),
        highlight: m === "delta" && side === "N",
        emphasis: side === "N",
      });
      return {
        id: a.id,
        header: (
          <button
            type="button"
            onClick={() => setOpen((s) => toggled(s, a.id))}
            className="inline-flex items-center gap-1 font-medium text-foreground"
            aria-expanded={expanded}
            title="Expand / collapse Aktiva and Passiva"
          >
            {expanded ? <ChevronDown className="size-4" /> : <ChevronRight className="size-4" />}
            {a.id === NO_APP ? a.name : `${a.id} · ${a.name}`}
          </button>
        ),
        columns: expanded ? [col("N", "Saldo"), col("A", "Aktiva"), col("P", "Passiva")] : [col("N", "Saldo")],
      };
    });

  const pending = compare.isPending || meta.isPending || params.isPending;

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="DtI comparison"
        title="Application Level"
        description={`Changes in the fixed cash flows (incl. corrections) per application, CHF M · ${scopeText(f, false)}`}
        actions={
          <div className="flex gap-3 text-sm">
            <Link href="/dti/positions" className="text-muted-foreground hover:text-foreground">
              LST Positions
            </Link>
            <Link href="/dti/product-groups" className="text-muted-foreground hover:text-foreground">
              Product Groups
            </Link>
          </div>
        }
      />
      <PageFilterBar page={PAGE} applicable={FILTERS} />
      <StateView
        pending={pending}
        error={compare.error}
        errorTitle="Comparison failed"
        skeleton={<Skeleton className="h-96 w-full" />}
        empty={apps.length === 0}
        emptyTitle="No fixed cash flows"
        emptyDescription="No deterministic flows for this selection."
      >
        {() => (
        <>
          <p className="text-sm text-muted-foreground">Delta = reference minus comparison. Click an application header to show Aktiva and Passiva.</p>
          <PivotTable minAbs={effective.minAbsDelta * 1e6} periods={periods} groups={groups("delta", pivot)} threshold={threshold} />
          <ForecastSection
            referenceDate={f.referenceDate}
            comparisonDate={f.comparisonDate}
            render={(m) => <PivotTable minAbs={effective.minAbsDelta * 1e6} periods={periods} groups={groups(m, pivot)} />}
          />
        </>
        )}
      </StateView>
    </div>
  );
}
