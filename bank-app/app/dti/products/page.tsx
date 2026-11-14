"use client";

import { ForecastSection, scopeText } from "@/components/dti/common";
import { PageFilterBar } from "@/components/dti/filter-bar";
import { PivotTable, type PivotGroup } from "@/components/dti/pivot-table";
import { EmptyState } from "@/components/empty-state";
import { StateView } from "@/components/query-view";
import { PageHeader } from "@/components/page-header";
import { Skeleton } from "@/components/ui/skeleton";
import { dtiQuery, useCompare, useDtiMeta, useParameters } from "@/lib/dti/client";
import { aggregate, cellOf, MEASURE_LABEL, sideKeys, SIDE_LABEL, valueOf, type Measure, type Pivot } from "@/lib/dti/pivot";
import { indexPositions, pathOf, periodColumns, thresholdFor, type PeriodColumn } from "@/lib/dti/positions";
import { usePageFilters } from "@/lib/filters/context";
import type { FilterKey } from "@/lib/filters/model";
import Link from "next/link";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { Suspense, useMemo } from "react";

const PAGE = "products";
const FILTERS: FilterKey[] = ["entities", "referenceDate", "comparisonDate", "valueType", "presentation", "includePreliminary", "minAbsDelta"];
const SELECT = "h-8 rounded-md border bg-background px-2 text-sm";

/**
 * UC2 6.6 Single Products per Cluster: cluster sums (Netto, Aktiva, Passiva)
 * and one block per single product (Δ, reference, comparison). A product
 * delta opens Deal Level (6.7) for that product and period.
 */
function SingleProducts() {
  const router = useRouter();
  const pathname = usePathname();
  const sp = useSearchParams();
  const cluster = sp.get("cluster") ?? "";
  const product = sp.get("product") ?? "";
  const { effective: f } = usePageFilters(PAGE, FILTERS);
  const meta = useDtiMeta();
  const params = useParameters();
  const compare = useCompare(f, { dims: "product,side", cluster: cluster || null });

  const idx = useMemo(() => indexPositions(meta.data?.positions ?? []), [meta.data]);
  const clusters = useMemo(
    () => (params.data?.clusters ?? []).map((c) => ({ id: String(c.PRODUCT_CLUSTER), name: String(c.CLUSTER_NAME), group: String(c.PRODUCT_GROUP) })),
    [params.data],
  );
  const info = clusters.find((c) => c.id === cluster);
  const points = cluster ? (compare.data ?? []) : [];
  const pivot = useMemo(
    () => aggregate(points, (p) => [...sideKeys("CLUSTER", p.side ?? null), `P:${p.product ?? "—"}`]),
    [points],
  );
  const periods = useMemo(() => periodColumns(points, f.presentation), [points, f.presentation]);
  const products = useMemo(() => [...new Set(points.filter((p) => !p.info).map((p) => p.product ?? "—"))].sort((a, b) => a.localeCompare(b)), [points]);
  const shown = product ? products.filter((p) => p === product) : products;
  const levelNames = useMemo(
    () => [...new Set(points.map((p) => pathOf(p.position, idx.byCode)[0]?.POSITION_NAME).filter(Boolean))].join(" / "),
    [points, idx],
  );
  const threshold = thresholdFor(f.entities, meta.data?.thresholds ?? {});

  const setParam = (k: string, v: string) => {
    const next = new URLSearchParams(sp.toString());
    if (v) next.set(k, v);
    else next.delete(k);
    if (k === "cluster") next.delete("product");
    router.replace(`${pathname}?${next.toString()}`);
  };

  const drill = (prod: string) => (col: PeriodColumn) =>
    router.push(
      `/dti/deals?${dtiQuery(f, {
        cluster,
        product: prod,
        period: col.key,
        refFrom: col.refFrom,
        refTo: col.refTo,
        compFrom: col.compFrom,
        compTo: col.compTo,
        from: `/dti/products?cluster=${cluster}`,
      })}`,
    );

  const groups = (m: Measure | "all", p: Pivot): PivotGroup[] => {
    const clusterCols = (["N", "A", "P"] as const).map((side) => ({
      id: `CLUSTER|${side}`,
      label: SIDE_LABEL[side],
      value: (period: string) => valueOf(cellOf(p, `CLUSTER|${side}`, period), m === "all" ? "delta" : m),
      highlight: m !== "ref" && m !== "comp" && side === "N",
      emphasis: side === "N",
    }));
    const productGroups = shown.map((prod) => {
      const key = `P:${prod}`;
      const measures: Measure[] = m === "all" ? ["delta", "ref", "comp"] : [m];
      return {
        id: key,
        header: <span className="font-mono text-xs">{prod}</span>,
        columns: measures.map((mm) => ({
          id: `${key}|${mm}`,
          label: MEASURE_LABEL[mm],
          value: (period: string) => valueOf(cellOf(p, key, period), mm),
          highlight: mm === "delta",
          onClick: mm === "delta" && prod !== "—" ? drill(prod) : undefined,
        })),
      };
    });
    return [{ id: "cluster", header: <span className="font-medium">Cluster sum (unfiltered)</span>, columns: clusterCols }, ...productGroups];
  };

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="DtI comparison"
        title="Single Products per Cluster"
        description={info ? `${levelNames || "—"} › ${info.group} › ${info.name} · CHF M · ${scopeText(f)}` : `Select a product cluster · ${scopeText(f)}`}
        actions={
          <div className="flex gap-3 text-sm">
            <Link href="/dti/product-groups" className="text-muted-foreground hover:text-foreground">
              Product Groups
            </Link>
            <Link href="/dti/deals" className="text-muted-foreground hover:text-foreground">
              Deal Level
            </Link>
          </div>
        }
      />
      <PageFilterBar page={PAGE} applicable={FILTERS} />
      <div className="flex flex-wrap items-end gap-4">
        <label className="flex flex-col gap-1 text-xs text-muted-foreground">
          <span>Product cluster</span>
          <select className={SELECT} value={cluster} onChange={(e) => setParam("cluster", e.target.value)}>
            <option value="">— select —</option>
            {clusters.map((c) => (
              <option key={c.id} value={c.id}>
                {c.group} › {c.name}
              </option>
            ))}
          </select>
        </label>
        <label className="flex flex-col gap-1 text-xs text-muted-foreground">
          <span>Product</span>
          <select className={SELECT} value={product} onChange={(e) => setParam("product", e.target.value)} disabled={!cluster}>
            <option value="">All</option>
            {products.map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
        </label>
      </div>
      {cluster ? (
        <StateView
          pending={compare.isPending || meta.isPending}
          error={compare.error}
          errorTitle="Comparison failed"
          skeleton={<Skeleton className="h-96 w-full" />}
          empty={periods.length === 0}
          emptyTitle="No flows"
          emptyDescription="This cluster has no flows for the selection."
        >
          {() => (
        <>
          <p className="text-sm text-muted-foreground">Click a product Δ to open its single positions. The cluster sum stays unfiltered when one product is selected.</p>
          <PivotTable minAbs={f.minAbsDelta * 1e6} periods={periods} groups={groups("all", pivot)} threshold={threshold} />
          <ForecastSection
            referenceDate={f.referenceDate}
            comparisonDate={f.comparisonDate}
            render={(m) => <PivotTable minAbs={f.minAbsDelta * 1e6} periods={periods} groups={groups(m, pivot)} />}
          />
        </>
          )}
        </StateView>
      ) : (
        <EmptyState title="No cluster selected" description="Choose a product cluster above, or click a cluster header on Product Groups." />
      )}
    </div>
  );
}

export default function DtiProductsPage() {
  return (
    <Suspense fallback={<Skeleton className="h-96 w-full" />}>
      <SingleProducts />
    </Suspense>
  );
}
