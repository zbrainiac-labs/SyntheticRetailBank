"use client";

import { ForecastSection, scopeText } from "@/components/dti/common";
import { PageFilterBar } from "@/components/dti/filter-bar";
import { PivotTable, type PivotGroup } from "@/components/dti/pivot-table";
import { StateView } from "@/components/query-view";
import { PageHeader } from "@/components/page-header";
import { Skeleton } from "@/components/ui/skeleton";
import { useCompare, useDtiMeta, useParameters } from "@/lib/dti/client";
import { aggregate, cellOf, sideKeys, SIDE_LABEL, valueOf, type Measure, type Pivot } from "@/lib/dti/pivot";
import { indexPositions, pathOf, periodColumns, thresholdFor } from "@/lib/dti/positions";
import { LST_LEVEL_COLORS } from "@/lib/constants";
import { usePageFilters } from "@/lib/filters/context";
import type { FilterKey } from "@/lib/filters/model";
import { ChevronDown, ChevronRight } from "lucide-react";
import Link from "next/link";
import type React from "react";
import { useMemo, useState } from "react";

const PAGE = "product-groups";
const FILTERS: FilterKey[] = ["entities", "referenceDate", "comparisonDate", "valueType", "presentation", "includePreliminary", "minAbsDelta"];
const LEVEL_ORDER = ["POOL", "DET", "INF", "OUT"];
const NONE = "NONE";

function toggle(s: Set<string>, k: string): Set<string> {
  const n = new Set(s);
  if (n.has(k)) n.delete(k);
  else n.add(k);
  return n;
}

/**
 * UC2 6.5 Product Groups: columns on three levels (Level I position ›
 * product group › product cluster), each split into Aktiva / Passiva.
 * A cluster header opens Single Products (6.6) for that cluster.
 */
export default function DtiProductGroupsPage() {
  const { effective: f } = usePageFilters(PAGE, FILTERS);
  const meta = useDtiMeta();
  const params = useParameters();
  const compare = useCompare(f, { dims: "cluster,side" });
  const [open, setOpen] = useState<Set<string>>(new Set());

  const idx = useMemo(() => indexPositions(meta.data?.positions ?? []), [meta.data]);
  const clusterInfo = useMemo(
    () => new Map((params.data?.clusters ?? []).map((c) => [String(c.PRODUCT_CLUSTER), { group: String(c.PRODUCT_GROUP), name: String(c.CLUSTER_NAME) }])),
    [params.data],
  );
  const groupOf = (cluster: string | null | undefined) => (cluster ? (clusterInfo.get(cluster)?.group ?? "Other") : "Corrections");

  const points = compare.data ?? [];
  const root = (code: string) => pathOf(code, idx.byCode)[0]?.POSITION_CODE ?? code;
  const pivot = useMemo(
    () =>
      aggregate(points, (p) => {
        const l = root(p.position);
        const g = groupOf(p.cluster);
        const c = p.cluster ?? NONE;
        return [...sideKeys(`L:${l}`, p.side ?? null), ...sideKeys(`G:${l}:${g}`, p.side ?? null), ...sideKeys(`C:${l}:${c}`, p.side ?? null)];
      }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [points, idx, clusterInfo],
  );
  const periods = useMemo(() => periodColumns(points, f.presentation), [points, f.presentation]);
  const threshold = thresholdFor(f.entities, meta.data?.thresholds ?? {});

  // Column tree present in the data, in Level I / cluster sort order
  const tree = useMemo(() => {
    const order = [...clusterInfo.keys()];
    const rank = (c: string) => (order.includes(c) ? order.indexOf(c) : order.length);
    const levels = new Map<string, Map<string, Set<string>>>();
    for (const p of points) {
      if (p.info) continue;
      const l = root(p.position);
      const g = groupOf(p.cluster);
      const byGroup = levels.get(l) ?? new Map<string, Set<string>>();
      byGroup.set(g, (byGroup.get(g) ?? new Set()).add(p.cluster ?? NONE));
      levels.set(l, byGroup);
    }
    return LEVEL_ORDER.filter((l) => levels.has(l)).map((l) => ({
      level: l,
      groups: [...(levels.get(l) ?? new Map()).entries()]
        .map(([g, cs]) => ({ group: g, clusters: [...cs].sort((a, b) => rank(a) - rank(b)) }))
        .sort((a, b) => rank(a.clusters[0]) - rank(b.clusters[0])),
    }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [points, idx, clusterInfo]);

  const name = (l: string) => idx.byCode.get(l)?.POSITION_NAME ?? l;
  const cols = (key: string, m: Measure, p: Pivot) =>
    (["N", "A", "P"] as const).map((side) => ({
      id: `${key}|${side}`,
      label: SIDE_LABEL[side],
      value: (period: string) => valueOf(cellOf(p, `${key}|${side}`, period), m),
      highlight: m === "delta" && side === "N",
      emphasis: side === "N",
    }));
  const header = (key: string, label: React.ReactNode, color?: string, expandable = true) =>
    expandable ? (
      <button type="button" onClick={() => setOpen((s) => toggle(s, key))} className="inline-flex items-center gap-1 font-medium text-foreground" aria-expanded={open.has(key)}>
        {open.has(key) ? <ChevronDown className="size-4" /> : <ChevronRight className="size-4" />}
        {color && <span className="size-2.5 rounded-full" style={{ background: color }} />}
        {label}
      </button>
    ) : (
      label
    );

  const groups = (m: Measure, p: Pivot): PivotGroup[] =>
    tree.flatMap(({ level, groups: gs }) => {
      const lk = `L:${level}`;
      if (!open.has(lk)) return [{ id: lk, header: header(lk, name(level), LST_LEVEL_COLORS[name(level)]), columns: cols(lk, m, p) }];
      return gs.flatMap(({ group, clusters }) => {
        const gk = `G:${level}:${group}`;
        if (!open.has(gk)) return [{ id: gk, header: header(gk, `${name(level)} › ${group}`), columns: cols(gk, m, p) }];
        return clusters.map((c) => {
          const ck = `C:${level}:${c}`;
          const label =
            c === NONE ? (
              `${group} (no cluster)`
            ) : (
              <Link href={`/dti/products?cluster=${encodeURIComponent(c)}`} className="font-medium text-primary hover:underline" title="Open Single Products for this cluster">
                ▸ {clusterInfo.get(c)?.name ?? c}
              </Link>
            );
          return { id: ck, header: label, columns: cols(ck, m, p) };
        });
      });
    });

  const pending = compare.isPending || meta.isPending || params.isPending;

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="DtI comparison"
        title="Product Groups"
        description={`Changes per Level I position › product group › product cluster, CHF M · ${scopeText(f)}`}
        actions={
          <div className="flex gap-3 text-sm">
            <Link href="/dti/positions" className="text-muted-foreground hover:text-foreground">
              LST Positions
            </Link>
            <Link href="/dti/products" className="text-muted-foreground hover:text-foreground">
              Single Products
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
        empty={tree.length === 0}
        emptyTitle="No flows"
        emptyDescription="No LST flows for this selection."
      >
        {() => (
        <>
          <p className="text-sm text-muted-foreground">
            Expand a Level I position to its product groups and a product group to its clusters; a cluster header (▸) opens its single products.
          </p>
          <PivotTable minAbs={f.minAbsDelta * 1e6} periods={periods} groups={groups("delta", pivot)} threshold={threshold} />
          <ForecastSection
            referenceDate={f.referenceDate}
            comparisonDate={f.comparisonDate}
            render={(m) => <PivotTable minAbs={f.minAbsDelta * 1e6} periods={periods} groups={groups(m, pivot)} />}
          />
        </>
        )}
      </StateView>
    </div>
  );
}
