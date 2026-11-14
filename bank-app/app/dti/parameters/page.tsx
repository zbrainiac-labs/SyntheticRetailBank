"use client";

import { EmptyState } from "@/components/empty-state";
import { PageHeader } from "@/components/page-header";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useDtiMeta, useParameters } from "@/lib/dti/client";
import { pathOf } from "@/lib/dti/positions";
import { entityLabel } from "@/lib/filters/model";
import { formatChfM, formatPercent, formatValue } from "@/lib/format";

type Row = Record<string, unknown>;

/** Bucket upper limit: "+3 months", "+1 day", or "open end" for the last bucket. */
function bucketLimit(v: unknown, r: Row): string {
  if (v === null) return "open end";
  const plural = Number(v) === 1 ? "" : "s";
  return `+${formatValue(v)} ${formatValue(r.OFFSET_UNIT).toLowerCase()}${plural}`;
}

function RefTable({ title, description, rows, cols }: { title: string; description?: string; rows: Row[]; cols: { key: string; label: string; fmt?: (v: unknown, r: Row) => string; numeric?: boolean }[] }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        {description && <CardDescription>{description}</CardDescription>}
      </CardHeader>
      <CardContent className="max-h-96 overflow-auto p-0">
        <Table>
          <TableHeader>
            <TableRow>
              {cols.map((c) => (
                <TableHead key={c.key} numeric={c.numeric}>
                  {c.label}
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((r) => (
              <TableRow key={cols.map((c) => String(r[c.key])).join("|")}>
                {cols.map((c) => (
                  <TableCell key={c.key} numeric={c.numeric}>
                    {c.fmt ? c.fmt(r[c.key], r) : formatValue(r[c.key])}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

/** UC2 6.10 Parameter Page: limits, LST position scheme, applications, product clusters, WLK clusters, buckets, calendar. */
export default function DtiParametersPage() {
  const params = useParameters();
  const meta = useDtiMeta();

  if (params.isPending || meta.isPending) return <Skeleton className="h-96 w-full" />;
  if (params.isError) return <EmptyState variant="error" title="Parameters unavailable" description={params.error.message} />;
  const p = params.data;
  const byCode = new Map((meta.data?.positions ?? []).map((x) => [x.POSITION_CODE, x]));
  const scheme = [...(meta.data?.positions ?? [])]
    .map((x) => ({ ...x, path: pathOf(x.POSITION_CODE, byCode) }))
    .sort((a, b) => a.path.map((q) => String(q.SORT_ORDER).padStart(3, "0")).join(".").localeCompare(b.path.map((q) => String(q.SORT_ORDER).padStart(3, "0")).join(".")));

  return (
    <div className="space-y-6">
      <PageHeader eyebrow="DtI comparison" title="Parameters" description="Every provision the DtI comparison relies on. Values are maintained in the reference tables of the liquidity book." />
      <div className="grid gap-6 lg:grid-cols-2">
        <RefTable
          title="Commenting thresholds"
          description="Deltas at or above the limit are highlighted and need a comment (entity limit for one entity, group limit otherwise)."
          rows={p.limits}
          cols={[
            { key: "ENTITY_ID", label: "Entity", fmt: (v) => (v === "GROUP" ? "Group" : entityLabel(String(v))) },
            { key: "BUCKET_CODE", label: "Bucket", fmt: (v) => (v === "*" ? "all" : String(v)) },
            { key: "COMMENT_THRESHOLD_CHF", label: "Threshold (CHF M)", numeric: true, fmt: (v) => formatChfM(v, 0) },
          ]}
        />
        <RefTable
          title="Legal entities"
          description="Group = sum of the selected entities; central bank cash is allocated by the fixed share."
          rows={p.entities}
          cols={[
            { key: "ENTITY_ID", label: "ID" },
            { key: "ENTITY_NAME", label: "Name" },
            { key: "CB_BALANCE_SHARE", label: "CB share", numeric: true, fmt: (v) => formatPercent(v) },
            { key: "IS_DEFAULT", label: "Default", fmt: (v) => (v ? "yes" : "") },
          ]}
        />
      </div>
      <Card>
        <CardHeader>
          <CardTitle>LST position scheme</CardTitle>
          <CardDescription>Level I–IV; leaves carry the flows, parents are sums.</CardDescription>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Position</TableHead>
                <TableHead>Code</TableHead>
                <TableHead>Level</TableHead>
                <TableHead>Leaf</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {scheme.map((x) => (
                <TableRow key={x.POSITION_CODE}>
                  <TableCell style={{ paddingLeft: `${(x.LEVEL_NO - 1) * 20 + 16}px` }} className={x.LEVEL_NO === 1 ? "font-semibold" : undefined}>
                    {x.POSITION_NAME}
                  </TableCell>
                  <TableCell className="font-mono text-xs">{x.POSITION_CODE}</TableCell>
                  <TableCell>{x.LEVEL_NO}</TableCell>
                  <TableCell>{x.IS_LEAF ? "yes" : ""}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
      <div className="grid gap-6 lg:grid-cols-2">
        <RefTable title="Applications" rows={p.applications} cols={[{ key: "APPLICATION_ID", label: "ID" }, { key: "APPLICATION_NAME", label: "Name" }]} />
        <RefTable
          title="Product groups and clusters"
          rows={p.clusters}
          cols={[
            { key: "PRODUCT_GROUP", label: "Product group" },
            { key: "PRODUCT_CLUSTER", label: "Cluster" },
            { key: "CLUSTER_NAME", label: "Name" },
          ]}
        />
        <RefTable
          title="WLK clusters"
          rows={p.wlk}
          cols={[
            { key: "WLK_CLUSTER", label: "Cluster" },
            { key: "CLUSTER_NAME", label: "Name" },
            { key: "HQLA_CATEGORY", label: "HQLA" },
            { key: "HAIRCUT", label: "Haircut", numeric: true, fmt: (v) => formatPercent(v) },
          ]}
        />
        <RefTable
          title="Time buckets"
          description="Defined relative to each data set's own reporting date."
          rows={p.buckets}
          cols={[
            { key: "BUCKET_CODE", label: "Bucket" },
            { key: "BUCKET_NAME", label: "Name" },
            { key: "OFFSET_VALUE", label: "Up to", fmt: (v, r) => bucketLimit(v, r) },
          ]}
        />
        <RefTable title="Bank holidays (Zurich)" description="Non-business days of the daily view." rows={p.holidays} cols={[{ key: "HOLIDAY_DATE", label: "Date" }, { key: "HOLIDAY_NAME", label: "Holiday" }]} />
        <RefTable
          title="Position assignment rules"
          description="Source product → LST leaf position and product cluster."
          rows={p.assignment}
          cols={[
            { key: "SOURCE_SYSTEM", label: "Source" },
            { key: "PRODUCT_TYPE", label: "Product" },
            { key: "FLOW_KIND", label: "Flow" },
            { key: "POSITION_CODE", label: "Position" },
            { key: "PRODUCT_CLUSTER", label: "Cluster" },
          ]}
        />
      </div>
    </div>
  );
}
