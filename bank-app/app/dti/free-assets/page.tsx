"use client";

import { ChartTooltip, CHART_THEME, formatTick } from "@/components/chart-utils";
import { Segmented } from "@/components/dti/common";
import { StateView } from "@/components/query-view";
import { PageHeader } from "@/components/page-header";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableFooter, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useDtiMeta, useFreeAssets } from "@/lib/dti/client";
import { useLiquidityBook } from "@/components/liquidity/book-badge";
import { ENTITIES, entityLabel } from "@/lib/filters/model";
import { formatChfM, formatPercent } from "@/lib/format";
import { defaultDate } from "@/lib/liquidity/book";
import { cn } from "@/lib/utils";
import Link from "next/link";
import { useState } from "react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

const HQLA = ["L1", "L2A", "L2B", "NONE"] as const;
const SELECT = "h-8 rounded-md border bg-background px-2 text-sm";
const BASIS_LABEL: Record<string, string> = { CV: "collateral value (LST base)", MV: "market value", NOMINAL: "nominal" };

/**
 * UC2 6.9 Securities - Additional Information: free (unencumbered) assets of
 * the collateral pool. Independent of the Summary filters; own reference date,
 * entity and valuation basis.
 */
export default function DtiFreeAssetsPage() {
  const meta = useDtiMeta();
  const book = useLiquidityBook();
  const [ref, setRef] = useState(defaultDate(meta.data?.dates ?? [], book.data?.asOf) ?? "");
  const [entities, setEntities] = useState<string[]>(["LE_01"]);
  const [basis, setBasis] = useState<"CV" | "NOMINAL" | "MV">("CV");
  const [day, setDay] = useState<"1" | "365">("1");
  const [breakdown, setBreakdown] = useState<"ISSUER_GROUP" | "INDUSTRY" | "CURRENCY">("ISSUER_GROUP");
  const [mobilBasis, setMobilBasis] = useState<"MV" | "NOMINAL">("MV");
  const data = useFreeAssets({ ref, entities: entities.join(","), basis, day, breakdown });
  const d = data.data;
  const basisLabel = BASIS_LABEL[basis] ?? "nominal";
  const entitiesText = entities.length === 1 ? entityLabel(entities[0]) : `${entities.length} entities`;

  const toggleEntity = (e: string) => {
    const next = entities.includes(e) ? entities.filter((x) => x !== e) : [...entities, e].sort((a, b) => a.localeCompare(b));
    if (next.length) setEntities(next);
  };

  const crossTotal = (h: string) => (d?.assets ?? []).filter((a) => a.hqla === h).reduce((s, a) => s + a.value, 0);

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="DtI comparison"
        title="Free Assets in the Collateral Pool"
        description={`Unencumbered liquid assets · ${entitiesText} · ${ref} · ${basisLabel}, CHF M`}
        actions={
          <div className="flex gap-3 text-sm">
            <Link href="/dti/securities" className="text-muted-foreground hover:text-foreground">
              WLK Securities
            </Link>
            <Link href="/dti/parameters" className="text-muted-foreground hover:text-foreground">
              Parameters
            </Link>
          </div>
        }
      />
      <div className="flex flex-wrap items-end gap-4 rounded-lg border bg-card p-3">
        <label className="flex flex-col gap-1 text-xs text-muted-foreground">
          <span>Reference date</span>
          <select className={SELECT} value={ref} onChange={(e) => setRef(e.target.value)}>
            {(meta.data?.dates ?? []).map((x) => (
              <option key={x} value={x}>
                {x}
              </option>
            ))}
          </select>
        </label>
        <div className="flex flex-col gap-1 text-xs text-muted-foreground">
          Legal entities
          <div className="flex gap-1">
            {ENTITIES.map((e) => (
              <button
                key={e}
                type="button"
                title={entityLabel(e)}
                aria-pressed={entities.includes(e)}
                onClick={() => toggleEntity(e)}
                className={cn("h-8 rounded-md border px-2 text-sm", entities.includes(e) ? "border-primary bg-primary/10 text-foreground" : "hover:bg-muted")}
              >
                {e.slice(3)}
              </button>
            ))}
          </div>
        </div>
        <div className="flex flex-col gap-1 text-xs text-muted-foreground">
          Valuation basis
          <Segmented
            value={basis}
            onChange={setBasis}
            options={[
              { v: "CV", label: "Collateral value" },
              { v: "NOMINAL", label: "Nominal" },
              { v: "MV", label: "Market value" },
            ]}
          />
        </div>
        <div className="flex flex-col gap-1 text-xs text-muted-foreground">
          LST day
          <Segmented
            value={day}
            onChange={setDay}
            options={[
              { v: "1", label: "t+1" },
              { v: "365", label: "t+365" },
            ]}
          />
        </div>
      </div>

      <StateView
        pending={!d && !data.isError}
        error={data.error}
        errorTitle="Free assets query failed"
        skeleton={<Skeleton className="h-96 w-full" />}
      >
        {() => d && (
        <>
          <Card>
            <CardHeader>
              <CardTitle>Available assets in the collateral pool</CardTitle>
              <CardDescription>WLK cluster × HQLA category, day {day === "1" ? "t+1 (first day of the LST period)" : "t+365 (last day; securities maturing earlier drop out)"}.</CardDescription>
            </CardHeader>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>WLK cluster</TableHead>
                    <TableHead numeric>Haircut</TableHead>
                    {HQLA.map((h) => (
                      <TableHead key={h} numeric>
                        {h === "NONE" ? "Non-HQLA" : h}
                      </TableHead>
                    ))}
                    <TableHead numeric>Total</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {d.assets.map((a) => (
                    <TableRow key={a.wlk}>
                      <TableCell>
                        <Link href={`/dti/securities?wlk=${a.wlk}&from=/dti/free-assets`} className="hover:underline">
                          <span className="font-medium">{a.wlk}</span> · {a.name}
                        </Link>
                      </TableCell>
                      <TableCell numeric>{formatPercent(a.haircut)}</TableCell>
                      {HQLA.map((h) => (
                        <TableCell key={h} numeric className={cn(a.hqla !== h && "text-muted-foreground/50")}>
                          {a.hqla === h ? formatChfM(a.value) : "–"}
                        </TableCell>
                      ))}
                      <TableCell numeric className="font-medium">
                        {formatChfM(a.value)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
                <TableFooter>
                  <TableRow>
                    <TableCell colSpan={2}>Total</TableCell>
                    {HQLA.map((h) => (
                      <TableCell key={h} numeric>
                        {formatChfM(crossTotal(h))}
                      </TableCell>
                    ))}
                    <TableCell numeric>{formatChfM(d.assets.reduce((s, a) => s + a.value, 0))}</TableCell>
                  </TableRow>
                </TableFooter>
              </Table>
            </CardContent>
          </Card>

          <div className="grid gap-6 lg:grid-cols-2">
            <Card>
              <CardHeader>
                <CardTitle>Collateralised business activities (nominal)</CardTitle>
                <CardDescription>Secured funding on {ref}, with the generally eligible collateral quality.</CardDescription>
              </CardHeader>
              <CardContent className="p-0">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Transaction type</TableHead>
                      <TableHead>Eligible collateral</TableHead>
                      <TableHead numeric>Deals</TableHead>
                      <TableHead numeric>Nominal (M)</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {d.secured.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={4} className="text-muted-foreground">
                          No secured funding for this selection.
                        </TableCell>
                      </TableRow>
                    ) : (
                      d.secured.map((s) => (
                        <TableRow key={s.type}>
                          <TableCell>{s.type.replaceAll("_", " ")}</TableCell>
                          <TableCell className="text-muted-foreground">{s.eligible}</TableCell>
                          <TableCell numeric>{s.deals}</TableCell>
                          <TableCell numeric>{formatChfM(s.nominal)}</TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex-row items-start justify-between gap-2 space-y-0">
                <div className="space-y-1.5">
                  <CardTitle>Breakdown of unencumbered liquid assets</CardTitle>
                  <CardDescription>Concentration by category, {basisLabel}.</CardDescription>
                </div>
                <select className={SELECT} value={breakdown} onChange={(e) => setBreakdown(e.target.value as typeof breakdown)} aria-label="Breakdown category">
                  <option value="ISSUER_GROUP">Issuer group</option>
                  <option value="INDUSTRY">Industry</option>
                  <option value="CURRENCY">Currency</option>
                </select>
              </CardHeader>
              <CardContent className="h-72">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={d.breakdownRows.map((r) => ({ category: r.category, value: Math.round(r.value / 1e5) / 10 }))} layout="vertical" margin={{ left: 8, right: 16 }}>
                    <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" horizontal={false} />
                    <XAxis type="number" tick={CHART_THEME.tick} stroke={CHART_THEME.axis} tickFormatter={formatTick} />
                    <YAxis type="category" dataKey="category" tick={CHART_THEME.tick} stroke={CHART_THEME.axis} width={120} />
                    <Tooltip content={<ChartTooltip />} />
                    <Bar dataKey="value" name="CHF M" fill="var(--chart-1)" radius={[0, 4, 4, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>
          </div>

          <Card>
            <CardHeader className="flex-row items-start justify-between gap-2 space-y-0">
              <div className="space-y-1.5">
                <CardTitle>Mobilisation status of the free assets</CardTitle>
                <CardDescription>Already mobilised vs. not yet mobilised, per WLK cluster, CHF M.</CardDescription>
              </div>
              <Segmented
                value={mobilBasis}
                onChange={setMobilBasis}
                options={[
                  { v: "MV", label: "Market value" },
                  { v: "NOMINAL", label: "Nominal" },
                ]}
              />
            </CardHeader>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>WLK cluster</TableHead>
                    <TableHead numeric>Mobilised</TableHead>
                    <TableHead numeric>Not mobilised</TableHead>
                    <TableHead numeric>Total</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {d.mobilisation.map((m) => {
                    const mob = mobilBasis === "MV" ? m.mobilisedMv : m.mobilisedNominal;
                    const free = mobilBasis === "MV" ? m.freeMv : m.freeNominal;
                    return (
                      <TableRow key={m.wlk}>
                        <TableCell>{m.wlk}</TableCell>
                        <TableCell numeric>{formatChfM(mob)}</TableCell>
                        <TableCell numeric>{formatChfM(free)}</TableCell>
                        <TableCell numeric className="font-medium">
                          {formatChfM(mob + free)}
                        </TableCell>
                      </TableRow>
                    );
                  })}
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
