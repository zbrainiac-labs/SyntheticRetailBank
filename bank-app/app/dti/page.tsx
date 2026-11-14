"use client";

import { ChartTooltip, CHART_THEME, formatTick, getYAxisWidth } from "@/components/chart-utils";
import { PageFilterBar } from "@/components/dti/filter-bar";
import { EmptyState } from "@/components/empty-state";
import { PageHeader } from "@/components/page-header";
import { StatCard, StatCardSkeleton } from "@/components/stat-card";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useSummary } from "@/lib/dti/client";
import type { EntityDti, ProjectionPoint } from "@/lib/dti/types";
import { LST_LEVEL_COLORS } from "@/lib/constants";
import { usePageFilters } from "@/lib/filters/context";
import { ENTITIES, entityLabel, scopeLabel, valueTypeLabel, type FilterKey } from "@/lib/filters/model";
import { formatChfM } from "@/lib/format";
import { CalendarClock, Hourglass, TrendingDown } from "lucide-react";
import Link from "next/link";
import { useMemo, useState } from "react";
import { CartesianGrid, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

const PAGE = "summary";
const FILTERS: FilterKey[] = ["entities", "referenceDate", "comparisonDate", "valueType", "includePreliminary"];

const LEVELS: { key: keyof ProjectionPoint; label: string }[] = [
  { key: "pool", label: "Collateral Pool" },
  { key: "det", label: "Deterministic Cash Flows" },
  { key: "inf", label: "Inflows" },
  { key: "out", label: "Outflows" },
];
const LEVEL_ROWS = [...LEVELS, { key: "total" as const, label: "Total liquidity balance" }];

type Dti = { days: number; date: string } | null;
type SummaryData = NonNullable<ReturnType<typeof useSummary>["data"]>;

function dtiText(d: { days: number } | null): string {
  return d ? `${d.days} days` : "> 365 days";
}

function dtiHint(d: Dti): string {
  return d ? `illiquid on ${d.date}` : "liquidity positive for 365 days";
}

/** "+3" / "-2" (sign always shown). */
function signed(n: number): string {
  return `${n >= 0 ? "+" : ""}${n}`;
}

/** Reference minus comparison DtI, or null when either side is > 365 days. */
function dtiChange(ref: Dti, comp: Dti): number | null {
  return ref && comp ? ref.days - comp.days : null;
}

const SLOTS = 5;

/** Entity DtI cell: "> 365" when liquid for a year, hidden while preliminary unless included. */
function entityDti(d: EntityDti | null, includePrelim: boolean): { text: string; days: number | null } {
  if (!d) return { text: "no run", days: null };
  if (d.status !== "FINAL" && !includePrelim) return { text: "preliminary", days: null };
  return { text: d.days === null ? "> 365" : String(d.days), days: d.days ?? 366 };
}

function KpiCards({ data, pending }: Readonly<{ data: SummaryData | undefined; pending: boolean }>) {
  if (pending || !data) return [0, 1, 2].map((i) => <StatCardSkeleton key={i} />);
  const change = dtiChange(data.ref.dti, data.comp.dti);
  return (
    <>
      <StatCard label="DtI reference date" value={dtiText(data.ref.dti)} hint={dtiHint(data.ref.dti)} icon={Hourglass} />
      <StatCard label="DtI comparison date" value={dtiText(data.comp.dti)} hint={dtiHint(data.comp.dti)} icon={CalendarClock} />
      <StatCard
        label="Change"
        value={change === null ? "—" : `${signed(change)} days`}
        hint="reference minus comparison"
        icon={TrendingDown}
      />
    </>
  );
}

type ChartPoint = {
  day: number;
  ref: number;
  comp: number | null;
};

function ProjectionChart({ chart, refDate, compDate }: Readonly<{ chart: ChartPoint[]; refDate: string; compDate: string }>) {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <LineChart data={chart} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
        <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" />
        <XAxis dataKey="day" tick={CHART_THEME.tick} stroke={CHART_THEME.axis} ticks={[0, 30, 60, 90, 180, 270, 365]} />
        <YAxis tick={CHART_THEME.tick} stroke={CHART_THEME.axis} tickFormatter={formatTick} width={getYAxisWidth(chart, "ref")} />
        <ReferenceLine y={0} stroke="var(--destructive)" strokeDasharray="4 4" />
        <Legend />
        <Tooltip content={<ChartTooltip />} labelFormatter={(d) => `Day ${d}`} />
        <Line type="monotone" dataKey="ref" name={`Reference ${refDate}`} stroke="var(--chart-1)" dot={false} strokeWidth={2} />
        <Line type="monotone" dataKey="comp" name={`Comparison ${compDate}`} stroke="var(--chart-2)" dot={false} strokeWidth={2} strokeDasharray="5 3" />
      </LineChart>
    </ResponsiveContainer>
  );
}

interface SlotRowProps {
  index: number;
  entity: string | null;
  data: SummaryData | undefined;
  pending: boolean;
  includePreliminary: boolean;
  onChange: (index: number, entity: string | null) => void;
}

function EntitySlotRow({ index, entity, data, pending, includePreliminary, onChange }: Readonly<SlotRowProps>) {
  const row = data?.entities.find((x) => x.entity === entity);
  const r = entityDti(row?.ref ?? null, includePreliminary);
  const c = entityDti(row?.comp ?? null, includePreliminary);
  const change = r.days !== null && c.days !== null && r.days <= 365 && c.days <= 365 ? r.days - c.days : null;
  return (
    <TableRow>
      <TableCell>
        <select
          aria-label={`Entity slot ${index + 1}`}
          value={entity ?? ""}
          onChange={(ev) => onChange(index, ev.target.value || null)}
          className="h-8 rounded-md border bg-background px-2 text-sm"
        >
          <option value="">—</option>
          {ENTITIES.map((x) => (
            <option key={x} value={x}>
              {entityLabel(x)}
            </option>
          ))}
        </select>
      </TableCell>
      {entity ? (
        <>
          <TableCell numeric>{pending ? "…" : r.text}</TableCell>
          <TableCell numeric>{pending ? "…" : c.text}</TableCell>
          <TableCell numeric>{change === null ? "—" : signed(change)}</TableCell>
        </>
      ) : (
        <TableCell colSpan={3} />
      )}
    </TableRow>
  );
}

/** Group DtI cell: "…" while loading, "> 365" when liquid for a year. */
function groupDtiCell(pending: boolean, d: Dti): string | number {
  if (pending) return "…";
  return d ? d.days : "> 365";
}

function GroupRow({ scope, data, pending }: Readonly<{ scope: string; data: SummaryData | undefined; pending: boolean }>) {
  const change = data ? dtiChange(data.ref.dti, data.comp.dti) : null;
  return (
    <TableRow className="font-semibold">
      <TableCell>{scope}</TableCell>
      <TableCell numeric>{groupDtiCell(pending, data?.ref.dti ?? null)}</TableCell>
      <TableCell numeric>{groupDtiCell(pending, data?.comp.dti ?? null)}</TableCell>
      <TableCell numeric>{change === null ? "—" : signed(change)}</TableCell>
    </TableRow>
  );
}

const horizon = (pts: ProjectionPoint[] | undefined, day: number) => pts?.find((p) => p.day === day);

function LevelRow({ level, data, pending }: Readonly<{ level: (typeof LEVEL_ROWS)[number]; data: SummaryData | undefined; pending: boolean }>) {
  const value = (pts: ProjectionPoint[] | undefined, day: number) => Number(horizon(pts, day)?.[level.key] ?? 0);
  const r30 = value(data?.ref.points, 30);
  const c30 = value(data?.comp.points, 30);
  const r365 = value(data?.ref.points, 365);
  const c365 = value(data?.comp.points, 365);
  const cells: [string, number][] = [
    ["r30", r30],
    ["c30", c30],
    ["d30", r30 - c30],
    ["r365", r365],
    ["c365", c365],
    ["d365", r365 - c365],
  ];
  const total = level.key === "total";
  return (
    <TableRow className={total ? "font-semibold" : undefined}>
      <TableCell>
        <span className="flex items-center gap-2">
          {LST_LEVEL_COLORS[level.label] && <span className="size-2.5 rounded-full" style={{ background: LST_LEVEL_COLORS[level.label] }} />}
          {total ? level.label : <Link href="/dti/positions" className="hover:underline">{level.label}</Link>}
        </span>
      </TableCell>
      {cells.map(([k, v]) => (
        <TableCell key={k} numeric>
          {pending ? "…" : formatChfM(v)}
        </TableCell>
      ))}
    </TableRow>
  );
}

/** UC2 6.1 Summary: DtI of both dates, Level I balances and the 365-day projection. */
export default function DtiSummaryPage() {
  const { effective: f } = usePageFilters(PAGE, FILTERS);
  const summary = useSummary(f);
  const data = summary.data;
  const pending = summary.isPending;

  const chart = useMemo<ChartPoint[]>(() => {
    if (!data) return [];
    const comp = new Map(data.comp.points.map((p) => [p.day, p.total]));
    return data.ref.points.map((p) => ({
      day: p.day,
      ref: Math.round(p.total / 1e5) / 10,
      comp: comp.has(p.day) ? Math.round((comp.get(p.day) ?? 0) / 1e5) / 10 : null,
    }));
  }, [data]);

  const scope = scopeLabel(f.entities);
  const values = valueTypeLabel(f.valueType);
  // entity slots of the prototype summary: own selection per slot, defaults to the selected entities
  const [slots, setSlots] = useState<(string | null)[] | null>(null);
  const shownSlots = slots ?? Array.from({ length: SLOTS }, (_, i) => f.entities[i] ?? null);
  const setSlot = (index: number, entity: string | null) => {
    const next = [...shownSlots];
    next[index] = entity;
    setSlots(next);
  };

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="Days to Illiquidity (DtI) comparison"
        title="Summary"
        description={`${scope} · ${values} values · ${f.referenceDate} vs. ${f.comparisonDate}`}
      />
      <PageFilterBar page={PAGE} applicable={FILTERS} />

      {summary.isError ? (
        <EmptyState variant="error" title="Summary failed" description={summary.error.message} />
      ) : (
        <>
          <div className="grid gap-4 sm:grid-cols-3">
            <KpiCards data={data} pending={pending} />
          </div>

          <Card>
            <CardHeader>
              <CardTitle>Liquidity projection</CardTitle>
              <CardDescription>Cumulative total liquidity balance in CHF M; DtI = first day at or below zero.</CardDescription>
            </CardHeader>
            <CardContent className="h-80">
              {pending ? (
                <Skeleton className="h-full w-full" />
              ) : (
                <ProjectionChart chart={chart} refDate={f.referenceDate} compDate={f.comparisonDate} />
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>DtI per entity</CardTitle>
              <CardDescription>
                Up to {SLOTS} entities and the group (sum of the selected entities); DtI in days, {values} values.
              </CardDescription>
            </CardHeader>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Entity</TableHead>
                    <TableHead numeric>DtI {f.referenceDate}</TableHead>
                    <TableHead numeric>DtI {f.comparisonDate}</TableHead>
                    <TableHead numeric>Change (days)</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {shownSlots.map((e, i) => (
                    // slots are positional (slot 1..5), so the slot number is the identity
                    <EntitySlotRow
                      key={`slot-${i + 1}`}
                      index={i}
                      entity={e}
                      data={data}
                      pending={pending}
                      includePreliminary={f.includePreliminary}
                      onChange={setSlot}
                    />
                  ))}
                  <GroupRow scope={scope} data={data} pending={pending} />
                </TableBody>
              </Table>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Level I positions</CardTitle>
              <CardDescription>Cumulative balance in CHF M after 30 and 365 days. Open LST Positions for the bucket view.</CardDescription>
            </CardHeader>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Position</TableHead>
                    <TableHead numeric>Ref. 30d</TableHead>
                    <TableHead numeric>Comp. 30d</TableHead>
                    <TableHead numeric>Delta 30d</TableHead>
                    <TableHead numeric>Ref. 365d</TableHead>
                    <TableHead numeric>Comp. 365d</TableHead>
                    <TableHead numeric>Delta 365d</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {LEVEL_ROWS.map((l) => (
                    <LevelRow key={l.key} level={l} data={data} pending={pending} />
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}
