"use client";

import { ChartTooltip, CHART_THEME } from "@/components/chart-utils";
import { EmptyState } from "@/components/empty-state";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useDtiEvents } from "@/lib/dti/client";
import { eventLabel, eventsOnHistory, historyChart, markEvents, type DtiEvent } from "@/lib/dti/events";
import { entityLabel, type DtiFilters } from "@/lib/filters/model";
import { formatChfM } from "@/lib/format";
import { useMemo } from "react";
import { CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

const EVENT_COLOR: Record<string, string> = {
  FUNDING_CLIFF: "var(--destructive)",
  FUNDING_ROLL: "var(--success)",
  DEPOSIT_WITHDRAWAL: "var(--warning)",
};

function HistoryChart({ data, events, f }: Readonly<{ data: { date: string; dti: number }[]; events: DtiEvent[]; f: DtiFilters }>) {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <LineChart data={data} margin={{ top: 16, right: 16, bottom: 0, left: 0 }}>
        <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" />
        <XAxis dataKey="date" tick={CHART_THEME.tick} stroke={CHART_THEME.axis} minTickGap={24} />
        <YAxis tick={CHART_THEME.tick} stroke={CHART_THEME.axis} width={40} domain={[0, "auto"]} allowDecimals={false} />
        <Tooltip content={<ChartTooltip />} />
        {events.map((e) => (
          <ReferenceLine
            key={e.id}
            x={e.date}
            stroke={EVENT_COLOR[e.type] ?? "var(--muted-foreground)"}
            strokeDasharray="3 3"
            label={{ value: e.id, position: "top", fontSize: 10 }}
          />
        ))}
        <ReferenceLine x={f.comparisonDate} stroke="var(--chart-2)" />
        <ReferenceLine x={f.referenceDate} stroke="var(--chart-1)" />
        <Line type="stepAfter" dataKey="dti" name="DtI (days)" stroke="var(--chart-1)" dot={false} strokeWidth={2} />
      </LineChart>
    </ResponsiveContainer>
  );
}

/**
 * DtI history of the selected entities with the events of the book. Events between the comparison and the
 * reference date explain the delta on this page; FUNDING_CLIFF / FUNDING_ROLL are the LCR stress events of
 * the same book (same contract on /liquidity/lcr).
 */
export function DtiEventsCard({ f }: Readonly<{ f: DtiFilters }>) {
  const q = useDtiEvents(f);
  const chart = useMemo(() => historyChart(q.data?.history ?? []), [q.data]);
  const markers = useMemo(() => eventsOnHistory(q.data?.events ?? [], q.data?.history ?? []), [q.data]);
  const events = useMemo(() => markEvents(q.data?.events ?? [], f.comparisonDate, f.referenceDate), [q.data, f.comparisonDate, f.referenceDate]);

  return (
    <Card data-testid="dti-events">
      <CardHeader>
        <CardTitle>DtI history and events</CardTitle>
        <CardDescription>
          Group DtI of the selected entities on each reporting date (stepped, days; top = more than 365) with the events of
          the liquidity book. Events marked &quot;in this comparison&quot; fall between {f.comparisonDate} and {f.referenceDate}.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {q.isError && <EmptyState variant="error" title="Events failed" description={q.error.message} />}
        {q.isPending && <Skeleton className="h-64 w-full" />}
        {q.data && (
          <>
            <div className="h-64">
              <HistoryChart data={chart} events={markers} f={f} />
            </div>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Date</TableHead>
                  <TableHead>Event</TableHead>
                  <TableHead>Entity</TableHead>
                  <TableHead>Description</TableHead>
                  <TableHead numeric>CHF M</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {events.map((e) => (
                  <TableRow key={e.id} className={e.inDelta ? "bg-muted/60" : undefined} data-in-delta={e.inDelta}>
                    <TableCell className="whitespace-nowrap">{e.date}</TableCell>
                    <TableCell className="whitespace-nowrap">
                      <span className="font-mono text-xs text-muted-foreground">{e.id}</span> {eventLabel(e.type)}
                      {e.inDelta && (
                        <Badge variant="warning" className="ml-2">
                          in this comparison
                        </Badge>
                      )}
                    </TableCell>
                    <TableCell className="whitespace-nowrap">{e.entity ? entityLabel(e.entity) : "—"}</TableCell>
                    <TableCell className="min-w-64">{e.description}</TableCell>
                    <TableCell numeric>{e.amount ? formatChfM(e.amount) : "—"}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </>
        )}
      </CardContent>
    </Card>
  );
}
