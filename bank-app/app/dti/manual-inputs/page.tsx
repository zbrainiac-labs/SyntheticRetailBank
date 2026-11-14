"use client";

import { QueryView } from "@/components/query-view";
import { PageHeader } from "@/components/page-header";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { dtiWrite, useCorrections, useDtiAccess, useDtiMeta, useLimits } from "@/lib/dti/client";
import { usePageFilters } from "@/lib/filters/context";
import { ENTITIES, entityLabel } from "@/lib/filters/model";
import { formatChfM, formatNumber } from "@/lib/format";
import { useQueryClient } from "@tanstack/react-query";
import { Lock, RefreshCw, Trash2 } from "lucide-react";
import { useState } from "react";

const INPUT = "h-8 rounded-md border bg-background px-2 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring";

/**
 * UC2 manual inputs: corrections on LST position x date (6.3, AC-4) and the
 * commenting thresholds (6.1/6.10). Everyone sees them; only DTI_OWNER edits.
 */
export default function DtiManualInputsPage() {
  const qc = useQueryClient();
  const access = useDtiAccess();
  const meta = useDtiMeta();
  const { effective: f } = usePageFilters("manual-inputs", ["referenceDate"]);
  const corrections = useCorrections(f.referenceDate);
  const limits = useLimits();
  const canEdit = access.data?.canEdit ?? false;
  const leaves = (meta.data?.positions ?? []).filter((p) => p.IS_LEAF);

  const [form, setForm] = useState({ entity: "LE_01", position: "POOL_CB_BALANCE", valueDate: "", amount: "", comment: "" });
  const [status, setStatus] = useState<{ kind: "ok" | "error"; text: string } | null>(null);
  const [busy, setBusy] = useState(false);

  const run = async (fn: () => Promise<unknown>, ok: string) => {
    setBusy(true);
    setStatus(null);
    try {
      await fn();
      setStatus({ kind: "ok", text: ok });
      await qc.invalidateQueries({ queryKey: ["dti"] });
    } catch (e) {
      setStatus({ kind: "error", text: (e as Error).message });
    } finally {
      setBusy(false);
    }
  };

  const add = () =>
    run(
      () =>
        dtiWrite("/api/dti/corrections", "POST", {
          ref: f.referenceDate,
          entity: form.entity,
          position: form.position,
          valueDate: form.valueDate || f.referenceDate,
          amount: Number(form.amount) * 1e6,
          comment: form.comment,
        }),
      "Correction saved. Refresh the DtI tables to include it in the comparison.",
    );

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="DtI comparison"
        title="Manual Inputs"
        description={`Corrections and commenting thresholds · reference date ${f.referenceDate} · role ${access.data?.role ?? "…"}`}
        actions={
          canEdit ? (
            <Button
              variant="outline"
              size="sm"
              disabled={busy}
              onClick={() => run(() => dtiWrite("/api/dti/refresh", "POST"), "DtI tables refreshed.")}
            >
              <RefreshCw className="size-4" /> Refresh DtI tables
            </Button>
          ) : (
            <Badge variant="secondary">
              <Lock className="mr-1 size-3" /> read-only ({access.data?.dtiRole ?? "…"})
            </Badge>
          )
        }
      />
      {status && (
        <output className={status.kind === "ok" ? "block text-sm text-success" : "block text-sm text-destructive"}>
          {status.text}
        </output>
      )}

      <Card>
        <CardHeader>
          <CardTitle>Corrections</CardTitle>
          <CardDescription>
            Shown as separate, flagged lines in the projection; corrected and uncorrected values stay traceable. Amounts in CHF M (negative reduces
            liquidity).
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4 p-0">
          {canEdit && (
            <form
              className="flex flex-wrap items-end gap-2 px-6"
              onSubmit={(e) => {
                e.preventDefault();
                void add();
              }}
            >
              <label className="flex flex-col gap-1 text-xs text-muted-foreground">
                <span>Entity</span>
                <select className={INPUT} value={form.entity} onChange={(e) => setForm({ ...form, entity: e.target.value })}>
                  {ENTITIES.map((x) => (
                    <option key={x} value={x}>
                      {entityLabel(x)}
                    </option>
                  ))}
                </select>
              </label>
              <label className="flex flex-col gap-1 text-xs text-muted-foreground">
                <span>LST position</span>
                <select className={INPUT} value={form.position} onChange={(e) => setForm({ ...form, position: e.target.value })}>
                  {leaves.map((p) => (
                    <option key={p.POSITION_CODE} value={p.POSITION_CODE}>
                      {p.POSITION_NAME}
                    </option>
                  ))}
                </select>
              </label>
              <label className="flex flex-col gap-1 text-xs text-muted-foreground">
                <span>Value date</span>
                <input
                  type="date"
                  className={INPUT}
                  min={f.referenceDate}
                  value={form.valueDate || f.referenceDate}
                  onChange={(e) => setForm({ ...form, valueDate: e.target.value })}
                />
              </label>
              <label className="flex flex-col gap-1 text-xs text-muted-foreground">
                <span>Amount (CHF M)</span>
                <input
                  type="number"
                  step="0.1"
                  required
                  className={`${INPUT} w-28 text-right`}
                  value={form.amount}
                  onChange={(e) => setForm({ ...form, amount: e.target.value })}
                />
              </label>
              <label className="flex min-w-64 flex-1 flex-col gap-1 text-xs text-muted-foreground">
                <span>Comment</span>
                <input
                  required
                  minLength={5}
                  maxLength={500}
                  className={INPUT}
                  value={form.comment}
                  onChange={(e) => setForm({ ...form, comment: e.target.value })}
                />
              </label>
              <Button type="submit" size="sm" disabled={busy}>
                Add correction
              </Button>
            </form>
          )}
          <QueryView q={corrections} skeleton={<Skeleton className="h-32 w-full" />} errorTitle="Corrections failed">
            {(rows) =>
              rows.length === 0 ? (
                <p className="px-6 pb-6 text-sm text-muted-foreground">No corrections on {f.referenceDate}.</p>
              ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>ID</TableHead>
                  <TableHead>Entity</TableHead>
                  <TableHead>Position</TableHead>
                  <TableHead>Value date</TableHead>
                  <TableHead numeric>Amount (M)</TableHead>
                  <TableHead>Comment</TableHead>
                  <TableHead>By</TableHead>
                  <TableHead>Status</TableHead>
                  {canEdit && <TableHead />}
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map((c) => (
                  <TableRow key={c.id}>
                    <TableCell className="font-mono text-xs">{c.id}</TableCell>
                    <TableCell>{c.entity.slice(3)}</TableCell>
                    <TableCell className="text-xs">{c.positionName}</TableCell>
                    <TableCell>{c.valueDate}</TableCell>
                    <TableCell numeric>{formatChfM(c.amount)}</TableCell>
                    <TableCell className="max-w-80 truncate" title={c.comment}>
                      {c.comment}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">{c.createdBy ?? "—"}</TableCell>
                    <TableCell>
                      <Badge variant={c.inEngine ? "success" : "warning"}>{c.inEngine ? "in projection" : "pending refresh"}</Badge>
                    </TableCell>
                    {canEdit && (
                      <TableCell>
                        <Button
                          variant="ghost"
                          size="sm"
                          aria-label={`Delete ${c.id}`}
                          disabled={busy}
                          onClick={() => run(() => dtiWrite(`/api/dti/corrections?id=${encodeURIComponent(c.id)}`, "DELETE"), `${c.id} deleted.`)}
                        >
                          <Trash2 className="size-4" />
                        </Button>
                      </TableCell>
                    )}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
              )
            }
          </QueryView>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Commenting thresholds</CardTitle>
          <CardDescription>Deltas with |value| at or above the threshold are highlighted and need a comment.</CardDescription>
        </CardHeader>
        <CardContent className="p-0">
          <QueryView q={limits} skeleton={<Skeleton className="h-32 w-full" />} errorTitle="Thresholds failed">
            {(rows) => (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Entity</TableHead>
                  <TableHead numeric>Threshold (CHF M)</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map((l) => (
                  <TableRow key={l.entity}>
                    <TableCell>{l.entity === "GROUP" ? "Group" : entityLabel(l.entity)}</TableCell>
                    <TableCell numeric>
                      {canEdit ? (
                        <input
                          type="number"
                          min={0}
                          step={0.5}
                          aria-label={`Threshold ${l.entity}`}
                          defaultValue={l.threshold / 1e6}
                          className={`${INPUT} w-28 text-right`}
                          onBlur={(e) => {
                            const v = Number(e.target.value) * 1e6;
                            if (Number.isFinite(v) && v !== l.threshold) {
                              void run(() => dtiWrite("/api/dti/limits", "PUT", { entity: l.entity, threshold: v }), `Threshold ${l.entity} saved.`);
                            }
                          }}
                        />
                      ) : (
                        formatNumber(l.threshold / 1e6, 1)
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            )}
          </QueryView>
        </CardContent>
      </Card>
    </div>
  );
}
