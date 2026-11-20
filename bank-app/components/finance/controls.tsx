"use client";

import { DataTable, Panel } from "@/components/data/data-table";
import { fmtM } from "@/components/finance/common";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { num, type LoaderRow } from "@/lib/loaders/client";
import { formatNumber } from "@/lib/format";

/** Status, difference and affected objects of a control, with or without the demo exception. */
export function checkView(r: LoaderRow, simulate: boolean) {
  return {
    status: String(simulate ? r.DEMO_STATUS : r.STATUS),
    diff: num(simulate ? r.DEMO_DIFFERENCE : r.DIFFERENCE),
    affected: (simulate ? r.DEMO_AFFECTED : r.AFFECTED) as string | null,
  };
}

export function Controls({
  checks,
  ic,
  simulate,
  onSimulate,
}: Readonly<{ checks: LoaderRow[]; ic: LoaderRow[]; simulate: boolean; onSimulate: (v: boolean) => void }>) {
  const failed = checks.filter((r) => checkView(r, simulate).status === "FAIL").length;
  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="text-sm text-muted-foreground" data-testid="controls-summary">
          {failed === 0
            ? `All ${checks.length} controls passed for this month.`
            : `${failed} of ${checks.length} controls failed: the consolidation is not ready for sign-off.`}
        </p>
        <Button
          variant={simulate ? "destructive" : "outline"}
          size="sm"
          onClick={() => onSimulate(!simulate)}
          aria-pressed={simulate}
          data-testid="simulate-exception"
        >
          {simulate ? "Reset exception" : "Simulate exception"}
        </Button>
      </div>
      <Panel title="Consolidation controls" description="Run on every refresh; a failed control blocks the sign-off">
        <div className="space-y-3" data-testid="controls-list">
          {checks.map((r) => {
            const v = checkView(r, simulate);
            const ok = v.status === "PASS";
            const unit = r.CHECK_ID === "FC-C5" ? "h since refresh" : "CHF difference";
            return (
              <div key={String(r.CHECK_ID)} className="rounded-lg border p-3" data-check={String(r.CHECK_ID)} data-status={v.status}>
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div className="flex items-center gap-2">
                    <Badge variant={ok ? "success" : "destructive"}>{ok ? "PASS" : "FAIL"}</Badge>
                    <span className="font-medium">{String(r.CHECK_NAME)}</span>
                    <span className="text-xs text-muted-foreground">{String(r.CHECK_ID)}</span>
                  </div>
                  <span className="text-sm tabular-nums text-muted-foreground">
                    {formatNumber(v.diff, 2)} {unit} (tolerance {formatNumber(num(r.TOLERANCE), 0)})
                  </span>
                </div>
                <p className="mt-1 text-sm text-muted-foreground">{String(r.DESCRIPTION)}</p>
                {!ok && (
                  <div className="mt-2 rounded-md bg-destructive/10 p-2 text-sm">
                    <p>
                      <span className="font-medium">Affected:</span> {v.affected ?? "n/a"}
                    </p>
                    <p>
                      <span className="font-medium">Resolution:</span> {String(r.RESOLUTION_HINT)}
                    </p>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </Panel>
      <Panel
        title="Intercompany reconciliation"
        description={
          simulate
            ? "Exception: Brokerage US kept the USD loan at last month's rate, so side B no longer matches side A"
            : "Side A (receivable) vs side B (payable) in CHF at the closing rate"
        }
      >
        <div data-testid="ic-table">
          <DataTable
            rows={ic.map((r) => ({
              ...r,
              SIDE_B: simulate ? r.DEMO_SIDE_B_CHF : r.SIDE_B_CHF,
              DIFF: simulate ? r.DEMO_DIFFERENCE_CHF : r.DIFFERENCE_CHF,
            }))}
            columns={[
              { key: "PAIR_ID", label: "Pair" },
              { key: "DESCRIPTION", label: "Description" },
              { key: "PAIR_CCY", label: "Ccy" },
              { key: "SIDE_A_CHF", label: "Side A CHF m", render: (r) => fmtM(r.SIDE_A_CHF, 2) },
              { key: "SIDE_B", label: "Side B CHF m", render: (r) => fmtM(r.SIDE_B, 2) },
              {
                key: "DIFF",
                label: "Difference CHF m",
                render: (r) => <span className={Math.abs(num(r.DIFF)) > 1 ? "font-semibold text-destructive" : ""}>{fmtM(r.DIFF, 2)}</span>,
              },
            ]}
          />
        </div>
      </Panel>
    </div>
  );
}
