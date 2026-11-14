"use client";

import { Badge } from "@/components/ui/badge";
import { formatNumber } from "@/lib/format";
import type { BookCheck, LiquidityBook } from "@/lib/liquidity/book";
import { useQuery } from "@tanstack/react-query";
import Link from "next/link";

export type LiquidityBookResponse = LiquidityBook & { check: BookCheck };

export function useLiquidityBook() {
  return useQuery({
    queryKey: ["liquidity", "book"],
    queryFn: async () => {
      const res = await fetch("/api/liquidity/book");
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return (await res.json()) as LiquidityBookResponse;
    },
    staleTime: 5 * 60_000,
  });
}

/**
 * Run, as-of date and the "same book" check shown on both liquidity pages (/liquidity/lcr and /dti),
 * with a link to the other page.
 */
export function BookBadge({ other }: Readonly<{ other: "lcr" | "dti" }>) {
  const { data } = useLiquidityBook();
  if (!data?.runId) return null;
  const { check } = data;
  const pool = data.lcrPoolChf ?? data.dtiPoolChf;
  return (
    <div
      data-testid="liquidity-book"
      data-run-id={data.runId}
      data-as-of={data.asOf ?? ""}
      className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground"
    >
      <span>Liquidity book</span>
      <Badge variant="outline" className="font-mono">{data.runId}</Badge>
      <span>as of {data.asOf}</span>
      {pool !== null && <span>free pool CHF {formatNumber(pool / 1e6, 0)}M</span>}
      {check.comparable ? (
        <Badge variant={check.ok ? "success" : "warning"} title="LCR and DtI end on the same date and value the same free pool">
          {check.ok ? "LCR and DtI on the same book" : "LCR and DtI differ"}
        </Badge>
      ) : (
        <Badge variant="outline" title="The current role cannot read both engines">
          LCR not visible for this role
        </Badge>
      )}
      {check.comparable && (
        <Link className="text-primary hover:underline" href={other === "lcr" ? "/liquidity/lcr" : "/dti"}>
          {other === "lcr" ? "Open LCR" : "Open DtI"}
        </Link>
      )}
    </div>
  );
}
