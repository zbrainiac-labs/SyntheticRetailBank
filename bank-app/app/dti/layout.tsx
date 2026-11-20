"use client";

import { StateView } from "@/components/query-view";
import { useLiquidityBook } from "@/components/liquidity/book-badge";
import { Skeleton } from "@/components/ui/skeleton";
import { useDtiMeta } from "@/lib/dti/client";
import { FilterProvider } from "@/lib/filters/context";
import { defaultDate, previousDate } from "@/lib/liquidity/book";
import type React from "react";

/** Shared frame of the DtI pages: reference data and filter state (title, book badge and sub-navigation: DtiHeader). */
export default function DtiLayout({ children }: { children: React.ReactNode }) {
  const meta = useDtiMeta();
  const book = useLiquidityBook();
  // open on the as-of date of the shared liquidity book (same default date as /liquidity/lcr)
  const dates = meta.data?.dates ?? [];
  const latest = defaultDate(dates, book.data?.asOf);
  const previous = previousDate(dates, latest);

  return (
    <div className="space-y-6">
      <StateView
        pending={meta.isPending || book.isPending}
        error={meta.error}
        errorTitle="DtI data unavailable"
        skeleton={
          <div className="space-y-3">
            <Skeleton className="h-8 w-64" />
            <Skeleton className="h-64 w-full" />
          </div>
        }
        empty={(meta.data?.dates.length ?? 0) < 2}
        emptyTitle="Not enough reporting dates"
        emptyDescription="The DtI comparison needs at least two LST runs."
      >
        {() => (
          <FilterProvider latestDate={latest!} previousDate={previous!} dates={dates}>
            {children}
          </FilterProvider>
        )}
      </StateView>
    </div>
  );
}
