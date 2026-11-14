"use client";

import { StateView } from "@/components/query-view";
import { BookBadge, useLiquidityBook } from "@/components/liquidity/book-badge";
import { Skeleton } from "@/components/ui/skeleton";
import { useDtiMeta } from "@/lib/dti/client";
import { DTI_PAGES } from "@/lib/constants";
import { FilterProvider } from "@/lib/filters/context";
import { defaultDate, previousDate } from "@/lib/liquidity/book";
import { cn } from "@/lib/utils";
import Link from "next/link";
import { usePathname } from "next/navigation";
import type React from "react";

/** Shared frame of the DtI pages: reference data, filter state and sub-navigation. */
export default function DtiLayout({ children }: { children: React.ReactNode }) {
  const meta = useDtiMeta();
  const book = useLiquidityBook();
  const pathname = usePathname();
  // open on the as-of date of the shared liquidity book (same default date as /liquidity/lcr)
  const dates = meta.data?.dates ?? [];
  const latest = defaultDate(dates, book.data?.asOf);
  const previous = previousDate(dates, latest);

  return (
    <main className="mx-auto w-full max-w-7xl space-y-6 px-4 py-6 sm:px-6 sm:py-8 lg:px-8">
      <nav aria-label="DtI" className="flex flex-wrap gap-1 border-b pb-2 text-sm">
        {DTI_PAGES.map((p) => (
          <Link
            key={p.href}
            href={p.href}
            className={cn(
              "rounded-md px-3 py-1.5 text-muted-foreground hover:bg-muted hover:text-foreground",
              pathname === p.href && "bg-muted font-medium text-foreground",
            )}
          >
            {p.label}
          </Link>
        ))}
      </nav>
      <BookBadge other="lcr" />
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
    </main>
  );
}
