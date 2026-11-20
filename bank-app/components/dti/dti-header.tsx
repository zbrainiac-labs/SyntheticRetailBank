"use client";

import { BookBadge } from "@/components/liquidity/book-badge";
import { PageHeader } from "@/components/page-header";
import { DTI_PAGES } from "@/lib/constants";
import { cn } from "@/lib/utils";
import Link from "next/link";
import { usePathname } from "next/navigation";
import type React from "react";

/**
 * Header of every DtI page: title, liquidity book badge and the DtI sub-navigation,
 * in the same order and tab style as /liquidity/lcr.
 */
export function DtiHeader(props: Readonly<React.ComponentProps<typeof PageHeader>>) {
  const pathname = usePathname();
  return (
    <div className="space-y-4">
      <PageHeader {...props} />
      <BookBadge other="lcr" />
      <nav aria-label="DtI" className="flex flex-wrap gap-1 border-b text-sm">
        {DTI_PAGES.map((p) => (
          <Link
            key={p.href}
            href={p.href}
            aria-current={pathname === p.href ? "page" : undefined}
            className={cn(
              "-mb-px border-b-2 px-3 py-2",
              pathname === p.href ? "border-primary font-medium text-foreground" : "border-transparent text-muted-foreground hover:text-foreground",
            )}
          >
            {p.label}
          </Link>
        ))}
      </nav>
    </div>
  );
}
