"use client";

import { EmptyState } from "@/components/empty-state";
import type { ReactNode } from "react";

interface QueryLike<T> {
  isPending: boolean;
  isError: boolean;
  error: Error | null;
  data?: T;
}

/**
 * Loading / error / content switch for a TanStack query result, so pages don't chain
 * `pending ? skeleton : error ? message : content` ternaries.
 */
export function QueryView<T>({
  q,
  skeleton,
  errorTitle,
  children,
}: Readonly<{ q: QueryLike<T>; skeleton: ReactNode; errorTitle: string; children: (data: T) => ReactNode }>) {
  if (q.isPending) return <>{skeleton}</>;
  if (q.isError || q.data === undefined) {
    return <EmptyState variant="error" title={errorTitle} description={q.error?.message} />;
  }
  return <>{children(q.data)}</>;
}

/**
 * Same switch for pages that combine several queries: pending -> skeleton, error -> error
 * state, empty -> empty state, else the content.
 */
export function StateView({
  pending,
  error,
  errorTitle,
  skeleton,
  empty = false,
  emptyTitle,
  emptyDescription,
  children,
}: Readonly<{
  pending: boolean;
  error?: Error | null;
  errorTitle: string;
  skeleton: ReactNode;
  empty?: boolean;
  emptyTitle?: string;
  emptyDescription?: string;
  children: () => ReactNode;
}>) {
  if (pending) return <>{skeleton}</>;
  if (error) return <EmptyState variant="error" title={errorTitle} description={error.message} />;
  if (empty) return <EmptyState title={emptyTitle ?? "No data"} description={emptyDescription} />;
  return <>{children()}</>;
}
