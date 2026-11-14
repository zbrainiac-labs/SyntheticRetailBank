"use client";

import { keepPreviousData, useQuery } from "@tanstack/react-query";

export type LoaderRow = Record<string, unknown>;

export interface LoaderResult<T extends LoaderRow = LoaderRow> {
  rows: T[];
  truncated: boolean;
  ms: number;
}

/**
 * Read a named loader (lib/loaders) through /api/data/<name>. Pass `null` as a
 * param value to hold the query until it is known.
 */
export function useLoader<T extends LoaderRow = LoaderRow>(
  name: string,
  params: Record<string, string | number | null | undefined> = {},
  opts: { enabled?: boolean; staleTime?: number } = {},
) {
  const ready = Object.values(params).every((v) => v !== null && v !== undefined);
  const qs = new URLSearchParams(
    Object.entries(params)
      .filter(([, v]) => v !== null && v !== undefined)
      .map(([k, v]) => [k, String(v)]),
  ).toString();
  return useQuery({
    queryKey: ["data", name, qs],
    enabled: ready && (opts.enabled ?? true),
    staleTime: opts.staleTime ?? 10 * 60_000,
    placeholderData: keepPreviousData,
    queryFn: async () => {
      const query = qs ? `?${qs}` : "";
      const res = await fetch(`/api/data/${name}${query}`);
      const body = await res.json();
      if (!res.ok) throw new Error(body.error ?? `HTTP ${res.status}`);
      return body as LoaderResult<T>;
    },
  });
}

/** Number from a Snowflake value (NUMBER columns can arrive as strings). */
export function num(v: unknown): number {
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : 0;
}
