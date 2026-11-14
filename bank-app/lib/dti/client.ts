"use client";

import type {
  ComparePoint,
  DealRow,
  DtiMeta,
  FreeAssetsResponse,
  ParametersResponse,
  SecuritiesResponse,
  SummaryResponse,
} from "@/lib/dti/types";
import type { DtiFilters } from "@/lib/filters/model";
import { useQuery } from "@tanstack/react-query";

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  const body = await res.json();
  if (!res.ok) throw new Error(body.error ?? `HTTP ${res.status}`);
  return body as T;
}

/** Query string shared by all DtI routes. */
export function dtiQuery(f: DtiFilters, extra: Record<string, string | null | undefined> = {}): string {
  const p = new URLSearchParams({
    ref: f.referenceDate,
    comp: f.comparisonDate,
    entities: f.entities.join(","),
    valueType: f.valueType,
    view: f.presentation,
    prelim: f.includePreliminary ? "1" : "0",
  });
  for (const [k, v] of Object.entries(extra)) if (v) p.set(k, v);
  return p.toString();
}

export function useDtiMeta() {
  return useQuery({ queryKey: ["dti", "meta"], queryFn: () => getJson<DtiMeta>("/api/dti/meta"), staleTime: 5 * 60_000 });
}

export function useCompare(f: DtiFilters, extra: Record<string, string | null | undefined> = {}) {
  const qs = dtiQuery(f, extra);
  return useQuery({
    queryKey: ["dti", "compare", qs],
    queryFn: () => getJson<{ rows: ComparePoint[] }>(`/api/dti/compare?${qs}`).then((r) => r.rows),
  });
}

export function useSummary(f: DtiFilters) {
  const qs = dtiQuery(f);
  return useQuery({ queryKey: ["dti", "summary", qs], queryFn: () => getJson<SummaryResponse>(`/api/dti/summary?${qs}`) });
}

export function useSecurities(f: DtiFilters, wlk: string) {
  const qs = dtiQuery(f, { wlk });
  return useQuery({ queryKey: ["dti", "securities", qs], queryFn: () => getJson<SecuritiesResponse>(`/api/dti/securities?${qs}`) });
}

export function useFreeAssets(params: Record<string, string>) {
  const qs = new URLSearchParams(params).toString();
  return useQuery({ queryKey: ["dti", "free-assets", qs], queryFn: () => getJson<FreeAssetsResponse>(`/api/dti/free-assets?${qs}`) });
}

export function useParameters() {
  return useQuery({ queryKey: ["dti", "parameters"], queryFn: () => getJson<ParametersResponse>("/api/dti/parameters"), staleTime: 5 * 60_000 });
}

export function useDeals(f: DtiFilters, cell: Record<string, string | null | undefined> | null) {
  const qs = cell ? dtiQuery(f, cell) : "";
  return useQuery({
    queryKey: ["dti", "deals", qs],
    queryFn: () => getJson<{ rows: DealRow[] }>(`/api/dti/deals?${qs}`).then((r) => r.rows),
    enabled: cell !== null,
  });
}

// ---- governance (step 7)
export interface DtiAccessInfo {
  role: string;
  dtiRole: "DTI_OWNER" | "DTI_LRC_READ" | "DTI_READ_ONLY" | "NONE";
  canExport: boolean;
  canEdit: boolean;
}

export interface CorrectionRow {
  id: string;
  entity: string;
  position: string;
  positionName: string;
  valueDate: string;
  amount: number;
  comment: string;
  createdBy: string | null;
  createdAt: string | null;
  /** already part of the engine output (false until the next DT refresh) */
  inEngine: boolean;
}

export function useDtiAccess() {
  return useQuery({ queryKey: ["dti", "me"], queryFn: () => getJson<DtiAccessInfo>("/api/dti/me"), staleTime: 5 * 60_000 });
}

export function useCorrections(ref: string) {
  return useQuery({
    queryKey: ["dti", "corrections", ref],
    queryFn: () => getJson<{ rows: CorrectionRow[] }>(`/api/dti/corrections?ref=${encodeURIComponent(ref)}`).then((r) => r.rows),
    enabled: Boolean(ref),
  });
}

export function useLimits() {
  return useQuery({
    queryKey: ["dti", "limits"],
    queryFn: () => getJson<{ rows: { entity: string; threshold: number }[] }>("/api/dti/limits").then((r) => r.rows),
  });
}

/** JSON write call to a DtI route; throws with the server message on 4xx/5xx. */
export async function dtiWrite<T = unknown>(url: string, method: "POST" | "PUT" | "DELETE", body?: unknown): Promise<T> {
  const res = await fetch(url, {
    method,
    headers: body ? { "content-type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const json = await res.json();
  if (!res.ok) throw new Error(json.error ?? `HTTP ${res.status}`);
  return json as T;
}
