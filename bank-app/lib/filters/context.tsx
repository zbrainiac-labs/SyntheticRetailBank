"use client";

import {
  activeOverrides,
  defaultFilters,
  effectiveFilters,
  setOverride,
  type DtiFilters,
  type FilterKey,
  type PageOverrides,
} from "@/lib/filters/model";
import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";

interface State {
  global: DtiFilters;
  version: number;
  pages: Record<string, PageOverrides>;
}

interface Ctx {
  state: State;
  setGlobal: (patch: Partial<DtiFilters>) => void;
  setPage: (page: string, o: PageOverrides) => void;
}

const STORAGE_KEY = "bank-app.dti-filters.v1";
const FilterCtx = createContext<Ctx | null>(null);

/**
 * Saved state from an older app version: filters it lacks take the defaults, and
 * dates that are no longer reporting dates (e.g. after a data reload) fall back to the defaults.
 */
function sanitize(defaults: DtiFilters, saved: State, dates: string[], latestDate: string, previousDate: string): State {
  const valid = (d: string | undefined) => d === undefined || dates.includes(d);
  const global = { ...defaults, ...saved.global };
  if (!valid(global.referenceDate)) global.referenceDate = latestDate;
  if (!valid(global.comparisonDate)) global.comparisonDate = previousDate;
  const pages: Record<string, PageOverrides> = {};
  for (const [page, o] of Object.entries(saved.pages ?? {})) {
    const values = { ...o.values };
    if (!valid(values.referenceDate)) delete values.referenceDate;
    if (!valid(values.comparisonDate)) delete values.comparisonDate;
    pages[page] = { ...o, values };
  }
  return { ...saved, global, pages };
}

export function FilterProvider({
  latestDate,
  previousDate,
  dates,
  children,
}: {
  latestDate: string;
  previousDate: string;
  dates: string[];
  children: ReactNode;
}) {
  const [state, setState] = useState<State>(() => ({
    global: defaultFilters(latestDate, previousDate),
    version: 0,
    pages: {},
  }));

  useEffect(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) setState((s) => sanitize(s.global, { ...s, ...(JSON.parse(saved) as State) }, dates, latestDate, previousDate));
    } catch {
      /* ignore corrupt storage */
    }
    // load once on mount; the dates are fixed for the lifetime of the provider
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
    } catch {
      /* storage unavailable (iframe) - keep in memory */
    }
  }, [state]);

  // A Filter Page change resets the local selections on all pages.
  const setGlobal = useCallback(
    (patch: Partial<DtiFilters>) =>
      setState((s) => ({ global: { ...s.global, ...patch }, version: s.version + 1, pages: {} })),
    [],
  );
  const setPage = useCallback(
    (page: string, o: PageOverrides) => setState((s) => ({ ...s, pages: { ...s.pages, [page]: o } })),
    [],
  );

  const value = useMemo(() => ({ state, setGlobal, setPage }), [state, setGlobal, setPage]);
  return <FilterCtx.Provider value={value}>{children}</FilterCtx.Provider>;
}

function useFilterCtx(): Ctx {
  const ctx = useContext(FilterCtx);
  if (!ctx) throw new Error("useFilterCtx outside FilterProvider");
  return ctx;
}

export function useGlobalFilters() {
  const { state, setGlobal } = useFilterCtx();
  return { filters: state.global, setGlobal };
}

/** Three-column filter state for one detail page: default | override | effective. */
export function usePageFilters(page: string, applicable: FilterKey[]) {
  const { state, setPage } = useFilterCtx();
  const overrides = activeOverrides(state.pages[page], state.version);
  const effective = effectiveFilters(state.global, overrides, applicable);
  return {
    defaults: state.global,
    overrides,
    effective,
    set: <K extends FilterKey>(key: K, value: DtiFilters[K] | undefined) =>
      setPage(page, setOverride(state.pages[page], state.version, key, value)),
    reset: () => setPage(page, { version: state.version, values: {} }),
  };
}
