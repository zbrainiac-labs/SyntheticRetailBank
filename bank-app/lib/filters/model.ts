/**
 * Filter model for the DtI pages (UC2 "General provisions"):
 * the Filter Page sets global defaults, every detail page may override single
 * filters locally, and the effective value = override ?? default.
 * Changing the Filter Page bumps `version`, which invalidates all overrides.
 */

export type ValueType = "BASE" | "STRESSED";
export type Presentation = "BUCKETS" | "DAYS";

export interface DtiFilters {
  entities: string[];
  referenceDate: string; // ISO yyyy-mm-dd
  comparisonDate: string;
  valueType: ValueType;
  presentation: Presentation;
  includePreliminary: boolean;
  /** Global amount filter (prototype FilterBetrag), CHF M: rows with all |values| below are hidden; control sums stay unfiltered */
  minAbsDelta: number;
}

export type FilterKey = keyof DtiFilters;

export interface PageOverrides {
  version: number;
  values: Partial<DtiFilters>;
}

export const ENTITIES = Array.from({ length: 9 }, (_, i) => `LE_0${i + 1}`);

export function entityLabel(id: string): string {
  return `Legal Entity ${Number(id.slice(3))}`;
}

/** Page scope: the entity name, or "Group (n entities)". */
export function scopeLabel(entities: string[]): string {
  return entities.length === 1 ? entityLabel(entities[0]) : `Group (${entities.length} entities)`;
}

/** "stressed" / "base" for descriptions. */
export function valueTypeLabel(valueType: DtiFilters["valueType"]): string {
  return valueType === "STRESSED" ? "stressed" : "base";
}

export function defaultFilters(latestDate: string, previousDate: string): DtiFilters {
  return {
    entities: ["LE_01", "LE_02", "LE_03"],
    referenceDate: latestDate,
    comparisonDate: previousDate,
    valueType: "STRESSED",
    presentation: "BUCKETS",
    includePreliminary: false,
    minAbsDelta: 0,
  };
}

/** Overrides from an older Filter Page version are ignored. */
export function activeOverrides(o: PageOverrides | undefined, version: number): Partial<DtiFilters> {
  return o && o.version === version ? o.values : {};
}

export function effectiveFilters(
  defaults: DtiFilters,
  overrides: Partial<DtiFilters>,
  applicable: FilterKey[],
): DtiFilters {
  const out = { ...defaults };
  for (const key of applicable) {
    const v = overrides[key];
    if (v !== undefined && !(Array.isArray(v) && v.length === 0)) {
      (out as Record<FilterKey, unknown>)[key] = v;
    }
  }
  return out;
}

export function setOverride<K extends FilterKey>(
  o: PageOverrides | undefined,
  version: number,
  key: K,
  value: DtiFilters[K] | undefined,
): PageOverrides {
  const values = { ...activeOverrides(o, version) };
  if (value === undefined) delete values[key];
  else values[key] = value;
  return { version, values };
}

/** Selection handed from one page to the next via URL search params. */
export function toSearchParams(sel: Record<string, string | number | undefined>): string {
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(sel)) if (v !== undefined && v !== "") p.set(k, String(v));
  return p.toString();
}

/** Filter state persisted in the browser; `asOf` = latest reporting date of the book it was saved for. */
export interface SavedFilters {
  global: DtiFilters;
  version: number;
  pages: Record<string, PageOverrides>;
  asOf?: string;
}

const DATE_KEYS = ["referenceDate", "comparisonDate"] as const;

/**
 * Restore saved filters: filters the saved state lacks take the defaults. Dates are kept only while the book
 * is the same (same latest reporting date) and still a reporting date - after a data reload the pages open on
 * the new as-of date again instead of a date picked in an earlier session.
 */
export function restoreSaved(
  defaults: DtiFilters,
  saved: Partial<SavedFilters>,
  dates: string[],
  latestDate: string,
): SavedFilters {
  const sameBook = saved.asOf === latestDate;
  const keep = (d: string | undefined) => sameBook && d !== undefined && dates.includes(d);
  const global = { ...defaults, ...saved.global };
  for (const k of DATE_KEYS) if (!keep(global[k])) global[k] = defaults[k];
  const pages: Record<string, PageOverrides> = {};
  for (const [page, o] of Object.entries(saved.pages ?? {})) {
    const values = { ...o.values };
    for (const k of DATE_KEYS) if (values[k] !== undefined && !keep(values[k])) delete values[k];
    pages[page] = { ...o, values };
  }
  return { global, version: saved.version ?? 0, pages, asOf: latestDate };
}
