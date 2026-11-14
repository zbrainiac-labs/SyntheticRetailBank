/**
 * One liquidity book behind /liquidity/lcr and /dti: both engines run on the same
 * generator run (LIQI_RAW_TB_GENERATION_RUN) and open on its as-of date.
 * Pure helpers here (unit tested); the route app/api/liquidity/book reads the data.
 */

export interface LiquidityBook {
  runId: string | null;
  asOf: string | null;
  windowStart: string | null;
  historyStart: string | null;
  lcrLatest: string | null;
  dtiLatest: string | null;
  /** Free (unencumbered, HQLA-eligible) securities in CHF on asOf: LCR engine before haircut / DtI pool base value. */
  lcrPoolChf: number | null;
  dtiPoolChf: number | null;
}

export interface BookCheck {
  /** null = an engine is not visible to the role (no grant), so the dates cannot be compared */
  sameDate: boolean | null;
  samePool: boolean | null;
  ok: boolean;
  comparable: boolean;
}

/** Relative tolerance for the pool comparison (rounding of CCY amounts x FX). */
export const POOL_TOLERANCE = 0.0005;

/**
 * Default reporting date of a page: the run's as-of date when the page has it, otherwise the latest
 * page date not after it, otherwise the latest page date. Works for ascending and descending lists.
 */
export function defaultDate(dates: readonly string[], asOf: string | null | undefined): string | null {
  if (dates.length === 0) return null;
  const sorted = [...dates].sort((a, b) => a.localeCompare(b));
  const latest = sorted.at(-1) ?? null;
  if (!asOf) return latest;
  return sorted.findLast((d) => d <= asOf) ?? latest;
}

/** Reporting date immediately before `date` (comparison date of the DtI pages). */
export function previousDate(dates: readonly string[], date: string | null): string | null {
  if (!date) return null;
  return [...dates].filter((d) => d < date).sort((a, b) => a.localeCompare(b)).at(-1) ?? null;
}

/**
 * Both engines end on the run's as-of date and value the same free pool. Parts the role cannot read are null
 * and do not fail the check (DTI_READ_ONLY has no LCR grants; the DtI pool is only complete with all entities).
 */
export function checkBook(b: LiquidityBook): BookCheck {
  const sameDate = b.lcrLatest === null || b.dtiLatest === null ? null : !!b.asOf && b.lcrLatest === b.asOf && b.dtiLatest === b.asOf;
  let samePool: boolean | null = null;
  if (b.lcrPoolChf !== null && b.dtiPoolChf !== null) {
    samePool = Math.abs(b.lcrPoolChf - b.dtiPoolChf) <= POOL_TOLERANCE * Math.max(Math.abs(b.lcrPoolChf), 1);
  }
  return { sameDate, samePool, ok: sameDate !== false && samePool !== false, comparable: sameDate !== null };
}
