import type { APIRequestContext } from "@playwright/test";

/**
 * Dates of the loaded liquidity book for the e2e tests, read from the APIs instead of hardcoded:
 * as-of date of the generator run (default date of /liquidity/lcr and /dti), the previous DtI
 * reporting date, and the date of the first showcase withdrawal (EVT_01 = DTI_01) with its previous date.
 */
export interface BookDates {
  runId: string;
  asOf: string;
  prev: string;
  withdrawal: string;
  beforeWithdrawal: string;
}

export async function bookDates(request: APIRequestContext): Promise<BookDates> {
  const book = await (await request.get("/api/liquidity/book")).json();
  const meta = await (await request.get("/api/dti/meta")).json();
  const events = (await (await request.get("/api/data/lcr.events")).json()).rows as { EVENT_ID: string; EVENT_DATE: string }[];
  const dates = [...(meta.dates as string[])].sort((a, b) => a.localeCompare(b));
  const before = (d: string) => dates.filter((x) => x < d).at(-1) as string;
  const withdrawal = events.find((e) => e.EVENT_ID === "EVT_01")!.EVENT_DATE;
  return { runId: book.runId, asOf: book.asOf, prev: before(book.asOf), withdrawal, beforeWithdrawal: before(withdrawal) };
}
