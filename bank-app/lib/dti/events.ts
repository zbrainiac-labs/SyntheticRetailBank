/**
 * DtI showcase and cross-engine events (LIQI_RAW_TB_DTI_EVENTS) and the DtI history they explain.
 * The same book feeds LCR and DtI, so the LCR stress events (funding cliff, roll, withdrawals) are
 * also DtI events and are shown as markers on the DtI history.
 */
export interface DtiEvent {
  id: string;
  date: string; // ISO yyyy-mm-dd
  entity: string | null;
  type: string;
  position: string | null;
  amount: number;
  description: string;
}

export interface DtiHistoryPoint {
  date: string;
  /** Group DtI in days of the selected entities; null = liquid for 365 days. */
  dti: number | null;
}

export interface MarkedEvent extends DtiEvent {
  /** Event inside (comparison, reference]: it explains the delta shown on the page. */
  inDelta: boolean;
}

/** Days shown for "> 365" on the history chart (top of the axis). */
export const LIQUID_DAYS = 366;

const LABELS: Record<string, string> = {
  DEPOSIT_WITHDRAWAL: "Deposit withdrawal",
  BUCKET_SHIFT: "Bucket shift",
  CORRECTION: "Manual correction",
  PRELIMINARY_DATA: "Preliminary data",
  SECURITY_PURCHASE: "Security purchase",
  WLK_DOWNGRADE: "WLK downgrade",
  SECURITY_MATURITY: "Security maturity",
  FUNDING_CLIFF: "Funding cliff",
  FUNDING_ROLL: "Funding roll",
};

export function eventLabel(type: string): string {
  return LABELS[type] ?? type;
}

/** Events sorted by date (then id), flagged when they fall between the comparison and the reference date. */
export function markEvents(events: DtiEvent[], comparisonDate: string, referenceDate: string): MarkedEvent[] {
  const [lo, hi] = comparisonDate <= referenceDate ? [comparisonDate, referenceDate] : [referenceDate, comparisonDate];
  return [...events]
    .sort((a, b) => a.date.localeCompare(b.date) || a.id.localeCompare(b.id))
    .map((e) => ({ ...e, inDelta: e.date > lo && e.date <= hi }));
}

/** History chart rows: DtI capped at LIQUID_DAYS so "> 365" stays on the chart. */
export function historyChart(points: DtiHistoryPoint[]): { date: string; dti: number }[] {
  return points.map((p) => ({ date: p.date, dti: p.dti === null ? LIQUID_DAYS : Math.min(p.dti, LIQUID_DAYS) }));
}

/** Events that have a reporting date on the history (markers need an x value of the chart). */
export function eventsOnHistory(events: DtiEvent[], points: DtiHistoryPoint[]): DtiEvent[] {
  const dates = new Set(points.map((p) => p.date));
  return events.filter((e) => dates.has(e.date));
}
