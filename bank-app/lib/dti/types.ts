/** Response shapes of the /api/dti routes. */

export interface PositionRow {
  POSITION_CODE: string;
  POSITION_NAME: string;
  PARENT_CODE: string | null;
  LEVEL_NO: number;
  SORT_ORDER: number;
  IS_LEAF: boolean;
}

export interface DtiMeta {
  dates: string[];
  /** reporting dates that are the last business day of their month */
  monthEnds: string[];
  positions: PositionRow[];
  thresholds: Record<string, number>;
  preliminary: { AS_OF_DATE: string; ENTITY_ID: string; DATA_STATUS: string }[];
}

export interface ComparePoint {
  position: string;
  /** set when requested via dims=app / side / cluster / product */
  application?: string | null;
  side?: string | null;
  cluster?: string | null;
  product?: string | null;
  period: string;
  sort: number;
  date: string | null;
  bucketRef: string | null;
  bucketComp: string | null;
  refFrom: string | null;
  refTo: string | null;
  compFrom: string | null;
  compTo: string | null;
  ref: number;
  comp: number;
  delta: number | null;
  info: boolean;
}

export interface ProjectionPoint {
  day: number;
  date: string;
  pool: number;
  det: number;
  inf: number;
  out: number;
  total: number;
  entities: number;
}

export interface EntityDti {
  /** null days = liquidity positive for 365 days */
  days: number | null;
  status: string;
}

export interface SummaryResponse {
  ref: { points: ProjectionPoint[]; dti: { days: number; date: string } | null };
  comp: { points: ProjectionPoint[]; dti: { days: number; date: string } | null };
  /** DtI of every entity on both dates (null = no run) */
  entities: { entity: string; ref: EntityDti | null; comp: EntityDti | null }[];
}

export interface SecurityRow {
  id: string;
  isin: string | null;
  name: string;
  issuer: string | null;
  currency: string;
  nominal: number | null;
  maturity: string | null;
  haircut: number | null;
  valueBase: number | null;
  valueStressed: number | null;
  encumbered: boolean;
  /** held on the comparison date only (sold or matured since) */
  onlyComparison: boolean;
  maturityBucket: string | null;
}

export interface SecurityPoint {
  id: string;
  period: string;
  sort: number;
  ref: number;
  comp: number;
}

export interface SecuritiesResponse {
  wlk: string;
  points: SecurityPoint[];
  rows: SecurityRow[];
}

export interface FreeAssetsResponse {
  ref: string;
  day: number;
  basis: string;
  breakdown: string;
  assets: { wlk: string; name: string; hqla: string; haircut: number; value: number }[];
  secured: { type: string; eligible: string; deals: number; nominal: number }[];
  breakdownRows: { category: string; value: number }[];
  mobilisation: { wlk: string; mobilisedMv: number; freeMv: number; mobilisedNominal: number; freeNominal: number }[];
}

export type ParametersResponse = Record<
  "limits" | "entities" | "applications" | "clusters" | "wlk" | "buckets" | "holidays" | "assignment",
  Record<string, unknown>[]
>;

export interface DealRow {
  deal: string;
  entity: string;
  position: string;
  product: string;
  cluster: string | null;
  application: string | null;
  side: string | null;
  currency: string;
  flowKind: string;
  correction: boolean;
  ref: number;
  comp: number;
  delta: number;
}
