/**
 * Server-side parsing of DtI request parameters. Every value that reaches SQL
 * is validated here and then passed as a bind, never concatenated.
 */
import { ENTITIES } from "@/lib/filters/model";

export const DB = "AAA_DEV_SYNTHETIC_BANK";
export const AGG = `${DB}.REP_AGG_V001`;
export const RAW = `${DB}.REP_RAW_V001`;

export interface DtiParams {
  ref: string;
  comp: string;
  entities: string[];
  valueType: "BASE" | "STRESSED";
  view: "BUCKETS" | "DAYS";
  includePreliminary: boolean;
}

const ISO = /^\d{4}-\d{2}-\d{2}$/;

export class ParamError extends Error {}

export function isoDate(v: string | null, name: string): string {
  if (!v || !ISO.test(v) || Number.isNaN(Date.parse(v))) throw new ParamError(`invalid ${name}`);
  return v;
}

export function parseDtiParams(sp: URLSearchParams): DtiParams {
  const entities = (sp.get("entities") ?? "").split(",").filter(Boolean);
  if (entities.length === 0 || entities.some((e) => !ENTITIES.includes(e))) throw new ParamError("invalid entities");
  const valueType = sp.get("valueType") ?? "STRESSED";
  if (valueType !== "BASE" && valueType !== "STRESSED") throw new ParamError("invalid valueType");
  const view = sp.get("view") ?? "BUCKETS";
  if (view !== "BUCKETS" && view !== "DAYS") throw new ParamError("invalid view");
  return {
    ref: isoDate(sp.get("ref"), "ref"),
    comp: isoDate(sp.get("comp"), "comp"),
    entities,
    valueType,
    view,
    includePreliminary: sp.get("prelim") === "1",
  };
}

/** Status filter matching FN_DTI_COMPARE's P_INCLUDE_PRELIM. */
export function statusFilter(p: DtiParams, col = "DATA_STATUS"): string {
  return p.includePreliminary ? "TRUE" : `${col} = 'FINAL'`;
}

/** `?, ?, ?` placeholders for an IN list. */
export function placeholders(n: number): string {
  return Array.from({ length: n }, () => "?").join(", ");
}
