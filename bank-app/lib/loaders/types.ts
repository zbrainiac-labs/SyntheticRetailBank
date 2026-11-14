import type { Bind } from "@/lib/db";

/**
 * Named, whitelisted queries for the ported pages (one per Streamlit loader).
 * The client only sends a loader name and parameter values; SQL text never
 * leaves the server. Parameters are validated by type and passed as binds.
 */
export type ParamType = "date" | "code" | "int" | "json";

export interface LoaderDef {
  sql: string;
  /** bind order = array order */
  params?: { name: string; type: ParamType; default?: string }[];
  /** upper bound on returned rows (safety net for wide tables) */
  maxRows?: number;
}

export class LoaderParamError extends Error {}

const RULES: Record<ParamType, RegExp> = {
  date: /^\d{4}-\d{2}-\d{2}$/,
  code: /^[A-Za-z0-9_ .&/-]{1,100}$/,
  int: /^-?\d{1,9}$/,
  json: /^\{[\s\S]{0,4000}\}$/,
};

export function bindParams(def: LoaderDef, sp: URLSearchParams): Bind[] {
  return (def.params ?? []).map((p) => {
    const raw = sp.get(p.name) ?? p.default;
    if (raw === undefined || raw === "") throw new LoaderParamError(`missing parameter ${p.name}`);
    if (!RULES[p.type].test(raw)) throw new LoaderParamError(`invalid parameter ${p.name}`);
    if (p.type === "json") {
      try {
        JSON.parse(raw);
      } catch {
        throw new LoaderParamError(`invalid JSON in ${p.name}`);
      }
    }
    return p.type === "int" ? Number(raw) : raw;
  });
}

export const DB = "AAA_DEV_SYNTHETIC_BANK";
