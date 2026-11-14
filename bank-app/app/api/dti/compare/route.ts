import { dtiSql } from "@/lib/dti/access";
import { respond } from "@/lib/dti/api";
import { AGG, ParamError, parseDtiParams } from "@/lib/dti/params";
import type { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

/** Optional breakdowns on top of position x period (whitelisted column names). */
const DIMS = {
  app: "APPLICATION_ID",
  side: "AKTIV_PASSIV",
  cluster: "PRODUCT_CLUSTER",
  product: "PRODUCT_TYPE",
} as const;
type Dim = keyof typeof DIMS;

const CODE = /^[A-Za-z0-9_]{1,50}$/;

/**
 * Reference vs. comparison date per leaf LST position and period
 * (bucket or business day), from REPP_AGG_FN_DTI_COMPARE.
 * `dims=app,side,cluster,product` adds breakdowns; `cluster` / `product` filter.
 */
export async function GET(req: NextRequest) {
  return respond(async () => {
    const query = dtiSql(req.headers);
    const sp = req.nextUrl.searchParams;
    const p = parseDtiParams(sp);
    const dims = (sp.get("dims") ?? "").split(",").filter(Boolean) as Dim[];
    if (dims.some((d) => !(d in DIMS))) throw new ParamError("invalid dims");
    const cols = dims.map((d) => DIMS[d]);
    const filters: string[] = [];
    const binds: (string | boolean)[] = [p.ref, p.comp, p.entities.join(","), p.valueType, p.view, p.includePreliminary];
    for (const key of ["cluster", "product"] as const) {
      const v = sp.get(key);
      if (!v) continue;
      if (!CODE.test(v)) throw new ParamError(`invalid ${key}`);
      filters.push(`${DIMS[key]} = ?`);
      binds.push(v);
    }
    const dimSelect = cols.map((c) => `${c}, `).join("");
    const { rows } = await query(
      `SELECT POSITION_CODE, ${dimSelect}PERIOD_KEY, PERIOD_SORT, TO_VARCHAR(PERIOD_DATE) AS PERIOD_DATE,
              MAX(BUCKET_REF) AS BUCKET_REF, MAX(BUCKET_COMP) AS BUCKET_COMP,
              TO_VARCHAR(MIN(REF_FROM)) AS REF_FROM, TO_VARCHAR(MAX(REF_TO)) AS REF_TO,
              TO_VARCHAR(MIN(COMP_FROM)) AS COMP_FROM, TO_VARCHAR(MAX(COMP_TO)) AS COMP_TO,
              SUM(REF_VALUE) AS REF_VALUE, SUM(COMP_VALUE) AS COMP_VALUE, SUM(DELTA) AS DELTA, IS_INFO_ROW
         FROM TABLE(${AGG}.REPP_AGG_FN_DTI_COMPARE(?::DATE, ?::DATE, SPLIT(?, ','), ?, ?, ?))
        ${filters.length ? `WHERE ${filters.join(" AND ")}` : ""}
        GROUP BY POSITION_CODE, ${dimSelect}PERIOD_KEY, PERIOD_SORT, PERIOD_DATE, IS_INFO_ROW
        ORDER BY PERIOD_SORT, POSITION_CODE`,
      binds,
    );
    return {
      rows: rows.map((r) => ({
        position: r.POSITION_CODE as string,
        application: (r.APPLICATION_ID as string | null | undefined) ?? null,
        side: (r.AKTIV_PASSIV as string | null | undefined) ?? null,
        cluster: (r.PRODUCT_CLUSTER as string | null | undefined) ?? null,
        product: (r.PRODUCT_TYPE as string | null | undefined) ?? null,
        period: r.PERIOD_KEY as string,
        sort: Number(r.PERIOD_SORT),
        date: (r.PERIOD_DATE as string | null) ?? null,
        bucketRef: (r.BUCKET_REF as string | null) ?? null,
        bucketComp: (r.BUCKET_COMP as string | null) ?? null,
        refFrom: (r.REF_FROM as string | null) ?? null,
        refTo: (r.REF_TO as string | null) ?? null,
        compFrom: (r.COMP_FROM as string | null) ?? null,
        compTo: (r.COMP_TO as string | null) ?? null,
        ref: Number(r.REF_VALUE),
        comp: Number(r.COMP_VALUE),
        delta: r.DELTA === null ? null : Number(r.DELTA),
        info: Boolean(r.IS_INFO_ROW),
      })),
    };
  });
}
