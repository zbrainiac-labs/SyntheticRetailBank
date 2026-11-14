import { dtiSql } from "@/lib/dti/access";
import { respond } from "@/lib/dti/api";
import { AGG, ParamError, RAW, isoDate, parseDtiParams, placeholders, statusFilter } from "@/lib/dti/params";
import type { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

const CODE = /^[A-Za-z0-9_]{1,50}$/;

/**
 * Deal level (UC2 6.7): single deals behind one position x period cell.
 * The period is given as the date range of each date (bucket bounds or one
 * business day); a missing reference range means "comparison only" (before
 * reference date). Modelled flows without deal key are grouped per product.
 */
export async function GET(req: NextRequest) {
  return respond(async () => {
    const query = dtiSql(req.headers);
    const sp = req.nextUrl.searchParams;
    const p = parseDtiParams(sp);
    const position = sp.get("position") || null;
    if (position !== null && !CODE.test(position)) throw new ParamError("invalid position");
    const cluster = sp.get("cluster") || null;
    const product = sp.get("product") || null;
    for (const [k, v] of [["cluster", cluster], ["product", product]] as const)
      if (v !== null && !CODE.test(v)) throw new ParamError(`invalid ${k}`);
    if (!position && !cluster && !product) throw new ParamError("position, cluster or product required");
    const compFrom = isoDate(sp.get("compFrom"), "compFrom");
    const compTo = isoDate(sp.get("compTo"), "compTo");
    const refFrom = sp.get("refFrom") ? isoDate(sp.get("refFrom"), "refFrom") : null;
    const refTo = sp.get("refTo") ? isoDate(sp.get("refTo"), "refTo") : null;
    const v = p.valueType === "BASE" ? "BASE_CHF" : "STRESSED_CHF";
    const flowDate = p.view === "DAYS" ? "cal.NEXT_BUSINESS_DAY" : "c.CF_DATE";

    const { rows } = await query(
      `WITH pos AS (
         SELECT POSITION_CODE FROM ${RAW}.LIQI_RAW_TB_DTI_LST_POSITIONS
         START WITH POSITION_CODE = COALESCE(?, POSITION_CODE) AND (PARENT_CODE IS NULL OR ? IS NOT NULL) CONNECT BY PARENT_CODE = PRIOR POSITION_CODE
       ),
       f AS (
         SELECT c.AS_OF_DATE, c.ENTITY_ID, c.POSITION_CODE, c.PRODUCT_TYPE, c.PRODUCT_CLUSTER, c.APPLICATION_ID,
                c.AKTIV_PASSIV, c.CURRENCY, c.SOURCE_ID, c.FLOW_KIND, c.IS_CORRECTION, c.${v} AS VAL
           FROM ${AGG}.REPP_AGG_DT_DTI_CASHFLOWS c
           JOIN ${AGG}.REPP_AGG_VW_DTI_CALENDAR cal ON cal.CAL_DATE = c.CF_DATE
          WHERE c.POSITION_CODE IN (SELECT POSITION_CODE FROM pos)
            AND (? IS NULL OR c.PRODUCT_CLUSTER = ?)
            AND (? IS NULL OR c.PRODUCT_TYPE = ?)
            AND c.ENTITY_ID IN (${placeholders(p.entities.length)})
            AND ${statusFilter(p, "c.DATA_STATUS")}
            AND ((c.AS_OF_DATE = ?::DATE AND ${flowDate} BETWEEN ?::DATE AND ?::DATE)
              OR (c.AS_OF_DATE = ?::DATE AND ${flowDate} BETWEEN ?::DATE AND ?::DATE))
       )
       SELECT COALESCE(SOURCE_ID, '(modelled)') AS DEAL_ID, ENTITY_ID, POSITION_CODE, PRODUCT_TYPE, PRODUCT_CLUSTER,
              APPLICATION_ID, AKTIV_PASSIV, CURRENCY, FLOW_KIND, BOOLOR_AGG(IS_CORRECTION) AS IS_CORRECTION,
              SUM(IFF(AS_OF_DATE = ?::DATE, VAL, 0)) AS REF_VALUE,
              SUM(IFF(AS_OF_DATE = ?::DATE, VAL, 0)) AS COMP_VALUE
         FROM f
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
        ORDER BY ABS(REF_VALUE - COMP_VALUE) DESC
        LIMIT 500`,
      [
        position, position,
        cluster, cluster, product, product,        ...p.entities,
        p.ref, refFrom ?? "1900-01-01", refTo ?? "1900-01-01",
        p.comp, compFrom, compTo,
        p.ref, p.comp,
      ],
    );
    return {
      rows: rows.map((r) => {
        const ref = refFrom ? Number(r.REF_VALUE) : 0;
        const comp = Number(r.COMP_VALUE);
        return {
          deal: r.DEAL_ID as string,
          entity: r.ENTITY_ID as string,
          position: r.POSITION_CODE as string,
          product: r.PRODUCT_TYPE as string,
          cluster: (r.PRODUCT_CLUSTER as string | null) ?? null,
          application: (r.APPLICATION_ID as string | null) ?? null,
          side: (r.AKTIV_PASSIV as string | null) ?? null,
          currency: r.CURRENCY as string,
          flowKind: r.FLOW_KIND as string,
          correction: Boolean(r.IS_CORRECTION),
          ref,
          comp,
          delta: ref - comp,
        };
      }),
    };
  });
}
