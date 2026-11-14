import { dtiSql } from "@/lib/dti/access";
import { respond } from "@/lib/dti/api";
import { AGG, ParamError, RAW, parseDtiParams, placeholders, statusFilter } from "@/lib/dti/params";
import type { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

const WLK = /^(I|II|III|IV|V|VI|VII|VIII|IX|X|XIa|XIb)$/;

/**
 * Securities per WLK cluster (UC2 6.8): liquidity value of every security of the
 * cluster per period for both dates. A free security is counterbalancing capacity
 * (repoable at the SNB), so its value sits on the day after the reporting date
 * (first bucket). Buckets are defined relative to each date; the daily view uses
 * business days from the reference date. Comparison flows dated before the
 * reference date form the BEFORE_REF info period (not part of the delta).
 */
export async function GET(req: NextRequest) {
  return respond(async () => {
    const query = dtiSql(req.headers);
    const sp = req.nextUrl.searchParams;
    const p = parseDtiParams(sp);
    const wlk = sp.get("wlk") ?? "II";
    if (!WLK.test(wlk)) throw new ParamError("invalid wlk");
    const v = p.valueType === "BASE" ? "BASE_CHF" : "STRESSED_CHF";
    const days = p.view === "DAYS";
    const ent = placeholders(p.entities.length);

    const bk = `bk AS (
         SELECT d.BASE, b.BUCKET_CODE, b.SORT_ORDER,
                COALESCE(CASE b.OFFSET_UNIT WHEN 'DAY' THEN DATEADD(day, b.OFFSET_VALUE, d.BASE)
                                            ELSE ADD_MONTHS(d.BASE, b.OFFSET_VALUE) END, '9999-12-31'::DATE) AS UPPER_DATE
           FROM (SELECT ?::DATE AS BASE UNION SELECT ?::DATE) d CROSS JOIN ${RAW}.LIQI_RAW_TB_DTI_BUCKETS b
       ),
       bkr AS (
         SELECT BASE, BUCKET_CODE, SORT_ORDER, UPPER_DATE,
                COALESCE(DATEADD(day, 1, LAG(UPPER_DATE) OVER (PARTITION BY BASE ORDER BY SORT_ORDER)), BASE) AS LOWER_DATE
           FROM bk
       )`;

    const [points, master] = await Promise.all([
      query(
        `WITH ${bk},
         sec AS (SELECT DISTINCT SECURITY_ID FROM ${RAW}.LIQI_RAW_TB_DTI_SECURITIES WHERE WLK_CLUSTER = ? AND AS_OF_DATE IN (?::DATE, ?::DATE)),
         f AS (
           SELECT c.AS_OF_DATE, c.SOURCE_ID, c.CF_DATE, cal.NEXT_BUSINESS_DAY AS BUS, c.${v} AS VAL
             FROM ${AGG}.REPP_AGG_DT_DTI_CASHFLOWS c
             JOIN ${AGG}.REPP_AGG_VW_DTI_CALENDAR cal ON cal.CAL_DATE = c.CF_DATE
            WHERE c.FLOW_KIND = 'POOL' AND c.AS_OF_DATE IN (?::DATE, ?::DATE)
              AND c.SOURCE_ID IN (SELECT SECURITY_ID FROM sec)
              AND c.ENTITY_ID IN (${ent}) AND ${statusFilter(p, "c.DATA_STATUS")}
         ),
         t AS (
           SELECT f.*, b.BUCKET_CODE, b.SORT_ORDER, b.LOWER_DATE, b.UPPER_DATE,
                  f.AS_OF_DATE = ?::DATE AND ?::DATE < ?::DATE AND ${days ? "f.BUS" : "f.CF_DATE"} < ?::DATE AS IS_BEFORE
             FROM f LEFT JOIN bkr b ON b.BASE = f.AS_OF_DATE AND f.CF_DATE BETWEEN b.LOWER_DATE AND b.UPPER_DATE
         )
         SELECT SOURCE_ID,
                CASE WHEN IS_BEFORE THEN 'BEFORE_REF' ${days ? "ELSE TO_VARCHAR(BUS)" : "ELSE BUCKET_CODE"} END AS PERIOD,
                CASE WHEN IS_BEFORE THEN -1 ${days ? "ELSE DATEDIFF(day, ?::DATE, BUS)" : "ELSE SORT_ORDER"} END AS SORT,
                SUM(IFF(AS_OF_DATE = ?::DATE AND NOT IS_BEFORE, VAL, 0)) AS REF_VALUE,
                SUM(IFF(AS_OF_DATE = ?::DATE, VAL, 0)) AS COMP_VALUE
           FROM t
          WHERE IS_BEFORE OR ${days ? "BUS BETWEEN ?::DATE AND DATEADD(day, 365, ?::DATE)" : "TRUE"}
          GROUP BY 1, 2, 3`,
        [
          p.ref, p.comp,
          wlk, p.ref, p.comp,
          p.ref, p.comp, ...p.entities,
          p.comp, p.comp, p.ref, p.ref,
          ...(days ? [p.ref] : []),
          p.ref, p.comp,
          ...(days ? [p.ref, p.ref] : []),
        ],
      ),
      query(
        `WITH ${bk},
         s AS (
           SELECT * FROM ${RAW}.LIQI_RAW_TB_DTI_SECURITIES
            WHERE WLK_CLUSTER = ? AND AS_OF_DATE IN (?::DATE, ?::DATE) AND ENTITY_ID IN (${ent})
           QUALIFY ROW_NUMBER() OVER (PARTITION BY SECURITY_ID ORDER BY AS_OF_DATE = ?::DATE DESC) = 1
         )
         SELECT s.SECURITY_ID, s.ISIN, s.SECURITY_NAME, s.ISSUER, s.CURRENCY, s.NOMINAL_CHF, TO_VARCHAR(s.MATURITY_DATE) AS MATURITY_DATE,
                s.HAIRCUT, s.MARKET_VALUE_CHF, s.COLLATERAL_VALUE_CHF, s.IS_ENCUMBERED, TO_VARCHAR(s.AS_OF_DATE) AS SEEN_ON,
                MIN_BY(b.BUCKET_CODE, b.SORT_ORDER) AS MATURITY_BUCKET
           FROM s LEFT JOIN bkr b ON b.BASE = ?::DATE AND b.UPPER_DATE >= s.MATURITY_DATE
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12`,
        [p.ref, p.comp, wlk, p.ref, p.comp, ...p.entities, p.ref, p.ref],
      ),
    ]);

    return {
      wlk,
      points: points.rows.map((r) => ({
        id: r.SOURCE_ID as string,
        period: r.PERIOD as string,
        sort: Number(r.SORT),
        ref: Number(r.REF_VALUE),
        comp: Number(r.COMP_VALUE),
      })),
      rows: master.rows.map((r) => ({
        id: r.SECURITY_ID as string,
        isin: (r.ISIN as string | null) ?? null,
        name: r.SECURITY_NAME as string,
        issuer: (r.ISSUER as string | null) ?? null,
        currency: r.CURRENCY as string,
        nominal: r.NOMINAL_CHF === null ? null : Number(r.NOMINAL_CHF),
        maturity: (r.MATURITY_DATE as string | null) ?? null,
        haircut: r.HAIRCUT === null ? null : Number(r.HAIRCUT),
        valueBase: r.MARKET_VALUE_CHF === null ? null : Number(r.MARKET_VALUE_CHF),
        valueStressed: r.COLLATERAL_VALUE_CHF === null ? null : Number(r.COLLATERAL_VALUE_CHF),
        encumbered: Boolean(r.IS_ENCUMBERED),
        onlyComparison: r.SEEN_ON !== p.ref,
        maturityBucket: (r.MATURITY_BUCKET as string | null) ?? null,
      })),
    };
  });
}
