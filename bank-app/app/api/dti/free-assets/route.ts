import { dtiSql } from "@/lib/dti/access";
import { respond } from "@/lib/dti/api";
import { ParamError, RAW, isoDate, placeholders } from "@/lib/dti/params";
import { ENTITIES } from "@/lib/filters/model";
import type { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

const BASIS = { CV: "COLLATERAL_VALUE_CHF", NOMINAL: "NOMINAL_CHF", MV: "MARKET_VALUE_CHF" } as const;
const BREAKDOWN = { ISSUER_GROUP: "ISSUER_GROUP", INDUSTRY: "INDUSTRY", CURRENCY: "CURRENCY" } as const;

/**
 * Securities - additional information (UC2 6.9): unencumbered assets of the
 * DtI collateral pool (LIQI_RAW_TB_DTI_SECURITIES) on one reporting date,
 * independent of the Summary filters. Day 365 drops securities that mature
 * within the LST period (e.g. the treasury bill ladder).
 */
export async function GET(req: NextRequest) {
  return respond(async () => {
    const query = dtiSql(req.headers);
    const sp = req.nextUrl.searchParams;
    const ref = isoDate(sp.get("ref"), "ref");
    const entities = (sp.get("entities") ?? "LE_01").split(",").filter(Boolean);
    if (entities.length === 0 || entities.some((e) => !ENTITIES.includes(e))) throw new ParamError("invalid entities");
    const basis = sp.get("basis") ?? "CV";
    if (!(basis in BASIS)) throw new ParamError("invalid basis");
    const breakdown = sp.get("breakdown") ?? "ISSUER_GROUP";
    if (!(breakdown in BREAKDOWN)) throw new ParamError("invalid breakdown");
    const day = sp.get("day") === "365" ? 365 : 1;
    const val = BASIS[basis as keyof typeof BASIS];
    const cat = BREAKDOWN[breakdown as keyof typeof BREAKDOWN];
    const ent = placeholders(entities.length);

    const free = (join = "") => `FROM ${RAW}.LIQI_RAW_TB_DTI_SECURITIES h ${join}
       WHERE h.AS_OF_DATE = ?::DATE AND h.ENTITY_ID IN (${ent}) AND NOT h.IS_ENCUMBERED
         AND (h.MATURITY_DATE IS NULL OR h.MATURITY_DATE > DATEADD(day, ?, ?::DATE))`;
    const freeBinds = [ref, ...entities, day, ref];

    const [assets, secured, breakdownRows, mobil] = await Promise.all([
      query(
        `SELECT w.WLK_CLUSTER, w.CLUSTER_NAME, w.HQLA_CATEGORY, w.HAIRCUT, w.SORT_ORDER, COALESCE(SUM(h.${val}), 0) AS VAL
           FROM ${RAW}.LIQI_RAW_TB_DTI_WLK_CLUSTERS w
           LEFT JOIN (SELECT h.* ${free()}) h ON h.WLK_CLUSTER = w.WLK_CLUSTER
          GROUP BY 1, 2, 3, 4, 5 ORDER BY w.SORT_ORDER`,
        freeBinds,
      ),
      query(
        `SELECT TRANSACTION_TYPE, ANY_VALUE(ELIGIBLE_COLLATERAL) AS ELIGIBLE, COUNT(*) AS DEALS, SUM(NOMINAL_CHF) AS NOMINAL
           FROM ${RAW}.LIQI_RAW_TB_DTI_SECURED_FUNDING
          WHERE AS_OF_DATE = ?::DATE AND ENTITY_ID IN (${ent})
          GROUP BY 1 ORDER BY NOMINAL DESC`,
        [ref, ...entities],
      ),
      query(`SELECT COALESCE(h.${cat}, 'n/a') AS CATEGORY, SUM(h.${val}) AS VAL ${free()} GROUP BY 1 ORDER BY VAL DESC`, freeBinds),
      query(
        `SELECT h.WLK_CLUSTER, ANY_VALUE(w.SORT_ORDER) AS SORT_ORDER,
                SUM(IFF(h.IS_MOBILISED, h.MARKET_VALUE_CHF, 0)) AS MOB_MV, SUM(IFF(h.IS_MOBILISED, 0, h.MARKET_VALUE_CHF)) AS FREE_MV,
                SUM(IFF(h.IS_MOBILISED, h.NOMINAL_CHF, 0)) AS MOB_NOM, SUM(IFF(h.IS_MOBILISED, 0, h.NOMINAL_CHF)) AS FREE_NOM
           ${free(`JOIN ${RAW}.LIQI_RAW_TB_DTI_WLK_CLUSTERS w ON w.WLK_CLUSTER = h.WLK_CLUSTER`)}
          GROUP BY 1`,
        freeBinds,
      ),
    ]);

    return {
      ref,
      day,
      basis,
      breakdown,
      assets: assets.rows.map((r) => ({
        wlk: r.WLK_CLUSTER as string,
        name: r.CLUSTER_NAME as string,
        hqla: r.HQLA_CATEGORY as string,
        haircut: Number(r.HAIRCUT),
        value: Number(r.VAL),
      })),
      secured: secured.rows.map((r) => ({
        type: r.TRANSACTION_TYPE as string,
        eligible: (r.ELIGIBLE as string | null) ?? "",
        deals: Number(r.DEALS),
        nominal: Number(r.NOMINAL),
      })),
      breakdownRows: breakdownRows.rows.map((r) => ({ category: r.CATEGORY as string, value: Number(r.VAL) })),
      mobilisation: [...mobil.rows]
        .sort((a, b) => Number(a.SORT_ORDER) - Number(b.SORT_ORDER))
        .map((r) => ({
          wlk: r.WLK_CLUSTER as string,
          mobilisedMv: Number(r.MOB_MV),
          freeMv: Number(r.FREE_MV),
          mobilisedNominal: Number(r.MOB_NOM),
          freeNominal: Number(r.FREE_NOM),
        })),
    };
  });
}
