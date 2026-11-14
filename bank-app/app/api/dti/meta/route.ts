import { dtiSql } from "@/lib/dti/access";
import { respond } from "@/lib/dti/api";
import { AGG, RAW } from "@/lib/dti/params";

import type { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

/** Reference data for the DtI pages: reporting dates, LST position tree, thresholds, run status. */
export async function GET(req: NextRequest) {
  return respond(async () => {
    const query = dtiSql(req.headers);
    const [dates, positions, limits, status, monthEnds] = await Promise.all([
      query(`SELECT DISTINCT TO_VARCHAR(AS_OF_DATE) AS D FROM ${AGG}.REPP_AGG_DT_DTI_SUMMARY ORDER BY D DESC`),
      query(
        `SELECT POSITION_CODE, POSITION_NAME, PARENT_CODE, LEVEL_NO, SORT_ORDER, IS_LEAF
           FROM ${RAW}.LIQI_RAW_TB_DTI_LST_POSITIONS ORDER BY LEVEL_NO, SORT_ORDER`,
      ),
      query(`SELECT ENTITY_ID, COMMENT_THRESHOLD_CHF FROM ${RAW}.LIQI_RAW_TB_DTI_LIMITS WHERE BUCKET_CODE = '*'`),
      query(
        `SELECT TO_VARCHAR(AS_OF_DATE) AS AS_OF_DATE, ENTITY_ID, DATA_STATUS
           FROM ${AGG}.REPP_AGG_DT_DTI_SUMMARY WHERE DATA_STATUS <> 'FINAL'`,
      ),
      // last business day of its month (Zurich calendar): the month-end LST runs of the history
      query(
        `SELECT DISTINCT TO_VARCHAR(s.AS_OF_DATE) AS D
           FROM ${AGG}.REPP_AGG_DT_DTI_SUMMARY s
           JOIN ${AGG}.REPP_AGG_VW_DTI_CALENDAR c ON c.CAL_DATE = DATEADD(day, 1, s.AS_OF_DATE)
          WHERE MONTH(c.NEXT_BUSINESS_DAY) <> MONTH(s.AS_OF_DATE)`,
      ),
    ]);
    return {
      dates: dates.rows.map((r) => r.D as string),
      monthEnds: monthEnds.rows.map((r) => r.D as string),
      positions: positions.rows,
      thresholds: Object.fromEntries(limits.rows.map((r) => [r.ENTITY_ID, Number(r.COMMENT_THRESHOLD_CHF)])),
      preliminary: status.rows,
    };
  });
}
