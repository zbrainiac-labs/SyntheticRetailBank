import { dtiSql } from "@/lib/dti/access";
import { respond } from "@/lib/dti/api";
import { AGG, RAW, parseDtiParams, placeholders } from "@/lib/dti/params";
import type { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

const HISTORY_DAYS = 100;

/**
 * DtI history and events (Summary page): group DtI of the selected entities on every reporting date of the
 * last HISTORY_DAYS before the reference date, plus the showcase and cross-engine events of the book
 * (LIQI_RAW_TB_DTI_EVENTS: withdrawals, the funding cliff and its roll, purchases, corrections).
 */
export async function GET(req: NextRequest) {
  return respond(async () => {
    const query = dtiSql(req.headers);
    const p = parseDtiParams(req.nextUrl.searchParams);
    const s = p.valueType === "BASE" ? "BASE" : "STRESSED";
    const [history, events] = await Promise.all([
      query(
        `WITH g AS (
           SELECT AS_OF_DATE, DAY_OFFSET, SUM(TOTAL_${s}_CHF) AS TOTAL
             FROM ${AGG}.REPP_AGG_DT_DTI_PROJECTION
            WHERE AS_OF_DATE BETWEEN DATEADD(day, -${HISTORY_DAYS}, ?::DATE) AND ?::DATE
              AND ENTITY_ID IN (${placeholders(p.entities.length)})
            GROUP BY 1, 2
         )
         SELECT TO_VARCHAR(AS_OF_DATE) AS AS_OF_DATE, MIN(IFF(TOTAL <= 0, DAY_OFFSET, NULL)) AS DTI
           FROM g GROUP BY 1 ORDER BY 1`,
        [p.ref, p.ref, ...p.entities],
      ),
      query(
        `SELECT EVENT_ID, TO_VARCHAR(EVENT_DATE) AS EVENT_DATE, ENTITY_ID, EVENT_TYPE, POSITION_ID, AMOUNT_CHF, DESCRIPTION
           FROM ${RAW}.LIQI_RAW_TB_DTI_EVENTS
          QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY INSERT_TIMESTAMP_UTC DESC) = 1
          ORDER BY EVENT_DATE, EVENT_ID`,
      ),
    ]);
    return {
      history: history.rows.map((r) => ({ date: r.AS_OF_DATE as string, dti: r.DTI === null ? null : Number(r.DTI) })),
      events: events.rows.map((r) => ({
        id: r.EVENT_ID as string,
        date: r.EVENT_DATE as string,
        entity: (r.ENTITY_ID as string | null) ?? null,
        type: r.EVENT_TYPE as string,
        position: (r.POSITION_ID as string | null) ?? null,
        amount: Number(r.AMOUNT_CHF ?? 0),
        description: (r.DESCRIPTION as string | null) ?? "",
      })),
    };
  });
}
