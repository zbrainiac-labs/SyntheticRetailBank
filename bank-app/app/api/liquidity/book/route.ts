import { queryAsCaller, type Row } from "@/lib/db";
import { AGG, RAW } from "@/lib/dti/params";
import { checkBook, type LiquidityBook } from "@/lib/liquidity/book";
import { NextResponse, type NextRequest } from "next/server";

export const dynamic = "force-dynamic";

const ENTITY_COUNT = 9;

/**
 * The liquidity book behind /liquidity/lcr and /dti: generator run, as-of date, latest date of each
 * engine and the free collateral pool valued by each engine on the as-of date. Runs with the viewer's
 * rights; parts the role cannot read are null (DtI row access: the pool is only compared when the role
 * sees all entities).
 */
export async function GET(req: NextRequest) {
  const q = async (sql: string, binds: (string | null)[] = []): Promise<Row | null> => {
    try {
      return (await queryAsCaller(req.headers, sql, binds)).rows[0] ?? null;
    } catch {
      return null;
    }
  };
  const [run, lcr, dti] = await Promise.all([
    q(`SELECT RUN_ID, TO_VARCHAR(AS_OF_DATE) AS AS_OF_DATE, TO_VARCHAR(WINDOW_START) AS WINDOW_START,
              TO_VARCHAR(HISTORY_START) AS HISTORY_START
         FROM ${RAW}.LIQI_RAW_TB_GENERATION_RUN
        ORDER BY INSERT_TIMESTAMP_UTC DESC LIMIT 1`),
    q(`SELECT TO_VARCHAR(MAX(AS_OF_DATE)) AS D FROM ${AGG}.REPP_AGG_DT_LCR_DAILY`),
    q(`SELECT TO_VARCHAR(MAX(AS_OF_DATE)) AS D FROM ${AGG}.REPP_AGG_DT_DTI_SUMMARY`),
  ]);
  const asOf = (run?.AS_OF_DATE as string | undefined) ?? null;
  const [lcrPool, dtiPool] = asOf
    ? await Promise.all([
        q(
          `SELECT SUM(AMOUNT_CHF) AS V FROM ${AGG}.REPP_AGG_DT_LCR_POSITIONS
            WHERE AS_OF_DATE = ?::DATE AND SOURCE_SYSTEM = 'HOLDINGS' AND PRODUCT_TYPE NOT LIKE 'CASH%'
              AND INCLUDED_IN_LCR AND DQ_STATUS = 'OK'`,
          [asOf],
        ),
        q(
          `SELECT SUM(BASE_CHF) AS V, COUNT(DISTINCT ENTITY_ID) AS N FROM ${AGG}.REPP_AGG_DT_DTI_CASHFLOWS
            WHERE AS_OF_DATE = ?::DATE AND FLOW_KIND = 'POOL' AND PRODUCT_TYPE LIKE 'WLK %'`,
          [asOf],
        ),
      ])
    : [null, null];
  const num = (v: unknown) => (v === null || v === undefined ? null : Number(v));
  const book: LiquidityBook = {
    runId: (run?.RUN_ID as string | undefined) ?? null,
    asOf,
    windowStart: (run?.WINDOW_START as string | undefined) ?? null,
    historyStart: (run?.HISTORY_START as string | undefined) ?? null,
    lcrLatest: (lcr?.D as string | undefined) ?? null,
    dtiLatest: (dti?.D as string | undefined) ?? null,
    lcrPoolChf: num(lcrPool?.V),
    dtiPoolChf: Number(dtiPool?.N ?? 0) === ENTITY_COUNT ? num(dtiPool?.V) : null,
  };
  return NextResponse.json({ ...book, check: checkBook(book) });
}
