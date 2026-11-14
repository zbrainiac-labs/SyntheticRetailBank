import { query } from "@/lib/db";
import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

/** Daily regulatory LCR (BASE) - first real data route, used by the home page. */
export async function GET() {
  try {
    const { rows } = await query(
      `SELECT AS_OF_DATE, LCR_RATIO, LCR_STATUS
         FROM AAA_DEV_SYNTHETIC_BANK.REP_AGG_V001.REPP_AGG_DT_LCR_DAILY
        ORDER BY AS_OF_DATE`,
    );
    return NextResponse.json({ rows });
  } catch (e) {
    return NextResponse.json({ error: (e as Error).message }, { status: 500 });
  }
}
