import { dtiSql, requireAccess } from "@/lib/dti/access";
import { respond } from "@/lib/dti/api";
import { AGG } from "@/lib/dti/params";
import type { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

/**
 * DTI_OWNER: refresh the DtI dynamic tables after editing corrections, so the
 * pages show them without waiting for the target lag (OPERATE grant).
 */
export async function POST(req: NextRequest) {
  return respond(async () => {
    await requireAccess(req.headers, "edit");
    const query = dtiSql(req.headers);
    const started = Date.now();
    for (const t of ["REPP_AGG_DT_DTI_CASHFLOWS", "REPP_AGG_DT_DTI_PROJECTION", "REPP_AGG_DT_DTI_SUMMARY"]) {
      await query(`ALTER DYNAMIC TABLE ${AGG}.${t} REFRESH`);
    }
    return { refreshed: true, seconds: Math.round((Date.now() - started) / 1000) };
  });
}
