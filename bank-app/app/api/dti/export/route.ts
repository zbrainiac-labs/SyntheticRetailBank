import { dtiSql, requireAccess } from "@/lib/dti/access";
import { errorStatus } from "@/lib/dti/api";
import { AGG, parseDtiParams } from "@/lib/dti/params";
import { NextResponse, type NextRequest } from "next/server";

export const dynamic = "force-dynamic";

const COLUMNS = [
  "POSITION_CODE", "PRODUCT_CLUSTER", "APPLICATION_ID", "AKTIV_PASSIV", "PRODUCT_TYPE", "PERIOD_KEY", "PERIOD_DATE",
  "BUCKET_REF", "BUCKET_COMP", "REF_VALUE", "COMP_VALUE", "DELTA", "IS_INFO_ROW",
] as const;

function csvCell(v: unknown): string {
  if (v === null || v === undefined) return "";
  const s = String(v);
  // quote when needed; prefix formula characters so spreadsheets do not evaluate them
  const safe = /^[=+\-@]/.test(s) && !/^-?\d/.test(s) ? `'${s}` : s;
  return /[",\n]/.test(safe) ? `"${safe.replace(/"/g, '""')}"` : safe;
}

/**
 * CSV export of the comparison at full grain (UC2 export), only for roles
 * with export rights (DTI_LRC_READ, DTI_OWNER). Row access applies, so the
 * file never holds entities the viewer cannot see.
 */
export async function GET(req: NextRequest) {
  try {
    const access = await requireAccess(req.headers, "export");
    const p = parseDtiParams(req.nextUrl.searchParams);
    const { rows } = await dtiSql(req.headers)(
      `SELECT ${COLUMNS.join(", ")}
         FROM TABLE(${AGG}.REPP_AGG_FN_DTI_COMPARE(?::DATE, ?::DATE, SPLIT(?, ','), ?, ?, ?))
        ORDER BY PERIOD_SORT, POSITION_CODE, PRODUCT_TYPE`,
      [p.ref, p.comp, p.entities.join(","), p.valueType, p.view, p.includePreliminary],
    );
    const lines = [
      `# DtI comparison ${p.ref} vs ${p.comp}; entities ${p.entities.join(" ")}; ${p.valueType}; ${p.view}; exported by role ${access.role}`,
      COLUMNS.join(","),
      ...rows.map((r) => COLUMNS.map((c) => csvCell(r[c])).join(",")),
    ];
    return new NextResponse(lines.join("\n") + "\n", {
      headers: {
        "content-type": "text/csv; charset=utf-8",
        "content-disposition": `attachment; filename="dti_comparison_${p.ref}_vs_${p.comp}.csv"`,
      },
    });
  } catch (e) {
    return NextResponse.json({ error: (e as Error).message }, { status: errorStatus(e) });
  }
}
