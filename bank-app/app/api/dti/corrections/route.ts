import { dtiSql, requireAccess } from "@/lib/dti/access";
import { respond } from "@/lib/dti/api";
import { AGG, ParamError, RAW, isoDate } from "@/lib/dti/params";
import { ENTITIES } from "@/lib/filters/model";
import type { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

const CODE = /^[A-Z0-9_]{1,50}$/;
const ID = /^CORR-[A-Z0-9-]{1,40}$/;

/**
 * Manual corrections on LST position and date level (UC2 6.3, AC-4).
 * GET lists the corrections of one reporting date (row access applies);
 * POST / DELETE are reserved for DTI_OWNER. The engine picks them up on the
 * next refresh of the DtI dynamic tables (see /api/dti/refresh).
 */
export async function GET(req: NextRequest) {
  return respond(async () => {
    const query = dtiSql(req.headers);
    const ref = isoDate(req.nextUrl.searchParams.get("ref"), "ref");
    const { rows } = await query(
      `SELECT k.CORRECTION_ID, k.ENTITY_ID, k.LST_POSITION, p.POSITION_NAME, TO_VARCHAR(k.VALUE_DATE) AS VALUE_DATE,
              k.AMOUNT_CHF, k.COMMENT, k.CREATED_BY, TO_VARCHAR(k.CREATED_AT_UTC) AS CREATED_AT_UTC,
              EXISTS (SELECT 1 FROM ${AGG}.REPP_AGG_DT_DTI_CASHFLOWS c
                       WHERE c.FLOW_KIND = 'CORRECTION' AND c.SOURCE_ID = k.CORRECTION_ID) AS IN_ENGINE
         FROM ${RAW}.LIQI_RAW_TB_DTI_CORRECTIONS k
         LEFT JOIN ${RAW}.LIQI_RAW_TB_DTI_LST_POSITIONS p ON p.POSITION_CODE = k.LST_POSITION
        WHERE k.AS_OF_DATE = ?::DATE
        QUALIFY ROW_NUMBER() OVER (PARTITION BY k.CORRECTION_ID ORDER BY k.INSERT_TIMESTAMP_UTC DESC) = 1
        ORDER BY k.CREATED_AT_UTC DESC`,
      [ref],
    );
    return {
      ref,
      rows: rows.map((r) => ({
        id: r.CORRECTION_ID as string,
        entity: r.ENTITY_ID as string,
        position: r.LST_POSITION as string,
        positionName: (r.POSITION_NAME as string | null) ?? r.LST_POSITION,
        valueDate: r.VALUE_DATE as string,
        amount: Number(r.AMOUNT_CHF),
        comment: (r.COMMENT as string | null) ?? "",
        createdBy: (r.CREATED_BY as string | null) ?? null,
        createdAt: (r.CREATED_AT_UTC as string | null) ?? null,
        inEngine: Boolean(r.IN_ENGINE),
      })),
    };
  });
}

export async function POST(req: NextRequest) {
  return respond(async () => {
    await requireAccess(req.headers, "edit");
    const query = dtiSql(req.headers);
    const b = (await req.json()) as Record<string, unknown>;
    const ref = isoDate(String(b.ref ?? ""), "ref");
    const valueDate = isoDate(String(b.valueDate ?? ""), "valueDate");
    const entity = String(b.entity ?? "");
    const position = String(b.position ?? "");
    const amount = Number(b.amount);
    const comment = String(b.comment ?? "").trim();
    if (!ENTITIES.includes(entity)) throw new ParamError("invalid entity");
    if (!CODE.test(position)) throw new ParamError("invalid position");
    if (!Number.isFinite(amount) || amount === 0 || Math.abs(amount) > 1e10) throw new ParamError("invalid amount");
    if (valueDate < ref) throw new ParamError("value date before reference date");
    if (comment.length < 5 || comment.length > 500) throw new ParamError("comment must have 5-500 characters");
    const leaf = await query(`SELECT 1 FROM ${RAW}.LIQI_RAW_TB_DTI_LST_POSITIONS WHERE POSITION_CODE = ? AND IS_LEAF`, [position]);
    if (leaf.rows.length === 0) throw new ParamError("position is not a leaf LST position");
    const { rows } = await query(
      `INSERT INTO ${RAW}.LIQI_RAW_TB_DTI_CORRECTIONS
         (CORRECTION_ID, AS_OF_DATE, ENTITY_ID, LST_POSITION, VALUE_DATE, AMOUNT_CHF, COMMENT, CREATED_BY, CREATED_AT_UTC)
       SELECT 'CORR-' || TO_VARCHAR(SYSDATE(), 'YYYYMMDD-HH24MISSFF3'), ?::DATE, ?, ?, ?::DATE, ?, ?, CURRENT_USER(), SYSDATE()`,
      [ref, entity, position, valueDate, amount, comment],
    );
    return { inserted: Number(rows[0]?.["number of rows inserted"] ?? 1) };
  });
}

export async function DELETE(req: NextRequest) {
  return respond(async () => {
    await requireAccess(req.headers, "edit");
    const id = req.nextUrl.searchParams.get("id") ?? "";
    if (!ID.test(id)) throw new ParamError("invalid id");
    const { rows } = await dtiSql(req.headers)(`DELETE FROM ${RAW}.LIQI_RAW_TB_DTI_CORRECTIONS WHERE CORRECTION_ID = ?`, [id]);
    return { deleted: Number(rows[0]?.["number of rows deleted"] ?? 0) };
  });
}
