import { dtiSql, requireAccess } from "@/lib/dti/access";
import { respond } from "@/lib/dti/api";
import { ParamError, RAW } from "@/lib/dti/params";
import { ENTITIES } from "@/lib/filters/model";
import type { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

/** Commenting thresholds per entity (UC2 6.1/6.10): everyone reads, DTI_OWNER edits. */
export async function GET(req: NextRequest) {
  return respond(async () => {
    const { rows } = await dtiSql(req.headers)(
      `SELECT ENTITY_ID, COMMENT_THRESHOLD_CHF FROM ${RAW}.LIQI_RAW_TB_DTI_LIMITS WHERE BUCKET_CODE = '*' ORDER BY ENTITY_ID`,
    );
    return { rows: rows.map((r) => ({ entity: r.ENTITY_ID as string, threshold: Number(r.COMMENT_THRESHOLD_CHF) })) };
  });
}

export async function PUT(req: NextRequest) {
  return respond(async () => {
    await requireAccess(req.headers, "edit");
    const b = (await req.json()) as Record<string, unknown>;
    const entity = String(b.entity ?? "");
    const threshold = Number(b.threshold);
    if (entity !== "GROUP" && !ENTITIES.includes(entity)) throw new ParamError("invalid entity");
    if (!Number.isFinite(threshold) || threshold < 0 || threshold > 1e10) throw new ParamError("invalid threshold");
    await dtiSql(req.headers)(
      `MERGE INTO ${RAW}.LIQI_RAW_TB_DTI_LIMITS t USING (SELECT ? AS ENTITY_ID, '*' AS BUCKET_CODE, ? AS V) s
          ON t.ENTITY_ID = s.ENTITY_ID AND t.BUCKET_CODE = s.BUCKET_CODE
        WHEN MATCHED THEN UPDATE SET t.COMMENT_THRESHOLD_CHF = s.V
        WHEN NOT MATCHED THEN INSERT (ENTITY_ID, BUCKET_CODE, COMMENT_THRESHOLD_CHF) VALUES (s.ENTITY_ID, s.BUCKET_CODE, s.V)`,
      [entity, threshold],
    );
    return { entity, threshold };
  });
}
