import { dtiSql } from "@/lib/dti/access";
import { respond } from "@/lib/dti/api";
import { AGG, parseDtiParams, placeholders, statusFilter } from "@/lib/dti/params";
import type { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

/**
 * Summary page (UC2 6.1): cumulative projection of the selected entities for
 * the reference and the comparison date (Level I and total), and the DtI of both.
 * Group = sum of the selected entities, so the DtI is computed on the summed curve.
 */
export async function GET(req: NextRequest) {
  return respond(async () => {
    const query = dtiSql(req.headers);
    const p = parseDtiParams(req.nextUrl.searchParams);
    const s = p.valueType === "BASE" ? "BASE" : "STRESSED";
    // per-entity DtI for the Summary entity slots (prototype: up to 5 entities + group)
    const perEntity = query(
      `SELECT TO_VARCHAR(AS_OF_DATE) AS AS_OF_DATE, ENTITY_ID, DATA_STATUS, DTI_DAYS_${s} AS DAYS
         FROM ${AGG}.REPP_AGG_DT_DTI_SUMMARY
        WHERE AS_OF_DATE IN (?::DATE, ?::DATE)`,
      [p.ref, p.comp],
    );
    const { rows } = await query(
      `SELECT TO_VARCHAR(AS_OF_DATE) AS AS_OF_DATE, DAY_OFFSET, TO_VARCHAR(MIN(PROJECTION_DATE)) AS PROJECTION_DATE,
              SUM(POOL_${s}_CHF) AS POOL, SUM(DET_${s}_CHF) AS DET, SUM(INF_${s}_CHF) AS INF,
              SUM(OUT_${s}_CHF) AS OUT, SUM(TOTAL_${s}_CHF) AS TOTAL, COUNT(DISTINCT ENTITY_ID) AS ENTITIES
         FROM ${AGG}.REPP_AGG_DT_DTI_PROJECTION
        WHERE AS_OF_DATE IN (?::DATE, ?::DATE)
          AND ENTITY_ID IN (${placeholders(p.entities.length)})
          AND ${statusFilter(p)}
        GROUP BY 1, 2
        ORDER BY 1, 2`,
      [p.ref, p.comp, ...p.entities],
    );
    const series = (d: string) =>
      rows
        .filter((r) => r.AS_OF_DATE === d)
        .map((r) => ({
          day: Number(r.DAY_OFFSET),
          date: r.PROJECTION_DATE as string,
          pool: Number(r.POOL),
          det: Number(r.DET),
          inf: Number(r.INF),
          out: Number(r.OUT),
          total: Number(r.TOTAL),
          entities: Number(r.ENTITIES),
        }));
    const dti = (pts: ReturnType<typeof series>) => {
      const hit = pts.find((x) => x.total <= 0);
      return hit ? { days: hit.day, date: hit.date } : null;
    };
    const ref = series(p.ref);
    const comp = series(p.comp);
    const ent = (await perEntity).rows;
    const daysOf = (d: string, e: string) => {
      const r = ent.find((x) => x.AS_OF_DATE === d && x.ENTITY_ID === e);
      return r ? { days: r.DAYS === null ? null : Number(r.DAYS), status: r.DATA_STATUS as string } : null;
    };
    const entities = [...new Set(ent.map((r) => r.ENTITY_ID as string))].sort((a, b) => a.localeCompare(b)).map((e) => ({
      entity: e,
      ref: daysOf(p.ref, e),
      comp: daysOf(p.comp, e),
    }));
    return { ref: { points: ref, dti: dti(ref) }, comp: { points: comp, dti: dti(comp) }, entities };
  });
}
