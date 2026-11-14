import { dtiSql } from "@/lib/dti/access";
import { respond } from "@/lib/dti/api";
import { RAW } from "@/lib/dti/params";

import type { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

/** Parameter Page (UC2 6.10): every reference list the DtI analysis relies on. */
export async function GET(req: NextRequest) {
  return respond(async () => {
    const query = dtiSql(req.headers);
    const t = (sql: string) => query(sql).then((r) => r.rows);
    const [limits, entities, applications, clusters, wlk, buckets, holidays, assignment] = await Promise.all([
      t(`SELECT ENTITY_ID, BUCKET_CODE, COMMENT_THRESHOLD_CHF FROM ${RAW}.LIQI_RAW_TB_DTI_LIMITS ORDER BY ENTITY_ID = 'GROUP' DESC, ENTITY_ID`),
      t(`SELECT ENTITY_ID, ENTITY_NAME, CB_BALANCE_SHARE, IS_DEFAULT FROM ${RAW}.LIQI_RAW_TB_DTI_ENTITIES ORDER BY SORT_ORDER`),
      t(`SELECT APPLICATION_ID, APPLICATION_NAME FROM ${RAW}.LIQI_RAW_TB_DTI_APPLICATIONS ORDER BY SORT_ORDER`),
      t(`SELECT PRODUCT_GROUP, PRODUCT_CLUSTER, CLUSTER_NAME FROM ${RAW}.LIQI_RAW_TB_DTI_PRODUCT_CLUSTERS ORDER BY SORT_ORDER`),
      t(`SELECT WLK_CLUSTER, CLUSTER_NAME, HQLA_CATEGORY, HAIRCUT FROM ${RAW}.LIQI_RAW_TB_DTI_WLK_CLUSTERS ORDER BY SORT_ORDER`),
      t(`SELECT BUCKET_CODE, BUCKET_NAME, OFFSET_UNIT, OFFSET_VALUE FROM ${RAW}.LIQI_RAW_TB_DTI_BUCKETS ORDER BY SORT_ORDER`),
      t(`SELECT TO_VARCHAR(HOLIDAY_DATE) AS HOLIDAY_DATE, HOLIDAY_NAME FROM ${RAW}.LIQI_RAW_TB_DTI_HOLIDAYS ORDER BY HOLIDAY_DATE`),
      t(`SELECT SOURCE_SYSTEM, PRODUCT_TYPE, FLOW_KIND, POSITION_CODE, PRODUCT_CLUSTER FROM ${RAW}.LIQI_RAW_TB_DTI_LST_ASSIGNMENT ORDER BY POSITION_CODE, SOURCE_SYSTEM, PRODUCT_TYPE`),
    ]);
    return { limits, entities, applications, clusters, wlk, buckets, holidays, assignment };
  });
}
