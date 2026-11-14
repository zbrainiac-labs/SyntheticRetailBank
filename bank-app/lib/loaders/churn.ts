import { DB, type LoaderDef } from "@/lib/loaders/types";

const DT = `${DB}.CRM_AGG_V001.CRMA_AGG_DT_CUSTOMER_360`;
const C360 = `${DB}.CRM_AGG_V001.CRMA_AGG_VW_CUSTOMER_360_ENRICHED`;

/** Dormant-list filter of load_dormant_accounts(). */
const DORMANT = `DAYS_SINCE_LAST_TRANSACTION > 180 AND LIFECYCLE_STAGE IN ('DORMANT', 'DECLINING')`;

/** Loaders of the ported churn page (port of the_bank_app/pages/churn_lifecycle.py). */
export const LOADERS_CHURN: Record<string, LoaderDef> = {
  /** KPIs + calculate_revenue_at_risk() (revenue in CHF thousands). */
  "churn.kpis": {
    sql: `SELECT COUNT(*) AS TOTAL_CUSTOMERS,
                 COUNT_IF(CHURN_PROBABILITY > 70) AS AT_RISK_CUSTOMERS,
                 COUNT_IF(CHURN_PROBABILITY > 90) AS CRITICAL_CUSTOMERS,
                 COUNT_IF(CHURN_PROBABILITY BETWEEN 70 AND 90) AS HIGH_CUSTOMERS,
                 COALESCE(SUM(IFF(CHURN_PROBABILITY > 70, TOTAL_BALANCE, 0)), 0) / 1000 AS TOTAL_REVENUE_AT_RISK,
                 COUNT_IF(DAYS_SINCE_LAST_TRANSACTION > 180) AS DORMANT_180
            FROM ${DT}`,
  },
  "churn.stage-summary": {
    sql: `SELECT LIFECYCLE_STAGE, COUNT(*) AS CUSTOMER_COUNT,
                 AVG(CHURN_PROBABILITY) AS AVG_CHURN_PROBABILITY,
                 AVG(DAYS_SINCE_LAST_TRANSACTION) AS AVG_DAYS_INACTIVE
            FROM ${DT}
           GROUP BY LIFECYCLE_STAGE
           ORDER BY CASE LIFECYCLE_STAGE WHEN 'NEW' THEN 1 WHEN 'ACTIVE' THEN 2 WHEN 'MATURE' THEN 3
                    WHEN 'DECLINING' THEN 4 WHEN 'DORMANT' THEN 5 WHEN 'CHURNED' THEN 6 ELSE 7 END`,
  },
  /** 20 bins of 5 pp (histogram of CHURN_PROBABILITY, 100 falls in the last bin). */
  "churn.probability-histogram": {
    sql: `SELECT LEAST(FLOOR(CHURN_PROBABILITY / 5), 19) * 5 AS BIN_START, COUNT(*) AS CUSTOMERS
            FROM ${DT}
           WHERE CHURN_PROBABILITY IS NOT NULL
           GROUP BY BIN_START
           ORDER BY BIN_START`,
  },
  "churn.by-tier": {
    sql: `SELECT ACCOUNT_TIER, ROUND(AVG(CHURN_PROBABILITY), 1) AS AVG_CHURN_PROBABILITY, COUNT(*) AS CUSTOMER_COUNT
            FROM ${DT}
           GROUP BY ACCOUNT_TIER
           ORDER BY AVG_CHURN_PROBABILITY DESC`,
  },
  "churn.premium-at-risk": {
    sql: `SELECT CUSTOMER_ID, FIRST_NAME, FAMILY_NAME, ACCOUNT_TIER, COUNTRY, LIFECYCLE_STAGE,
                 ROUND(CHURN_PROBABILITY, 1) AS CHURN_PROBABILITY, DAYS_SINCE_LAST_TRANSACTION,
                 TO_VARCHAR(LAST_TRANSACTION_DATE) AS LAST_TRANSACTION_DATE, EMAIL, PHONE, PREFERRED_CONTACT_METHOD
            FROM ${C360}
           WHERE ACCOUNT_TIER IN ('GOLD', 'PLATINUM') AND CHURN_PROBABILITY > 70
           ORDER BY CHURN_PROBABILITY DESC, CUSTOMER_ID`,
  },
  "churn.dormant-stats": {
    sql: `SELECT COUNT(*) AS DORMANT_COUNT, AVG(DAYS_SINCE_LAST_TRANSACTION) AS AVG_DAYS_INACTIVE,
                 MAX(DAYS_SINCE_LAST_TRANSACTION) AS MAX_DAYS_INACTIVE,
                 COUNT_IF(ACCOUNT_TIER IN ('GOLD', 'PLATINUM')) AS PREMIUM_DORMANT
            FROM ${DT}
           WHERE ${DORMANT}`,
  },
  /** 30 equal-width bins between min and max days inactive (px.histogram nbins=30). */
  "churn.dormant-histogram": {
    sql: `WITH d AS (SELECT DAYS_SINCE_LAST_TRANSACTION AS D FROM ${DT} WHERE ${DORMANT}),
               r AS (SELECT MIN(D) AS LO, GREATEST((MAX(D) - MIN(D)) / 30, 1) AS W FROM d)
          SELECT ROUND(r.LO + LEAST(FLOOR((d.D - r.LO) / r.W), 29) * r.W) AS BIN_START, COUNT(*) AS CUSTOMERS
            FROM d, r
           GROUP BY BIN_START
           ORDER BY BIN_START`,
  },
};
