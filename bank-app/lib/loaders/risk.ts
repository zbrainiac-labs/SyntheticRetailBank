import { DB, type LoaderDef } from "@/lib/loaders/types";

const C360 = `${DB}.CRM_AGG_V001.CRMA_AGG_VW_CUSTOMER_360_ENRICHED`;

/** Risk & Compliance (port of the_bank_app/pages/risk_compliance.py loaders). */
export const LOADERS_RISK: Record<string, LoaderDef> = {
  /** Overview + KYC KPIs (load_customer_360 / load_compliance_risk_summary), aggregated in SQL. */
  "risk.kpis": {
    sql: `SELECT COUNT(*) AS TOTAL_CUSTOMERS,
                 COUNT_IF(OVERALL_RISK_RATING IN ('CRITICAL', 'HIGH')) AS CRITICAL_HIGH,
                 COUNT_IF(REQUIRES_EXPOSED_PERSON_REVIEW) AS PEP_REVIEW,
                 COUNT_IF(REQUIRES_SANCTIONS_REVIEW) AS SANCTIONS_REVIEW,
                 COUNT_IF(HIGH_RISK_CUSTOMER) AS HIGH_RISK_COUNT,
                 ROUND(COUNT_IF(HIGH_RISK_CUSTOMER) / NULLIF(COUNT(*), 0) * 100, 1) AS HIGH_RISK_PCT,
                 COUNT_IF(HAS_ANOMALY) AS ANOMALY_COUNT,
                 ROUND(AVG(OVERALL_RISK_SCORE), 2) AS AVG_RISK_SCORE
            FROM ${C360}`,
  },
  "risk.rating-distribution": {
    sql: `SELECT OVERALL_RISK_RATING, COUNT(*) AS CUSTOMERS
            FROM ${C360}
           GROUP BY OVERALL_RISK_RATING
           ORDER BY CUSTOMERS DESC`,
  },
  /** value_counts of PEP / sanctions match type over all customers */
  "risk.screening-status": {
    sql: `SELECT 'PEP' AS SCREENING, EXPOSED_PERSON_MATCH_TYPE AS MATCH_TYPE, COUNT(*) AS CUSTOMERS
            FROM ${C360} GROUP BY 2
          UNION ALL
          SELECT 'SANCTIONS', SANCTIONS_MATCH_TYPE, COUNT(*)
            FROM ${C360} GROUP BY 2
           ORDER BY SCREENING, CUSTOMERS DESC`,
  },
  "risk.high-risk-customers": {
    sql: `SELECT CUSTOMER_ID, FULL_NAME, COUNTRY, ACCOUNT_TIER, OVERALL_RISK_RATING,
                 ROUND(OVERALL_RISK_SCORE, 1) AS OVERALL_RISK_SCORE,
                 EXPOSED_PERSON_MATCH_TYPE, SANCTIONS_MATCH_TYPE,
                 CASE WHEN REQUIRES_EXPOSED_PERSON_REVIEW THEN 'PEP Review'
                      WHEN REQUIRES_SANCTIONS_REVIEW THEN 'Sanctions Review'
                      ELSE 'Risk Assessment' END AS ACTION_REQUIRED
            FROM ${C360}
           WHERE HIGH_RISK_CUSTOMER OR REQUIRES_EXPOSED_PERSON_REVIEW OR REQUIRES_SANCTIONS_REVIEW
           ORDER BY OVERALL_RISK_SCORE DESC, CUSTOMER_ID`,
  },
  /** plot_compliance_risk_heatmap: average risk score by country (top 20) */
  "risk.country-risk": {
    sql: `SELECT COUNTRY, ROUND(AVG(OVERALL_RISK_SCORE), 2) AS AVG_RISK_SCORE, COUNT(*) AS CUSTOMERS
            FROM ${C360}
           GROUP BY COUNTRY
           ORDER BY AVG_RISK_SCORE DESC
           LIMIT 20`,
  },
  "risk.sanctions-matches": {
    sql: `SELECT CUSTOMER_ID, FULL_NAME, COUNTRY, SANCTIONS_MATCH_TYPE, SANCTIONS_MATCH_ACCURACY_PERCENT,
                 REQUIRES_SANCTIONS_REVIEW, OVERALL_SANCTIONS_RISK
            FROM ${C360}
           WHERE SANCTIONS_MATCH_TYPE <> 'NO_MATCH'
           ORDER BY SANCTIONS_MATCH_ACCURACY_PERCENT DESC NULLS LAST, CUSTOMER_ID`,
  },
  "risk.pep-matches": {
    sql: `SELECT CUSTOMER_ID, FULL_NAME, COUNTRY, EXPOSED_PERSON_MATCH_TYPE, EXPOSED_PERSON_MATCH_ACCURACY_PERCENT,
                 REQUIRES_EXPOSED_PERSON_REVIEW, OVERALL_EXPOSED_PERSON_RISK
            FROM ${C360}
           WHERE EXPOSED_PERSON_MATCH_TYPE <> 'NO_MATCH'
           ORDER BY EXPOSED_PERSON_MATCH_ACCURACY_PERCENT DESC NULLS LAST, CUSTOMER_ID`,
  },
  /** load_kyc_completeness (non-null counts; values are masked for CICD but stay non-null) */
  "risk.kyc-completeness": {
    sql: `SELECT COUNTRY, COUNT(*) AS TOTAL_CUSTOMERS, COUNT(EMAIL) AS EMAIL_COMPLETE,
                 COUNT(PHONE) AS PHONE_COMPLETE, COUNT(EMPLOYER) AS EMPLOYER_COMPLETE,
                 ROUND(COUNT(EMAIL) / COUNT(*) * 100, 2) AS EMAIL_PCT,
                 ROUND(COUNT(PHONE) / COUNT(*) * 100, 2) AS PHONE_PCT
            FROM ${C360}
           GROUP BY COUNTRY
           ORDER BY TOTAL_CUSTOMERS DESC, COUNTRY`,
  },
};
