import { DB, type LoaderDef } from "@/lib/loaders/types";

const C360 = `${DB}.CRM_AGG_V001.CRMA_AGG_VW_CUSTOMER_360_ENRICHED`;

/** "_" = no filter for string params (empty values are rejected by bindParams). */
const ANY = "_";

/**
 * Loaders of the ported customer360 page (port of the_bank_app/pages/customer_360.py,
 * which loaded the full CRMA_AGG_VW_CUSTOMER_360_ENRICHED view into pandas).
 * PII columns are masked by policy for the app role.
 */
export const LOADERS_CUSTOMER360: Record<string, LoaderDef> = {
  "customer360.kpis": {
    sql: `SELECT COUNT(*) AS TOTAL_CUSTOMERS,
                 COUNT_IF(OVERALL_RISK_RATING IN ('CRITICAL', 'HIGH')) AS CRITICAL_HIGH,
                 COUNT_IF(EXPOSED_PERSON_MATCH_TYPE <> 'NO_MATCH') AS PEP_MATCHES,
                 ROUND(AVG(OVERALL_RISK_SCORE), 1) AS AVG_RISK_SCORE
            FROM ${C360}`,
  },
  "customer360.filter-options": {
    sql: `SELECT 'COUNTRY' AS KIND, COUNTRY AS VALUE FROM ${C360} WHERE COUNTRY IS NOT NULL GROUP BY COUNTRY
          UNION ALL
          SELECT 'ACCOUNT_TIER', ACCOUNT_TIER FROM ${C360} WHERE ACCOUNT_TIER IS NOT NULL GROUP BY ACCOUNT_TIER
          UNION ALL
          SELECT 'RISK', OVERALL_RISK_RATING FROM ${C360} WHERE OVERALL_RISK_RATING IS NOT NULL GROUP BY OVERALL_RISK_RATING
          ORDER BY KIND, VALUE`,
  },
  /** Filtered customer list (all matches; the table pages, the picker is searchable). MATCH_COUNT = match count. */
  "customer360.list": {
    sql: `WITH p AS (SELECT ?::STRING AS CUST, ?::STRING AS NAME, ?::STRING AS COUNTRY, ?::STRING AS TIERS,
                            ?::STRING AS RISK, ?::INT AS HIGH_RISK, ?::INT AS PEP, ?::INT AS REVIEW, ?::INT AS ANOMALY)
          SELECT c.CUSTOMER_ID, c.FULL_NAME, c.COUNTRY, c.ACCOUNT_TIER, c.OVERALL_RISK_RATING,
                 ROUND(c.OVERALL_RISK_SCORE, 1) AS OVERALL_RISK_SCORE,
                 c.EXPOSED_PERSON_MATCH_TYPE, c.SANCTIONS_MATCH_TYPE,
                 COUNT(*) OVER () AS MATCH_COUNT
            FROM ${C360} c, p
           WHERE (p.CUST = '${ANY}' OR c.CUSTOMER_ID = p.CUST)
             AND (p.NAME = '${ANY}' OR CONTAINS(LOWER(c.FULL_NAME), LOWER(p.NAME)))
             AND (p.COUNTRY = '${ANY}' OR c.COUNTRY = p.COUNTRY)
             AND (p.TIERS = '${ANY}' OR ARRAY_CONTAINS(c.ACCOUNT_TIER::VARIANT, SPLIT(p.TIERS, ' ')))
             AND (p.RISK = '${ANY}' OR c.OVERALL_RISK_RATING = p.RISK)
             AND (p.HIGH_RISK = 0 OR c.OVERALL_RISK_RATING IN ('CRITICAL', 'HIGH'))
             AND (p.PEP = 0 OR c.EXPOSED_PERSON_MATCH_TYPE <> 'NO_MATCH')
             AND (p.REVIEW = 0 OR c.REQUIRES_EXPOSED_PERSON_REVIEW OR c.REQUIRES_SANCTIONS_REVIEW)
             AND (p.ANOMALY = 0 OR c.HAS_ANOMALY)
           ORDER BY c.CUSTOMER_ID
           LIMIT 10000`,
    params: [
      { name: "customer", type: "code", default: ANY },
      { name: "name", type: "code", default: ANY },
      { name: "country", type: "code", default: ANY },
      { name: "tiers", type: "code", default: ANY },
      { name: "risk", type: "code", default: ANY },
      { name: "highRisk", type: "int", default: "0" },
      { name: "pep", type: "int", default: "0" },
      { name: "review", type: "int", default: "0" },
      { name: "anomaly", type: "int", default: "0" },
    ],
    maxRows: 10000,
  },
  "customer360.detail": {
    sql: `SELECT CUSTOMER_ID, FULL_NAME, FIRST_NAME, FAMILY_NAME, TO_VARCHAR(DATE_OF_BIRTH) AS DATE_OF_BIRTH,
                 TO_VARCHAR(ONBOARDING_DATE) AS ONBOARDING_DATE, REPORTING_CURRENCY, CURRENT_STATUS,
                 EMAIL, PHONE, PREFERRED_CONTACT_METHOD,
                 EMPLOYER, POSITION, EMPLOYMENT_TYPE, INCOME_RANGE, ACCOUNT_TIER, CREDIT_SCORE_BAND, RISK_CLASSIFICATION,
                 STREET_ADDRESS, CITY, STATE, ZIPCODE, COUNTRY,
                 TOTAL_ACCOUNTS, CHECKING_ACCOUNTS, SAVINGS_ACCOUNTS, INVESTMENT_ACCOUNTS, ACCOUNT_TYPES, CURRENCIES,
                 OVERALL_RISK_RATING, OVERALL_RISK_SCORE,
                 EXPOSED_PERSON_MATCH_TYPE, EXPOSED_PERSON_MATCH_ACCURACY_PERCENT, OVERALL_EXPOSED_PERSON_RISK,
                 SANCTIONS_MATCH_TYPE, SANCTIONS_MATCH_ACCURACY_PERCENT, OVERALL_SANCTIONS_RISK,
                 HIGH_RISK_CUSTOMER, REQUIRES_EXPOSED_PERSON_REVIEW, REQUIRES_SANCTIONS_REVIEW, HAS_ANOMALY
            FROM ${C360}
           WHERE CUSTOMER_ID = ?`,
    params: [{ name: "id", type: "code" }],
  },
  "customer360.address-history": {
    // SCD Type 2 validity from the history DT, address details joined on the fly from the raw versions
    // (masked like the current address); one row per version
    sql: `SELECT TO_VARCHAR(h.VALID_FROM) AS VALID_FROM, TO_VARCHAR(h.VALID_TO) AS VALID_TO,
                 IFF(h.IS_CURRENT, 'Current', 'Previous') AS STATUS,
                 a.STREET_ADDRESS, a.CITY, a.STATE, a.ZIPCODE, h.COUNTRY
            FROM ${DB}.CRM_AGG_V001.CRMA_AGG_DT_ADDRESSES_HISTORY h
            JOIN ${DB}.CRM_RAW_V001.CRMI_RAW_TB_ADDRESSES a
              ON a.CUSTOMER_ID = h.CUSTOMER_ID AND a.INSERT_TIMESTAMP_UTC = h.INSERT_TIMESTAMP_UTC
           WHERE h.CUSTOMER_ID = ?
         QUALIFY ROW_NUMBER() OVER (PARTITION BY h.INSERT_TIMESTAMP_UTC ORDER BY a.STREET_ADDRESS) = 1
           ORDER BY h.VALID_FROM DESC`,
    params: [{ name: "id", type: "code" }],
  },
};
