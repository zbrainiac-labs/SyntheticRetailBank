import { DB, type LoaderDef } from "@/lib/loaders/types";

/** Loaders of the ported settings page(s); names are "settings.<loader>". */
export const LOADERS_SETTINGS: Record<string, LoaderDef> = {
  /** load_data_quality_metrics(): completeness of the enriched customer 360 */
  "settings.data-quality": {
    sql: `SELECT COUNT(*) AS TOTAL_RECORDS,
                 COUNT(DISTINCT CUSTOMER_ID) AS UNIQUE_CUSTOMERS,
                 COUNT(*) - COUNT(EMAIL) AS MISSING_EMAIL,
                 COUNT(*) - COUNT(PHONE) AS MISSING_PHONE,
                 COUNT(*) - COUNT(DATE_OF_BIRTH) AS MISSING_DOB,
                 COUNT(*) - COUNT(STREET_ADDRESS) AS MISSING_ADDRESS,
                 ROUND(100 * COUNT(EMAIL) / NULLIF(COUNT(*), 0), 2) AS EMAIL_COMPLETENESS,
                 ROUND(100 * COUNT(PHONE) / NULLIF(COUNT(*), 0), 2) AS PHONE_COMPLETENESS,
                 ROUND(100 * COUNT(DATE_OF_BIRTH) / NULLIF(COUNT(*), 0), 2) AS DOB_COMPLETENESS,
                 ROUND(100 * COUNT(STREET_ADDRESS) / NULLIF(COUNT(*), 0), 2) AS ADDRESS_COMPLETENESS
            FROM ${DB}.CRM_AGG_V001.CRMA_AGG_VW_CUSTOMER_360_ENRICHED`,
  },
  /** Snowflake session context (Streamlit: CURRENT_DATABASE/SCHEMA/USER) */
  "settings.session": {
    sql: `SELECT CURRENT_USER() AS USER_NAME, CURRENT_ROLE() AS ROLE_NAME, CURRENT_DATABASE() AS DATABASE_NAME,
                 CURRENT_SCHEMA() AS SCHEMA_NAME, CURRENT_WAREHOUSE() AS WAREHOUSE_NAME, CURRENT_VERSION() AS SNOWFLAKE_VERSION,
                 TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS TZH:TZM') AS SERVER_TIME`,
  },
};
