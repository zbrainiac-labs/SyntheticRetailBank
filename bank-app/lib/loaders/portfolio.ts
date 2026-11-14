import { DB, type LoaderDef } from "@/lib/loaders/types";

const C360 = `${DB}.CRM_AGG_V001.CRMA_AGG_VW_CUSTOMER_360_ENRICHED`;

/** Loaders of the ported portfolio page (port of the_bank_app/pages/portfolio_analytics.py). */
export const LOADERS_PORTFOLIO: Record<string, LoaderDef> = {
  "portfolio.kpis": {
    sql: `SELECT COUNT(*) AS TOTAL_CUSTOMERS,
                 ROUND(AVG(TOTAL_ACCOUNTS), 1) AS AVG_ACCOUNTS,
                 COUNT_IF(CONTAINS(CURRENCIES, ',')) AS MULTI_CURRENCY,
                 COUNT_IF(ACCOUNT_TIER IN ('PREMIUM', 'PLATINUM')) AS PREMIUM_PLATINUM,
                 COUNT_IF(INVESTMENT_ACCOUNTS > 0) AS INVESTMENT_HOLDERS
            FROM ${C360}`,
  },
  "portfolio.tier-distribution": {
    sql: `SELECT ACCOUNT_TIER, COUNT(*) AS CUSTOMERS
            FROM ${C360}
           GROUP BY ACCOUNT_TIER
           ORDER BY CUSTOMERS DESC`,
  },
  "portfolio.country-distribution": {
    sql: `SELECT COUNTRY, COUNT(*) AS CUSTOMERS
            FROM ${C360}
           GROUP BY COUNTRY
           ORDER BY CUSTOMERS DESC`,
  },
  "portfolio.tier-holdings": {
    sql: `SELECT ACCOUNT_TIER,
                 ROUND(AVG(TOTAL_ACCOUNTS), 1) AS TOTAL_ACCOUNTS,
                 ROUND(AVG(CHECKING_ACCOUNTS), 1) AS CHECKING_ACCOUNTS,
                 ROUND(AVG(SAVINGS_ACCOUNTS), 1) AS SAVINGS_ACCOUNTS,
                 ROUND(AVG(BUSINESS_ACCOUNTS), 1) AS BUSINESS_ACCOUNTS,
                 ROUND(AVG(INVESTMENT_ACCOUNTS), 1) AS INVESTMENT_ACCOUNTS
            FROM ${C360}
           GROUP BY ACCOUNT_TIER
           ORDER BY ACCOUNT_TIER`,
  },
};
