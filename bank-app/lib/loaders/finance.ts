import { DB, type LoaderDef } from "@/lib/loaders/types";

const REP = `${DB}.REP_AGG_V001`;
const MONTH = [{ name: "month", type: "date" as const }];

/** Loaders of the Financial Control consolidation page; names are "finance.<loader>". */
export const LOADERS_FINANCE: Record<string, LoaderDef> = {
  /** reporting months, newest first; IS_COMPLETE = the snapshot is the calendar month-end */
  "finance.periods": {
    sql: `SELECT TO_VARCHAR(PERIOD_MONTH) AS PERIOD_MONTH, TO_VARCHAR(PERIOD_DATE) AS PERIOD_DATE,
                 PERIOD_DATE = LAST_DAY(PERIOD_MONTH) AS IS_COMPLETE
            FROM ${REP}.FCOA_AGG_DT_FX_PERIODS
           WHERE CURRENCY = 'CHF'
           ORDER BY PERIOD_MONTH DESC`,
  },
  "finance.entities": {
    sql: `SELECT ENTITY_ID, DISPLAY_NAME, COUNTRY, FUNCTIONAL_CCY, OWNERSHIP_PCT, SORT_ORDER
            FROM ${REP}.FCOA_AGG_VW_GROUP_STRUCTURE
           ORDER BY SORT_ORDER`,
  },
  /** consolidation sheet: one row per FS line and column (entity, IC_ELIM, NCI, GROUP) */
  "finance.consolidation": {
    sql: `SELECT STATEMENT, SECTION, FS_LINE, LABEL, SORT_ORDER,
                 IFF(COLUMN_TYPE = 'ENTITY', ENTITY_ID, COLUMN_TYPE) AS COL,
                 SUM(AMOUNT_CHF) AS AMOUNT_CHF
            FROM ${REP}.FCOA_AGG_DT_GROUP_CONSOLIDATION
           WHERE PERIOD_MONTH = ?
           GROUP BY ALL
           ORDER BY SORT_ORDER`,
    params: MONTH,
  },
  /** answer trail per entity and FS line (also the local currency amounts of the sheet) */
  "finance.trail": {
    sql: `SELECT STATEMENT, FS_LINE, LABEL, ENTITY_ID, DISPLAY_NAME, SOURCE_POSITIONS, SOURCE_PRODUCTS, HOLDING_CCYS,
                 FUNCTIONAL_CCY, AMOUNT_LCY, RATE_TYPE, TRANSLATION_RATE, ENTITY_CHF, IC_ELIM_CHF, NCI_CHF,
                 GROUP_CONTRIBUTION_CHF
            FROM ${REP}.FCOA_AGG_VW_CONSOLIDATION_TRAIL
           WHERE PERIOD_MONTH = ?
           ORDER BY ENTITY_ID, FS_LINE`,
    params: MONTH,
  },
  "finance.bridge": {
    sql: `SELECT TO_VARCHAR(PREV_PERIOD_MONTH) AS PREV_PERIOD_MONTH, METRIC, COMPONENT, ENTITY_ID, SORT_ORDER, AMOUNT_CHF
            FROM ${REP}.FCOA_AGG_VW_CONSOLIDATION_BRIDGE
           WHERE PERIOD_MONTH = ?
           ORDER BY METRIC, SORT_ORDER, ENTITY_ID`,
    params: MONTH,
  },
  "finance.translation": {
    sql: `SELECT ENTITY_ID, FUNCTIONAL_CCY, CLOSING_RATE, AVERAGE_RATE, HISTORICAL_RATE, TOTAL_ASSETS_LCY, TOTAL_ASSETS_CHF,
                 NET_ASSETS_LCY, NET_ASSETS_CHF, SHARE_CAPITAL_CHF, RESERVES_CHF, CTA_CHF, CTA_MOVEMENT_CHF,
                 NET_PROFIT_LCY, NET_PROFIT_CHF
            FROM ${REP}.FCOA_AGG_VW_FX_TRANSLATION
           WHERE PERIOD_MONTH = ?
           ORDER BY ENTITY_ID`,
    params: MONTH,
  },
  /** external balances by holding currency (currency mix of the group) */
  "finance.currency-mix": {
    sql: `SELECT CURRENCY, AKTIV_PASSIV, SUM(AMOUNT_CHF) AS AMOUNT_CHF, SUM(POSITION_COUNT) AS POSITIONS
            FROM ${REP}.FCOA_AGG_DT_ENTITY_POSITIONS
           WHERE PERIOD_MONTH = ?
           GROUP BY ALL
           ORDER BY AMOUNT_CHF DESC`,
    params: MONTH,
  },
  "finance.checks": {
    sql: `SELECT CHECK_ID, CHECK_NAME, DESCRIPTION, TOLERANCE, DIFFERENCE, STATUS, AFFECTED, RESOLUTION_HINT,
                 DEMO_DIFFERENCE, DEMO_STATUS, DEMO_AFFECTED
            FROM ${REP}.FCOA_AGG_VW_CONSOLIDATION_CHECKS
           WHERE PERIOD_MONTH = ?
           ORDER BY CHECK_ID`,
    params: MONTH,
  },
  "finance.ic": {
    sql: `SELECT PAIR_ID, IC_TYPE, DESCRIPTION, LENDER_ID, BORROWER_ID, PAIR_CCY, AMOUNT_PAIR_CCY, SIDE_A_CHF, SIDE_B_CHF,
                 DIFFERENCE_CHF, IS_DEMO_EXCEPTION, DEMO_SIDE_B_CHF, DEMO_DIFFERENCE_CHF, RESOLUTION_HINT
            FROM ${REP}.FCOA_AGG_VW_IC_RECONCILIATION
           WHERE PERIOD_MONTH = ?
           ORDER BY PAIR_ID`,
    params: MONTH,
  },
  /** data freshness of the consolidation (refresh of the position dynamic table) */
  "finance.freshness": {
    sql: `SELECT TO_VARCHAR(MAX(CALCULATION_TIMESTAMP), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS REFRESHED_AT
            FROM ${REP}.FCOA_AGG_DT_ENTITY_POSITIONS`,
  },
};
