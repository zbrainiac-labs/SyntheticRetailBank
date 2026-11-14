{# semantic_views='false' is only used for the first deploy into an empty database (see 715) #}
{% if semantic_views == 'true' %}
/*
 * 716_DTIS_semantic_views.sql
 * DtI / Stichtagsvergleich (UC2) semantic view for Cortex Analyst and the
 * DTI_Compare tool of LIQUIDITY_RISK_AGENT. Row access (REPP_AGG_RAP_DTI_ENTITY)
 * applies to the underlying tables, so answers only cover the caller's entities.
 */

DEFINE SEMANTIC VIEW {{ db }}.{{ rep_agg }}.DTIS_SV_DTI_COMPARISON
tables (
  dti AS {{ db }}.{{ rep_agg }}.REPP_AGG_DT_DTI_SUMMARY
    PRIMARY KEY (AS_OF_DATE, ENTITY_ID)
    WITH SYNONYMS = ('distance to illiquidity', 'DtI', 'survival horizon')
    COMMENT = 'Distance-to-Illiquidity per legal entity and reporting date (one row per entity x date)',
  flows AS {{ db }}.{{ rep_agg }}.REPP_AGG_DT_DTI_CASHFLOWS
    WITH SYNONYMS = ('LST flows', 'liquidity flows', 'cash flows')
    COMMENT = 'Signed LST flows per reporting date: + = liquidity available / inflow, - = outflow',
  pos AS {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_LST_POSITIONS
    PRIMARY KEY (POSITION_CODE)
    COMMENT = 'LST position scheme (Level I-IV)',
  ent AS {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_ENTITIES
    PRIMARY KEY (ENTITY_ID)
    COMMENT = 'Legal entities LE_01..LE_09'
)
relationships (
  flows_to_pos AS flows (POSITION_CODE) REFERENCES pos,
  flows_to_ent AS flows (ENTITY_ID) REFERENCES ent,
  dti_to_ent AS dti (ENTITY_ID) REFERENCES ent
)
facts (
  flows.BASE_CHF as BASE_CHF
    comment='Base value of the flow in CHF (signed)',
  flows.STRESSED_CHF as STRESSED_CHF
    WITH SYNONYMS = ('stressed value', 'stress value')
    comment='Stressed value of the flow in CHF (signed; COMBINED behavioral rules, WLK haircuts)',
  flows.DAYS_FROM_REPORTING_DATE as DATEDIFF(day, flows.AS_OF_DATE, flows.CF_DATE)
    comment='Projection day of the flow relative to its own reporting date (0 = reporting date)',
  dti.DTI_DAYS_STRESSED as DTI_DAYS_STRESSED
    WITH SYNONYMS = ('DtI days', 'days to illiquidity', 'stressed DtI')
    comment='Stressed DtI in days; NULL when liquidity stays positive for 365 days (> 365)',
  dti.DTI_DAYS_BASE as DTI_DAYS_BASE
    comment='Base DtI in days; NULL when > 365',
  dti.MIN_TOTAL_STRESSED_CHF as MIN_TOTAL_STRESSED_CHF
    comment='Lowest stressed cumulative liquidity within 365 days in CHF'
)
dimensions (
  dti.AS_OF_DATE as AS_OF_DATE
    WITH SYNONYMS = ('reporting date', 'Stichtag', 'date')
    comment='Reporting date (Zurich business days and month-ends)',
  dti.ENTITY_ID as ENTITY_ID
    comment='Legal entity code',
  dti.DATA_STATUS as DATA_STATUS
    comment='FINAL or PRELIMINARY run status',
  dti.DTI_DATE_STRESSED as DTI_DATE_STRESSED
    comment='Date on which the stressed cumulative liquidity first reaches zero',
  flows.FLOW_AS_OF_DATE as flows.AS_OF_DATE
    WITH SYNONYMS = ('flow reporting date')
    comment='Reporting date of the flow (compare two dates with conditional sums)',
  flows.FLOW_ENTITY_ID as flows.ENTITY_ID
    comment='Legal entity of the flow',
  flows.CF_DATE as CF_DATE
    WITH SYNONYMS = ('value date', 'cash flow date')
    comment='Projection date of the flow',
  flows.POSITION_CODE as POSITION_CODE
    comment='Leaf LST position code',
  flows.PRODUCT_CLUSTER as PRODUCT_CLUSTER
    WITH SYNONYMS = ('product group', 'cluster')
    comment='Product cluster (e.g. PC_CORP_DEP corporate deposits, PC_GOV government securities)',
  flows.APPLICATION_ID as APPLICATION_ID
    WITH SYNONYMS = ('application', 'source system')
    comment='Source application (HYP, KRE, GEL, WPA, EIN, DER)',
  flows.PRODUCT_TYPE as PRODUCT_TYPE
    WITH SYNONYMS = ('product', 'single product')
    comment='Single product, e.g. deposit type, loan product, WLK cluster of a security (WLK II)',
  flows.FLOW_KIND as FLOW_KIND
    comment='CONTRACTUAL, POOL, RUNOFF, DRAWDOWN, RENEWAL, INFLOW, PLANNING or CORRECTION',
  flows.FLOW_STATUS as flows.DATA_STATUS
    comment='FINAL or PRELIMINARY status of the flow',
  pos.POSITION_NAME as POSITION_NAME
    WITH SYNONYMS = ('LST position', 'position')
    comment='Display name of the LST position',
  pos.LEVEL_NO as LEVEL_NO
    comment='Level of the position in the LST tree (1..4)',
  ent.ENTITY_NAME as ENTITY_NAME
    WITH SYNONYMS = ('legal entity', 'entity')
    comment='Display name of the legal entity'
)
metrics (
  flows.TOTAL_STRESSED_CHF as SUM(flows.STRESSED_CHF)
    WITH SYNONYMS = ('stressed liquidity', 'stressed balance')
    comment='Sum of stressed flow values in CHF',
  flows.TOTAL_BASE_CHF as SUM(flows.BASE_CHF)
    comment='Sum of base flow values in CHF',
  dti.AVG_DTI_DAYS_STRESSED as AVG(dti.DTI_DAYS_STRESSED)
    comment='Average stressed DtI in days over the selected entities and dates (entities > 365 excluded)'
)
COMMENT = 'UC2 Stichtagsvergleich: Distance-to-Illiquidity per entity and date, and the LST flows behind it (positions, product clusters, applications). Compare two reporting dates with conditional sums on FLOW_AS_OF_DATE.'
AI_SQL_GENERATION 'Values are in CHF, signed: positive = liquidity available or inflow, negative = outflow. Use STRESSED values unless the user asks for base values. A DtI of NULL means more than 365 days. Default dates: the latest AS_OF_DATE (reference) and the one before it (comparison). To compare two dates, aggregate with SUM(IFF(FLOW_AS_OF_DATE = <ref>, STRESSED_CHF, 0)) - SUM(IFF(FLOW_AS_OF_DATE = <comp>, STRESSED_CHF, 0)) and restrict flows to the first 365 days (DAYS_FROM_REPORTING_DATE <= 365). Group = sum of entities; never average DtI days to get a group DtI. Exclude PRELIMINARY data unless asked.'

AI_VERIFIED_QUERIES (
  dti_latest_per_entity AS (
    QUESTION 'What is the Distance-to-Illiquidity per legal entity on the latest reporting date?'
    VERIFIED_AT 1794528000
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT ent.ENTITY_NAME, dti.DTI_DAYS_STRESSED, dti.DTI_DATE_STRESSED, dti.DATA_STATUS FROM __dti AS dti JOIN __ent AS ent ON ent.ENTITY_ID = dti.ENTITY_ID WHERE dti.AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM __dti) ORDER BY dti.DTI_DAYS_STRESSED NULLS LAST'
  ),
  dti_change_vs_previous AS (
    QUESTION 'How did the DtI change between the latest and the previous reporting date?'
    VERIFIED_AT 1794528000
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'WITH d AS (SELECT DISTINCT AS_OF_DATE FROM __dti QUALIFY DENSE_RANK() OVER (ORDER BY AS_OF_DATE DESC) <= 2) SELECT dti.ENTITY_ID, MAX(IFF(dti.AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM d), dti.DTI_DAYS_STRESSED, NULL)) AS DTI_REFERENCE, MAX(IFF(dti.AS_OF_DATE = (SELECT MIN(AS_OF_DATE) FROM d), dti.DTI_DAYS_STRESSED, NULL)) AS DTI_COMPARISON FROM __dti AS dti WHERE dti.AS_OF_DATE IN (SELECT AS_OF_DATE FROM d) GROUP BY dti.ENTITY_ID ORDER BY dti.ENTITY_ID'
  ),
  largest_position_deltas AS (
    QUESTION 'Which LST positions changed most between the latest and the previous reporting date?'
    VERIFIED_AT 1794528000
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'WITH d AS (SELECT DISTINCT FLOW_AS_OF_DATE AS AS_OF_DATE FROM __flows QUALIFY DENSE_RANK() OVER (ORDER BY FLOW_AS_OF_DATE DESC) <= 2) SELECT pos.POSITION_NAME, SUM(IFF(flows.FLOW_AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM d), flows.STRESSED_CHF, 0)) - SUM(IFF(flows.FLOW_AS_OF_DATE = (SELECT MIN(AS_OF_DATE) FROM d), flows.STRESSED_CHF, 0)) AS DELTA_CHF FROM __flows AS flows JOIN __pos AS pos ON pos.POSITION_CODE = flows.POSITION_CODE WHERE flows.FLOW_AS_OF_DATE IN (SELECT AS_OF_DATE FROM d) AND flows.DAYS_FROM_REPORTING_DATE <= 365 AND flows.FLOW_STATUS = ''FINAL'' GROUP BY pos.POSITION_NAME ORDER BY ABS(DELTA_CHF) DESC LIMIT 10'
  ),
  stressed_outflows_30d_by_cluster AS (
    QUESTION 'What are the stressed outflows within 30 days by product cluster on the latest reporting date?'
    VERIFIED_AT 1794528000
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT flows.PRODUCT_CLUSTER, SUM(flows.STRESSED_CHF) AS STRESSED_CHF FROM __flows AS flows WHERE flows.FLOW_AS_OF_DATE = (SELECT MAX(FLOW_AS_OF_DATE) FROM __flows) AND flows.DAYS_FROM_REPORTING_DATE BETWEEN 1 AND 30 AND flows.STRESSED_CHF < 0 GROUP BY flows.PRODUCT_CLUSTER ORDER BY STRESSED_CHF'
  )
);
{% endif %}
