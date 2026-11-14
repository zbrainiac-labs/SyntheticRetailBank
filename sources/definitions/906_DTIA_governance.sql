/*
 * 906_DTIA_governance.sql
 * DtI / Stichtagsvergleich (UC2) governance: roles, grants, entity row access.
 *
 *   DTI_READ_ONLY  read DtI pages for the mapped entities only (no export, no edits)
 *   DTI_LRC_READ   liquidity risk control: all entities, export      (inherits READ_ONLY)
 *   DTI_OWNER      DtI owner: corrections and limits editor           (inherits LRC_READ)
 *
 * The row access policy is DEFINEd here; DCM cannot attach policies yet, so
 * post_deploy.sql attaches it to the DtI tables (ON ENTITY_ID).
 */

DEFINE ROLE DTI_READ_ONLY
    COMMENT = 'UC2 DtI read-only user: DtI pages for the entities mapped in LIQI_RAW_TB_DTI_ENTITY_ACCESS, no export, no edits.';

DEFINE ROLE DTI_LRC_READ
    COMMENT = 'UC2 DtI liquidity risk control: read all entities and export. Inherits DTI_READ_ONLY.';

DEFINE ROLE DTI_OWNER
    COMMENT = 'UC2 DtI owner: all entities, export, maintains manual corrections and commenting thresholds. Inherits DTI_LRC_READ.';

DEFINE ROLE DTI_WAREHOUSE_USER
    COMMENT = 'Warehouse access for the UC2 DtI roles (warehouse grants need an account role).';

GRANT ROLE DTI_READ_ONLY TO ROLE DTI_LRC_READ;
GRANT ROLE DTI_LRC_READ TO ROLE DTI_OWNER;
GRANT ROLE DTI_OWNER TO ROLE CICD;
GRANT ROLE DTI_WAREHOUSE_USER TO ROLE DTI_READ_ONLY;

GRANT USAGE ON WAREHOUSE {{ wh }} TO ROLE DTI_WAREHOUSE_USER;

GRANT USAGE ON DATABASE {{ db }} TO ROLE DTI_READ_ONLY;
GRANT USAGE ON SCHEMA {{ db }}.{{ rep_raw }} TO ROLE DTI_READ_ONLY;
GRANT USAGE ON SCHEMA {{ db }}.{{ rep_agg }} TO ROLE DTI_READ_ONLY;

-- Read access: engine output, comparison function and the reference data the DtI pages show
{% for t in ['REPP_AGG_DT_DTI_CASHFLOWS', 'REPP_AGG_DT_DTI_PROJECTION', 'REPP_AGG_DT_DTI_SUMMARY'] %}
GRANT SELECT ON DYNAMIC TABLE {{ db }}.{{ rep_agg }}.{{ t }} TO ROLE DTI_READ_ONLY;
{% endfor %}
GRANT SELECT ON VIEW {{ db }}.{{ rep_agg }}.REPP_AGG_VW_DTI_CALENDAR TO ROLE DTI_READ_ONLY;
{% if semantic_views == 'true' %}
GRANT SELECT ON SEMANTIC VIEW {{ db }}.{{ rep_agg }}.DTIS_SV_DTI_COMPARISON TO ROLE DTI_READ_ONLY;
{% endif %}
GRANT USAGE ON FUNCTION {{ db }}.{{ rep_agg }}.REPP_AGG_FN_DTI_COMPARE(DATE, DATE, ARRAY, VARCHAR, VARCHAR, BOOLEAN) TO ROLE DTI_READ_ONLY;
{% for t in ['LIQI_RAW_TB_DTI_ENTITIES', 'LIQI_RAW_TB_DTI_APPLICATIONS', 'LIQI_RAW_TB_DTI_BUCKETS', 'LIQI_RAW_TB_DTI_HOLIDAYS',
             'LIQI_RAW_TB_DTI_LST_POSITIONS', 'LIQI_RAW_TB_DTI_LST_ASSIGNMENT', 'LIQI_RAW_TB_DTI_PRODUCT_CLUSTERS',
             'LIQI_RAW_TB_DTI_WLK_CLUSTERS', 'LIQI_RAW_TB_DTI_LIMITS', 'LIQI_RAW_TB_DTI_CORRECTIONS', 'LIQI_RAW_TB_DTI_EVENTS',
             'LIQI_RAW_TB_DTI_SECURITIES', 'LIQI_RAW_TB_DTI_SECURED_FUNDING', 'LIQI_RAW_TB_GENERATION_RUN'] %}
GRANT SELECT ON TABLE {{ db }}.{{ rep_raw }}.{{ t }} TO ROLE DTI_READ_ONLY;
{% endfor %}

-- Owner: manual inputs (UC2 6.3 corrections, 6.1/6.10 thresholds) and an on-demand refresh after an edit
GRANT INSERT, UPDATE, DELETE ON TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_CORRECTIONS TO ROLE DTI_OWNER;
GRANT INSERT, UPDATE, DELETE ON TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_LIMITS TO ROLE DTI_OWNER;
{% for t in ['REPP_AGG_DT_DTI_CASHFLOWS', 'REPP_AGG_DT_DTI_PROJECTION', 'REPP_AGG_DT_DTI_SUMMARY'] %}
GRANT OPERATE ON DYNAMIC TABLE {{ db }}.{{ rep_agg }}.{{ t }} TO ROLE DTI_OWNER;
{% endfor %}

-- ============================================================
-- ENTITY ROW ACCESS
-- ============================================================

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_ENTITY_ACCESS (
    ROLE_NAME VARCHAR(100) NOT NULL COMMENT 'Account role that may see the entity (evaluated with IS_ROLE_IN_SESSION, so role hierarchy applies).',
    ENTITY_ID VARCHAR(10) NOT NULL COMMENT 'Legal entity (LE_01..LE_09) visible to the role.',
    PRIMARY KEY (ROLE_NAME, ENTITY_ID)
)
COMMENT = 'Entity entitlements for the DtI row access policy REPP_AGG_RAP_DTI_ENTITY (roles below DTI_LRC_READ). Seeded by post_deploy.sql; maintained by the security administration.';

DEFINE ROW ACCESS POLICY {{ db }}.{{ rep_agg }}.REPP_AGG_RAP_DTI_ENTITY
    AS (ENTITY_VAL VARCHAR)
    RETURNS BOOLEAN ->
    IS_ROLE_IN_SESSION('DTI_LRC_READ')
    OR IS_ROLE_IN_SESSION('ACCOUNTADMIN')
    OR EXISTS (
        SELECT 1 FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_ENTITY_ACCESS a
        WHERE a.ENTITY_ID = ENTITY_VAL AND IS_ROLE_IN_SESSION(a.ROLE_NAME)
    )
    COMMENT = 'UC2 DtI entity filter on ENTITY_ID: DTI_LRC_READ and above (incl. DTI_OWNER, CICD) see all entities; other roles only the entities mapped in LIQI_RAW_TB_DTI_ENTITY_ACCESS. Attached in post_deploy.sql.';
