USE DATABASE {{ db }};
USE SCHEMA {{ rep_agg }};

-- ============================================================
-- MASTER AGENT - Advanced Multi-Agent Orchestrator
-- Enhanced with: hybrid routing (regex + AI_CLASSIFY), response
-- caching, observability (native + router log), golden regression
-- testing, follow-up context propagation
-- ============================================================

-- ============================================================
-- SECTION 1: Helper UDFs
-- ============================================================

CREATE OR REPLACE FUNCTION BUILD_SUB_AGENT_USER_QUERY(USER_QUERY VARCHAR, PRIOR_CONTEXT VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
SELECT CASE
  WHEN PRIOR_CONTEXT IS NULL OR TRIM(PRIOR_CONTEXT) = '' THEN COALESCE(USER_QUERY, '')
  ELSE 'Context from prior turn: ' || TRIM(PRIOR_CONTEXT) || CHAR(10) || CHAR(10)
    || 'Follow-up question: ' || COALESCE(USER_QUERY, '')
END
$$;

CREATE OR REPLACE FUNCTION PARSE_AGENT_TEXT_ANSWER(CONTENT_ARRAY VARIANT)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
SELECT COALESCE(
  LISTAGG(f.value:text::STRING, '') WITHIN GROUP (ORDER BY f.index),
  ''
)
FROM TABLE(FLATTEN(input => CONTENT_ARRAY)) f
WHERE f.value:type::STRING = 'text'
$$;

CREATE OR REPLACE FUNCTION EXTRACT_SQL_FROM_AGENT_CONTENT(CONTENT_ARRAY VARIANT)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
SELECT COALESCE(
  LISTAGG(sql_text, CHAR(10) || CHAR(10) || '---' || CHAR(10) || CHAR(10)) WITHIN GROUP (ORDER BY min_idx),
  ''
)
FROM (
  SELECT sql_text, MIN(idx) AS min_idx
  FROM (
    SELECT
      f.index AS idx,
      TRIM(COALESCE(
        f.value:tool_use:sql::STRING,
        f.value:tool_result:sql::STRING,
        f.value:tool_use:input:sql::STRING,
        f.value:tool_result:content[0]:json:sql::STRING
      )) AS sql_text
    FROM TABLE(FLATTEN(input => CONTENT_ARRAY)) f
    WHERE f.value:type::STRING IN ('tool_use', 'tool_result')
  )
  WHERE sql_text IS NOT NULL AND sql_text <> ''
  GROUP BY sql_text
)
$$;

CREATE OR REPLACE FUNCTION BUILD_AGENT_RESPONSE(ANSWER_TEXT VARCHAR, SQL_QUERIES VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
SELECT CASE
  WHEN SQL_QUERIES IS NULL OR TRIM(SQL_QUERIES) = '' THEN COALESCE(ANSWER_TEXT, '')
  WHEN CONTAINS(UPPER(COALESCE(ANSWER_TEXT, '')), 'SQL QUERIES USED') THEN COALESCE(ANSWER_TEXT, '')
  ELSE COALESCE(ANSWER_TEXT, '') || CHAR(10) || CHAR(10)
    || '**SQL Queries Used:**' || CHAR(10) || CHAR(10)
    || '```sql' || CHAR(10) || SQL_QUERIES || CHAR(10) || '```'
END
$$;

CREATE OR REPLACE FUNCTION IS_BANK_ROUTE(QUESTION VARCHAR)
RETURNS OBJECT
LANGUAGE SQL
COMMENT = 'Regex-based route classifier for banking sub-agents. Returns {route, match_strength}.'
AS $$
SELECT CASE
  -- Cross-domain signals -> PARTIAL (let AI_CLASSIFY or LLM handle)
  -- Exclude compound single-domain phrases (e.g. "loan portfolio" is loans, not loans+wealth)
  WHEN (
    REGEXP_LIKE(q, '.*(loan|ltv|mortgage|collateral|affordability|lending|borrower).*')
    AND REGEXP_LIKE(q, '.*(lcr|hqla|liquidity|aum|portfolio|headcount|kyc|aml|churn|employee|compliance).*')
    AND NOT REGEXP_LIKE(q, '.*(loan portfolio|loan exposure|lending exposure).*')
  ) OR (
    REGEXP_LIKE(q, '.*(lcr|hqla|liquidity).*')
    AND REGEXP_LIKE(q, '.*(loan|aum|portfolio|headcount|kyc|churn|employee|wealth|compliance).*')
    AND NOT REGEXP_LIKE(q, '.*(liquidity ratio|liquidity position|liquidity coverage).*')
  ) OR (
    REGEXP_LIKE(q, '.*(aum|portfolio|wealth|sharpe|irb).*')
    AND REGEXP_LIKE(q, '.*(loan|lcr|headcount|kyc|aml|churn|employee|compliance).*')
    AND NOT REGEXP_LIKE(q, '.*(portfolio return|portfolio performance|portfolio aum|investment portfolio|loan portfolio|loan exposure).*')
  ) OR REGEXP_LIKE(q, '.*(dashboard|cross-check|compare .* with|executive summary|executive risk).*')
  THEN OBJECT_CONSTRUCT('route', 'MULTI', 'match_strength', 'PARTIAL')

  -- Loan domain
  WHEN REGEXP_LIKE(q, '.*(loan application|ltv ratio|ltv |affordability|collateral|mortgage|lending exposure|borrower|loan portfolio|loan exposure|loan status).*')
  THEN OBJECT_CONSTRUCT('route', 'loan_portfolio', 'match_strength', 'EXACT')

  -- Liquidity domain
  WHEN REGEXP_LIKE(q, '.*(lcr ratio|lcr |hqla|deposit outflow|finma|liquidity ratio|nsfr|cash buffer|liquidity position|liquidity coverage).*')
  THEN OBJECT_CONSTRUCT('route', 'liquidity_risk', 'match_strength', 'EXACT')

  -- Wealth domain
  WHEN REGEXP_LIKE(q, '.*(aum |total aum|portfolio return|sharpe ratio|irb credit|equity p.l|wealth client|investment portfolio|portfolio performance|portfolio aum).*')
  THEN OBJECT_CONSTRUCT('route', 'wealth_advisor', 'match_strength', 'EXACT')

  -- HR domain
  WHEN REGEXP_LIKE(q, '.*(headcount|org structure|tenure|certification|employee |fte |attrition|human resources|staffing|workforce).*')
  THEN OBJECT_CONSTRUCT('route', 'hr_employee', 'match_strength', 'EXACT')

  -- CRM domain
  WHEN REGEXP_LIKE(q, '.*(kyc |aml |pep |sanction|churn risk|churn |segmentation|demographic|transaction anomal|fraud flag|customer 360|customer lifecycle|platinum customer|customer segment|compliance flag|compliance rate).*')
  THEN OBJECT_CONSTRUCT('route', 'crm_customer_360', 'match_strength', 'EXACT')

  ELSE OBJECT_CONSTRUCT('route', 'UNKNOWN', 'match_strength', 'NONE')
END
FROM (SELECT LOWER(COALESCE(QUESTION, '')) AS q) x
$$;

CREATE OR REPLACE FUNCTION CLASSIFY_BANK_ROUTE(QUESTION VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'AI_CLASSIFY wrapper for ambiguous queries. Returns route name or MULTI.'
AS $$
SELECT SPLIT_PART(
  SNOWFLAKE.CORTEX.AI_CLASSIFY(
    QUESTION,
    ARRAY_CONSTRUCT(
      'loan_portfolio: loan applications, LTV ratios, affordability checks, collateral valuation, mortgage lending, borrower risk',
      'liquidity_risk: LCR ratio, HQLA buffer, deposit outflows, FINMA liquidity requirements, NSFR, cash buffers',
      'wealth_advisor: portfolio AUM, investment returns, Sharpe ratio, IRB credit risk, equity P&L, wealth client management',
      'crm_customer_360: KYC/AML compliance, customer demographics, churn risk, segmentation, transaction anomalies, fraud detection',
      'hr_employee: employee headcount, org structure, tenure analysis, certifications, FTE counts, attrition rates',
      'MULTI: cross-domain question requiring multiple banking agents to answer'
    )
  )['label']::VARCHAR,
  ':',
  1
)
$$;

CREATE OR REPLACE FUNCTION NORMALIZE_AGENT_QUERY(QUESTION VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Lowercase, trim, collapse whitespace for stable cache keys'
AS $$
SELECT REGEXP_REPLACE(LOWER(TRIM(COALESCE(QUESTION, ''))), '\\s+', ' ')
$$;

CREATE OR REPLACE FUNCTION AGENT_CACHE_KEY(AGENT_NAME VARCHAR, USER_QUERY VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
SELECT MD5(NORMALIZE_AGENT_QUERY(USER_QUERY) || '|' || COALESCE(AGENT_NAME, ''))
$$;

CREATE OR REPLACE FUNCTION AGENT_CACHE_TTL_HOURS(QUESTION VARCHAR)
RETURNS NUMBER
LANGUAGE SQL
COMMENT = 'Shorter TTL for relative-date questions'
AS $$
SELECT CASE
  WHEN REGEXP_LIKE(LOWER(COALESCE(QUESTION, '')), '.*(last month|last quarter|last week|yesterday|today|this month|this quarter|h1 |h2 |q1 |q2 |q3 |q4 |year-to-date|ytd).*')
  THEN 4
  ELSE 24
END
$$;

CREATE OR REPLACE FUNCTION IS_COMPLEX_QUESTION(QUESTION VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Returns LOW, MEDIUM, or COMPLEX complexity tier'
AS $$
SELECT CASE
  WHEN REGEXP_LIKE(q, '.*(loan|ltv|mortgage|collateral|lending|borrower).*')
    AND REGEXP_LIKE(q, '.*(lcr|hqla|liquidity|aum|portfolio|headcount|kyc|aml|churn|employee|wealth).*')
  THEN 'COMPLEX'
  WHEN REGEXP_LIKE(q, '.*(top \\d+|compare|rank|distribution|funnel|versus|vs\\.|dashboard|executive summary).*')
  THEN 'MEDIUM'
  ELSE 'LOW'
END
FROM (SELECT LOWER(COALESCE(QUESTION, '')) AS q) x
$$;


-- ============================================================
-- SECTION 2: Tables
-- ============================================================

CREATE TABLE IF NOT EXISTS AGENT_RESPONSE_CACHE (
    QUERY_HASH      VARCHAR PRIMARY KEY,
    USER_QUERY      VARCHAR,
    RESPONSE        VARCHAR,
    AGENT_NAME      VARCHAR,
    COMPLEXITY_TIER VARCHAR,
    TTL_HOURS       NUMBER DEFAULT 24,
    CACHED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS ROUTER_LOG (
    LOG_TIMESTAMP    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    USER_QUERY       VARCHAR,
    ROUTE_SELECTED   VARCHAR,
    ROUTE_CONFIDENCE VARCHAR,
    ROUTE_METHOD     VARCHAR,
    CACHE_HIT        BOOLEAN,
    COMPLEXITY_TIER  VARCHAR,
    LATENCY_MS       NUMBER,
    STATUS           VARCHAR
);

CREATE TABLE IF NOT EXISTS GOLDEN_DATASET (
    TEST_ID            NUMBER,
    QUESTION           VARCHAR,
    EXPECTED_ANSWER    VARCHAR,
    VALIDATION_METRIC  VARCHAR,
    MAX_LATENCY_MS     NUMBER DEFAULT 90000,
    EXPECTED_SUB_AGENT VARCHAR,
    TARGET_AGENT       VARCHAR DEFAULT 'MASTER_AGENT',
    ACTIVE             BOOLEAN DEFAULT TRUE,
    IS_DEPLOY_SMOKE    BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS GOLDEN_REGRESSION_RESULTS (
    RUN_ID          NUMBER AUTOINCREMENT,
    RUN_TIMESTAMP   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RUN_MODE        VARCHAR,
    TEST_ID         NUMBER,
    QUESTION        VARCHAR,
    LATENCY_MS      NUMBER,
    STATUS          VARCHAR,
    PASS_FAIL       VARCHAR,
    FAIL_REASON     VARCHAR,
    ANSWER_PREVIEW  VARCHAR,
    ROUTE_SELECTED  VARCHAR
);


-- ============================================================
-- SECTION 3: Procedures
-- ============================================================

CREATE OR REPLACE PROCEDURE RESOLVE_ROUTE(QUESTION VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Three-tier route resolver: regex -> AI_CLASSIFY -> UNCERTAIN. Returns JSON {route, confidence, method}.'
AS
DECLARE
    regex_result VARIANT;
    route_name VARCHAR;
    match_strength VARCHAR;
    classified VARCHAR;
BEGIN
    regex_result := IS_BANK_ROUTE(:QUESTION);
    route_name := :regex_result:route::VARCHAR;
    match_strength := :regex_result:match_strength::VARCHAR;

    IF (:match_strength = 'EXACT') THEN
        RETURN OBJECT_CONSTRUCT('route', :route_name, 'confidence', 'HIGH', 'method', 'REGEX')::VARCHAR;
    END IF;

    -- AI_CLASSIFY for PARTIAL or NONE matches
    classified := CLASSIFY_BANK_ROUTE(:QUESTION);
    IF (:classified != 'MULTI') THEN
        RETURN OBJECT_CONSTRUCT('route', :classified, 'confidence', 'MEDIUM', 'method', 'AI_CLASSIFY')::VARCHAR;
    END IF;

    RETURN OBJECT_CONSTRUCT('route', 'MULTI', 'confidence', 'LOW', 'method', 'UNCERTAIN')::VARCHAR;
END;


CREATE OR REPLACE PROCEDURE CALL_SUB_AGENT(
    AGENT_FQN VARCHAR,
    USER_QUERY VARCHAR,
    PRIOR_CONTEXT VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Generic sub-agent caller via DATA_AGENT_RUN. Parses response and appends SQL.'
AS
DECLARE
    effective_query VARCHAR;
    request_body VARCHAR;
    raw_result VARCHAR;
    parsed VARIANT;
    content VARIANT;
    text_answer VARCHAR DEFAULT '';
    sql_queries VARCHAR DEFAULT NULL;
BEGIN
    effective_query := BUILD_SUB_AGENT_USER_QUERY(:USER_QUERY, :PRIOR_CONTEXT);

    request_body := OBJECT_CONSTRUCT(
        'messages', ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT(
                'role', 'user',
                'content', ARRAY_CONSTRUCT(
                    OBJECT_CONSTRUCT('type', 'text', 'text', :effective_query)
                )
            )
        ),
        'stream', FALSE
    )::VARCHAR;

    raw_result := (SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(:AGENT_FQN, :request_body));

    parsed := PARSE_JSON(:raw_result);
    content := :parsed:content;
    text_answer := PARSE_AGENT_TEXT_ANSWER(:content);

    BEGIN
        sql_queries := EXTRACT_SQL_FROM_AGENT_CONTENT(:content);
    EXCEPTION
        WHEN OTHER THEN
            sql_queries := NULL;
    END;

    IF (:text_answer = '' AND (:sql_queries IS NULL OR :sql_queries = '')) THEN
        RETURN :raw_result;
    END IF;

    RETURN BUILD_AGENT_RESPONSE(:text_answer, :sql_queries);
END;


CREATE OR REPLACE PROCEDURE ASK_MASTER_ROUTED(
    USER_QUERY VARCHAR,
    PRIOR_CONTEXT VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Main dispatcher: cache -> hybrid route (regex + AI_CLASSIFY) -> sub-agent dispatch -> log.'
AS
DECLARE
    effective_query VARCHAR;
    normalized_query VARCHAR;
    cache_key VARCHAR;
    regex_result VARIANT;
    route VARCHAR;
    match_strength VARCHAR;
    confidence VARCHAR;
    method VARCHAR;
    tier VARCHAR;
    agent_fqn VARCHAR;
    start_ts TIMESTAMP_NTZ;
    latency_ms NUMBER;
    response VARCHAR DEFAULT NULL;
    cached_response VARCHAR DEFAULT NULL;
    status VARCHAR DEFAULT 'OK';
    classified VARCHAR;
    ttl_hours NUMBER;
BEGIN
    start_ts := CURRENT_TIMESTAMP();
    effective_query := BUILD_SUB_AGENT_USER_QUERY(:USER_QUERY, :PRIOR_CONTEXT);
    normalized_query := NORMALIZE_AGENT_QUERY(:effective_query);
    cache_key := AGENT_CACHE_KEY('MASTER_AGENT', :normalized_query);
    tier := IS_COMPLEX_QUESTION(:effective_query);

    -- Step 1: Check cache
    SELECT MAX(c.RESPONSE)
    INTO :cached_response
    FROM AGENT_RESPONSE_CACHE c
    WHERE c.QUERY_HASH = :cache_key
      AND DATEADD(HOUR, c.TTL_HOURS, c.CACHED_AT) > CURRENT_TIMESTAMP();

    IF (:cached_response IS NOT NULL) THEN
        latency_ms := TIMESTAMPDIFF(MILLISECOND, :start_ts, CURRENT_TIMESTAMP());
        INSERT INTO ROUTER_LOG (USER_QUERY, ROUTE_SELECTED, ROUTE_CONFIDENCE, ROUTE_METHOD, CACHE_HIT, COMPLEXITY_TIER, LATENCY_MS, STATUS)
        VALUES (:effective_query, 'CACHE', 'HIGH', 'CACHE', TRUE, :tier, :latency_ms, 'CACHE_HIT');
        RETURN :cached_response;
    END IF;

    -- Step 2: Hybrid routing (regex first, then AI_CLASSIFY)
    regex_result := IS_BANK_ROUTE(:effective_query);
    route := :regex_result:route::VARCHAR;
    match_strength := :regex_result:match_strength::VARCHAR;

    IF (:match_strength = 'EXACT') THEN
        confidence := 'HIGH';
        method := 'REGEX';
    ELSE
        classified := CLASSIFY_BANK_ROUTE(:effective_query);
        IF (:classified != 'MULTI') THEN
            route := :classified;
            confidence := 'MEDIUM';
            method := 'AI_CLASSIFY';
        ELSE
            route := 'MULTI';
            confidence := 'LOW';
            method := 'UNCERTAIN';
        END IF;
    END IF;

    -- Step 3: If low confidence, return ROUTE_UNCERTAIN for LLM orchestration
    IF (:confidence = 'LOW') THEN
        latency_ms := TIMESTAMPDIFF(MILLISECOND, :start_ts, CURRENT_TIMESTAMP());
        INSERT INTO ROUTER_LOG (USER_QUERY, ROUTE_SELECTED, ROUTE_CONFIDENCE, ROUTE_METHOD, CACHE_HIT, COMPLEXITY_TIER, LATENCY_MS, STATUS)
        VALUES (:effective_query, :route, :confidence, :method, FALSE, :tier, :latency_ms, 'ROUTE_UNCERTAIN');
        RETURN '[[ROUTE_UNCERTAIN]]' || CHAR(10)
            || 'Suggested route: ' || COALESCE(:route, 'MULTI') || CHAR(10)
            || 'Reason: Question signals are ambiguous -- use full multi-agent orchestration.' || CHAR(10)
            || 'user_query: ' || COALESCE(:USER_QUERY, '') || CHAR(10)
            || 'prior_context: ' || COALESCE(:PRIOR_CONTEXT, '');
    END IF;

    -- Step 4: Map route to agent FQN
    agent_fqn := CASE :route
        WHEN 'loan_portfolio'    THEN '{{ db }}.{{ rep_agg }}.LOAN_PORTFOLIO_AGENT'
        WHEN 'liquidity_risk'    THEN '{{ db }}.{{ rep_agg }}.LIQUIDITY_RISK_AGENT'
        WHEN 'wealth_advisor'    THEN '{{ db }}.{{ rep_agg }}.WEALTH_ADVISOR_AGENT'
        WHEN 'crm_customer_360' THEN '{{ db }}.{{ crm_agg }}.CRM_CUSTOMER_360'
        WHEN 'hr_employee'       THEN '{{ db }}.{{ crm_agg }}.HR_EMPLOYEE_AGENT'
        ELSE NULL
    END;

    IF (:agent_fqn IS NULL) THEN
        latency_ms := TIMESTAMPDIFF(MILLISECOND, :start_ts, CURRENT_TIMESTAMP());
        INSERT INTO ROUTER_LOG (USER_QUERY, ROUTE_SELECTED, ROUTE_CONFIDENCE, ROUTE_METHOD, CACHE_HIT, COMPLEXITY_TIER, LATENCY_MS, STATUS)
        VALUES (:effective_query, :route, :confidence, :method, FALSE, :tier, :latency_ms, 'ERROR');
        RETURN '[[ROUTE_UNCERTAIN]]' || CHAR(10) || 'Reason: No agent mapped for route ' || COALESCE(:route, 'NULL');
    END IF;

    -- Step 5: Call sub-agent
    BEGIN
        CALL CALL_SUB_AGENT(:agent_fqn, :USER_QUERY, :PRIOR_CONTEXT) INTO :response;
    EXCEPTION
        WHEN OTHER THEN
            status := 'ERROR';
            response := 'Error: sub-agent call failed for route ' || COALESCE(:route, 'NULL');
    END;

    latency_ms := TIMESTAMPDIFF(MILLISECOND, :start_ts, CURRENT_TIMESTAMP());

    -- Step 6: Log
    INSERT INTO ROUTER_LOG (USER_QUERY, ROUTE_SELECTED, ROUTE_CONFIDENCE, ROUTE_METHOD, CACHE_HIT, COMPLEXITY_TIER, LATENCY_MS, STATUS)
    VALUES (:effective_query, :route, :confidence, :method, FALSE, :tier, :latency_ms, :status);

    -- Step 7: Cache on success
    IF (:status = 'OK' AND :response IS NOT NULL) THEN
        ttl_hours := AGENT_CACHE_TTL_HOURS(:effective_query);
        MERGE INTO AGENT_RESPONSE_CACHE AS t
        USING (
            SELECT
                :cache_key       AS qh,
                :effective_query AS uq,
                :response        AS resp,
                :route           AS an,
                :tier            AS ct,
                :ttl_hours       AS th
        ) AS s
        ON t.QUERY_HASH = s.qh
        WHEN MATCHED THEN UPDATE SET
            USER_QUERY = s.uq, RESPONSE = s.resp, AGENT_NAME = s.an,
            COMPLEXITY_TIER = s.ct, TTL_HOURS = s.th, CACHED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (QUERY_HASH, USER_QUERY, RESPONSE, AGENT_NAME, COMPLEXITY_TIER, TTL_HOURS)
            VALUES (s.qh, s.uq, s.resp, s.an, s.ct, s.th);
    END IF;

    RETURN :response;
END;


CREATE OR REPLACE PROCEDURE RUN_GOLDEN_REGRESSION(RUN_MODE VARCHAR DEFAULT 'DEPLOY')
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Runs golden dataset tests against ASK_MASTER_ROUTED. Validates route, latency, answer.'
AS
DECLARE
    run_mode_upper VARCHAR;
    test_rs RESULTSET;
    test_id NUMBER;
    question VARCHAR;
    expected_answer VARCHAR;
    validation_metric VARCHAR;
    max_latency NUMBER;
    expected_sub_agent VARCHAR;
    start_time TIMESTAMP_NTZ;
    end_time TIMESTAMP_NTZ;
    latency_ms NUMBER;
    response VARCHAR;
    pass_fail VARCHAR;
    fail_reason VARCHAR DEFAULT NULL;
    pass_count NUMBER DEFAULT 0;
    fail_count NUMBER DEFAULT 0;
    total_count NUMBER DEFAULT 0;
    route_selected VARCHAR;
BEGIN
    run_mode_upper := UPPER(:RUN_MODE);

    test_rs := (
        SELECT TEST_ID, QUESTION, EXPECTED_ANSWER, VALIDATION_METRIC, MAX_LATENCY_MS, EXPECTED_SUB_AGENT
        FROM GOLDEN_DATASET
        WHERE ACTIVE = TRUE
          AND (
            (:run_mode_upper = 'DEPLOY' AND IS_DEPLOY_SMOKE = TRUE)
            OR (:run_mode_upper = 'FULL')
          )
        ORDER BY TEST_ID
    );

    FOR rec IN test_rs DO
        test_id := rec.TEST_ID;
        question := rec.QUESTION;
        expected_answer := rec.EXPECTED_ANSWER;
        validation_metric := rec.VALIDATION_METRIC;
        max_latency := rec.MAX_LATENCY_MS;
        expected_sub_agent := rec.EXPECTED_SUB_AGENT;

        start_time := CURRENT_TIMESTAMP();
        pass_fail := 'PASS';
        fail_reason := NULL;
        response := NULL;
        route_selected := NULL;

        BEGIN
            CALL ASK_MASTER_ROUTED(:question, NULL) INTO :response;

            -- Get the route from the latest ROUTER_LOG entry for this question
            SELECT MAX(ROUTE_SELECTED)
            INTO :route_selected
            FROM ROUTER_LOG
            WHERE USER_QUERY = :question
              AND LOG_TIMESTAMP >= :start_time;
        EXCEPTION
            WHEN OTHER THEN
                response := 'ERROR: agent call failed';
                pass_fail := 'FAIL';
                fail_reason := 'AGENT_CALL_FAILED';
        END;

        end_time := CURRENT_TIMESTAMP();
        latency_ms := TIMESTAMPDIFF(MILLISECOND, :start_time, :end_time);

        -- Validate: non-empty response
        IF (:response IS NULL OR STARTSWITH(:response, 'Error:') OR STARTSWITH(:response, 'ERROR:')) THEN
            pass_fail := 'FAIL';
            fail_reason := COALESCE(:fail_reason, 'EMPTY_OR_ERROR_RESPONSE');
        END IF;

        -- Validate: latency
        IF (:latency_ms > :max_latency) THEN
            pass_fail := 'FAIL';
            fail_reason := COALESCE(:fail_reason, 'LATENCY_EXCEEDED');
        END IF;

        -- Validate: route match (skip for MULTI/ROUTE_UNCERTAIN expected routes)
        IF (
            :expected_sub_agent IS NOT NULL
            AND :expected_sub_agent != 'MULTI'
            AND :route_selected IS NOT NULL
            AND :route_selected != 'CACHE'
            AND :route_selected != :expected_sub_agent
        ) THEN
            pass_fail := 'FAIL';
            fail_reason := COALESCE(:fail_reason, 'ROUTE_MISMATCH');
        END IF;

        -- Validate: expected answer substring
        IF (
            :validation_metric ILIKE '%Exact numeric%'
            AND :expected_answer IS NOT NULL
            AND NOT CONTAINS(COALESCE(:response, ''), TRIM(:expected_answer))
        ) THEN
            pass_fail := 'FAIL';
            fail_reason := COALESCE(:fail_reason, 'ANSWER_MISMATCH');
        END IF;

        IF (:pass_fail = 'PASS') THEN
            pass_count := :pass_count + 1;
        ELSE
            fail_count := :fail_count + 1;
        END IF;
        total_count := :total_count + 1;

        INSERT INTO GOLDEN_REGRESSION_RESULTS (
            RUN_MODE, TEST_ID, QUESTION, LATENCY_MS, STATUS,
            PASS_FAIL, FAIL_REASON, ANSWER_PREVIEW, ROUTE_SELECTED
        ) VALUES (
            :run_mode_upper, :test_id, :question, :latency_ms,
            IFF(:pass_fail = 'PASS', 'SUCCESS', 'FAIL'),
            :pass_fail, :fail_reason, LEFT(COALESCE(:response, ''), 500),
            COALESCE(:route_selected, :expected_sub_agent)
        );
    END FOR;

    RETURN 'Mode=' || :run_mode_upper
        || ' Total=' || :total_count
        || ' Pass=' || :pass_count
        || ' Fail=' || :fail_count;
END;


-- ============================================================
-- SECTION 4: Views
-- ============================================================

CREATE OR REPLACE VIEW ROUTER_METRICS_VW AS
SELECT
    ROUTE_METHOD,
    COMPLEXITY_TIER,
    COUNT(*)                                                                  AS call_count,
    AVG(LATENCY_MS)                                                           AS avg_latency_ms,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY LATENCY_MS)                   AS p50_latency_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY LATENCY_MS)                  AS p95_latency_ms,
    SUM(IFF(STATUS = 'ERROR', 1, 0)) / NULLIF(COUNT(*), 0)                   AS error_rate,
    SUM(IFF(CACHE_HIT, 1, 0)) / NULLIF(COUNT(*), 0)                          AS cache_hit_rate
FROM ROUTER_LOG
WHERE LOG_TIMESTAMP >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
GROUP BY ROUTE_METHOD, COMPLEXITY_TIER;

CREATE OR REPLACE VIEW ROUTER_ALERTS_VW AS
SELECT *
FROM ROUTER_METRICS_VW
WHERE p95_latency_ms > 90000
   OR error_rate > 0.05
   OR (cache_hit_rate < 0.20 AND call_count > 10);

CREATE OR REPLACE VIEW ROUTE_METHOD_VW AS
SELECT
    ROUTE_METHOD,
    ROUTE_SELECTED,
    COUNT(*)                                                     AS call_count,
    AVG(LATENCY_MS)                                               AS avg_latency_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY LATENCY_MS)     AS p95_latency_ms
FROM ROUTER_LOG
WHERE LOG_TIMESTAMP >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY ROUTE_METHOD, ROUTE_SELECTED;

CREATE OR REPLACE VIEW GOLDEN_LATEST_VW AS
SELECT *
FROM GOLDEN_REGRESSION_RESULTS r
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TEST_ID, RUN_MODE
    ORDER BY RUN_TIMESTAMP DESC
) = 1;

CREATE OR REPLACE VIEW GOLDEN_FAILING_VW AS
SELECT
    r.RUN_TIMESTAMP, r.RUN_MODE, r.TEST_ID,
    g.QUESTION, g.EXPECTED_SUB_AGENT, g.MAX_LATENCY_MS,
    r.LATENCY_MS, r.STATUS, r.PASS_FAIL, r.FAIL_REASON,
    r.ANSWER_PREVIEW, r.ROUTE_SELECTED
FROM GOLDEN_LATEST_VW r
JOIN GOLDEN_DATASET g ON g.TEST_ID = r.TEST_ID
WHERE r.PASS_FAIL = 'FAIL';


-- ============================================================
-- SECTION 5: Golden Dataset Seed
-- ============================================================

MERGE INTO GOLDEN_DATASET AS t
USING (
    SELECT column1 AS TEST_ID, column2 AS QUESTION, column3 AS EXPECTED_ANSWER,
           column4 AS VALIDATION_METRIC, column5 AS MAX_LATENCY_MS,
           column6 AS EXPECTED_SUB_AGENT, column7 AS IS_DEPLOY_SMOKE
    FROM VALUES
        (1,  'What is our current LCR ratio?',                                           NULL, 'Route match only', 30000, 'liquidity_risk',    TRUE),
        (2,  'Show me total HQLA buffer and deposit outflow breakdown',                  NULL, 'Route match only', 30000, 'liquidity_risk',    TRUE),
        (3,  'What is the total loan portfolio exposure by country?',                    NULL, 'Route match only', 30000, 'loan_portfolio',    TRUE),
        (4,  'List pending mortgage applications with LTV above 80%',                    NULL, 'Route match only', 60000, 'loan_portfolio',    FALSE),
        (5,  'What is the total AUM across all advisors?',                               NULL, 'Route match only', 30000, 'wealth_advisor',    TRUE),
        (6,  'Show portfolio returns and Sharpe ratios by investment strategy',          NULL, 'Route match only', 60000, 'wealth_advisor',    FALSE),
        (7,  'Show me headcount by region',                                              NULL, 'Route match only', 30000, 'hr_employee',       TRUE),
        (8,  'What are the current KYC compliance rates?',                               NULL, 'Route match only', 30000, 'crm_customer_360', TRUE),
        (9,  'Find PLATINUM customers with anomalous transactions',                      NULL, 'Route match only', 60000, 'crm_customer_360', FALSE),
        (10, 'Which high-AUM wealth clients also have compliance flags?',                NULL, 'Route match only', 90000, 'MULTI',             TRUE),
        (11, 'Give me an executive risk dashboard: LCR ratio, total loan exposure, and top 5 wealth clients by AUM', NULL, 'Route match only', 120000, 'MULTI', FALSE),
        (12, 'Cross-check: do any high churn-risk customers have pending loan applications?', NULL, 'Route match only', 90000, 'MULTI',        FALSE)
) AS s
ON t.TEST_ID = s.TEST_ID
WHEN NOT MATCHED THEN INSERT (TEST_ID, QUESTION, EXPECTED_ANSWER, VALIDATION_METRIC, MAX_LATENCY_MS, EXPECTED_SUB_AGENT, IS_DEPLOY_SMOKE)
    VALUES (s.TEST_ID, s.QUESTION, s.EXPECTED_ANSWER, s.VALIDATION_METRIC, s.MAX_LATENCY_MS, s.EXPECTED_SUB_AGENT, s.IS_DEPLOY_SMOKE);


-- ============================================================
-- SECTION 6: Agent DDL (Enhanced)
-- ============================================================

DROP AGENT IF EXISTS MASTER_AGENT;

CREATE OR REPLACE AGENT MASTER_AGENT
  COMMENT = 'Advanced multi-agent orchestrator with hybrid routing (regex + AI_CLASSIFY), response caching, parallel/DAG execution, metric-level guardrails, authoritative ownership, and execution audit trail.'
  PROFILE = '{"display_name": "Master Agent", "avatar": "SparklesAgentIcon", "color": "#1A237E"}'
  FROM SPECIFICATION
  $$

  # --- Cortex Agent spec has 3 distinct keys named "orchestration" ---
  # models.orchestration  = LLM model selection (auto picks best available)
  # orchestration         = runtime budget + tool-access config
  # instructions.orchestration = natural-language routing/planning instructions
  # These are NOT duplicates; each serves a different purpose in the schema.

  models:
    orchestration: auto

  orchestration:
    budget:
      seconds: 120
      tokens: 32000
    tool_not_accessible: accept

  instructions:
    sample_questions:
      - question: "What is our current LCR ratio?"
      - question: "Show me total HQLA buffer and deposit outflow breakdown"
      - question: "What is the total loan portfolio exposure by country?"
      - question: "List pending mortgage applications with LTV above 80%"
      - question: "What is the total AUM across all advisors?"
      - question: "Show portfolio returns and Sharpe ratios by investment strategy"
      - question: "Show me headcount by region"
      - question: "What are the current KYC compliance rates?"
      - question: "Find PLATINUM customers with anomalous transactions"
      - question: "Which high-AUM wealth clients also have compliance flags?"
      - question: "Give me an executive risk dashboard: LCR ratio, total loan exposure, and top 5 wealth clients by AUM"
      - question: "Cross-check: do any high churn-risk customers have pending loan applications?"

    response: |
      Respond in the same language the user writes in. Structure every response as follows:

      1. EXECUTION SUMMARY (concise, user-facing)
         A brief one-line summary of which domains were queried and how:
         "Queried liquidity, lending, and wealth domains in parallel."
         Do NOT expose internal agent names, tool IDs, intermediate prompts, or row-level data in the user-facing response.
         Do NOT expose [[ROUTE_UNCERTAIN]] markers to the user.

      2. FINAL ANSWER / SYNTHESIS
         - Direct answer with key metrics
         - Analysis and context (trends, comparisons, concentrations)
         - Cross-domain insights when relevant (e.g., flag if a high-LTV borrower also has compliance issues)

         Currency: use CHF, EUR, GBP, or USD as appropriate to the metric.
         Regulatory context: include ONLY when the metric has a regulatory threshold.
           - Swiss entities / CHF liquidity -> FINMA Circular 2015/2, Basel III
           - UK entities / GBP lending -> FCA, PRA
           - German entities / EUR lending -> BaFin
           Do NOT append all regulators to every metric.

      3. METRIC-LEVEL CONFIDENCE (per metric, not global)
         For each key metric in the response, classify as one of:
           - Verified: returned by the authoritative agent, internally consistent
           - Partially verified: returned by a non-authoritative agent or with caveats (state the caveat)
           - Conflicting: two agents returned different values (report BOTH values, state the discrepancy and likely cause)
           - Unavailable: no agent could provide this metric (state which agent was tried)
         Do NOT discard all verified results because one metric is conflicting. Present verified results normally and qualify only the affected metrics.

    orchestration: |
      You are an advanced multi-agent orchestrator. Follow this framework for every query:

      STEP 0 - FOLLOW-UP / CHAT CONTEXT (mandatory before any tool call)
        Sub-agents do NOT see chat history unless YOU pass prior_context + user_query.
        On follow-ups ("these", "those", "how many of", references to prior results):
          1. Read prior USER question and YOUR prior answer in this chat.
          2. Fill prior_context with ALL active filters: dates, countries, customer segments, metric names, specific values from the prior answer.
          3. Fill user_query with only the new follow-up ask.
          4. Never call a tool with empty prior_context on a follow-up.

      STEP 0.5 - TOOL CALL FLOW
        For single-domain questions: Call ASK_MASTER_ROUTED first with user_query (and prior_context if follow-up).
          - If it returns a normal answer (no [[ROUTE_UNCERTAIN]] prefix), use it directly as the fast path. Present it per the response format above.
          - If it returns [[ROUTE_UNCERTAIN]], proceed to full decomposition (Steps 1-6 below).
        For obviously multi-domain questions (dashboard, cross-check, executive summary, "compare X with Y across domains"):
          Skip ASK_MASTER_ROUTED and go directly to parallel agent_toolset calls (Steps 1-6).

      STEP 1 - TASK DECOMPOSITION
      - Parse the user query into logical sub-tasks
      - Determine the minimal set of sub-agents required
      - Map out the dependency graph (which tasks are independent vs dependent)

      STEP 2 - EXECUTION PLANNING
      - Independent sub-tasks: call sub-agents in PARALLEL to minimize latency
      - Dependent sub-tasks: chain SEQUENTIALLY (DAG pipeline), feeding output from earlier agents as context to later ones
      - Single-domain queries: call exactly one sub-agent

      STEP 3 - AUTHORITATIVE AGENT OWNERSHIP
      Each metric has exactly ONE authoritative agent. Other agents may provide context but must not override the authoritative source.

      Metric / Capability               | Authoritative Agent    | Context Agent(s)
      ----------------------------------|------------------------|------------------
      Loan application status, LTV,     | loan_portfolio         | crm_customer_360
        affordability, collateral       |                        |
      Customer KYC/AML status, PEP,     | crm_customer_360       | loan_portfolio
        sanctions screening, lifecycle  |                        |
      Customer demographics, addresses, | crm_customer_360       | -
        segmentation, churn             |                        |
      Advisor performance, capacity,    | crm_customer_360       | wealth_advisor
        client assignments              |                        |
      LCR, HQLA, deposit outflows,      | liquidity_risk         | -
        FINMA liquidity ratios          |                        |
      Portfolio AUM, returns, Sharpe,   | wealth_advisor         | crm_customer_360
        IRB credit risk, equity P&L     |                        |
      Employee headcount, org structure,| hr_employee            | crm_customer_360
        tenure, certifications          |                        |
      Transaction anomaly detection,    | crm_customer_360       | loan_portfolio
        fraud flags                     |                        |

      When a metric appears in responses from multiple agents, the AUTHORITATIVE agents value takes precedence. Note the discrepancy only if the difference exceeds 5%.

      STEP 4 - CROSS-DOMAIN JOIN RULES
      When synthesizing across agents, use these conventions:
      - Customer join key: CUSTOMER_ID (format CUST_XXXXX), consistent across all agents
      - Advisor join key: EMPLOYEE_ID (format EMP_XXXXX)
      - Reporting window: use the most recent available date from each source; note if sources have different as-of dates
      - Currency: all agents report in CHF unless stated otherwise; do not mix currency bases
      - Grain: distinguish between customer-level counts, account-level counts, and relationship-level counts; state the grain explicitly when comparing cross-domain numbers
      - Scope: all data is single-entity (Synthetic Retail Bank); no group consolidation applies

      STEP 5 - CONFLICT RESOLUTION (metric-level, not global)
      - If two agents return different values for the SAME metric:
        a) Use the authoritative agents value as primary
        b) Note the secondary agents value and explain the likely cause (different grain, different population, different as-of date)
        c) Do NOT discard the entire response; present all verified metrics normally
      - If the authoritative agent itself returns internally inconsistent data:
        a) Flag the specific inconsistency
        b) Present the raw values without correction
        c) Recommend validation of the upstream data source

      STEP 6 - OVERLAP ROUTING
      - "Customer risk + loan" -> loan_portfolio (authoritative for loans) + crm_customer_360 (authoritative for customer risk)
      - "Customer risk + portfolio" -> wealth_advisor + crm_customer_360
      - "Advisor + portfolio" -> wealth_advisor (has advisor relationships built in)
      - "Advisor performance + client counts" -> crm_customer_360 (authoritative for advisor metrics)
      - "Employee + advisor capacity" -> hr_employee + crm_customer_360
      - "Regulatory compliance + loans" -> loan_portfolio
      - "Regulatory compliance + liquidity" -> liquidity_risk
      - Cross-domain dashboards -> call all relevant authoritative agents in PARALLEL

    system: |
      You are the MASTER AGENT, an advanced multi-agent orchestrator for the Synthetic Retail Bank operating within Snowflake. You receive complex user queries, decompose them into logical sub-tasks, execute appropriate sub-agents (in parallel or DAG-style sequential pipelines), synthesize their outputs, and present a single, coherent final response.

      CRITICAL GUARDRAILS:
      1. NO HALLUCINATION: Base your final analysis strictly on the verified outputs received from invoked sub-agents. Never extrapolate beyond what the data shows. Never infer missing details or invent facts.
      2. METRIC-LEVEL CONFIDENCE: Do NOT apply a global pass/fail to the entire response. Instead, classify each metric independently as verified, partially verified, conflicting, or unavailable. Present verified results normally and qualify only affected metrics. This ensures that one problematic data point does not invalidate an otherwise complete answer.
      3. AUTHORITATIVE SOURCES: When the same metric appears from multiple agents, defer to the authoritative agent as defined in the ownership table. Note discrepancies only when material (>5% difference).
      4. EXECUTION SUMMARY: Every response includes a concise user-facing summary of which domains were queried. Internal agent names, tool IDs, and intermediate prompts stay in thinking tokens only -- do not expose them to the end user.
      5. NEVER refuse a question by saying you lack data without first trying the most relevant sub-agent. If partial data is available, query it and explain scope.
      6. Respond in the same language the user writes in. Use appropriate currency formatting. Apply regulatory context conditionally:
         - Swiss / CHF metrics -> FINMA, Basel III
         - UK / GBP metrics -> FCA, PRA
         - German / EUR metrics -> BaFin
         Do not append all regulators to every metric.
      7. SECURITY: Do not send customer PII, account numbers, or internal identifiers to the web_search tool. Minimize sensitive data in code_execution outputs. Sub-agent results may contain masked PII -- present masked values as-is, do not attempt to unmask.
      8. ROUTE_UNCERTAIN: When ASK_MASTER_ROUTED returns [[ROUTE_UNCERTAIN]], treat it as "the fast router could not handle this" and proceed with full multi-agent orchestration using the agent_toolset tools. Never show [[ROUTE_UNCERTAIN]] to the user.

  tools:
    - tool_spec:
        type: generic
        name: ASK_MASTER_ROUTED
        description: "Primary fast path: cache lookup, hybrid routing (regex + AI_CLASSIFY), direct sub-agent dispatch. Call first on single-domain questions. Returns [[ROUTE_UNCERTAIN]] when the question needs full multi-agent orchestration."
        input_schema:
          type: object
          properties:
            user_query:
              type: string
              description: "The user's question. On follow-ups, include only the new question (pass prior context separately)."
            prior_context:
              type: string
              description: "Summary of prior turn with all active filters (dates, segments, metrics). Empty string if standalone question."
          required: [user_query]
    - tool_spec:
        type: web_search
        name: Web Search
    - tool_spec:
        type: code_execution
        name: code_execution
    - tool_spec:
        type: agent_toolset
        name: loan_portfolio
    - tool_spec:
        type: agent_toolset
        name: liquidity_risk
    - tool_spec:
        type: agent_toolset
        name: wealth_advisor
    - tool_spec:
        type: agent_toolset
        name: crm_customer_360
    - tool_spec:
        type: agent_toolset
        name: hr_employee

  tool_resources:
    ASK_MASTER_ROUTED:
      identifier: "{{ db }}.{{ rep_agg }}.ASK_MASTER_ROUTED"
      name: "ASK_MASTER_ROUTED(VARCHAR, VARCHAR)"
      type: "procedure"
      execution_environment:
        type: "warehouse"
        warehouse: "{{ wh }}"
        query_timeout: 120
    loan_portfolio:
      agent_name: {{ db }}.{{ rep_agg }}.LOAN_PORTFOLIO_AGENT
    liquidity_risk:
      agent_name: {{ db }}.{{ rep_agg }}.LIQUIDITY_RISK_AGENT
    wealth_advisor:
      agent_name: {{ db }}.{{ rep_agg }}.WEALTH_ADVISOR_AGENT
    crm_customer_360:
      agent_name: {{ db }}.{{ crm_agg }}.CRM_CUSTOMER_360
    hr_employee:
      agent_name: {{ db }}.{{ crm_agg }}.HR_EMPLOYEE_AGENT
  $$;

GRANT USAGE ON AGENT {{ db }}.{{ rep_agg }}.MASTER_AGENT TO ROLE ACCOUNTADMIN;
GRANT USAGE ON AGENT {{ db }}.{{ rep_agg }}.MASTER_AGENT TO ROLE PUBLIC;

ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT
  ADD AGENT {{ db }}.{{ rep_agg }}.MASTER_AGENT;

SELECT 'MASTER_AGENT created successfully! Enhanced with hybrid routing, caching, observability, and golden regression.' AS STATUS;
