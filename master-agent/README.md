# Master Agent

Advanced multi-agent orchestrator for the Synthetic Retail Bank with hybrid routing, response caching, observability, and golden regression testing.

## Why

Every user question -- no matter how simple -- used to go through a full LLM orchestration cycle: parse the prompt, reason about which sub-agent to call, formulate the tool call, wait for the response, synthesize. For single-domain questions like "What is our current LCR ratio?" this reasoning step is pure overhead.

The enhanced Master Agent adds a **pre-screening router** that short-circuits simple queries through a deterministic fast path, while preserving the full LLM orchestration for complex multi-domain questions. The result: ~85-90% of queries answered faster and cheaper, with no loss in quality for complex queries.

Key patterns adopted from a customer production implementation (AIM_AGENT_V3), improved with Snowflake-native features:

| Pattern | Customer (AIM_AGENT_V3) | Master Agent (enhanced) |
|---------|------------------------|------------------------|
| Routing | Regex-only | Hybrid: regex + AI_CLASSIFY + LLM |
| Caching | Custom MD5 cache | Same pattern, TTL-aware |
| Observability | Full custom log table | Native AI_OBSERVABILITY_EVENTS + minimal router log |
| Regression testing | Golden dataset + procedure | Same pattern |
| Sub-agent dispatch | 5 separate wrappers | 1 generic CALL_SUB_AGENT |
| Orchestration depth | 2-tool cap, sequential | Unlimited parallel/DAG execution |

## Architecture

```
                         User Query
                             |
                    +--------v--------+
                    |  MASTER_AGENT   |
                    |  (Cortex Agent) |
                    +--------+--------+
                             |
               Step 0.5: Single-domain?
                      /            \
                    YES             NO (multi-domain)
                     |               |
            +--------v---------+     +--------v---------+
            | ASK_MASTER_ROUTED|     | Parallel/DAG     |
            | (generic tool)   |     | agent_toolset    |
            +--------+---------+     | calls (Steps 1-6)|
                     |               +---------+--------+
         +-----------+-----------+            |
         |           |           |     loan_portfolio
   Cache hit?   IS_BANK_ROUTE    |     liquidity_risk
   (641ms)     (regex, ~1ms)     |     wealth_advisor
         |           |           |     crm_customer_360
         |     EXACT? --------+  |     hr_employee
         |       YES    NO    |  |
         |        |      |    |  |
         |        | CLASSIFY_BANK_ROUTE
         |        | (AI_CLASSIFY, ~300ms)
         |        |      |
         |        |  Single domain?
         |        |   YES    NO
         |        |    |      |
         |        v    v      v
         |   CALL_SUB_AGENT  [[ROUTE_UNCERTAIN]]
         |   (DATA_AGENT_RUN) --> falls back to
         |        |               full LLM orchestration
         v        v
     ROUTER_LOG + AGENT_RESPONSE_CACHE
```

**Expected query distribution:**

| Tier | ~% of queries | Mechanism | Latency | Cost |
|------|:---:|-----------|---------|------|
| Cache hit | varies | MD5 lookup | ~600ms | 0 |
| Regex fast path | 60-70% | IS_BANK_ROUTE | ~1-5ms + sub-agent | 0 routing overhead |
| AI_CLASSIFY | 20-25% | SNOWFLAKE.CORTEX.AI_CLASSIFY | ~300ms + sub-agent | Minimal |
| Full LLM orchestration | 10-15% | Parallel agent_toolset | 2-5s+ | Full token budget |

## Files

| File | Purpose |
|------|---------|
| `post_deploy_master-agent.sql` | All objects: 10 UDFs, 4 tables, 4 procedures, 5 views, 12 golden test cases, agent DDL |
| `master-agent/Snowflake_codes/` | Customer reference implementation (AIM_AGENT_V3) used as design input |
| `deploy.sh` | Deployment script: `./deploy.sh DEV` chains DCM deploy + post_deploy + master-agent |

## Deployed Objects

**UDFs:** IS_BANK_ROUTE, CLASSIFY_BANK_ROUTE, NORMALIZE_AGENT_QUERY, AGENT_CACHE_KEY, AGENT_CACHE_TTL_HOURS, IS_COMPLEX_QUESTION, BUILD_SUB_AGENT_USER_QUERY, PARSE_AGENT_TEXT_ANSWER, EXTRACT_SQL_FROM_AGENT_CONTENT, BUILD_AGENT_RESPONSE

**Tables:** AGENT_RESPONSE_CACHE, ROUTER_LOG, GOLDEN_DATASET, GOLDEN_REGRESSION_RESULTS

**Procedures:** RESOLVE_ROUTE, CALL_SUB_AGENT, ASK_MASTER_ROUTED, RUN_GOLDEN_REGRESSION

**Views:** ROUTER_METRICS_VW, ROUTER_ALERTS_VW, ROUTE_METHOD_VW, GOLDEN_LATEST_VW, GOLDEN_FAILING_VW

## Observability

**Agent telemetry** (latency, tokens, tool execution, errors, user feedback) comes from Snowflake's native system -- no custom logging needed:

```sql
SELECT * FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
    'AAA_DEV_SYNTHETIC_BANK', 'REP_AGG_V001', 'MASTER_AGENT', 'CORTEX AGENT'
));
```

**Router telemetry** (cache hits, route method, confidence) is tracked in ROUTER_LOG with three views:

```sql
SELECT * FROM ROUTER_METRICS_VW;   -- 24h rolling: latency, error rate, cache hit rate
SELECT * FROM ROUTER_ALERTS_VW;    -- flags: p95 > 90s, error > 5%, low cache hits
SELECT * FROM ROUTE_METHOD_VW;     -- 7-day breakdown: REGEX vs AI_CLASSIFY vs UNCERTAIN
```

## Testing

Run the golden regression suite after deployment:

```sql
-- Smoke tests (8 deploy-critical questions)
CALL RUN_GOLDEN_REGRESSION('DEPLOY');

-- Full suite (all 12 questions)
CALL RUN_GOLDEN_REGRESSION('FULL');

-- Check failures
SELECT * FROM GOLDEN_FAILING_VW;
```
