/*
 * tasks_and_dts.sql - suspend or resume the tasks and dynamic tables of the database
 *
 * The objects are read from SHOW TASKS / SHOW DYNAMIC TABLES at run time, so new or
 * renamed objects need no change here.
 *   suspend: root tasks before their children, then the dynamic tables
 *   resume:  dynamic tables, then child tasks before their roots
 *
 * Usage (from the repo root):
 *   all:        snow sql -c <conn> --role CICD -f operation/tasks_and_dts.sql --enable-templating JINJA \
 *                 -D db=AAA_DEV_SYNTHETIC_BANK -D action=suspend
 *   after a DCM deploy (the deploy resumes every task) suspend only the LIQI load tasks:
 *               ... -D action=suspend -D task_like=LIQI_RAW_TK_% -D dt_like=-
 *
 * Variables: action  suspend | resume (required)
 *            task_like  LIKE filter on task names (default %)
 *            dt_like    LIKE filter on dynamic table names (default %; '-' matches none)
 */
USE DATABASE {{ db }};

EXECUTE IMMEDIATE $$
DECLARE
    n INT DEFAULT 0;
    rs RESULTSET DEFAULT (SHOW TASKS LIKE '{{ task_like | default("%") }}' IN DATABASE {{ db }});
    c CURSOR FOR
        SELECT '"' || "database_name" || '"."' || "schema_name" || '"."' || "name" || '"' AS FQN
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
{% if action == "suspend" %}
        WHERE "state" = 'started'
        ORDER BY IFF(ARRAY_SIZE("predecessors") = 0, 0, 1);
{% else %}
        WHERE "state" = 'suspended'
        ORDER BY IFF(ARRAY_SIZE("predecessors") = 0, 1, 0);
{% endif %}
BEGIN
    FOR r IN c DO
        EXECUTE IMMEDIATE 'ALTER TASK ' || r.FQN || ' {{ action | upper }}';
        n := n + 1;
    END FOR;
    RETURN n || ' tasks {{ 'suspended' if action == 'suspend' else 'resumed' }}';
END;
$$;

EXECUTE IMMEDIATE $$
DECLARE
    n INT DEFAULT 0;
    rs RESULTSET DEFAULT (SHOW DYNAMIC TABLES LIKE '{{ dt_like | default("%") }}' IN DATABASE {{ db }});
    c CURSOR FOR
        SELECT '"' || "database_name" || '"."' || "schema_name" || '"."' || "name" || '"' AS FQN
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        WHERE "scheduling_state" = '{{ "ACTIVE" if action == "suspend" else "SUSPENDED" }}';
BEGIN
    FOR r IN c DO
        EXECUTE IMMEDIATE 'ALTER DYNAMIC TABLE ' || r.FQN || ' {{ action | upper }}';
        n := n + 1;
    END FOR;
    RETURN n || ' dynamic tables {{ 'suspended' if action == 'suspend' else 'resumed' }}';
END;
$$;
