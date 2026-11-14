import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import snowflake from "snowflake-sdk";
import { parse as parseToml } from "smol-toml";

/**
 * Snowflake access for the bank app.
 *
 * Two runtimes, one interface:
 * - Local (Docker Desktop / npm run dev): reads a named connection from the
 *   Snowflake CLI config (~/.snowflake, mounted read-only in Docker) and authenticates with its programmatic access token.
 * - SPCS: uses the OAuth token Snowflake injects at /snowflake/session/token.
 *   For caller's rights the service token is combined with the
 *   Sf-Context-Current-User-Token header of the incoming request.
 *
 * Secrets are read at runtime only; they are never logged or returned.
 */

snowflake.configure({ logLevel: "ERROR" });

const SPCS_TOKEN_FILE = "/snowflake/session/token";

export type Row = Record<string, unknown>;
export type Bind = string | number | boolean | null;

export interface QueryResult<T extends Row = Row> {
  rows: T[];
  queryId: string;
}

type ConnOptions = snowflake.ConnectionOptions;

export function isSpcs(): boolean {
  return fs.existsSync(SPCS_TOKEN_FILE);
}

/** Session context shared by both runtimes; non-empty env vars override the CLI config. */
function sessionContext(cfg: Record<string, unknown> = {}) {
  return {
    warehouse: process.env.SNOWFLAKE_WAREHOUSE || (cfg.warehouse as string | undefined),
    database: process.env.SNOWFLAKE_DATABASE || (cfg.database as string | undefined) || "AAA_DEV_SYNTHETIC_BANK",
    schema: process.env.SNOWFLAKE_SCHEMA || (cfg.schema as string | undefined),
    role: process.env.SNOWFLAKE_ROLE || (cfg.role as string | undefined),
  };
}

/**
 * Resolve a named connection the same way the Snowflake CLI does:
 * connections.toml (top-level tables) wins over config.toml ([connections.*]).
 * SNOWFLAKE_HOME overrides ~/.snowflake (Docker mounts it read-only).
 */
function readCliConnection(): Record<string, unknown> {
  const dir = process.env.SNOWFLAKE_HOME ?? path.join(os.homedir(), ".snowflake");
  const name = process.env.SNOWFLAKE_CONNECTION ?? "zs28104-svc_mdaeppen";
  const candidates: [string, (doc: Record<string, unknown>) => unknown][] = [
    [path.join(dir, "connections.toml"), (doc) => doc[name]],
    [path.join(dir, "config.toml"), (doc) => (doc.connections as Record<string, unknown> | undefined)?.[name]],
  ];
  for (const [file, pick] of candidates) {
    if (!fs.existsSync(/*turbopackIgnore: true*/ file)) continue;
    const conn = pick(parseToml(fs.readFileSync(/*turbopackIgnore: true*/ file, "utf8")) as Record<string, unknown>);
    if (conn) return conn as Record<string, unknown>;
  }
  throw new Error(`Connection "${name}" not found in ${dir}/connections.toml or config.toml`);
}

function localOptions(): ConnOptions {
  const cfg = readCliConnection();
  const token = (cfg.token ?? cfg.password) as string | undefined;
  if (!token) throw new Error("Selected connection has no programmatic access token");
  return {
    account: cfg.account as string,
    username: cfg.user as string,
    authenticator: "PROGRAMMATIC_ACCESS_TOKEN",
    token,
    ...sessionContext(cfg),
  } as ConnOptions;
}

function spcsOptions(callerToken?: string): ConnOptions {
  const serviceToken = fs.readFileSync(SPCS_TOKEN_FILE, "utf8").trim();
  return {
    accessUrl: `https://${process.env.SNOWFLAKE_HOST}`,
    account: process.env.SNOWFLAKE_ACCOUNT as string,
    host: process.env.SNOWFLAKE_HOST,
    authenticator: "OAUTH",
    token: callerToken ? `${serviceToken}.${callerToken}` : serviceToken,
    ...sessionContext(),
  } as ConnOptions;
}

function connect(options: ConnOptions): Promise<snowflake.Connection> {
  return new Promise((resolve, reject) => {
    snowflake.createConnection(options).connect((err, conn) => (err ? reject(err) : resolve(conn)));
  });
}

function execute<T extends Row>(
  conn: snowflake.Connection,
  sqlText: string,
  binds: Bind[],
): Promise<QueryResult<T>> {
  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText,
      binds: binds as snowflake.Binds,
      complete: (err, stmt, rows) =>
        err ? reject(err) : resolve({ rows: (rows ?? []) as T[], queryId: stmt.getQueryId() }),
    });
  });
}

/**
 * Call a Snowflake REST API (e.g. Cortex Agents `:run`) as the app identity.
 * Local: PAT of the CLI connection; SPCS: the injected OAuth token. The token
 * never leaves the server; the caller gets the raw Response (SSE for agents).
 */
export async function snowflakeRest(pathAndQuery: string, init: { method?: string; body?: unknown; signal?: AbortSignal } = {}): Promise<Response> {
  let base: string;
  let token: string;
  let tokenType: string;
  let role: string | undefined;
  if (isSpcs()) {
    base = `https://${process.env.SNOWFLAKE_HOST}`;
    token = fs.readFileSync(SPCS_TOKEN_FILE, "utf8").trim();
    tokenType = "OAUTH";
    role = sessionContext().role;
  } else {
    const cfg = readCliConnection();
    // REST hosts use hyphens: underscores in account names are not valid in TLS host names
    const host = (cfg.host as string | undefined) ?? `${cfg.account}.snowflakecomputing.com`;
    base = `https://${host.replace(/_/g, "-")}`;
    token = (cfg.token ?? cfg.password) as string;
    tokenType = "PROGRAMMATIC_ACCESS_TOKEN";
    role = sessionContext(cfg).role;
  }
  return fetch(`${base}${pathAndQuery}`, {
    method: init.method ?? "POST",
    signal: init.signal,
    headers: {
      Authorization: `Bearer ${token}`,
      "X-Snowflake-Authorization-Token-Type": tokenType,
      ...(role ? { "X-Snowflake-Role": role } : {}),
      "Content-Type": "application/json",
      Accept: "text/event-stream, application/json",
    },
    body: init.body === undefined ? undefined : JSON.stringify(init.body),
  });
}

// Owner connection: created lazily, reused, recreated once if it went stale.
let ownerConn: Promise<snowflake.Connection> | null = null;

function ownerConnection(): Promise<snowflake.Connection> {
  ownerConn ??= connect(isSpcs() ? spcsOptions() : localOptions()).catch((e) => {
    ownerConn = null;
    throw e;
  });
  return ownerConn;
}

/** Run one statement as the app identity. Always pass user input via binds. */
export async function query<T extends Row = Row>(sqlText: string, binds: Bind[] = []): Promise<QueryResult<T>> {
  try {
    return await execute<T>(await ownerConnection(), sqlText, binds);
  } catch (e) {
    const code = (e as { code?: number | string }).code;
    // 390114 = auth token expired; reconnect once (SPCS rotates its token file).
    if (String(code) === "390114" || /terminated|not up/i.test(String((e as Error).message))) {
      ownerConn = null;
      return execute<T>(await ownerConnection(), sqlText, binds);
    }
    throw e;
  }
}

/**
 * Run one statement with the calling user's rights (row access / masking
 * policies evaluate against the viewer). On SPCS this needs the request's
 * Sf-Context-Current-User-Token header; locally it falls back to the owner
 * connection, whose role is set by SNOWFLAKE_ROLE.
 */
export async function queryAsCaller<T extends Row = Row>(
  headers: Headers,
  sqlText: string,
  binds: Bind[] = [],
): Promise<QueryResult<T> & { identity: "caller" | "dev" }> {
  if (!isSpcs()) return { ...(await query<T>(sqlText, binds)), identity: "dev" };
  const callerToken = headers.get("sf-context-current-user-token");
  if (!callerToken) throw new Error("Missing caller context (Sf-Context-Current-User-Token)");
  const conn = await connect(spcsOptions(callerToken));
  try {
    return { ...(await execute<T>(conn, sqlText, binds)), identity: "caller" };
  } finally {
    conn.destroy(() => undefined);
  }
}
