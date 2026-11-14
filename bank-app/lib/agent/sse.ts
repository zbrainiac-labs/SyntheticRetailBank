/**
 * Incremental parser for the Cortex Agents `:run` SSE stream (port of the
 * event handling in the_bank_app/utils/agent_caller.py). Pure: feed raw text
 * chunks in any split, read the accumulated state.
 */

export interface AgentTable {
  title?: string;
  columns: string[];
  rows: Record<string, unknown>[];
}

export interface AgentTool {
  id: string;
  name: string;
  type?: string;
  status: string;
}

export interface AgentState {
  answer: string;
  thinking: string;
  statuses: string[];
  tools: AgentTool[];
  tables: AgentTable[];
  charts: unknown[];
  error: string | null;
  done: boolean;
}

export const emptyState = (): AgentState => ({
  answer: "",
  thinking: "",
  statuses: [],
  tools: [],
  tables: [],
  charts: [],
  error: null,
  done: false,
});

type Json = Record<string, unknown>;

/** First argument that is a string or number, as a string (never "[object Object]"). */
function firstStr(...vals: unknown[]): string {
  for (const v of vals) {
    if (typeof v === "string") return v;
    if (typeof v === "number" || typeof v === "boolean") return String(v);
  }
  return "";
}

/** Cortex result_set (SQL API shape) -> rows keyed by column name. */
export function toTable(rs: unknown, title?: string): AgentTable | null {
  const r = rs as { data?: unknown[][]; resultSetMetaData?: { rowType?: { name: string }[] } } | undefined;
  const columns = r?.resultSetMetaData?.rowType?.map((c) => c.name) ?? [];
  if (!r || !Array.isArray(r.data) || columns.length === 0) return null;
  const rows = r.data.map((row) => Object.fromEntries(columns.map((c, i) => [c, row[i]])));
  return { title, columns, rows };
}

function upsertTool(s: AgentState, id: string, patch: Partial<AgentTool>) {
  const t = s.tools.find((x) => x.id === id);
  if (t) Object.assign(t, patch);
  else s.tools.push({ id, name: patch.name ?? id, status: patch.status ?? "running", type: patch.type });
}

type Handler = (s: AgentState, d: Json, raw: string) => void;

function onThinking(s: AgentState, d: Json) {
  // full thinking block; keep the deltas if they already covered it
  if (typeof d.text !== "string" || s.thinking.includes(d.text)) return;
  const sep = s.thinking ? "\n\n" : "";
  s.thinking += sep + d.text;
}

function onStatus(s: AgentState, d: Json) {
  const m = firstStr(d.message, d.status);
  if (m && s.statuses[s.statuses.length - 1] !== m) s.statuses.push(m);
}

function onToolResult(s: AgentState, d: Json) {
  const id = firstStr(d.tool_use_id);
  const t = s.tools.find((x) => x.id === id) ?? s.tools[s.tools.length - 1];
  if (t) t.status = firstStr(d.status, "done");
}

function onTable(s: AgentState, d: Json) {
  const t = toTable(d.result_set, d.title as string | undefined);
  if (t) s.tables.push(t);
}

function onResponse(s: AgentState, d: Json) {
  // final aggregated message; fill gaps if deltas were missed
  const content = (d.content as Json[] | undefined) ?? [];
  if (!s.answer) s.answer = content.filter((c) => c.type === "text").map((c) => firstStr(c.text)).join("");
  if (s.tables.length === 0) {
    for (const c of content.filter((x) => x.type === "table")) {
      const tb = c.table as Json | undefined;
      const t = toTable(tb?.result_set, tb?.title as string | undefined);
      if (t) s.tables.push(t);
    }
  }
  s.done = true;
}

function onError(s: AgentState, d: Json, raw: string) {
  const code = firstStr(d.code);
  s.error = firstStr(d.message, d.error, raw) + (code ? ` (${code})` : "");
}

const handlers: Record<string, Handler> = {
  "response.text.delta": (s, d) => {
    s.answer += firstStr(d.text);
  },
  "response.thinking.delta": (s, d) => {
    s.thinking += firstStr(d.text);
  },
  "response.thinking": onThinking,
  "response.text": (s, d) => {
    if (typeof d.text === "string" && !s.answer) s.answer = d.text;
  },
  "response.status": onStatus,
  "response.tool_use": (s, d) =>
    upsertTool(s, firstStr(d.tool_use_id, d.name), { name: firstStr(d.name, "tool"), type: d.type as string, status: "running" }),
  "response.tool_result.status": (s, d) =>
    upsertTool(s, firstStr(d.tool_use_id), { status: firstStr(d.status, d.message, "running") }),
  "response.tool_result": onToolResult,
  "response.table": onTable,
  "response.chart": (s, d) => {
    s.charts.push(d.chart_spec ?? d);
  },
  response: onResponse,
  error: onError,
};

/** Apply one SSE event to the state (mutates `s`). */
export function applyEvent(s: AgentState, event: string, raw: string) {
  let d: Json = {};
  try {
    d = raw ? (JSON.parse(raw) as Json) : {};
  } catch {
    if (event === "error") s.error = raw;
    return;
  }
  const handler = Object.hasOwn(handlers, event) ? handlers[event] : undefined;
  handler?.(s, d, raw);
}

/** Stateful parser: push(chunk) as bytes arrive, end() when the stream closes. */
export class AgentSseParser {
  state: AgentState = emptyState();
  private buf = "";
  private event = "message";
  private data: string[] = [];

  push(chunk: string): AgentState {
    this.buf += chunk;
    let nl: number;
    while ((nl = this.buf.indexOf("\n")) >= 0) {
      const line = this.buf.slice(0, nl).replace(/\r$/, "");
      this.buf = this.buf.slice(nl + 1);
      this.line(line);
    }
    return this.state;
  }

  end(): AgentState {
    if (this.buf) this.line(this.buf.replace(/\r$/, ""));
    this.buf = "";
    this.dispatch();
    this.state.done = true;
    return this.state;
  }

  private line(line: string) {
    if (line === "") return this.dispatch();
    if (line.startsWith(":")) return; // keep-alive comment
    const i = line.indexOf(":");
    const field = i < 0 ? line : line.slice(0, i);
    const value = i < 0 ? "" : line.slice(i + 1).replace(/^ /, "");
    if (field === "event") this.event = value;
    else if (field === "data") this.data.push(value);
  }

  private dispatch() {
    if (this.data.length) applyEvent(this.state, this.event, this.data.join("\n"));
    this.event = "message";
    this.data = [];
  }
}

/** Convenience: parse a complete stream text. */
export function parseAgentSse(text: string): AgentState {
  const p = new AgentSseParser();
  p.push(text);
  return p.end();
}
