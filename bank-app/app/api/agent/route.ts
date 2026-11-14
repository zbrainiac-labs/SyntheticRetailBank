import { query, snowflakeRest } from "@/lib/db";
import { NextResponse, type NextRequest } from "next/server";

export const dynamic = "force-dynamic";

const DB = "AAA_DEV_SYNTHETIC_BANK";
const NAME = /^[A-Za-z0-9_]{1,100}$/;

/** Agents the app may call (Ask AI): the agents in the bank's AGG schemas. */
export async function GET() {
  try {
    const { rows } = await query(`SHOW AGENTS IN DATABASE ${DB}`);
    const agents = rows
      .map((r) => ({
        schema: String(r.schema_name),
        name: String(r.name),
        comment: (r.comment as string | null) ?? "",
        profile: (() => {
          try {
            return JSON.parse(String(r.profile ?? "{}")) as { display_name?: string };
          } catch {
            return {};
          }
        })(),
      }))
      .filter((a) => /_AGG_V\d+$/i.test(a.schema))
      .map((a) => ({ id: `${a.schema}.${a.name}`, label: a.profile.display_name || a.name, comment: a.comment }));
    return NextResponse.json({ agents });
  } catch (e) {
    return NextResponse.json({ error: (e as Error).message }, { status: 500 });
  }
}

/**
 * POST { agent: "<SCHEMA>.<NAME>", messages: [{ role, content: [{ type: "text", text }] }] }
 * Streams the Cortex Agents :run SSE response through unchanged, so the
 * browser renders thinking, tool use and the answer as they arrive.
 */
export async function POST(req: NextRequest) {
  const body = (await req.json().catch(() => null)) as { agent?: string; messages?: unknown } | null;
  const [schema, name] = String(body?.agent ?? "").split(".");
  if (!schema || !name || !NAME.test(schema) || !NAME.test(name) || !/_AGG_V\d+$/i.test(schema)) {
    return NextResponse.json({ error: "invalid agent" }, { status: 400 });
  }
  if (!Array.isArray(body?.messages) || body.messages.length === 0 || body.messages.length > 40) {
    return NextResponse.json({ error: "invalid messages" }, { status: 400 });
  }
  const upstream = await snowflakeRest(`/api/v2/databases/${DB}/schemas/${schema}/agents/${name}:run`, {
    body: { messages: body.messages },
    signal: req.signal,
  });
  if (!upstream.ok || !upstream.body) {
    const text = await upstream.text().catch(() => "");
    return NextResponse.json({ error: `Agent call failed (HTTP ${upstream.status}) ${text.slice(0, 400)}` }, { status: 502 });
  }
  return new Response(upstream.body, {
    headers: { "content-type": "text/event-stream; charset=utf-8", "cache-control": "no-cache, no-transform" },
  });
}
