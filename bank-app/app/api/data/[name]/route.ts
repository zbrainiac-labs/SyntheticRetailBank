import { queryAsCaller } from "@/lib/db";
import { LOADERS } from "@/lib/loaders";
import { LoaderParamError, bindParams } from "@/lib/loaders/types";
import { NextResponse, type NextRequest } from "next/server";

export const dynamic = "force-dynamic";

/**
 * Generic read endpoint for the ported pages: GET /api/data/<loader>?<params>.
 * Only registered loaders run; values are validated and bound. Runs with the
 * viewer's rights (row access / masking policies apply per user on SPCS).
 */
export async function GET(req: NextRequest, ctx: { params: Promise<{ name: string }> }) {
  const { name } = await ctx.params;
  const def = LOADERS[name];
  if (!def) return NextResponse.json({ error: `unknown loader ${name}` }, { status: 404 });
  try {
    const binds = bindParams(def, req.nextUrl.searchParams);
    const started = Date.now();
    const { rows } = await queryAsCaller(req.headers, def.sql, binds);
    const max = def.maxRows ?? 5000;
    return NextResponse.json({
      rows: rows.slice(0, max),
      truncated: rows.length > max,
      ms: Date.now() - started,
    });
  } catch (e) {
    const status = e instanceof LoaderParamError ? 400 : 500;
    return NextResponse.json({ error: (e as Error).message }, { status });
  }
}
