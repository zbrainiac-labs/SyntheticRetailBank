import { isSpcs, query } from "@/lib/db";
import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

/** Liveness for Docker/SPCS. `?deep=1` also checks the Snowflake session. */
export async function GET(request: Request) {
  const deep = new URL(request.url).searchParams.get("deep") === "1";
  const runtime = isSpcs() ? "spcs" : "local";
  if (!deep) return NextResponse.json({ status: "ok", runtime });
  try {
    const { rows } = await query<{ ACCOUNT: string; ROLE: string; WAREHOUSE: string; DATABASE: string | null }>(
      "SELECT CURRENT_ACCOUNT() AS ACCOUNT, CURRENT_ROLE() AS ROLE, CURRENT_WAREHOUSE() AS WAREHOUSE, CURRENT_DATABASE() AS DATABASE",
    );
    return NextResponse.json({ status: "ok", runtime, snowflake: rows[0] });
  } catch (e) {
    return NextResponse.json({ status: "error", runtime, error: (e as Error).message }, { status: 503 });
  }
}
