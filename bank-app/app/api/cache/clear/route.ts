import { clearResultCache } from "@/lib/db";
import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

/** POST /api/cache/clear: drop the server-side result cache (used by the "Refresh data" actions). */
export function POST() {
  clearResultCache();
  return NextResponse.json({ ok: true });
}
