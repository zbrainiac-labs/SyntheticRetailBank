import { dtiAccess } from "@/lib/dti/access";
import { respond } from "@/lib/dti/api";
import type { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

/** Role and capabilities of the viewer (UI hides export / editor accordingly). */
export async function GET(req: NextRequest) {
  return respond(() => dtiAccess(req.headers));
}
