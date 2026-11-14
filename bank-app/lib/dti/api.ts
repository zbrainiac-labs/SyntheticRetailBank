import { ForbiddenError } from "@/lib/dti/access";
import { ParamError } from "@/lib/dti/params";
import { NextResponse } from "next/server";

/** HTTP status for an API error: bad parameters -> 400, missing DtI role privilege -> 403, else 500. */
export function errorStatus(e: unknown): number {
  if (e instanceof ParamError) return 400;
  if (e instanceof ForbiddenError) return 403;
  return 500;
}

/** JSON response; errors are mapped with errorStatus. */
export async function respond(fn: () => Promise<unknown>) {
  try {
    return NextResponse.json(await fn());
  } catch (e) {
    return NextResponse.json({ error: (e as Error).message }, { status: errorStatus(e) });
  }
}
