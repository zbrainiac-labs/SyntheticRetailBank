import { queryAsCaller, type Bind, type Row } from "@/lib/db";

/**
 * DtI governance (UC2 step 7). The Snowflake role decides what a user may do:
 *   DTI_OWNER      all entities, export, corrections and limits editor
 *   DTI_LRC_READ   all entities, export
 *   DTI_READ_ONLY  mapped entities only (row access policy), no export, no edits
 * Locally the role is SNOWFLAKE_ROLE; on SPCS it is the caller's role.
 * Snowflake enforces the same rules (grants, row access policy); the app only
 * mirrors them so the UI hides what the role cannot do.
 */
export type DtiRole = "DTI_OWNER" | "DTI_LRC_READ" | "DTI_READ_ONLY" | "NONE";

export interface DtiAccess {
  role: string;
  dtiRole: DtiRole;
  canExport: boolean;
  canEdit: boolean;
}

export class ForbiddenError extends Error {}

/** Highest DtI role in the session (role hierarchy: OWNER > LRC_READ > READ_ONLY). */
function highestDtiRole(inSession: { owner: boolean; lrc: boolean; readOnly: boolean }): DtiRole {
  if (inSession.owner) return "DTI_OWNER";
  if (inSession.lrc) return "DTI_LRC_READ";
  if (inSession.readOnly) return "DTI_READ_ONLY";
  return "NONE";
}

export function accessFrom(role: string, inSession: { owner: boolean; lrc: boolean; readOnly: boolean }): DtiAccess {
  const dtiRole = highestDtiRole(inSession);
  return { role, dtiRole, canExport: dtiRole === "DTI_OWNER" || dtiRole === "DTI_LRC_READ", canEdit: dtiRole === "DTI_OWNER" };
}

/** Statement runner with the viewer's rights (row access policy evaluates per user). */
export function dtiSql(headers: Headers) {
  return <T extends Row = Row>(sqlText: string, binds: Bind[] = []) => queryAsCaller<T>(headers, sqlText, binds);
}

export async function dtiAccess(headers: Headers): Promise<DtiAccess> {
  const { rows } = await dtiSql(headers)(
    `SELECT CURRENT_ROLE() AS R, IS_ROLE_IN_SESSION('DTI_OWNER') AS O, IS_ROLE_IN_SESSION('DTI_LRC_READ') AS L,
            IS_ROLE_IN_SESSION('DTI_READ_ONLY') AS RO`,
  );
  const r = rows[0] ?? {};
  return accessFrom(String(r.R ?? ""), { owner: Boolean(r.O), lrc: Boolean(r.L), readOnly: Boolean(r.RO) });
}

export async function requireAccess(headers: Headers, need: "export" | "edit"): Promise<DtiAccess> {
  const a = await dtiAccess(headers);
  if (need === "export" ? !a.canExport : !a.canEdit) {
    throw new ForbiddenError(`Role ${a.role} may not ${need === "export" ? "export DtI data" : "edit DtI manual inputs"}`);
  }
  return a;
}
