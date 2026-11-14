import { describe, expect, it, vi } from "vitest";

vi.mock("@/lib/db", () => ({ queryAsCaller: vi.fn() }));

import { queryAsCaller } from "@/lib/db";
import { accessFrom, dtiAccess, ForbiddenError, requireAccess } from "@/lib/dti/access";
import { dtiQuery } from "@/lib/dti/client";
import { isoDate, ParamError, parseDtiParams, placeholders, statusFilter } from "@/lib/dti/params";
import { defaultFilters } from "@/lib/filters/model";

const qs = (o: Record<string, string>) => new URLSearchParams(o);
const valid = { ref: "2026-09-14", comp: "2026-09-04", entities: "LE_01,LE_02" };

describe("dti params", () => {
  it("parses a valid request with defaults", () => {
    expect(parseDtiParams(qs(valid))).toEqual({
      ref: "2026-09-14",
      comp: "2026-09-04",
      entities: ["LE_01", "LE_02"],
      valueType: "STRESSED",
      view: "BUCKETS",
      includePreliminary: false,
    });
    expect(parseDtiParams(qs({ ...valid, valueType: "BASE", view: "DAYS", prelim: "1" }))).toMatchObject({
      valueType: "BASE",
      view: "DAYS",
      includePreliminary: true,
    });
  });

  it.each([
    [{ ...valid, entities: "" }, "invalid entities"],
    [{ ...valid, entities: "LE_01,LE_99" }, "invalid entities"],
    [{ ...valid, entities: "LE_01');DROP TABLE x;--" }, "invalid entities"],
    [{ ...valid, valueType: "STRESS" }, "invalid valueType"],
    [{ ...valid, view: "WEEKS" }, "invalid view"],
    [{ ...valid, ref: "2026-13-45" }, "invalid ref"],
    [{ ...valid, comp: "yesterday" }, "invalid comp"],
  ])("rejects %j", (params, message) => {
    expect(() => parseDtiParams(qs(params))).toThrow(new ParamError(message));
  });

  it("validates ISO dates", () => {
    expect(isoDate("2026-02-28", "d")).toBe("2026-02-28");
    expect(() => isoDate(null, "d")).toThrow(ParamError);
    expect(() => isoDate("2026-9-1", "d")).toThrow(ParamError);
  });

  it("builds the status filter and bind placeholders", () => {
    const p = parseDtiParams(qs(valid));
    expect(statusFilter(p)).toBe("DATA_STATUS = 'FINAL'");
    expect(statusFilter({ ...p, includePreliminary: true }, "S")).toBe("TRUE");
    expect(placeholders(3)).toBe("?, ?, ?");
    expect(placeholders(0)).toBe("");
  });
});

describe("dti access", () => {
  const none = { owner: false, lrc: false, readOnly: false };

  it("maps the highest DtI role in the session", () => {
    expect(accessFrom("CICD", { ...none, owner: true, readOnly: true })).toEqual({
      role: "CICD",
      dtiRole: "DTI_OWNER",
      canExport: true,
      canEdit: true,
    });
    expect(accessFrom("R", { ...none, lrc: true })).toMatchObject({ dtiRole: "DTI_LRC_READ", canExport: true, canEdit: false });
    expect(accessFrom("R", { ...none, readOnly: true })).toMatchObject({ dtiRole: "DTI_READ_ONLY", canExport: false, canEdit: false });
    expect(accessFrom("R", none)).toMatchObject({ dtiRole: "NONE", canExport: false, canEdit: false });
  });

  it("reads the session roles with the viewer's rights", async () => {
    vi.mocked(queryAsCaller).mockResolvedValueOnce({ rows: [{ R: "DTI_READ_ONLY", O: false, L: false, RO: true }] } as never);
    await expect(dtiAccess(new Headers())).resolves.toMatchObject({ role: "DTI_READ_ONLY", dtiRole: "DTI_READ_ONLY" });
  });

  it("refuses export and edits the role may not do", async () => {
    vi.mocked(queryAsCaller).mockResolvedValue({ rows: [{ R: "DTI_READ_ONLY", O: false, L: false, RO: true }] } as never);
    await expect(requireAccess(new Headers(), "export")).rejects.toThrow(ForbiddenError);
    await expect(requireAccess(new Headers(), "edit")).rejects.toThrow("may not edit DtI manual inputs");
    vi.mocked(queryAsCaller).mockResolvedValue({ rows: [{ R: "CICD", O: true, L: false, RO: false }] } as never);
    await expect(requireAccess(new Headers(), "edit")).resolves.toMatchObject({ canEdit: true });
  });
});

describe("dti query string", () => {
  it("carries the filters and only non-empty extras", () => {
    const f = { ...defaultFilters("2026-09-14", "2026-09-04"), includePreliminary: true };
    const p = new URLSearchParams(dtiQuery(f, { position: "DET_FIX_MM_BORROW", period: "", wlk: null }));
    expect(Object.fromEntries(p)).toEqual({
      ref: "2026-09-14",
      comp: "2026-09-04",
      entities: "LE_01,LE_02,LE_03",
      valueType: "STRESSED",
      view: "BUCKETS",
      prelim: "1",
      position: "DET_FIX_MM_BORROW",
    });
  });
});
