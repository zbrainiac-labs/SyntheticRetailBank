import { accessFrom } from "@/lib/dti/access";
import { describe, expect, it } from "vitest";

describe("DtI access from the session roles", () => {
  it("owner may export and edit", () => {
    expect(accessFrom("CICD", { owner: true, lrc: true, readOnly: true })).toMatchObject({ dtiRole: "DTI_OWNER", canExport: true, canEdit: true });
  });

  it("LRC read may export but not edit", () => {
    expect(accessFrom("DTI_LRC_READ", { owner: false, lrc: true, readOnly: true })).toMatchObject({ canExport: true, canEdit: false });
  });

  it("read-only may neither export nor edit", () => {
    expect(accessFrom("DTI_READ_ONLY", { owner: false, lrc: false, readOnly: true })).toMatchObject({
      dtiRole: "DTI_READ_ONLY",
      canExport: false,
      canEdit: false,
    });
  });

  it("roles outside the DtI hierarchy get nothing", () => {
    expect(accessFrom("PUBLIC", { owner: false, lrc: false, readOnly: false })).toMatchObject({ dtiRole: "NONE", canExport: false, canEdit: false });
  });
});
