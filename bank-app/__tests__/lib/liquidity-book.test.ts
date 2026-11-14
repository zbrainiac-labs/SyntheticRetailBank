import { checkBook, defaultDate, previousDate, type LiquidityBook } from "@/lib/liquidity/book";
import { describe, expect, it } from "vitest";

const book = (over: Partial<LiquidityBook> = {}): LiquidityBook => ({
  runId: "LIQ-TEST",
  asOf: "2026-10-23",
  windowStart: "2026-07-26",
  historyStart: "2025-04-30",
  lcrLatest: "2026-10-23",
  dtiLatest: "2026-10-23",
  lcrPoolChf: 2_060_900_000,
  dtiPoolChf: 2_060_900_001,
  ...over,
});

describe("liquidity book", () => {
  it("defaults to the run as-of date in ascending and descending lists", () => {
    expect(defaultDate(["2026-10-21", "2026-10-22", "2026-10-23"], "2026-10-23")).toBe("2026-10-23");
    expect(defaultDate(["2026-10-23", "2026-10-22"], "2026-10-23")).toBe("2026-10-23");
  });

  it("never opens after the as-of date (LCR weekend snapshots of an older run)", () => {
    expect(defaultDate(["2026-10-23", "2026-10-24", "2026-10-25"], "2026-10-23")).toBe("2026-10-23");
    expect(defaultDate(["2026-10-21", "2026-10-22"], "2026-10-23")).toBe("2026-10-22");
  });

  it("falls back to the latest date without a run", () => {
    expect(defaultDate(["2026-10-21", "2026-10-22"], null)).toBe("2026-10-22");
    expect(defaultDate([], "2026-10-23")).toBeNull();
  });

  it("previous reporting date", () => {
    expect(previousDate(["2026-10-23", "2026-10-22", "2026-09-30"], "2026-10-23")).toBe("2026-10-22");
    expect(previousDate(["2026-10-23"], "2026-10-23")).toBeNull();
  });

  it("same book: same latest date and pool within tolerance", () => {
    expect(checkBook(book())).toEqual({ sameDate: true, samePool: true, ok: true, comparable: true });
    expect(checkBook(book({ dtiLatest: "2026-10-22" })).ok).toBe(false);
    expect(checkBook(book({ dtiPoolChf: 2_000_000_000 }))).toMatchObject({ samePool: false, ok: false });
  });

  it("pool not visible to the role does not fail the check", () => {
    expect(checkBook(book({ lcrPoolChf: null }))).toEqual({ sameDate: true, samePool: null, ok: true, comparable: true });
  });

  it("role without LCR access: not comparable, not a mismatch (DTI_READ_ONLY)", () => {
    expect(checkBook(book({ lcrLatest: null, lcrPoolChf: null, dtiPoolChf: null }))).toEqual({
      sameDate: null,
      samePool: null,
      ok: true,
      comparable: false,
    });
  });
});
