import { describe, expect, it } from "vitest";
import { eventLabel, eventsOnHistory, historyChart, LIQUID_DAYS, markEvents, type DtiEvent } from "@/lib/dti/events";

const ev = (id: string, date: string, type = "DEPOSIT_WITHDRAWAL"): DtiEvent => ({
  id, date, type, entity: "LE_02", position: null, amount: 1e6, description: "",
});

describe("DtI events", () => {
  const events = [ev("DTI_10", "2026-10-20"), ev("DTI_08", "2026-09-08", "FUNDING_CLIFF"), ev("DTI_03", "2026-10-23", "CORRECTION")];

  it("sorts by date and flags events between comparison and reference date", () => {
    const m = markEvents(events, "2026-10-19", "2026-10-23");
    expect(m.map((e) => e.id)).toEqual(["DTI_08", "DTI_10", "DTI_03"]);
    expect(m.map((e) => e.inDelta)).toEqual([false, true, true]);
  });

  it("comparison date itself is not in the delta, swapped dates work too", () => {
    expect(markEvents(events, "2026-10-20", "2026-10-23").find((e) => e.id === "DTI_10")?.inDelta).toBe(false);
    expect(markEvents(events, "2026-10-23", "2026-09-01").filter((e) => e.inDelta).map((e) => e.id)).toEqual(["DTI_08", "DTI_10", "DTI_03"]);
  });

  it("history caps > 365 and keeps only markers on reporting dates", () => {
    const pts = [{ date: "2026-09-07", dti: 54 }, { date: "2026-09-08", dti: 22 }, { date: "2026-10-23", dti: null }];
    expect(historyChart(pts)).toEqual([{ date: "2026-09-07", dti: 54 }, { date: "2026-09-08", dti: 22 }, { date: "2026-10-23", dti: LIQUID_DAYS }]);
    expect(eventsOnHistory(events, pts).map((e) => e.id)).toEqual(["DTI_08", "DTI_03"]);
  });

  it("labels known types and passes unknown ones through", () => {
    expect(eventLabel("FUNDING_CLIFF")).toBe("Funding cliff");
    expect(eventLabel("SOMETHING_NEW")).toBe("SOMETHING_NEW");
  });
});
