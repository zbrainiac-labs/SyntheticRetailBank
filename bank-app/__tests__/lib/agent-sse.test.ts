import { AgentSseParser, parseAgentSse } from "@/lib/agent/sse";
import { describe, expect, it } from "vitest";

// Excerpts of a real Cortex Agents :run stream (LIQUIDITY_RISK_AGENT, "What is the current LCR ratio?")
const SAMPLE = [
  "event: response.thinking.delta",
  'data: {"content_index":0,"sequence_number":0,"text":"This question looks like a verified query. Let me get a better understanding of the semantic context."}',
  "",
  "event: response.thinking",
  'data: {"content_index":0,"sequence_number":1,"text":"This question looks like a verified query. Let me get a better understanding of the semantic context."}',
  "",
  "event: response.status",
  'data: {"message":"Choosing data sources to use","sequence_number":2,"status":"extracting_tool_calls"}',
  "",
  "event: response.status",
  'data: {"message":"Getting additional context","sequence_number":3,"status":"executing_tools"}',
  "",
  "event: response.tool_use",
  'data: {"client_side_execute":false,"content_index":1,"input":{},"name":"LCR_Current_Status","sequence_number":4,"tool_use_id":"tooluse_3e08","type":"system_agentic_semantic_context"}',
  "",
  "event: response.tool_result.status",
  'data: {"message":"Running LCR_Current_Status","sequence_number":5,"status":"executing_tool","tool_type":"system_agentic_semantic_context","tool_use_id":"tooluse_3e08"}',
  "",
  "event: response.tool_result",
  'data: {"content":[{"json":{"custom_instructions":{}},"type":"json"}],"name":"LCR_Current_Status","status":"success","tool_use_id":"tooluse_3e08"}',
  "",
  ": keep-alive",
  "",
  "event: response.status",
  'data: {"message":"Planning the next steps","sequence_number":9,"status":"planning"}',
  "",
  "event: response.status",
  'data: {"message":"Planning the next steps","sequence_number":10,"status":"planning"}',
  "",
  "event: response.table",
  'data: {"content_index":3,"result_set":{"data":[["2026-10-25","123.4"]],"resultSetMetaData":{"rowType":[{"name":"AS_OF_DATE"},{"name":"LCR_RATIO"}]}},"title":"Current LCR"}',
  "",
  "event: response.text.delta",
  'data: {"content_index":4,"text":"The current **LCR** is "}',
  "",
  "event: response.text.delta",
  'data: {"content_index":4,"text":"123.4%."}',
  "",
  "event: response",
  'data: {"content":[{"type":"text","text":"The current **LCR** is 123.4%."}],"role":"assistant"}',
  "",
].join("\n");

describe("agent SSE parser", () => {
  it("builds answer, thinking, statuses, tools and tables", () => {
    const s = parseAgentSse(SAMPLE);
    expect(s.answer).toBe("The current **LCR** is 123.4%.");
    expect(s.thinking).toBe("This question looks like a verified query. Let me get a better understanding of the semantic context.");
    expect(s.statuses).toEqual(["Choosing data sources to use", "Getting additional context", "Planning the next steps"]);
    expect(s.tools).toEqual([{ id: "tooluse_3e08", name: "LCR_Current_Status", type: "system_agentic_semantic_context", status: "success" }]);
    expect(s.tables).toHaveLength(1);
    expect(s.tables[0].rows[0]).toEqual({ AS_OF_DATE: "2026-10-25", LCR_RATIO: "123.4" });
    expect(s.error).toBeNull();
    expect(s.done).toBe(true);
  });

  it("is independent of chunk boundaries (incl. CRLF and split UTF-8-free mid-JSON cuts)", () => {
    const whole = parseAgentSse(SAMPLE);
    for (const size of [1, 3, 7, 64]) {
      const p = new AgentSseParser();
      const text = SAMPLE.replace(/\n/g, "\r\n");
      for (let i = 0; i < text.length; i += size) p.push(text.slice(i, i + size));
      expect(p.end()).toEqual(whole);
    }
  });

  it("streams state before the end", () => {
    const p = new AgentSseParser();
    const cut = SAMPLE.indexOf("event: response.text.delta");
    const mid = p.push(SAMPLE.slice(0, cut));
    expect(mid.statuses.length).toBe(3);
    expect(mid.answer).toBe("");
    expect(mid.done).toBe(false);
  });

  it("falls back to the final response event and surfaces errors", () => {
    const s = parseAgentSse('event: response\ndata: {"content":[{"type":"text","text":"Hi"}]}\n\nevent: error\ndata: {"code":"399504","message":"Agent failed"}\n\n');
    expect(s.answer).toBe("Hi");
    expect(s.error).toBe("Agent failed (399504)");
  });

  it("takes tables from the final response, keeps non-JSON errors, ignores unknown events", () => {
    const table = '{"title":"T","result_set":{"data":[["1"]],"resultSetMetaData":{"rowType":[{"name":"N"}]}}}';
    const s = parseAgentSse(
      [
        "event: response.chart",
        'data: {"chart_spec":{"mark":"bar"}}',
        "",
        "event: response.unknown",
        'data: {"x":1}',
        "",
        "event: response",
        `data: {"content":[{"type":"table","table":${table}}]}`,
        "",
        "event: error",
        "data: upstream timeout",
        "",
      ].join("\n"),
    );
    expect(s.charts).toEqual([{ mark: "bar" }]);
    expect(s.tables[0]).toEqual({ title: "T", columns: ["N"], rows: [{ N: "1" }] });
    expect(s.error).toBe("upstream timeout");
  });

  it("never stringifies objects into the state", () => {
    const s = parseAgentSse(
      [
        "event: response.tool_use",
        'data: {"name":{"nested":true},"tool_use_id":"t1"}',
        "",
        "event: response.tool_result.status",
        'data: {"tool_use_id":"t1","status":{"x":1},"message":"Running"}',
        "",
        "event: response.status",
        'data: {"message":{"x":1},"status":"planning"}',
        "",
      ].join("\n"),
    );
    expect(s.tools).toEqual([{ id: "t1", name: "tool", type: undefined, status: "Running" }]);
    expect(s.statuses).toEqual(["planning"]);
  });
});
