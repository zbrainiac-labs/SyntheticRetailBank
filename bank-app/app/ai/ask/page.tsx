"use client";

import { Markdown } from "@/components/ai/markdown";
import { DataTable, Panel } from "@/components/data/data-table";
import { EmptyState } from "@/components/empty-state";
import { PageHeader } from "@/components/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { AgentSseParser, emptyState, type AgentState } from "@/lib/agent/sse";
import { COWORK_URL } from "@/lib/constants";
import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";

const DEFAULT_AGENT = "REP_AGG_V001.MASTER_AGENT";

interface Agent {
  id: string;
  label: string;
  comment?: string;
}
interface Turn {
  /** stable React key for the chat list */
  id: number;
  role: "user" | "assistant";
  text: string;
  result?: AgentState;
}

let turnSeq = 0;
const nextTurnId = () => ++turnSeq;

const GENERIC = [
  "What is the current LCR ratio?",
  "How many customers do we have by country?",
  "Which customers have the highest AML risk?",
  "Summarise the loan portfolio by product.",
];
const SAMPLES: [RegExp, string[]][] = [
  [/LIQUIDITY|LCR/, ["What is the current LCR ratio?", "Show the HQLA composition for the latest date.", "Which days had an LCR below 110%?", "What drives net cash outflows?"]],
  [/CRM|CUSTOMER/, ["How many customers do we have by country?", "List the top 10 customers by total balance.", "How many customers are PEPs?", "Show customers with high risk classification."]],
  [/LOAN|CREDIT|MORTGAGE/, ["Summarise the loan portfolio by product.", "What is the average LTV of mortgages?", "Which loans are in arrears?", "Show exposure by credit rating."]],
  [/WEALTH|EQUITY|PORTFOLIO/, ["What are the largest portfolio positions?", "Show assets under management by currency.", "Which clients hold the most equities?", "What is the total P&L this month?"]],
  [/AML|COMPLIANCE|SANCTION/, ["Which customers have the highest AML risk?", "How many sanctions alerts are open?", "Show suspicious transactions of the last 30 days.", "Summarise KYC review status."]],
];
const samplesFor = (id: string) => SAMPLES.find(([re]) => re.test(id.toUpperCase()))?.[1] ?? GENERIC;

/** Ask AI - streaming multi-turn chat with the bank's Cortex Agents (replaces the CoWork link page). */
export default function AskAiPage() {
  const agents = useQuery({
    queryKey: ["agents"],
    staleTime: 10 * 60_000,
    queryFn: async () => {
      const res = await fetch("/api/agent");
      const body = await res.json();
      if (!res.ok) throw new Error(body.error ?? `HTTP ${res.status}`);
      return body.agents as Agent[];
    },
  });
  const [agent, setAgent] = useState<string>("");
  const [turns, setTurns] = useState<Turn[]>([]);
  const [live, setLive] = useState<AgentState | null>(null);
  const [input, setInput] = useState("");
  const abort = useRef<AbortController | null>(null);
  const bottom = useRef<HTMLDivElement>(null);
  const running = live !== null;

  useEffect(() => {
    const list = agents.data ?? [];
    if (!agent && list.length) setAgent(list.find((a) => a.id === DEFAULT_AGENT)?.id ?? list[0].id);
  }, [agents.data, agent]);
  // braces: never return scrollIntoView's result (a Promise in current Chromium) as the effect cleanup
  useEffect(() => {
    bottom.current?.scrollIntoView({ block: "end" });
  }, [turns, live]);

  async function ask(question: string) {
    const q = question.trim();
    if (!q || running || !agent) return;
    const history: Turn[] = [...turns, { id: nextTurnId(), role: "user", text: q }];
    setTurns(history);
    setInput("");
    const ctrl = new AbortController();
    abort.current = ctrl;
    const parser = new AgentSseParser();
    setLive({ ...emptyState() });
    let final: AgentState;
    try {
      const res = await fetch("/api/agent", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        signal: ctrl.signal,
        body: JSON.stringify({
          agent,
          messages: history.filter((t) => t.text).map((t) => ({ role: t.role, content: [{ type: "text", text: t.text }] })),
        }),
      });
      if (!res.ok || !res.body) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error ?? body.message ?? `HTTP ${res.status}`);
      }
      const reader = res.body.getReader();
      const dec = new TextDecoder();
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        const s = parser.push(dec.decode(value, { stream: true }));
        setLive(structuredClone(s));
      }
      final = parser.end();
      if (!final.answer && !final.error && final.tables.length === 0) final.error = "No answer received from the agent (empty stream).";
    } catch (e) {
      final = parser.state;
      if ((e as Error).name === "AbortError") final.error = final.error ?? "Stopped by user.";
      else final.error = (e as Error).message;
    }
    abort.current = null;
    setLive(null);
    setTurns([...history, { id: nextTurnId(), role: "assistant", text: final.answer, result: structuredClone(final) }]);
  }

  const newChat = () => {
    abort.current?.abort();
    setTurns([]);
    setInput("");
  };
  const current = agents.data?.find((a) => a.id === agent);

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="AI"
        title="Ask AI"
        description="Ask questions in plain language across all banking domains"
        actions={
          <Button variant="outline" asChild>
            <a href={COWORK_URL} target="_blank" rel="noreferrer">Open Snowflake CoWork</a>
          </Button>
        }
      />

      <div className="flex flex-wrap items-center gap-3 rounded-lg border bg-card p-3">
        <label className="flex items-center gap-2 text-sm text-muted-foreground">
          Agent
          {agents.isPending ? (
            <Skeleton className="h-8 w-64" />
          ) : (
            <select
              aria-label="Agent"
              className="h-8 rounded-md border bg-background px-2 text-sm text-foreground"
              value={agent}
              disabled={running}
              onChange={(e) => {
                setAgent(e.target.value);
                setTurns([]);
              }}
            >
              {(agents.data ?? []).map((a) => (
                <option key={a.id} value={a.id}>{a.label} ({a.id})</option>
              ))}
            </select>
          )}
        </label>
        <Button variant="outline" size="sm" onClick={newChat} disabled={turns.length === 0 && !running}>New chat</Button>
        {current?.comment && <span className="min-w-0 flex-1 truncate text-xs text-muted-foreground" title={current.comment}>{current.comment}</span>}
      </div>
      {agents.isError && <EmptyState variant="error" title="Agents unavailable" description={agents.error.message} />}

      <div className="space-y-4" data-testid="chat">
        {turns.length === 0 && !running && agent && (
          <Panel title="Try a question" description="Each turn sends the full conversation to the agent">
            <div className="flex flex-wrap gap-2">
              {samplesFor(agent).map((q) => (
                <Button key={q} variant="secondary" size="sm" onClick={() => ask(q)}>{q}</Button>
              ))}
            </div>
          </Panel>
        )}
        {turns.map((t) =>
          t.role === "user" ? (
            <UserBubble key={t.id} text={t.text} />
          ) : (
            <AssistantMessage key={t.id} s={t.result ?? { ...emptyState(), answer: t.text, done: true }} />
          ),
        )}
        {live && <AssistantMessage s={live} running />}
        <div ref={bottom} />
      </div>

      <form
        className="sticky bottom-0 flex gap-2 border-t bg-background py-3"
        onSubmit={(e) => {
          e.preventDefault();
          void ask(input);
        }}
      >
        <textarea
          aria-label="Question"
          className="min-h-10 flex-1 resize-y rounded-md border bg-background px-3 py-2 text-sm"
          rows={2}
          placeholder="Ask about liquidity, customers, loans, compliance..."
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              void ask(input);
            }
          }}
        />
        {running ? (
          <Button type="button" variant="destructive" onClick={() => abort.current?.abort()}>Stop</Button>
        ) : (
          <Button type="submit" disabled={!input.trim() || !agent}>Send</Button>
        )}
      </form>
    </div>
  );
}

function UserBubble({ text }: { text: string }) {
  return (
    <div className="flex justify-end">
      <div className="max-w-[80%] whitespace-pre-wrap rounded-lg bg-primary px-4 py-2 text-sm text-primary-foreground">{text}</div>
    </div>
  );
}

function AssistantMessage({ s, running = false }: { s: AgentState; running?: boolean }) {
  const lastStatus = s.statuses[s.statuses.length - 1];
  return (
    <div className="space-y-3 rounded-lg border bg-card p-4" data-testid={running ? "assistant-live" : "assistant-message"}>
      {(s.statuses.length > 0 || s.tools.length > 0 || running) && (
        <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
          {running && <span className="inline-block size-2 animate-pulse rounded-full bg-primary" />}
          {running && <span>{lastStatus ?? "Contacting agent..."}</span>}
          {s.tools.map((t) => (
            <Badge key={t.id} variant="outline" title={t.type}>
              {t.name}: {t.status}
            </Badge>
          ))}
          {!running && s.statuses.length > 0 && (
            <details>
              <summary className="cursor-pointer">{s.statuses.length} steps</summary>
              <ol className="ml-5 list-decimal">{s.statuses.map((m, i) => <li key={i}>{m}</li>)}</ol>
            </details>
          )}
        </div>
      )}
      {s.thinking && (
        <details className="rounded-md bg-muted/50 px-3 py-2 text-xs" open={running && !s.answer}>
          <summary className="cursor-pointer font-medium text-muted-foreground">Thinking</summary>
          <p className="mt-1 whitespace-pre-wrap text-muted-foreground">{s.thinking}</p>
        </details>
      )}
      {s.answer && <div data-testid="answer"><Markdown text={s.answer} /></div>}
      {s.tables.map((t, i) => (
        <div key={i} className="space-y-1">
          {t.title && <p className="text-sm font-medium">{t.title}</p>}
          <DataTable rows={t.rows} columns={t.columns} />
        </div>
      ))}
      {s.charts.length > 0 && <p className="text-xs text-muted-foreground">{s.charts.length} chart(s) returned - open in Snowflake CoWork to view.</p>}
      {s.error && (
        <Alert variant="destructive">
          <AlertTitle>Agent error</AlertTitle>
          <AlertDescription>{s.error}</AlertDescription>
        </Alert>
      )}
    </div>
  );
}
