"use client";

import { CommandPalette } from "@/components/command-palette";
import { ThemeToggle } from "@/components/theme-toggle";
import { APP_TITLE, COWORK_URL, LOGO_SRC, NAV_SECTIONS } from "@/lib/constants";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import Image from "next/image";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";

interface Health {
  status: "ok" | "error";
  snowflake?: { DATABASE?: string | null };
  error?: string;
}

const timeOf = (d: Date) => d.toLocaleTimeString("de-CH", { hour12: false });

/** Dot colour and caption of the connection status (as app.py: database name or the error). */
function connectionView(pending: boolean, h: Health | undefined): { dot: string; text: string; detail?: string } {
  if (pending) return { dot: "bg-muted-foreground", text: "Checking connection…" };
  if (h?.status === "ok") return { dot: "bg-green-500", text: "Connected to Snowflake", detail: h.snowflake?.DATABASE ?? undefined };
  return { dot: "bg-red-500", text: "Snowflake unavailable", detail: h?.error };
}

/** Left navigation, grouped like the Streamlit sidebar in the_bank_app/app.py. */
export function AppSidebar() {
  const pathname = usePathname();
  const queryClient = useQueryClient();
  const health = useQuery({
    queryKey: ["health-deep"],
    // 503 still carries a JSON body with the error text
    queryFn: async () => (await fetch("/api/health?deep=1")).json() as Promise<Health>,
    staleTime: 60_000,
  });
  // set after mount: a server-rendered clock would not match the client (hydration)
  const [refreshedAt, setRefreshedAt] = useState("");
  useEffect(() => setRefreshedAt(timeOf(new Date())), []);
  const conn = connectionView(health.isPending, health.data);
  const refresh = () => {
    void queryClient.invalidateQueries();
    setRefreshedAt(timeOf(new Date()));
  };

  return (
    <aside
      className="sticky top-0 flex h-screen w-64 shrink-0 flex-col gap-4 overflow-y-auto border-r border-border/70 bg-muted/30 px-3 py-4"
      aria-label="Sidebar"
    >
      <Link href="/" className="flex items-center gap-2.5 px-2">
        {LOGO_SRC && <Image src={LOGO_SRC} alt={`${APP_TITLE} logo`} width={28} height={28} className="shrink-0" />}
        <span className="truncate text-sm font-semibold tracking-tight">{APP_TITLE}</span>
      </Link>
      <p className="px-2 text-xs text-muted-foreground">Customer, risk and liquidity intelligence</p>

      <nav className="flex flex-col gap-4 text-sm" aria-label="Main">
        {NAV_SECTIONS.map((section) => (
          <div key={section.label} className="flex flex-col gap-0.5">
            <span className="px-2 pb-1 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
              {section.label}
            </span>
            {section.items.map((item) => {
              const active = pathname === item.href || pathname.startsWith(`${item.href}/`);
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  aria-current={active ? "page" : undefined}
                  className={`rounded-md px-2 py-1.5 ${
                    active ? "bg-primary/10 font-medium text-foreground" : "text-muted-foreground hover:bg-muted hover:text-foreground"
                  }`}
                >
                  {item.label}
                </Link>
              );
            })}
          </div>
        ))}
      </nav>

      <div className="mt-auto flex flex-col gap-2 border-t border-border/70 px-2 pt-3 text-xs">
        <div data-testid="connection-status">
          <span className="flex items-center gap-2">
            <span className={`h-2 w-2 rounded-full ${conn.dot}`} />
            {conn.text}
          </span>
          {conn.detail && <span className="block truncate pl-4 text-muted-foreground" title={conn.detail}>{conn.detail}</span>}
        </div>
        <a href={COWORK_URL} target="_blank" rel="noopener noreferrer" className="rounded-md border border-border px-2 py-1.5 text-center hover:bg-muted">
          Open Snowflake CoWork
        </a>
        <button type="button" onClick={refresh} className="rounded-md border border-border px-2 py-1.5 hover:bg-muted">
          Refresh data
        </button>
        {refreshedAt && <span className="text-muted-foreground">Last refresh {refreshedAt}</span>}
        <div className="flex items-center justify-between">
          <CommandPalette />
          <ThemeToggle />
        </div>
        <span className="text-[11px] text-muted-foreground">v2.0 | 2026 | Next.js + Snowflake</span>
      </div>
    </aside>
  );
}
