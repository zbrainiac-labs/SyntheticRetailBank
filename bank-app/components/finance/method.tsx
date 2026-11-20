"use client";

import { ChartTooltip, CHART_COLORS, CHART_THEME, formatTick } from "@/components/chart-utils";
import { Panel } from "@/components/data/data-table";
import { fmtM, fmtRate } from "@/components/finance/common";
import { num, type LoaderRow } from "@/lib/loaders/client";
import { Bar, BarChart, CartesianGrid, Cell, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

function Row({ label, value, strong }: Readonly<{ label: string; value: string; strong?: boolean }>) {
  return (
    <div className={`flex justify-between gap-4 border-b py-1 text-sm last:border-0 ${strong ? "font-medium" : ""}`}>
      <span className="text-muted-foreground">{label}</span>
      <span className="tabular-nums">{value}</span>
    </div>
  );
}

/** Accounting assumptions with worked examples from the selected month (feedback: clarify assumptions). */
export function Method({
  translation,
  mix,
  names,
  entities,
}: Readonly<{ translation: LoaderRow[]; mix: LoaderRow[]; names: Record<string, string>; entities: LoaderRow[] }>) {
  const us = translation.find((r) => r.FUNCTIONAL_CCY === "USD");
  const eu = translation.find((r) => r.FUNCTIONAL_CCY === "EUR");
  const nciEntities = entities.filter((e) => num(e.OWNERSHIP_PCT) < 1);
  const equityOf = (id: string) => num(translation.find((t) => t.ENTITY_ID === id)?.NET_ASSETS_CHF);

  const ccyMix = Object.values(
    mix
      .filter((r) => r.AKTIV_PASSIV === "A")
      .reduce<Record<string, { name: string; value: number }>>((acc, r) => {
        const c = String(r.CURRENCY);
        (acc[c] ??= { name: c, value: 0 }).value += num(r.AMOUNT_CHF);
        return acc;
      }, {}),
  ).sort((a, b) => b.value - a.value);
  const ccyTotal = ccyMix.reduce((s, c) => s + c.value, 0);
  const cta = translation
    .filter((r) => r.FUNCTIONAL_CCY !== "CHF")
    .map((r) => ({ name: names[String(r.ENTITY_ID)] ?? String(r.ENTITY_ID), CTA: num(r.CTA_CHF) / 1e6 }));

  return (
    <div className="space-y-4" data-testid="method">
      <div className="rounded-lg border border-dashed p-4 text-sm">
        <p className="font-medium">Where the numbers come from</p>
        <ul className="mt-2 list-disc space-y-1 pl-5 text-muted-foreground">
          <li>
            Balances are the synthetic liquidity book positions (loans, securities, deposits, interbank) of nine fictional entities at the
            last snapshot of each month, held in CHF, EUR and USD.
          </li>
          <li>
            Functional-currency amounts are derived from these positions at the month-end closing rate; the funding residual of each entity is
            shown as debt issued / other assets so that equity is a fixed share of assets.
          </li>
          <li>The P&amp;L is a management view derived from the balances and stated rate, fee and cost assumptions, not a ledger.</li>
          <li>FX rates are quoted against USD; EUR/CHF is crossed via USD (USD/CHF ÷ USD/EUR).</li>
        </ul>
      </div>
      <div className="grid gap-4 lg:grid-cols-2">
        {us && (
          <Panel title={`Worked example: ${names[String(us.ENTITY_ID)]} (USD)`} description="Closing vs average vs historical rate and the CTA">
            <Row label="Closing rate USD/CHF (balance sheet)" value={fmtRate(us.CLOSING_RATE)} />
            <Row label="Average rate USD/CHF (P&L, reserves)" value={fmtRate(us.AVERAGE_RATE)} />
            <Row label="Historical rate USD/CHF (share capital)" value={fmtRate(us.HISTORICAL_RATE)} />
            <Row label="Total assets USD m × closing rate" value={`${fmtM(us.TOTAL_ASSETS_LCY)} → CHF ${fmtM(us.TOTAL_ASSETS_CHF)}`} />
            <Row label="Net profit (month) USD m × average rate" value={`${fmtM(us.NET_PROFIT_LCY, 2)} → CHF ${fmtM(us.NET_PROFIT_CHF, 2)}`} />
            <Row label="Net assets at closing rate" value={`CHF ${fmtM(us.NET_ASSETS_CHF)}`} />
            <Row label="− Share capital at historical rate" value={`CHF ${fmtM(us.SHARE_CAPITAL_CHF)}`} />
            <Row label="− Reserves at average rate" value={`CHF ${fmtM(us.RESERVES_CHF)}`} />
            <Row label="= Currency translation reserve (CTA)" value={`CHF ${fmtM(us.CTA_CHF, 2)}`} strong />
          </Panel>
        )}
        <div className="space-y-4">
          {eu && (
            <Panel title={`EUR cross rate: ${names[String(eu.ENTITY_ID)]}`} description="Rates arrive as USD quotes">
              <Row label="EUR/CHF closing (USD/CHF ÷ USD/EUR)" value={fmtRate(eu.CLOSING_RATE)} />
              <Row label="EUR/CHF average of the month" value={fmtRate(eu.AVERAGE_RATE)} />
              <Row label="CTA of the entity" value={`CHF ${fmtM(eu.CTA_CHF, 2)}`} />
            </Panel>
          )}
          <Panel title="Ownership and NCI" description="Full consolidation; the minority share moves to NCI">
            {nciEntities.map((e) => (
              <Row
                key={String(e.ENTITY_ID)}
                label={`${String(e.DISPLAY_NAME)}: ${Math.round(num(e.OWNERSHIP_PCT) * 100)} % owned`}
                value={`NCI ${Math.round((1 - num(e.OWNERSHIP_PCT)) * 100)} % of CHF ${fmtM(equityOf(String(e.ENTITY_ID)))}M equity = CHF ${fmtM((1 - num(e.OWNERSHIP_PCT)) * equityOf(String(e.ENTITY_ID)))}M`}
              />
            ))}
            <p className="mt-2 text-xs text-muted-foreground">
              100 % of assets, liabilities and profit are consolidated; the minority share of equity is shown as NCI and the minority share of
              profit is deducted to reach the profit attributable to shareholders.
            </p>
          </Panel>
        </div>
      </div>
      <div className="grid gap-4 lg:grid-cols-2">
        <Panel title="Currency mix of group assets" description="External assets by holding currency (CHF value)">
          <div className="flex items-center gap-6">
            <ResponsiveContainer width="50%" height={200}>
              <PieChart>
                <Pie data={ccyMix} dataKey="value" nameKey="name" innerRadius={50} outerRadius={80}>
                  {ccyMix.map((c, i) => (
                    <Cell key={c.name} fill={CHART_COLORS[i % CHART_COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip content={<ChartTooltip />} />
              </PieChart>
            </ResponsiveContainer>
            <ul className="space-y-1 text-sm" data-testid="currency-mix">
              {ccyMix.map((c, i) => (
                <li key={c.name} className="flex items-center gap-2">
                  <span className="h-3 w-3 rounded-sm" style={{ background: CHART_COLORS[i % CHART_COLORS.length] }} />
                  {c.name} {ccyTotal ? Math.round((c.value / ccyTotal) * 100) : 0} %
                </li>
              ))}
            </ul>
          </div>
        </Panel>
        <Panel title="CTA by foreign entity" description="Currency translation reserve, CHF million">
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={cta} layout="vertical" margin={{ left: 20 }}>
              <CartesianGrid stroke={CHART_THEME.grid} strokeDasharray="3 3" horizontal={false} />
              <XAxis type="number" tick={CHART_THEME.tick} tickFormatter={formatTick} />
              <YAxis type="category" dataKey="name" tick={CHART_THEME.tick} width={120} />
              <Tooltip content={<ChartTooltip />} />
              <Bar dataKey="CTA" name="CTA (CHF m)">
                {cta.map((c) => (
                  <Cell key={c.name} fill={c.CTA >= 0 ? "var(--success)" : "var(--destructive)"} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </Panel>
      </div>
    </div>
  );
}
