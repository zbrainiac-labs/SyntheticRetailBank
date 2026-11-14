"use client";

import { HBarChart } from "@/components/banking/charts";
import { DataTable, LoaderState, Panel } from "@/components/data/data-table";
import { PageHeader } from "@/components/page-header";
import { StatCard, StatCardSkeleton } from "@/components/stat-card";
import { num, useLoader } from "@/lib/loaders/client";
import { formatNumber } from "@/lib/format";

/** Portfolio Analytics - port of the_bank_app/pages/portfolio_analytics.py. */
export default function PortfolioPage() {
  const kpis = useLoader("portfolio.kpis");
  const tiers = useLoader("portfolio.tier-distribution");
  const countries = useLoader("portfolio.country-distribution");
  const holdings = useLoader("portfolio.tier-holdings");
  const k = kpis.data?.rows[0];

  return (
    <div className="space-y-6">
      <PageHeader eyebrow="Banking" title="Portfolio Analytics" description="Customer portfolio composition and performance" />
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {k ? (
          <>
            <StatCard label="Avg Accounts per Customer" value={formatNumber(num(k.AVG_ACCOUNTS), 1)} />
            <StatCard label="Multi-Currency Customers" value={formatNumber(num(k.MULTI_CURRENCY))} />
            <StatCard label="Premium/Platinum" value={formatNumber(num(k.PREMIUM_PLATINUM))} />
            <StatCard label="Investment Holders" value={formatNumber(num(k.INVESTMENT_HOLDERS))} />
          </>
        ) : (
          [0, 1, 2, 3].map((i) => <StatCardSkeleton key={i} />)
        )}
      </div>
      <div className="grid gap-4 lg:grid-cols-2">
        <Panel title="Account Tier Distribution" description="Customers by account tier">
          <LoaderState q={tiers}>
            {(rows) => <HBarChart rows={rows} category="ACCOUNT_TIER" value="CUSTOMERS" name="Customers" color="var(--chart-1)" />}
          </LoaderState>
        </Panel>
        <Panel title="Geographic Distribution" description="Customers by country">
          <LoaderState q={countries}>
            {(rows) => <HBarChart rows={rows} category="COUNTRY" value="CUSTOMERS" name="Customers" color="var(--chart-2)" />}
          </LoaderState>
        </Panel>
      </div>
      <Panel title="Account Type Holdings by Tier" description="Average number of accounts per customer, by type">
        <LoaderState q={holdings}>
          {(rows) => (
            <DataTable
              rows={rows}
              columns={["ACCOUNT_TIER", "TOTAL_ACCOUNTS", "CHECKING_ACCOUNTS", "SAVINGS_ACCOUNTS", "BUSINESS_ACCOUNTS", "INVESTMENT_ACCOUNTS"]}
            />
          )}
        </LoaderState>
      </Panel>
    </div>
  );
}
