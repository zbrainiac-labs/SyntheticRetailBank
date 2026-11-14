import { PageHeader } from '@/components/page-header';
import { Card, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { NAV_SECTIONS } from '@/lib/constants';
import Link from 'next/link';

/** One line per page, as the Streamlit landing (the_bank_app/app.py) introduced each area. */
const DESCRIPTIONS: Record<string, string> = {
  '/banking/customer-360': 'Complete view of a customer: profile, accounts, activity and risk.',
  '/banking/portfolio': 'Portfolio performance, allocation and returns.',
  '/banking/churn': 'Customer lifecycle, dormancy and churn-risk signals.',
  '/risk/compliance': 'Customer risk, sanctions and PEP screening, data quality.',
  '/risk/aml': 'Transaction anomalies, AML alerts and fraud monitoring.',
  '/risk/lending': 'Loan applications, affordability, compliance and credit risk.',
  '/risk/wealth': 'Advisor performance, client portfolios and capacity.',
  '/dti': 'Distance-to-Illiquidity of two reporting dates, LST positions and drill-down to deals.',
  '/liquidity/lcr': 'FINMA Liquidity Coverage Ratio, stress scenarios and what-if simulation.',
  '/ai/ask': 'Ask the bank\u2019s Cortex Agents in plain language.',
  '/settings': 'Data freshness, Snowflake connection and access.',
};

export default function Home() {
  return (
    <div className="mx-auto w-full max-w-7xl space-y-8">
      <PageHeader
        eyebrow="Synthetic Retail Bank"
        title="Bank App"
        description="Customer, risk and liquidity intelligence on Snowflake."
      />
      {NAV_SECTIONS.map((section) => (
        <section key={section.label} className="space-y-3">
          <h2 className="text-sm font-medium uppercase tracking-wider text-muted-foreground">{section.label}</h2>
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            {section.items.map((p) => (
              <Link key={p.href} href={p.href} className="group">
                <Card className="h-full transition-colors group-hover:border-primary">
                  <CardHeader>
                    <CardTitle>{p.label}</CardTitle>
                    <CardDescription>{DESCRIPTIONS[p.href]}</CardDescription>
                  </CardHeader>
                </Card>
              </Link>
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}
