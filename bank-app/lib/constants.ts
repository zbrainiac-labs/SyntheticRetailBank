/** App title — displayed in the nav header and browser tab */
export const APP_TITLE = 'Synthetic Retail Bank';

/** Path to the logo in /public (used in the header and as favicon) */
export const LOGO_SRC = '/icon.svg';

/**
 * Pages listed under "Go to" in the ⌘K command palette. The group is hidden
 * while this is empty. Add an entry ("/orders") for every page you create.
 */
/** DtI comparison pages (UC2), shown as sub-navigation inside /dti. */
export const DTI_PAGES: { label: string; href: string }[] = [
  { label: 'Summary', href: '/dti' },
  { label: 'Filter', href: '/dti/filter' },
  { label: 'LST Positions', href: '/dti/positions' },
  { label: 'Application', href: '/dti/applications' },
  { label: 'Product Groups', href: '/dti/product-groups' },
  { label: 'Single Products', href: '/dti/products' },
  { label: 'Deal Level', href: '/dti/deals' },
  { label: 'WLK Securities', href: '/dti/securities' },
  { label: 'Free Assets', href: '/dti/free-assets' },
  { label: 'Parameters', href: '/dti/parameters' },
  { label: 'Manual Inputs', href: '/dti/manual-inputs' },
];

export const NAV_ITEMS: { label: string; href: string }[] = [
  { label: 'Home', href: '/' },
  ...DTI_PAGES.map((p) => ({ label: `DtI ${p.label}`, href: p.href })),
];

/** Header navigation by domain; pages are added as they are built or ported. */
export const NAV_SECTIONS: { label: string; items: { label: string; href: string }[] }[] = [
  {
    label: 'Banking',
    items: [
      { label: 'Customer 360', href: '/banking/customer-360' },
      { label: 'Portfolio Analytics', href: '/banking/portfolio' },
      { label: 'Churn & Lifecycle', href: '/banking/churn' },
    ],
  },
  {
    label: 'Risk & Compliance',
    items: [
      { label: 'Risk & Compliance', href: '/risk/compliance' },
      { label: 'AML & Fraud', href: '/risk/aml' },
      { label: 'Lending & Loans', href: '/risk/lending' },
      { label: 'Wealth Management', href: '/risk/wealth' },
    ],
  },
  {
    label: 'Liquidity',
    items: [
      { label: 'DtI comparison', href: '/dti' },
      { label: 'LCR Monitoring', href: '/liquidity/lcr' },
    ],
  },
  {
    label: 'AI & Settings',
    items: [
      { label: 'Ask AI', href: '/ai/ask' },
      { label: 'Settings', href: '/settings' },
    ],
  },
];

/** Fixed colours of the four LST Level I positions, used on every DtI page and chart. */
export const LST_LEVEL_COLORS: Record<string, string> = {
  'Collateral Pool': 'var(--chart-1)',
  'Deterministic Cash Flows': 'var(--chart-2)',
  Inflows: 'var(--chart-3)',
  Outflows: 'var(--chart-4)',
};

/** Snowflake CoWork (Snowflake Intelligence) entry point for this account. */
export const COWORK_URL = "https://ai.snowflake.com/sfseeurope/demo_mdaeppen";
