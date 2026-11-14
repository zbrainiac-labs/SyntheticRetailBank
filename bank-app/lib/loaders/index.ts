import { LOADERS_AML } from "@/lib/loaders/aml";
import { LOADERS_ASKAI } from "@/lib/loaders/askai";
import { LOADERS_CHURN } from "@/lib/loaders/churn";
import { LOADERS_CUSTOMER360 } from "@/lib/loaders/customer360";
import { LCR_LOADERS } from "@/lib/loaders/lcr";
import { LOADERS_LENDING } from "@/lib/loaders/lending";
import { LOADERS_PORTFOLIO } from "@/lib/loaders/portfolio";
import { LOADERS_RISK } from "@/lib/loaders/risk";
import { LOADERS_SETTINGS } from "@/lib/loaders/settings";
import { LOADERS_WEALTH } from "@/lib/loaders/wealth";
import type { LoaderDef } from "@/lib/loaders/types";

/**
 * Registry of all named loaders. Each ported page owns one module
 * (lib/loaders/<page>.ts); names are "<page>.<loader>".
 */
export const LOADERS: Record<string, LoaderDef> = {
  ...LCR_LOADERS,
  ...LOADERS_CUSTOMER360,
  ...LOADERS_PORTFOLIO,
  ...LOADERS_CHURN,
  ...LOADERS_RISK,
  ...LOADERS_AML,
  ...LOADERS_LENDING,
  ...LOADERS_WEALTH,
  ...LOADERS_SETTINGS,
  ...LOADERS_ASKAI,
};
