/** Free assets page (UC2 6.9): one row per WLK cluster of the collateral pool. */
export interface FreeAsset {
  wlk: string;
  name: string;
  hqla: string;
  haircut: number;
  value: number;
}

/** WLK cluster of central bank balances and vault cash (not a security, so not in the securities book). */
export const CASH_CLUSTER = "I";

/**
 * Adds the central bank and vault cash of the selected entities (allocated by the fixed CB share, as in the
 * DtI engine) to WLK cluster I: the securities book has no cash rows, so without this the cluster would show 0
 * while the LCR counts the same cash as Level 1. Cash has no haircut, so every basis (CV, MV, nominal) is the
 * cash amount itself.
 */
export function withCentralBankCash(assets: FreeAsset[], cashChf: number): FreeAsset[] {
  return assets.map((a) => (a.wlk === CASH_CLUSTER ? { ...a, value: a.value + cashChf } : a));
}
