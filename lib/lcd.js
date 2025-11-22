// lib/lcd.js
import { fetch } from 'undici';
import { setTimeout as sleep } from 'node:timers/promises';
import { debug, warn } from './log.js';

const LCDS = [process.env.LCD_PRIMARY, process.env.LCD_BACKUP].filter(Boolean);
let lcdIndex = 0;

async function httpJSON(baseList, idxRef, path) {
  if (!baseList.length) throw new Error('no LCD endpoints configured');
  for (let attempt = 0;; attempt++) {
    const base = baseList[(idxRef + attempt) % baseList.length];
    try {
      const url = `${base}${path}`;
      debug('LCD →', url);
      const r = await fetch(url, { headers: { accept: 'application/json' } });
      if (r.status === 429 || r.status >= 500) throw new Error(`HTTP ${r.status}`);
      return await r.json();
    } catch (e) {
      const backoff = Math.min(1000 * Math.pow(1.5, attempt), 10_000) + Math.floor(Math.random()*250);
      warn(`lcd ${e.message} ${base}${path} → retry in ${backoff}ms`);
      await sleep(backoff);
    }
  }
}

export const lcd = (path) => httpJSON(LCDS, lcdIndex++, path);

// Helpers used throughout the app
export const lcdDenomsMetadata = (denom) =>
  lcd(`/cosmos/bank/v1beta1/denoms_metadata/${encodeURIComponent(denom)}`);

export const lcdFactoryDenom = (denom) =>
  lcd(`/zigchain/factory/denom/${encodeURIComponent(denom)}`);

export const lcdDenomOwners = (denom, nextKey) => {
  const q = nextKey ? `?pagination.key=${encodeURIComponent(nextKey)}` : '';
  return lcd(`/cosmos/bank/v1beta1/denom_owners/${encodeURIComponent(denom)}${q}`);
};

export const lcdSmart = (contract, msgObj) => {
  const msg = Buffer.from(JSON.stringify(msgObj)).toString('base64');
  return lcd(`/cosmwasm/wasm/v1/contract/${contract}/smart/${msg}`);
};

/** NEW: IBC denom trace (accepts 'ibc/<HASH>' or just '<HASH>') */
export const lcdIbcDenomTrace = (ibcId) => {
  const id = (ibcId || '').startsWith('ibc/') ? ibcId : `ibc/${ibcId}`;
  // Path expects the full 'ibc/<HASH>' URL-encoded (e.g. ibc%2FABC...)
  return lcd(`/ibc/apps/transfer/v1/denoms/${encodeURIComponent(id)}`);
};

export default {
  lcd,
  lcdDenomsMetadata,
  lcdFactoryDenom,
  lcdDenomOwners,
  lcdSmart,
  lcdIbcDenomTrace,  // <-- export
};
