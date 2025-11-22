// core/prices.js
import { DB } from '../lib/db.js';
import { lcdSmart } from '../lib/lcd.js';
import TTLCache from '../lib/cache.js';

// ---- small, safe caches to avoid LCD spam ---------------------------------
const reservesCache = new TTLCache({ max: 1000, ttlMs: 2000 }); // 2s is enough per block
const inflight = new Map(); // pair_contract -> Promise

export async function upsertPrice(token_id, pool_id, price_in_zig, is_native) {
  console.log('the price being updated', price_in_zig);

  await DB.query(
    `
    INSERT INTO prices(token_id, pool_id, price_in_zig, is_pair_native, updated_at)
    VALUES ($1,$2,$3,$4, now())
  `,
    [token_id, pool_id, price_in_zig, is_native ? 1 : 0],
  );

  await DB.query(
    `
    INSERT INTO price_ticks(pool_id, token_id, price_in_zig, ts)
    VALUES ($1,$2,$3, now())
  `,
    [pool_id, token_id, price_in_zig],
  );
}

/**
 * Fetch pool reserves via LCD smart query `{ pool: {} }` with:
 *  - TTL cache (~2s)
 *  - in-flight request coalescing
 */
export async function fetchPoolReserves(pair_contract) {
  if (!pair_contract) return [];
  const cached = reservesCache.get(pair_contract);
  if (cached) return cached;

  if (inflight.has(pair_contract)) return inflight.get(pair_contract);

  const p = (async () => {
    try {
      const j = await lcdSmart(pair_contract, { pool: {} });
      const assets = j?.data?.assets || j?.assets || [];
      const out = [];
      for (const a of assets) {
        const amount = String(a?.amount ?? '0');
        const denom =
          a?.info?.native_token?.denom ??
          a?.info?.token?.contract_addr ??
          null;
        if (denom && /^\d+$/.test(amount)) {
          out.push({ denom, amount_base: amount });
        }
      }
      reservesCache.set(pair_contract, out);
      return out;
    } finally {
      inflight.delete(pair_contract);
    }
  })();

  inflight.set(pair_contract, p);
  return p;
}

/**
 * Price for UZIG-quoted pools, **exponent-aware** and orientation-correct:
 * price(base in ZIG) = (Rq / 10^6) / (Rb / 10^base_exp)
 */
export function priceFromReserves_UZIGQuote({ base_denom, base_exp }, reserves) {
  const rb = reserves.find((r) => r.denom === base_denom);
  const rq = reserves.find((r) => r.denom === 'uzig');
  if (!rb || !rq) return null;

  const Rb = Number(rb.amount_base || 0);
  const Rq = Number(rq.amount_base || 0);
  if (!(Rb > 0) || !(Rq > 0)) return null;

  const quoteExp = 6;
  console.log('base raw log', base_exp);
  const baseExp = base_exp;
  console.log('exponent in price core', baseExp);

  const hb = Rb / Math.pow(10, baseExp);
  const hq = Rq / Math.pow(10, quoteExp);
  if (!(hb > 0) || !(hq > 0)) return null;
  console.log('the return price from core price', hq / hb);

  return hq / hb;
}
