// jobs/fasttrack-listener.js
import { DB } from '../lib/db.js';
import { info, warn, debug } from '../lib/log.js';
import { pgListen } from '../lib/pg_notify.js';

import { refreshMetaOnce } from './meta-refresher.js';
import { refreshHoldersOnce } from './holders-refresher.js';
import { scanTokenOnce } from './token-security.js';
import {
  refreshPoolMatrixOnce,
  refreshTokenMatrixOnce,
} from './matrix-rollups.js';

// ➕ price & ohlcv helpers
import { fetchPoolReserves, priceFromReserves_UZIGQuote, upsertPrice } from '../core/prices.js';
import { upsertOHLCV1m } from '../core/ohlcv.js';

/**
 * Helper to load pool+tokens by pair_contract (or pool_id in payload)
 */
async function loadPoolContext(payload) {
  if (!payload) return null;

  const by = payload.pool_id ? { col: 'p.pool_id', val: payload.pool_id }
                             : payload.pair_contract ? { col: 'p.pair_contract', val: payload.pair_contract }
                             : null;
  if (!by) return null;

  const { rows } = await DB.query(`
    SELECT
      p.pool_id, p.pair_contract, p.is_uzig_quote, p.created_at,
      p.base_token_id, b.denom AS base_denom,
      p.quote_token_id, q.denom AS quote_denom
    FROM pools p
    JOIN tokens b ON b.token_id=p.base_token_id
    JOIN tokens q ON q.token_id=p.quote_token_id
    WHERE ${by.col} = $1
  `, [by.val]);

  return rows[0] || null;
}

async function holdersCount(tokenId) {
  const { rows } = await DB.query(
    `SELECT holders_count::BIGINT AS c
     FROM token_holders_stats
     WHERE token_id=$1`,
    [tokenId]
  );
  return Number(rows?.[0]?.c || 0);
}

// minute-floor helper
function minuteFloor(d) {
  const t = new Date(d instanceof Date ? d : new Date(d));
  t.setSeconds(0, 0);
  return t;
}

export function startFasttrackListener() {
  // 1) listen for NOTIFY pair_created
  pgListen('pair_created', async (payload) => {
    try {
      const ctx = await loadPoolContext(payload);
      if (!ctx) return warn('[fasttrack] no context for payload', payload);

      info('[fasttrack] pair_created received', {
        pool_id: ctx.pool_id,
        pair: ctx.pair_contract,
        base: ctx.base_denom,
        quote: ctx.quote_denom
      });

      // 2) metadata (both legs, in parallel; errors tolerated)
      await Promise.allSettled([
        refreshMetaOnce(ctx.base_denom),
        refreshMetaOnce(ctx.quote_denom),
      ]);

      // 3) holders for base token (and optionally quote if non-uzig)
      await Promise.allSettled([
        refreshHoldersOnce(ctx.base_token_id, ctx.base_denom),
        (ctx.quote_denom !== 'uzig')
          ? refreshHoldersOnce(ctx.quote_token_id, ctx.quote_denom)
          : Promise.resolve(),
      ]);

      // log current counts
      const baseHC = await holdersCount(ctx.base_token_id);
      const quoteHC = (ctx.quote_denom !== 'uzig') ? await holdersCount(ctx.quote_token_id) : 0;
      debug('[fasttrack] holders counts', { base: baseHC, quote: quoteHC });

      // optional retry if zero
      if (baseHC === 0) {
        debug('[fasttrack] base holders 0, retrying once…', ctx.base_denom);
        await refreshHoldersOnce(ctx.base_token_id, ctx.base_denom);
      }
      if (ctx.quote_denom !== 'uzig' && quoteHC === 0) {
        debug('[fasttrack] quote holders 0, retrying once…', ctx.quote_denom);
        await refreshHoldersOnce(ctx.quote_token_id, ctx.quote_denom);
      }

      // 4) security scan (base + maybe quote)
      await Promise.allSettled([
        scanTokenOnce(ctx.base_token_id, ctx.base_denom),
        (ctx.quote_denom !== 'uzig')
          ? scanTokenOnce(ctx.quote_token_id, ctx.quote_denom)
          : Promise.resolve(),
      ]);

      // 5) matrix (pool + tokens) across all standard buckets
      await Promise.allSettled([
        refreshPoolMatrixOnce(ctx.pool_id),
        refreshTokenMatrixOnce(ctx.base_token_id),
        (ctx.quote_denom !== 'uzig')
          ? refreshTokenMatrixOnce(ctx.quote_token_id)
          : Promise.resolve(),
      ]);

      // 6) ➕ Initial price & OHLCV seed from current reserves (UZIG-quoted only),
      //     after meta is ready; idempotent with your ohlcv upsert.
      try {
        if (ctx.quote_denom === 'uzig') {
          // wait-for-meta: require exponent available
          const { rows: rExp } = await DB.query(
            'SELECT exponent AS exp FROM tokens WHERE token_id=$1',
            [ctx.base_token_id]
          );
          const baseExp = rExp?.[0]?.exp;
          if (baseExp == null) {
            debug('[fasttrack/init skip] meta not ready', { pool_id: ctx.pool_id, denom: ctx.base_denom });
          } else {
            const reserves = await fetchPoolReserves(ctx.pair_contract);
            const price = priceFromReserves_UZIGQuote(
              { base_denom: ctx.base_denom, base_exp: Number(baseExp) },
              reserves
            );
            if (price != null && Number.isFinite(price) && price > 0) {
              // seed price (like we discussed)
              await upsertPrice(ctx.base_token_id, ctx.pool_id, price, true);
              debug('[fasttrack/init-price]', ctx.pair_contract, ctx.base_denom, 'px_zig=', price);

              // seed OHLCV at the minute of pool creation
              const bucket = minuteFloor(ctx.created_at);
              await upsertOHLCV1m({
                pool_id: ctx.pool_id,
                bucket_start: bucket,
                price,
                vol_zig: 0,     // no volume for the seed candle
                trade_inc: 0,   // zero trades for the seed
                // liquidity_zig: null // (optional) we can set later if you want to reflect TVL
              });
              debug('[fasttrack/init-ohlcv]', { pool_id: ctx.pool_id, bucket: bucket.toISOString(), price });
            } else {
              debug('[fasttrack/init-ohlcv skip] reserves not ready/non-positive', { pool_id: ctx.pool_id });
            }
          }
        }
      } catch (e) {
        warn('[fasttrack/init-ohlcv]', e.message);
      }

      info('[fasttrack] done for pool', ctx.pool_id);
    } catch (e) {
      warn('[fasttrack]', e.message);
    }
  });
}
