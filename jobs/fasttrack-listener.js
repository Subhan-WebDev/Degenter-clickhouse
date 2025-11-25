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

// price & ohlcv helpers
import {
  fetchPoolReserves,
  priceFromReserves_UZIGQuote,
  upsertPrice,
} from '../core/prices.js';
import { upsertOHLCV1m } from '../core/ohlcv.js';

/* -------------------------- helpers -------------------------- */

// minute-floor helper
function minuteFloor(d) {
  const t = new Date(d instanceof Date ? d : new Date(d));
  t.setSeconds(0, 0);
  return t;
}

/**
 * Helper to load pool+tokens by pair_contract (or pool_id in payload)
 * Now also pulls exponents so we can do generic decimal math.
 */
async function loadPoolContext(payload) {
  if (!payload) return null;

  const by = payload.pool_id
    ? { col: 'p.pool_id', val: payload.pool_id }
    : payload.pair_contract
    ? { col: 'p.pair_contract', val: payload.pair_contract }
    : null;

  if (!by) return null;

  const { rows } = await DB.query(
    `
    SELECT
      p.pool_id,
      p.pair_contract,
      p.is_uzig_quote,
      p.created_at,
      p.base_token_id,
      b.denom   AS base_denom,
      b.exponent AS base_exp,
      p.quote_token_id,
      q.denom   AS quote_denom,
      q.exponent AS quote_exp
    FROM pools p
    JOIN tokens b ON b.token_id = p.base_token_id
    JOIN tokens q ON q.token_id = p.quote_token_id
    WHERE ${by.col} = $1
    `,
    [by.val],
  );

  return rows[0] || null;
}

async function holdersCount(tokenId) {
  const { rows } = await DB.query(
    `SELECT holders_count::BIGINT AS c
     FROM token_holders_stats
     WHERE token_id = $1`,
    [tokenId],
  );
  return Number(rows?.[0]?.c || 0);
}

/**
 * Try to seed initial price + OHLCV from the FIRST provide_liquidity trade
 * for this pool. Falls back to LCD reserves if needed.
 *
 * ctx:
 *   pool_id, pair_contract, created_at,
 *   base_token_id, base_denom, base_exp,
 *   quote_token_id, quote_denom, quote_exp,
 *   is_uzig_quote
 */
async function seedInitialFromFirstProvide(ctx) {
  // Only meaningful for UZIG-quoted pools (your main case)
  if (ctx.quote_denom !== 'uzig') {
    debug('[fasttrack/init skip] non-uzig quote, ctx.quote_denom=', ctx.quote_denom);
    return;
  }

  // 1) try to read exponents from ctx; fallback to DB if missing.
  let baseExp = ctx.base_exp;
  if (baseExp == null) {
    const { rows: rB } = await DB.query(
      'SELECT exponent AS exp FROM tokens WHERE token_id = $1',
      [ctx.base_token_id],
    );
    baseExp = rB?.[0]?.exp;
  }
  if (baseExp == null) {
    debug('[fasttrack/init skip] base exponent missing', {
      pool_id: ctx.pool_id,
      base_token_id: ctx.base_token_id,
      denom: ctx.base_denom,
    });
    return;
  }

  // for uzig we know exponent 6, but also use DB value if present
  let quoteExp =
    ctx.quote_exp != null
      ? ctx.quote_exp
      : 6; // hard default for uzig

  // 2) fetch the FIRST provide_liquidity trade for this pool
  const { rows: trows } = await DB.query(
    `
    SELECT
      reserve_asset1_denom,
      reserve_asset1_amount_base,
      reserve_asset2_denom,
      reserve_asset2_amount_base,
      created_at
    FROM trades
    WHERE pool_id = $1
      AND action   = 'provide'
    ORDER BY height ASC, msg_index ASC
    LIMIT 1
    `,
    [ctx.pool_id],
  );

  const t = trows[0];
  if (!t) {
    debug('[fasttrack/init] no provide_liquidity trade yet, falling back to LCD reserves', {
      pool_id: ctx.pool_id,
    });
    // fall back to current LCD-based method
    await seedInitialFromLCD(ctx, Number(baseExp));
    return;
  }

  const d1 = t.reserve_asset1_denom;
  const d2 = t.reserve_asset2_denom;
  const a1Raw = Number(t.reserve_asset1_amount_base || 0);
  const a2Raw = Number(t.reserve_asset2_amount_base || 0);

  let RbRaw = null; // base raw
  let RqRaw = null; // quote raw (uzig)

  if (d1 === ctx.base_denom && d2 === ctx.quote_denom) {
    RbRaw = a1Raw;
    RqRaw = a2Raw;
  } else if (d2 === ctx.base_denom && d1 === ctx.quote_denom) {
    RbRaw = a2Raw;
    RqRaw = a1Raw;
  } else {
    debug('[fasttrack/init] cannot map provide reserves to base/quote, falling back to LCD', {
      pool_id: ctx.pool_id,
      base: ctx.base_denom,
      quote: ctx.quote_denom,
      d1,
      d2,
    });
    await seedInitialFromLCD(ctx, Number(baseExp));
    return;
  }

  if (!(RbRaw > 0) || !(RqRaw > 0)) {
    debug('[fasttrack/init] non-positive provide reserves, falling back to LCD', {
      pool_id: ctx.pool_id,
      RbRaw,
      RqRaw,
    });
    await seedInitialFromLCD(ctx, Number(baseExp));
    return;
  }

  const be = Number(baseExp);
  const qe = Number(quoteExp ?? 6);

  // human amounts:
  // base_human = RbRaw / 10^be
  // quote_human = RqRaw / 10^qe
  // price(base in quote) = quote_human / base_human
  const baseHuman = RbRaw / Math.pow(10, be);
  const quoteHuman = RqRaw / Math.pow(10, qe);

  if (!(baseHuman > 0) || !(quoteHuman > 0)) {
    debug('[fasttrack/init] non-positive human reserves, falling back to LCD', {
      pool_id: ctx.pool_id,
      baseHuman,
      quoteHuman,
    });
    await seedInitialFromLCD(ctx, be);
    return;
  }

  const price = quoteHuman / baseHuman;

  if (!(price > 0) || !Number.isFinite(price)) {
    debug('[fasttrack/init] computed non-finite price from provide, falling back to LCD', {
      pool_id: ctx.pool_id,
      price,
    });
    await seedInitialFromLCD(ctx, be);
    return;
  }

  // 3) Seed price + OHLCV
  await upsertPrice(ctx.base_token_id, ctx.pool_id, price, true);
  debug('[fasttrack/init-price/provide]', ctx.pair_contract, ctx.base_denom, 'px_zig=', price);

  // Seed OHLCV at minute of provide.created_at (not pair_created timestamp)
  const bucket = minuteFloor(t.created_at);
  await upsertOHLCV1m({
    pool_id: ctx.pool_id,
    bucket_start: bucket,
    price,
    vol_zig: 0,   // no trading volume for initial liquidity
    trade_inc: 0, // zero trades counted in this seed candle
  });
  debug('[fasttrack/init-ohlcv/provide]', {
    pool_id: ctx.pool_id,
    bucket: bucket.toISOString(),
    price,
  });
}

/**
 * Fallback: seed initial price & OHLCV from LCD pool query
 * (previous behavior), only used when first provide trade isn't usable.
 */
async function seedInitialFromLCD(ctx, baseExp) {
  try {
    if (ctx.quote_denom !== 'uzig') return;

    const reserves = await fetchPoolReserves(ctx.pair_contract);
    const price = priceFromReserves_UZIGQuote(
      { base_denom: ctx.base_denom, base_exp: Number(baseExp) },
      reserves,
    );

    if (!price || !Number.isFinite(price) || !(price > 0)) {
      debug('[fasttrack/init-ohlcv/LCD skip] reserves not ready/non-positive', {
        pool_id: ctx.pool_id,
      });
      return;
    }

    await upsertPrice(ctx.base_token_id, ctx.pool_id, price, true);
    debug('[fasttrack/init-price/LCD]', ctx.pair_contract, ctx.base_denom, 'px_zig=', price);

    const bucket = minuteFloor(ctx.created_at);
    await upsertOHLCV1m({
      pool_id: ctx.pool_id,
      bucket_start: bucket,
      price,
      vol_zig: 0,
      trade_inc: 0,
    });
    debug('[fasttrack/init-ohlcv/LCD]', {
      pool_id: ctx.pool_id,
      bucket: bucket.toISOString(),
      price,
    });
  } catch (e) {
    warn('[fasttrack/init-ohlcv/LCD]', e.message);
  }
}

/* ---------------------- main listener ------------------------ */

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
        quote: ctx.quote_denom,
      });

      // 2) metadata (both legs, in parallel; errors tolerated)
      await Promise.allSettled([
        refreshMetaOnce(ctx.base_denom),
        refreshMetaOnce(ctx.quote_denom),
      ]);

      // 3) holders for base token (and optionally quote if non-uzig)
      await Promise.allSettled([
        refreshHoldersOnce(ctx.base_token_id, ctx.base_denom),
        ctx.quote_denom !== 'uzig'
          ? refreshHoldersOnce(ctx.quote_token_id, ctx.quote_denom)
          : Promise.resolve(),
      ]);

      // log current counts
      const baseHC = await holdersCount(ctx.base_token_id);
      const quoteHC =
        ctx.quote_denom !== 'uzig'
          ? await holdersCount(ctx.quote_token_id)
          : 0;
      debug('[fasttrack] holders counts', { base: baseHC, quote: quoteHC });

      // optional retry if zero
      if (baseHC === 0) {
        debug(
          '[fasttrack] base holders 0, retrying once…',
          ctx.base_denom,
        );
        await refreshHoldersOnce(ctx.base_token_id, ctx.base_denom);
      }
      if (ctx.quote_denom !== 'uzig' && quoteHC === 0) {
        debug(
          '[fasttrack] quote holders 0, retrying once…',
          ctx.quote_denom,
        );
        await refreshHoldersOnce(ctx.quote_token_id, ctx.quote_denom);
      }

      // 4) security scan (base + maybe quote)
      await Promise.allSettled([
        scanTokenOnce(ctx.base_token_id, ctx.base_denom),
        ctx.quote_denom !== 'uzig'
          ? scanTokenOnce(ctx.quote_token_id, ctx.quote_denom)
          : Promise.resolve(),
      ]);

      // 5) matrix (pool + tokens) across all standard buckets
      await Promise.allSettled([
        refreshPoolMatrixOnce(ctx.pool_id),
        refreshTokenMatrixOnce(ctx.base_token_id),
        ctx.quote_denom !== 'uzig'
          ? refreshTokenMatrixOnce(ctx.quote_token_id)
          : Promise.resolve(),
      ]);

      // 6) Initial price & OHLCV seed:
      //    prefer first provide_liquidity trade reserves;
      //    fall back to LCD pool reserves if needed.
      try {
        await seedInitialFromFirstProvide(ctx);
      } catch (e) {
        warn('[fasttrack/init-from-provide]', e.message);
      }

      info('[fasttrack] done for pool', ctx.pool_id);
    } catch (e) {
      warn('[fasttrack]', e.message);
    }
  });
}
