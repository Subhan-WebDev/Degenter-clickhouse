// api/routes/swap.js
import express from 'express';
import { DB } from '../../lib/db.js';
import { resolveTokenId, getZigUsd } from '../util/resolve-token.js';

const router = express.Router();

/* ───────────────────────── helpers ───────────────────────── */

const UZIG_ALIASES = new Set(['uzig','zig','uZIG','UZIG']);
const isUzigRef = (s) => !!s && UZIG_ALIASES.has(String(s).trim().toLowerCase());

async function resolveRef(ref) {
  if (isUzigRef(ref)) return { type: 'uzig' };
  const tok = await resolveTokenId(ref); // must return { token_id, denom, symbol, exponent, ... }
  if (!tok) return null;
  return { type: 'token', token: tok };
}

/** Oroswap pair type → taker fee fraction */
function pairFee(pairType) {
  if (!pairType) return 0.003;
  const t = String(pairType).toLowerCase();
  if (t === 'xyk') return 0.0001;
  if (t === 'concentrated') return 0.01;
  const m = t.match(/xyk[_-](\d+)/);
  if (m) {
    const bps = Number(m[1]);
    if (Number.isFinite(bps)) return bps / 10_000;
  }
  return 0.003;
}

/** XYK simulation (fee-on-input). Rz = zig reserve, Rt = token reserve. */
function simulateXYK({ fromIsZig, amountIn, Rz, Rt, fee }) {
  if (!(Rz > 0 && Rt > 0) || !(amountIn > 0)) {
    return { out: 0, price: 0, impact: 0 };
  }
  const mid = Rz / Rt; // zig per token
  const xin = amountIn * (1 - fee);

  if (fromIsZig) {
    // ZIG -> Token
    const outToken = (xin * Rt) / (Rz + xin);
    const effZigPerToken = amountIn / Math.max(outToken, 1e-18);
    const impact = mid > 0 ? (effZigPerToken / mid) - 1 : 0;
    return { out: outToken, price: effZigPerToken, impact };
  } else {
    // Token -> ZIG
    const outZig = (xin * Rz) / (Rt + xin);
    const effZigPerToken = outZig / amountIn; // executable zig per 1 token
    const impact = mid > 0 ? (mid / Math.max(effZigPerToken, 1e-18)) - 1 : 0;
    return { out: outZig, price: effZigPerToken, impact };
  }
}

/** Load all UZIG-quoted pools for a token, including mid price & reserves (display units). */
async function loadUzigPoolsForToken(tokenId, { minTvlZig = 0 } = {}) {
  const { rows } = await DB.query(
    `
    SELECT
      p.pool_id,
      p.pair_contract,
      p.pair_type,
      pr.price_in_zig,           -- mid zig per token
      ps.reserve_base_base   AS res_base_base,
      ps.reserve_quote_base  AS res_quote_base,
      tb.exponent            AS base_exp,
      tq.exponent            AS quote_exp,
      COALESCE(pm.tvl_zig,0) AS tvl_zig,
      tb.denom               AS base_denom,
      tq.denom               AS quote_denom
    FROM pools p
    JOIN prices pr           ON pr.pool_id = p.pool_id AND pr.token_id = $1
    LEFT JOIN pool_state ps  ON ps.pool_id = p.pool_id
    JOIN tokens tb           ON tb.token_id = p.base_token_id
    JOIN tokens tq           ON tq.token_id = p.quote_token_id
    LEFT JOIN pool_matrix pm ON pm.pool_id = p.pool_id AND pm.bucket = '24h'
    WHERE p.is_uzig_quote = TRUE
    `,
    [tokenId]
  );

  return rows
    .map(r => {
      const Rt = Number(r.res_base_base  || 0) / Math.pow(10, Number(r.base_exp  || 0)); // token reserve
      const Rz = Number(r.res_quote_base || 0) / Math.pow(10, Number(r.quote_exp || 0)); // zig reserve
      return {
        poolId:       String(r.pool_id),
        pairContract: r.pair_contract,
        pairType:     r.pair_type,
        priceInZig:   Number(r.price_in_zig || 0), // **mid** zig per token
        tokenReserve: Rt,
        zigReserve:   Rz,
        tvlZig:       Number(r.tvl_zig || 0),
      };
    })
    .filter(p => p.tvlZig >= minTvlZig);
}

/** Pick best pool by sim (maximize out). */
function pickBySimulation(pools, side, { fromIsZig, amountIn }) {
  let best = null;
  for (const p of pools) {
    const fee = pairFee(p.pairType);
    const hasRes = p.zigReserve > 0 && p.tokenReserve > 0;
    const sim = hasRes
      ? simulateXYK({ fromIsZig, amountIn, Rz: p.zigReserve, Rt: p.tokenReserve, fee })
      : null;
    const score = sim ? sim.out : 0;
    const cand = { ...p, fee, sim, score };
    if (!best || cand.score > best.score) best = cand;
  }
  return best;
}

/** Default notional (~$100) when amt not provided. */
function defaultAmount(side, { zigUsd, pools }) {
  const targetUsd = 100;
  const zigAmt = targetUsd / Math.max(zigUsd, 1e-9);
  if (side === 'buy') return zigAmt; // from ZIG
  const avgMid = pools.length
    ? pools.reduce((s, p) => s + (p.priceInZig || 0), 0) / pools.length
    : 1;
  return zigAmt / Math.max(avgMid, 1e-12); // from token
}

/** Render diagnostics block for one leg. Includes both EXEC (sim) and MID (DB) prices. */
function makePairBlock({ side, pool, sim, fee, zigUsd, amountIn }) {
  const price_native_exec = sim ? sim.price : null; // executable zig per 1 token (for that amount)
  const price_usd_exec    = price_native_exec != null ? price_native_exec * zigUsd : null;

  const price_native_mid  = pool.priceInZig;        // mid zig per 1 token (DB)
  const price_usd_mid     = price_native_mid * zigUsd;

  return {
    poolId: pool.poolId,
    pairContract: pool.pairContract,
    pairType: pool.pairType,
    side,
    // EXEC (diagnostic)
    price_native_exec,
    price_usd_exec,
    // MID (stable unit valuation)
    price_native_mid,
    price_usd_mid,
    // sim payload
    amount_in: amountIn ?? null,
    amount_out: sim ? sim.out : null,
    price_impact: sim ? sim.impact : null,
    fee
  };
}

/* ─────────────────────── per-side selectors ─────────────────────── */

async function bestBuyPool(tokenId, { amountIn, minTvlZig, zigUsd }) {
  const pools = await loadUzigPoolsForToken(tokenId, { minTvlZig });
  if (!pools.length) return null;
  const amt = Number.isFinite(amountIn) ? Number(amountIn) : defaultAmount('buy', { zigUsd, pools });
  const pick = pickBySimulation(pools, 'buy', { fromIsZig: true, amountIn: amt });
  if (!pick) return null;
  return { ...pick, amtUsed: amt };
}

async function bestSellPool(tokenId, { amountIn, minTvlZig, zigUsd }) {
  const pools = await loadUzigPoolsForToken(tokenId, { minTvlZig });
  if (!pools.length) return null;
  const amt = Number.isFinite(amountIn) ? Number(amountIn) : defaultAmount('sell', { zigUsd, pools });
  const pick = pickBySimulation(pools, 'sell', { fromIsZig: false, amountIn: amt });
  if (!pick) return null;
  return { ...pick, amtUsed: amt };
}

/* ─────────────────────────── route API ─────────────────────────── */

router.get('/', async (req, res) => {
  try {
    const fromRef = req.query.from;
    const toRef   = req.query.to;
    if (!fromRef || !toRef) return res.status(400).json({ success:false, error:'missing from/to' });

    const zigUsd    = await getZigUsd();           // from exchange_rates
    const amt       = req.query.amt ? Number(req.query.amt) : undefined;
    const minTvlZig = req.query.minTvl ? Number(req.query.minTvl) : 0;

    const from = await resolveRef(fromRef);
    const to   = await resolveRef(toRef);
    if (!from) return res.status(404).json({ success:false, error:'from token not found' });
    if (!to)   return res.status(404).json({ success:false, error:'to token not found' });

    /* ── ZIG → TOKEN (BUY) ─────────────────────────────────────── */
    if (from.type === 'uzig' && to.type === 'token') {
      const buy = await bestBuyPool(to.token.token_id, { amountIn: amt, minTvlZig, zigUsd });
      if (!buy) {
        return res.json({ success:true, data:{
          route:['uzig', to.token.denom || to.token.symbol], pairs:[],
          price_native:null, price_usd:null, cross:{ zig_per_from:1, usd_per_from:zigUsd },
          usd_baseline:{ from_usd: zigUsd, to_usd: null }, source:'direct_uzig'
        }});
      }

      const pairBlock = makePairBlock({
        side:'buy', pool: buy, sim: buy.sim, fee: buy.fee, zigUsd, amountIn: buy.amtUsed
      });

      // top snapshot (executable per-unit for that amount)
      const price_native = pairBlock.price_native_exec;
      const price_usd    = pairBlock.price_usd_exec;

      // baselines for $ labels in UI
      const from_usd = zigUsd;
      const to_usd   = pairBlock.price_native_mid * zigUsd; // mid( token )

      return res.json({
        success: true,
        data: {
          route: ['uzig', to.token.denom || to.token.symbol || String(to.token.token_id)],
          pairs: [ pairBlock ],
          price_native,                      // exec zig per 1 token (diagnostic)
          price_usd,                         // exec USD per 1 token (diagnostic)
          cross: { zig_per_from: 1, usd_per_from: zigUsd },
          usd_baseline: { from_usd, to_usd },// **UI should use these for $**
          source: 'direct_uzig',
          diagnostics: {
            side: 'buy',
            poolId: buy.poolId,
            pairType: buy.pairType,
            tvl_zig: buy.tvlZig,
            reserves: { zig: buy.zigReserve, token: buy.tokenReserve },
            sim: buy.sim || null,
            params: { amt: amt ?? null, minTvlZig }
          }
        }
      });
    }

    /* ── TOKEN → ZIG (SELL) ────────────────────────────────────── */
    if (from.type === 'token' && to.type === 'uzig') {
      const sell = await bestSellPool(from.token.token_id, { amountIn: amt, minTvlZig, zigUsd });
      if (!sell) {
        return res.json({ success:true, data:{
          route:[from.token.denom || from.token.symbol, 'uzig'], pairs:[],
          price_native:null, price_usd:null,
          cross:{ zig_per_from:null, usd_per_from:null },
          usd_baseline:{ from_usd: null, to_usd: zigUsd }, source:'direct_uzig'
        }});
      }

      const pairBlock = makePairBlock({
        side:'sell', pool: sell, sim: sell.sim, fee: sell.fee, zigUsd, amountIn: sell.amtUsed
      });

      const price_native = pairBlock.price_native_exec; // exec zig per 1 token
      const price_usd    = price_native != null ? price_native * zigUsd : null;

      const from_usd = sell.priceInZig * zigUsd; // mid( token )
      const to_usd   = zigUsd;

      return res.json({
        success: true,
        data: {
          route: [from.token.denom || from.token.symbol || String(from.token.token_id), 'uzig'],
          pairs: [ pairBlock ],
          price_native,
          price_usd,
          cross: { zig_per_from: price_native, usd_per_from: from_usd },
          usd_baseline: { from_usd, to_usd },  // **UI should use these for $**
          source: 'direct_uzig',
          diagnostics: {
            side: 'sell',
            poolId: sell.poolId,
            pairType: sell.pairType,
            tvl_zig: sell.tvlZig,
            reserves: { zig: sell.zigReserve, token: sell.tokenReserve },
            sim: sell.sim || null,
            params: { amt: amt ?? null, minTvlZig }
          }
        }
      });
    }

    /* ── TOKEN A → TOKEN B (via UZIG) ──────────────────────────── */
    if (from.type === 'token' && to.type === 'token') {
      const sellA = await bestSellPool(from.token.token_id, { amountIn: amt, minTvlZig, zigUsd });
      const zigOut = sellA?.sim ? sellA.sim.out : undefined;
      const buyB  = await bestBuyPool(to.token.token_id,   { amountIn: zigOut, minTvlZig, zigUsd });

      if (!sellA || !buyB) {
        return res.json({
          success: true,
          data: {
            route: [
              from.token.denom || from.token.symbol || String(from.token.token_id),
              'uzig',
              to.token.denom || to.token.symbol || String(to.token.token_id)
            ],
            pairs: [],
            price_native: null,
            price_usd: null,
            cross: { zig_per_from: null, usd_per_from: null },
            usd_baseline: { from_usd: null, to_usd: null },
            source: 'via_uzig',
            diagnostics: { sellA: !!sellA, buyB: !!buyB }
          }
        });
      }

      const sellBlock = makePairBlock({
        side:'sell', pool: sellA, sim: sellA.sim, fee: sellA.fee, zigUsd, amountIn: sellA.amtUsed
      });
      const buyBlock  = makePairBlock({
        side:'buy',  pool: buyB,  sim: buyB.sim,  fee: buyB.fee,  zigUsd, amountIn: buyB.amtUsed
      });

      // Executable cross-rate (B per 1 A): (zig/A) / (zig/B)
      const bPerA = sellA.priceInZig / Math.max(buyB.priceInZig, 1e-18);

      // Baselines for $ labels
      const from_usd = sellA.priceInZig * zigUsd; // mid(A)
      const to_usd   = buyB.priceInZig  * zigUsd; // mid(B)

      return res.json({
        success: true,
        data: {
          route: [
            from.token.denom || from.token.symbol || String(from.token.token_id),
            'uzig',
            to.token.denom || to.token.symbol || String(to.token.token_id)
          ],
          pairs: [ sellBlock, buyBlock ],          // exec + mid per leg (diagnostic)
          price_native: bPerA,                     // B per A (exec snapshot)
          price_usd: null,
          cross: { zig_per_from: sellA.priceInZig, usd_per_from: from_usd }, // exec sell-side rate
          usd_baseline: { from_usd, to_usd },      // **UI should use these for $**
          source: 'via_uzig',
          diagnostics: {
            sell_leg: {
              side: 'sell',
              poolId: sellA.poolId,
              pairType: sellA.pairType,
              tvl_zig: sellA.tvlZig,
              reserves: { zig: sellA.zigReserve, token: sellA.tokenReserve },
              sim: sellA.sim || null
            },
            buy_leg: {
              side: 'buy',
              poolId: buyB.poolId,
              pairType: buyB.pairType,
              tvl_zig: buyB.tvlZig,
              reserves: { zig: buyB.zigReserve, token: buyB.tokenReserve },
              sim: buyB.sim || null
            }
          }
        }
      });
    }

    return res.status(400).json({ success:false, error:'unsupported route (check from/to)' });
  } catch (e) {
    console.error('[swap] error:', e);
    res.status(500).json({ success:false, error: e.message });
  }
});

export default router;
