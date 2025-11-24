// api/routes/tokens.js
import express from 'express';
import { DB } from '../../lib/db.js';
import { resolveTokenId, getZigUsd } from '../util/resolve-token.js';
// keep legacy utilities for optional paths
import { resolvePoolSelection, changePctForMinutes } from '../util/pool-select.js';
import { getCandles, ensureTf } from '../util/ohlcv-agg.js';
import e from 'express';
import log from '../../lib/log.js';

const router = express.Router();

const toNum = x => (x == null ? null : Number(x));
const disp = (base, exp) => (base == null ? null : Number(base) / (10 ** (exp || 0)));
const esc = v => `'${String(v).replace(/'/g, "''")}'`;

/* ───────────────────────── helpers FROM /swap ───────────────────────── */

// Oroswap pair type → taker fee fraction (same as /swap)
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

/** XYK simulation (fee-on-input). Rz = zig reserve, Rt = token reserve. (same as /swap) */
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

/**
 * Load all UZIG-quoted pools for a token, including mid price & reserves (display units). (ClickHouse version)
 * Uses argMax() for latest price per (pool, token).
 */
/**
 * Load all UZIG-quoted pools for a token, including mid price & reserves (display units). (ClickHouse version)
 * Uses argMax() for latest price per (pool, token).
 */
async function loadUzigPoolsForToken(tokenId, { minTvlZig = 0 } = {}) {
  // 🔒 Guard against bad tokenId so ClickHouse never sees '' / 'undefined'
  const tid = Number(tokenId);
  if (!Number.isFinite(tid) || tid <= 0) {
    return []; // no valid pools
  }

  const { rows } = await DB.query(
    `
    WITH latest_prices AS (
      SELECT
        pool_id,
        token_id,
        argMax(price_in_zig, updated_at) AS price_in_zig
      FROM prices
      GROUP BY pool_id, token_id
    )
    SELECT
      p.pool_id           AS pool_id,
      p.pair_contract     AS pair_contract,
      p.pair_type         AS pair_type,
      lp.price_in_zig     AS price_in_zig,
      ps.reserve_base_base   AS res_base_base,
      ps.reserve_quote_base  AS res_quote_base,
      tb.exponent            AS base_exp,
      tq.exponent            AS quote_exp,
      COALESCE(pm.tvl_zig, 0) AS tvl_zig
    FROM pools AS p
    INNER JOIN tokens AS tb ON tb.token_id = p.base_token_id
    INNER JOIN tokens AS tq ON tq.token_id = p.quote_token_id
    LEFT JOIN pool_state AS ps
      ON ps.pool_id = p.pool_id
    LEFT JOIN pool_matrix AS pm
      ON pm.pool_id = p.pool_id
     AND pm.bucket = '24h'
    LEFT JOIN latest_prices AS lp
      ON lp.pool_id  = p.pool_id
     AND lp.token_id = p.base_token_id
    WHERE p.is_uzig_quote = 1
      AND p.base_token_id = $1
    `,
    [String(tid)]        // 👈 always a clean "29", never "", never "undefined"
  );

  return rows
    .map((r) => {
      const poolIdRaw = r.pool_id ?? r.POOL_ID ?? r.poolId ?? null;
      if (poolIdRaw == null) return null;

      const Rt = Number(r.res_base_base || 0) / Math.pow(10, Number(r.base_exp || 0));   // token reserve
      const Rz = Number(r.res_quote_base || 0) / Math.pow(10, Number(r.quote_exp || 0)); // zig reserve

      return {
        poolId: String(poolIdRaw),
        pairContract: r.pair_contract,
        pairType: r.pair_type,
        priceInZig: Number(r.price_in_zig || 0), // mid zig per token
        tokenReserve: Rt,
        zigReserve: Rz,
        tvlZig: Number(r.tvl_zig || 0),
      };
    })
    .filter((p) => p && p.tvlZig >= minTvlZig);
}


/** Pick best pool by sim (maximize out). (same as /swap) */
function pickBySimulation(pools, { fromIsZig, amountIn }) {
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

/** Default notional (~$100) when amt not provided. (same as /swap) */
function defaultAmount(side, { zigUsd, pools }) {
  const targetUsd = 100;
  const zigAmt = targetUsd / Math.max(zigUsd, 1e-9);
  if (side === 'buy') return zigAmt; // from ZIG
  const avgMid = pools.length
    ? pools.reduce((s, p) => s + (p.priceInZig || 0), 0) / pools.length
    : 1;
  return zigAmt / Math.max(avgMid, 1e-12); // from token
}

/** best pool for ZIG→TOKEN (buy) and TOKEN→ZIG (sell) — identical to /swap **/
async function bestBuyPool(tokenId, { amountIn, minTvlZig, zigUsd }) {
  const pools = await loadUzigPoolsForToken(tokenId, { minTvlZig });
  if (!pools.length) return null;
  const amt = Number.isFinite(amountIn) ? Number(amountIn) : defaultAmount('buy', { zigUsd, pools });
  const pick = pickBySimulation(pools, { fromIsZig: true, amountIn: amt });
  if (!pick) return null;
  return { ...pick, amtUsed: amt };
}

async function bestSellPool(tokenId, { amountIn, minTvlZig, zigUsd }) {
  const pools = await loadUzigPoolsForToken(tokenId, { minTvlZig });
  if (!pools.length) return null;
  const amt = Number.isFinite(amountIn) ? Number(amountIn) : defaultAmount('sell', { zigUsd, pools });
  const pick = pickBySimulation(pools, { fromIsZig: false, amountIn: amt });
  if (!pick) return null;
  return { ...pick, amtUsed: amt };
}

/* ================================ LIST: GET /tokens ================================ */
/* Now uses bestSellPool() for change% pool selection. Optional includeBest=1 returns the chosen pool. */
router.get('/', async (req, res) => {
  try {
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const sort = (req.query.sort || 'mcap').toLowerCase();
    const dir  = (req.query.dir || 'desc').toLowerCase() === 'asc' ? 'ASC' : 'DESC';
    const includeChange = req.query.includeChange === '1';
    const includeBest   = req.query.includeBest === '1';
    const minTvlZigBest = Number(req.query.minBestTvl || '0');
    const amtParam      = req.query.amt ? Number(req.query.amt) : undefined; // optional sizing to mirror /swap
    const limit  = Math.max(1, Math.min(parseInt(req.query.limit || '50', 10), 200));
    const offset = Math.max(0, parseInt(req.query.offset || '0', 10));
    const zigUsd = await getZigUsd();

    const bucketLit = esc(bucket);

    const sql = `
      WITH agg AS (
        SELECT
          p.base_token_id AS token_id,
          SUM(pm.vol_buy_zig + pm.vol_sell_zig) AS vol_zig,
          SUM(pm.tx_buy + pm.tx_sell) AS tx
        FROM pool_matrix AS pm
        INNER JOIN pools AS p ON p.pool_id = pm.pool_id
        WHERE pm.bucket = ${bucketLit}
        GROUP BY p.base_token_id
      ),
      base AS (
        SELECT
          t.token_id,
          t.denom,
          t.symbol,
          t.name,
          t.image_uri,
          t.created_at,
          tm.price_in_zig,
          tm.mcap_zig,
          tm.fdv_zig,
          tm.holders,
          a.vol_zig,
          a.tx
        FROM tokens AS t
        LEFT JOIN token_matrix AS tm
          ON tm.token_id = t.token_id
         AND tm.bucket = ${bucketLit}
        LEFT JOIN agg AS a ON a.token_id = t.token_id
      ),
      ranked AS (
        SELECT
          b.*,
          count() OVER () AS total
        FROM base AS b
      )
      SELECT *
      FROM ranked
      ORDER BY
        ${
          sort === 'created' ? `created_at ${dir}` :
          sort === 'volume'  ? `COALESCE(vol_zig, 0) ${dir}` :
          sort === 'tx'      ? `COALESCE(tx, 0) ${dir}` :
          sort === 'price'   ? `COALESCE(price_in_zig, 0) ${dir}` :
          sort === 'traders' ? `COALESCE(holders, 0) ${dir}` :
                               `COALESCE(mcap_zig, 0) ${dir}`
        }
      LIMIT ${limit}
      OFFSET ${offset}
    `;

    const rows = await DB.query(sql);

    // compute best pool per row (used for change%, and optionally returned)
    const bestMap = new Map();
    if ((includeChange || includeBest) && rows.rows.length) {
      const picks = await Promise.all(rows.rows.map(async r => {
        const pick = await bestSellPool(r.token_id, { amountIn: amtParam, minTvlZig: minTvlZigBest, zigUsd });
        return { id: r.token_id, pick };
      }));
      for (const x of picks) bestMap.set(String(x.id), x.pick);
    }

    // optional change% from best pool (sell leg)
    const changeMap = new Map();
    if (includeChange && rows.rows.length) {
      await Promise.all(rows.rows.map(async r => {
        const pick = bestMap.get(String(r.token_id));
        if (!pick?.poolId) { changeMap.set(String(r.token_id), null); return; }
        const pct = await changePctForMinutes(pick.poolId, 1440);
        changeMap.set(String(r.token_id), pct);
      }));
    }

    const data = rows.rows.map(r => {
      const priceN = toNum(r.price_in_zig);
      const mcapN  = toNum(r.mcap_zig);
      const fdvN   = toNum(r.fdv_zig);
      const volN   = toNum(r.vol_zig) || 0;

      const base = {
        tokenId: r.token_id,
        denom: r.denom,
        symbol: r.symbol,
        name: r.name,
        imageUri: r.image_uri,
        createdAt: r.created_at,
        priceNative: priceN,
        priceUsd: priceN != null ? priceN * zigUsd : null,
        mcapNative: mcapN,
        mcapUsd: mcapN != null ? mcapN * zigUsd : null,
        fdvNative: fdvN,
        fdvUsd: fdvN != null ? fdvN * zigUsd : null,
        holders: toNum(r.holders) || 0,
        volNative: volN,
        volUsd: volN * zigUsd,
        tx: toNum(r.tx) || 0,
        ...(includeChange ? { change24hPct: changeMap.get(String(r.token_id)) ?? null } : {})
      };

      if (includeBest) {
        const bp = bestMap.get(String(r.token_id)) || null;
        base.bestPool = bp ? {
          poolId: bp.poolId,
          pairContract: bp.pairContract,
          pairType: bp.pairType,
          fee: bp.fee,
          priceNativeMid: bp.priceInZig,
          tvlNative: bp.tvlZig,
          reserves: { zig: bp.zigReserve, token: bp.tokenReserve },
          sim: bp.sim || null,
          amtUsed: bp.amtUsed
        } : null;
      }
      return base;
    });

    const total = rows.rows[0]?.total ?? 0;
    res.json({ success: true, data, meta: { bucket, sort, dir, limit, offset, total, includeBest: includeBest ? 1 : 0 } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/* ============================ GAINERS / LOSERS ============================ */
/* Now change% computed from bestSellPool() like /swap selects a pool. */
router.get('/gainers', async (req, res) => {
  try {
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const outLimit = Math.max(1, Math.min(parseInt(req.query.limit || '100', 10), 200));
    const outOffset = Math.max(0, parseInt(req.query.offset || '0', 10));
    const minTvlZigBest = Number(req.query.minBestTvl || '0');
    const amtParam      = req.query.amt ? Number(req.query.amt) : undefined;
    const zigUsd = await getZigUsd();

    const FETCH_LIMIT = 1000;
    const bucketLit = esc(bucket);

    const sql = `
      WITH agg AS (
        SELECT
          p.base_token_id AS token_id,
          SUM(pm.vol_buy_zig + pm.vol_sell_zig) AS vol_zig,
          SUM(pm.tx_buy + pm.tx_sell) AS tx
        FROM pool_matrix AS pm
        INNER JOIN pools AS p ON p.pool_id = pm.pool_id
        WHERE pm.bucket = ${bucketLit}
        GROUP BY p.base_token_id
      ),
      base AS (
        SELECT
          t.token_id,
          t.denom,
          t.symbol,
          t.name,
          t.image_uri,
          t.created_at,
          tm.price_in_zig,
          tm.mcap_zig,
          tm.fdv_zig,
          tm.holders,
          a.vol_zig,
          a.tx
        FROM tokens AS t
        LEFT JOIN token_matrix AS tm
          ON tm.token_id = t.token_id
         AND tm.bucket = ${bucketLit}
        LEFT JOIN agg AS a ON a.token_id = t.token_id
      )
      SELECT *
      FROM base
      LIMIT ${FETCH_LIMIT}
      OFFSET 0
    `;

    const rows = await DB.query(sql);

    const changeMap = new Map();
    await Promise.all(rows.rows.map(async r => {
      const pick = await bestSellPool(r.token_id, { amountIn: amtParam, minTvlZig: minTvlZigBest, zigUsd });
      if (!pick?.poolId) { changeMap.set(String(r.token_id), null); return; }
      const pct = await changePctForMinutes(pick.poolId, 1440);
      changeMap.set(String(r.token_id), pct);
    }));

    const data = rows.rows.map(r => {
      const priceN = r.price_in_zig == null ? null : Number(r.price_in_zig);
      const volN   = r.vol_zig == null ? 0 : Number(r.vol_zig);
      const mcapN  = r.mcap_zig == null ? null : Number(r.mcap_zig);
      const fdvN   = r.fdv_zig  == null ? null : Number(r.fdv_zig);
      return {
        tokenId: r.token_id,
        denom: r.denom,
        symbol: r.symbol,
        name: r.name,
        imageUri: r.image_uri,
        createdAt: r.created_at,
        priceNative: priceN,
        priceUsd: priceN != null ? priceN * zigUsd : null,
        mcapNative: mcapN, mcapUsd: mcapN != null ? mcapN * zigUsd : null,
        fdvNative: fdvN,  fdvUsd:  fdvN  != null ? fdvN  * zigUsd : null,
        holders: Number(r.holders || 0),
        volNative: volN, volUsd: volN * zigUsd,
        tx: Number(r.tx || 0),
        change24hPct: changeMap.get(String(r.token_id))
      };
    });

    const sorted = data
      .filter(x => x.change24hPct != null)
      .sort((a,b) => b.change24hPct - a.change24hPct);

    const total = sorted.length;
    const pageItems = sorted.slice(outOffset, outOffset + outLimit);

    res.json({
      success: true,
      data: pageItems,
      meta: { board: 'gainers', bucket, limit: outLimit, offset: outOffset, total }
    });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

router.get('/losers', async (req, res) => {
  try {
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const outLimit = Math.max(1, Math.min(parseInt(req.query.limit || '100', 10), 200));
    const outOffset = Math.max(0, parseInt(req.query.offset || '0', 10));
    const minTvlZigBest = Number(req.query.minBestTvl || '0');
    const amtParam      = req.query.amt ? Number(req.query.amt) : undefined;
    const zigUsd = await getZigUsd();

    const FETCH_LIMIT = 1000;
    const bucketLit = esc(bucket);

    const sql = `
      WITH agg AS (
        SELECT
          p.base_token_id AS token_id,
          SUM(pm.vol_buy_zig + pm.vol_sell_zig) AS vol_zig,
          SUM(pm.tx_buy + pm.tx_sell) AS tx
        FROM pool_matrix AS pm
        INNER JOIN pools AS p ON p.pool_id = pm.pool_id
        WHERE pm.bucket = ${bucketLit}
        GROUP BY p.base_token_id
      ),
      base AS (
        SELECT
          t.token_id,
          t.denom,
          t.symbol,
          t.name,
          t.image_uri,
          t.created_at,
          tm.price_in_zig,
          tm.mcap_zig,
          tm.fdv_zig,
          tm.holders,
          a.vol_zig,
          a.tx
        FROM tokens AS t
        LEFT JOIN token_matrix AS tm
          ON tm.token_id = t.token_id
         AND tm.bucket = ${bucketLit}
        LEFT JOIN agg AS a ON a.token_id = t.token_id
      )
      SELECT *
      FROM base
      LIMIT ${FETCH_LIMIT}
      OFFSET 0
    `;

    const rows = await DB.query(sql);

    const changeMap = new Map();
    await Promise.all(rows.rows.map(async r => {
      const pick = await bestSellPool(r.token_id, { amountIn: amtParam, minTvlZig: minTvlZigBest, zigUsd });
      if (!pick?.poolId) { changeMap.set(String(r.token_id), null); return; }
      const pct = await changePctForMinutes(pick.poolId, 1440);
      changeMap.set(String(r.token_id), pct);
    }));

    const data = rows.rows.map(r => {
      const priceN = r.price_in_zig == null ? null : Number(r.price_in_zig);
      const volN   = r.vol_zig == null ? 0 : Number(r.vol_zig);
      const mcapN  = r.mcap_zig == null ? null : Number(r.mcap_zig);
      const fdvN   = r.fdv_zig  == null ? null : Number(r.fdv_zig);
      return {
        tokenId: r.token_id,
        denom: r.denom,
        symbol: r.symbol,
        name: r.name,
        imageUri: r.image_uri,
        createdAt: r.created_at,
        priceNative: priceN,
        priceUsd: priceN != null ? priceN * zigUsd : null,
        mcapNative: mcapN, mcapUsd: mcapN != null ? mcapN * zigUsd : null,
        fdvNative: fdvN,  fdvUsd:  fdvN  != null ? fdvN  * zigUsd : null,
        holders: Number(r.holders || 0),
        volNative: volN, volUsd: volN * zigUsd,
        tx: Number(r.tx || 0),
        change24hPct: changeMap.get(String(r.token_id))
      };
    });

    const sorted = data
      .filter(x => x.change24hPct != null)
      .sort((a,b) => a.change24hPct - b.change24hPct);

    const total = sorted.length;
    const pageItems = sorted.slice(outOffset, outOffset + outLimit);

    res.json({
      success: true,
      data: pageItems,
      meta: { board: 'losers', bucket, limit: outLimit, offset: outOffset, total }
    });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/* =========================== SWAP LIST: GET /tokens/swap-list =========================== */
/* Optional includeBest=1 — returns the same bestSellPool() block used by /swap. */
router.get('/swap-list', async (req, res) => {
  try {
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const limit  = Math.max(1, Math.min(parseInt(req.query.limit || '200', 10), 500));
    const offset = Math.max(0, parseInt(req.query.offset || '0', 10));
    const includeBest   = req.query.includeBest === '1';
    const minTvlZigBest = Number(req.query.minBestTvl || '0');
    const amtParam      = req.query.amt ? Number(req.query.amt) : undefined;
    const zigUsd = await getZigUsd();

    const bucketLit = esc(bucket);

    const sql = `
      WITH agg AS (
        SELECT
          p.base_token_id AS token_id,
          SUM(pm.vol_buy_zig + pm.vol_sell_zig) AS vol_zig,
          SUM(pm.tx_buy + pm.tx_sell) AS tx,
          SUM(pm.tvl_zig) AS tvl_zig
        FROM pool_matrix AS pm
        INNER JOIN pools AS p ON p.pool_id = pm.pool_id
        WHERE pm.bucket = ${bucketLit}
        GROUP BY p.base_token_id
      )
      SELECT
        t.token_id,
        t.symbol,
        t.name,
        t.denom,
        t.image_uri,
        t.exponent,
        tm.price_in_zig,
        tm.mcap_zig,
        tm.fdv_zig,
        a.vol_zig,
        a.tx,
        a.tvl_zig
      FROM tokens AS t
      LEFT JOIN token_matrix AS tm
        ON tm.token_id = t.token_id
       AND tm.bucket = ${bucketLit}
      LEFT JOIN agg AS a ON a.token_id = t.token_id
      ORDER BY COALESCE(a.vol_zig, 0) DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `;

    const rows = await DB.query(sql);

    // best pool (sell leg) per token if requested
    const bestMap = new Map();
    if (includeBest && rows.rows.length) {
      const picks = await Promise.all(rows.rows.map(async r => {
        const pick = await bestSellPool(r.token_id, { amountIn: amtParam, minTvlZig: minTvlZigBest, zigUsd });
        return { id: r.token_id, pick };
      }));
      for (const x of picks) bestMap.set(String(x.id), x.pick);
    }

    const data = rows.rows.map(r => {
      const priceN = toNum(r.price_in_zig);
      const mcapN  = toNum(r.mcap_zig);
      const fdvN   = toNum(r.fdv_zig);
      const volN   = toNum(r.vol_zig) || 0;
      const tvlN   = toNum(r.tvl_zig) || 0;

      const base = {
        tokenId: r.token_id,
        symbol: r.symbol,
        exponent: r.exponent,
        name: r.name,
        denom: r.denom,
        imageUri: r.image_uri,
        priceNative: priceN,
        priceUsd: priceN != null ? priceN * zigUsd : null,
        mcapNative: mcapN,
        mcapUsd: mcapN != null ? mcapN * zigUsd : null,
        fdvNative: fdvN,
        fdvUsd: fdvN != null ? fdvN * zigUsd : null,
        volNative: volN,
        volUsd: volN * zigUsd,
        tvlNative: tvlN,
        tvlUsd: tvlN * zigUsd,
        tx: toNum(r.tx) || 0,
      };

      if (includeBest) {
        const bp = bestMap.get(String(r.token_id)) || null;
        base.bestPool = bp ? {
          poolId: bp.poolId,
          pairContract: bp.pairContract,
          pairType: bp.pairType,
          fee: bp.fee,
          priceNativeMid: bp.priceInZig,
          tvlNative: bp.tvlZig,
          reserves: { zig: bp.zigReserve, token: bp.tokenReserve },
          sim: bp.sim || null,
          amtUsed: bp.amtUsed
        } : null;
      }

      return base;
    });

    res.json({ success: true, data, meta: { bucket, limit, offset, includeBest: includeBest ? 1 : 0 }});
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/* =============================== TOKEN PAGE: GET /tokens/:id =============================== */
/* Price & pool now taken from bestSellPool() so it matches /swap (from=token, to=uzig). */
router.get('/:id', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const zigUsd = await getZigUsd();
    const includeBest   = req.query.includeBest === '1';
    const minTvlZigBest = Number(req.query.minBestTvl || '0');
    const amtParam      = req.query.amt ? Number(req.query.amt) : undefined;

    const best = await bestSellPool(tok.token_id, { amountIn: amtParam, minTvlZig: minTvlZigBest, zigUsd });
    const tid = Number(tok.token_id);

    // if no pool, return minimal object
    if (!best?.poolId) {
      const srow = await DB.query(`
        SELECT exponent, image_uri, website, twitter, telegram, description
        FROM tokens
        WHERE token_id = ${tid}
      `);
      const s = srow.rows[0] || {};
      return res.json({ success: true, data: {
        tokenId: String(tok.token_id),
        denom: tok.denom, symbol: tok.symbol, name: tok.name,
        exponent: s.exponent != null ? Number(s.exponent) : 6,
        imageUri: s.image_uri, website: s.website, twitter: s.twitter, telegram: s.telegram,
        description: s.description,
        price: { source: 'best', poolId: null, native: null, usd: null, changePct: { '30m':0, '1h':0, '4h':0, '24h': null } },
        liquidity: 0, liquidityNative: 0,
        ...(includeBest ? { bestPool: null } : {})
      }});
    }

    // current mid price from that pool
    const pr = await DB.query(`
      SELECT price_in_zig
      FROM prices
      WHERE token_id = ${tid}
        AND pool_id = ${Number(best.poolId)}
      ORDER BY updated_at DESC
      LIMIT 1
    `);
    const priceNative = pr.rows[0]?.price_in_zig != null ? Number(pr.rows[0].price_in_zig) : null;

    // static fields + supply
    const srow = await DB.query(`
      SELECT
        total_supply_base,
        max_supply_base,
        exponent,
        image_uri,
        website,
        twitter,
        telegram,
        description
      FROM tokens
      WHERE token_id = ${tid}
    `);
    const s = srow.rows[0] || {};
    const exp = s.exponent != null ? Number(s.exponent) : 6;
    const circ = disp(s.total_supply_base, exp);
    const max  = disp(s.max_supply_base,   exp);

    // LIVE TVL sum across UZIG pools
    const live = await DB.query(`
      SELECT
        ps.reserve_base_base   AS res_base_base,
        ps.reserve_quote_base  AS res_quote_base,
        tb.exponent            AS base_exp,
        tq.exponent            AS quote_exp,
        pr.price_in_zig
      FROM pools AS p
      LEFT JOIN pool_state AS ps ON ps.pool_id = p.pool_id
      INNER JOIN tokens AS tb ON tb.token_id = p.base_token_id
      INNER JOIN tokens AS tq ON tq.token_id = p.quote_token_id
      LEFT JOIN (
        SELECT
          pool_id,
          token_id,
          argMax(price_in_zig, updated_at) AS price_in_zig
        FROM prices
        GROUP BY pool_id, token_id
      ) AS pr
        ON pr.pool_id = p.pool_id
       AND pr.token_id = p.base_token_id
      WHERE p.base_token_id = ${tid}
        AND p.is_uzig_quote = 1
    `);

    let tvlZigSum = 0;
    for (const r of live.rows) {
      const Rt = Number(r.res_base_base  || 0) / Math.pow(10, Number(r.base_exp  ?? 0));
      const Rz = Number(r.res_quote_base || 0) / Math.pow(10, Number(r.quote_exp ?? 0));
      const midZig = Number(r.price_in_zig || 0);
      const tvlZigPool = (Rt * midZig) + Rz;
      if (Number.isFinite(tvlZigPool)) tvlZigSum += tvlZigPool;
    }
    const liquidityNativeZig = tvlZigSum;
    const liquidityUSD       = liquidityNativeZig * zigUsd;

    // rollups
    const buckets = ['30m','1h','4h','24h'];
    const bucketList = buckets.map(b => esc(b)).join(', ');

    const agg = await DB.query(`
      SELECT
        pm.bucket,
        COALESCE(SUM(pm.vol_buy_zig), 0)    AS vbuy,
        COALESCE(SUM(pm.vol_sell_zig), 0)   AS vsell,
        COALESCE(SUM(pm.tx_buy), 0)         AS tbuy,
        COALESCE(SUM(pm.tx_sell), 0)        AS tsell,
        COALESCE(SUM(pm.unique_traders), 0) AS uniq,
        COALESCE(SUM(pm.tvl_zig), 0)        AS tvl
      FROM pools AS p
      INNER JOIN pool_matrix AS pm ON pm.pool_id = p.pool_id
      WHERE p.base_token_id = ${tid}
        AND pm.bucket IN (${bucketList})
      GROUP BY pm.bucket
    `);

    const map = new Map(agg.rows.map(r => [r.bucket, {
      vbuy: Number(r.vbuy || 0),
      vsell: Number(r.vsell || 0),
      tbuy: Number(r.tbuy || 0),
      tsell: Number(r.tsell || 0),
      uniq: Number(r.uniq || 0),
      tvl:  Number(r.tvl  || 0),
    }]));

    const vol = {}, volUSD = {}, txBuckets = {};
    for (const b of buckets) {
      const r = map.get(b);
      const v = r ? (r.vbuy + r.vsell) : 0;
      vol[b] = v;
      volUSD[b] = v * zigUsd;
      txBuckets[b] = r ? (r.tbuy + r.tsell) : 0;
    }
    const r24 = map.get('24h') || { vbuy:0, vsell:0, tbuy:0, tsell:0, uniq:0, tvl:0 };

    const priceChange = {
      '30m': await changePctForMinutes(best.poolId, 30),
      '1h' : await changePctForMinutes(best.poolId, 60),
      '4h' : await changePctForMinutes(best.poolId, 240),
      '24h': await changePctForMinutes(best.poolId, 1440),
    };

    const mcNative  = (priceNative != null && circ != null) ? circ * priceNative : null;
    const fdvNative = (priceNative != null && max  != null) ? max  * priceNative : null;

    const poolsCount = (await DB.query(
      `SELECT toInt32(count()) AS c FROM pools WHERE base_token_id = ${tid}`
    )).rows[0]?.c || 0;

    const holders = (await DB.query(
      `SELECT holders_count FROM token_holders_stats WHERE token_id = ${tid}`
    )).rows[0]?.holders_count || 0;

    const creation = (await DB.query(
      `SELECT min(created_at) AS first_ts FROM pools WHERE base_token_id = ${tid}`
    )).rows[0]?.first_ts || null;

    const tw = await DB.query(`
      SELECT
        handle,
        user_id,
        name,
        is_blue_verified,
        verified_type,
        profile_picture,
        cover_picture,
        followers,
        following,
        created_at_twitter,
        last_refreshed
      FROM token_twitter
      WHERE token_id = ${tid}
    `);

    const bestBlock = includeBest ? {
      poolId: best.poolId,
      pairContract: best.pairContract,
      pairType: best.pairType,
      fee: best.fee,
      priceNativeMid: best.priceInZig,
      tvlNative: best.tvlZig,
      reserves: { zig: best.zigReserve, token: best.tokenReserve },
      sim: best.sim || null,
      amtUsed: best.amtUsed
    } : undefined;

    res.json({
      success: true,
      data: {
        tokenId: String(tok.token_id),
        denom: tok.denom,
        symbol: tok.symbol,
        name: tok.name,
        exponent: exp,
        imageUri: s.image_uri,
        website: s.website, twitter: s.twitter, telegram: s.telegram,
        description: s.description,
        socials: tw.rows[0] ? {
          twitter: {
            handle: tw.rows[0].handle,
            userId: tw.rows[0].user_id,
            name: tw.rows[0].name,
            isBlueVerified: !!tw.rows[0].is_blue_verified,
            verifiedType: tw.rows[0].verified_type,
            profilePicture: tw.rows[0].profile_picture,
            coverPicture: tw.rows[0].cover_picture,
            followers: toNum(tw.rows[0].followers),
            following: toNum(tw.rows[0].following),
            createdAtTwitter: tw.rows[0].created_at_twitter,
            lastRefreshed: tw.rows[0].last_refreshed
          }
        } : {},

        price: {
          source: 'best',
          poolId: String(best.poolId),
          native: priceNative,
          usd: priceNative != null ? priceNative * zigUsd : null,
          changePct: priceChange
        },

        supply: { circulating: circ, max },
        mcap:   { native: mcNative, usd: mcNative != null ? mcNative * zigUsd : null },
        fdv:    { native: fdvNative, usd: fdvNative != null ? fdvNative * zigUsd : null },

        // Legacy / compat fields
        priceInNative: priceNative,
        priceInUsd: priceNative != null ? priceNative * zigUsd : null,
        priceSource: 'best',
        poolId: String(best.poolId),
        pools: poolsCount,
        holder: holders,
        creationTime: creation,
        supply: max ?? circ,
        circulatingSupply: circ,
        fdvNative,
        fdv: fdvNative != null ? fdvNative * zigUsd : null,
        mcNative,
        mc: mcNative != null ? mcNative * zigUsd : null,
        priceChange,
        volume: vol,
        volumeUSD: volUSD,
        txBuckets,
        uniqueTraders: r24.uniq,
        trade: r24.tbuy + r24.tsell,
        sell:  r24.tsell,
        buy:   r24.tbuy,
        v: r24.vbuy + r24.vsell,
        vBuy: r24.vbuy,
        vSell: r24.vsell,
        vUSD: (r24.vbuy + r24.vsell) * zigUsd,
        vBuyUSD: r24.vbuy * zigUsd,
        vSellUSD: r24.vsell * zigUsd,

        liquidity: liquidityUSD,
        liquidityNative: liquidityNativeZig,

        ...(includeBest ? { bestPool: bestBlock } : {})
      }
    });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/* ============================== POOLS: GET /tokens/:id/pools ============================== */
router.get('/:id/pools', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const includeCaps = req.query.includeCaps === '1';
    const zigUsd = await getZigUsd();
    const tid = Number(tok.token_id);
    const bucketLit = esc(bucket);

    const header = await DB.query(`
      SELECT
        token_id,
        symbol,
        denom,
        image_uri,
        total_supply_base,
        max_supply_base,
        exponent
      FROM tokens
      WHERE token_id = ${tid}
    `);
    const h = header.rows[0];
    const exp = h.exponent != null ? Number(h.exponent) : 6;
    const circ = disp(h.total_supply_base, exp);
    const max  = disp(h.max_supply_base, exp);

    const rows = await DB.query(`
      SELECT
        p.pool_id,
        p.pair_contract,
        p.base_token_id,
        p.quote_token_id,
        p.is_uzig_quote,
        p.created_at,
        b.symbol AS base_symbol,
        b.denom AS base_denom,
        b.exponent AS base_exp,
        q.symbol AS quote_symbol,
        q.denom AS quote_denom,
        q.exponent AS quote_exp,
        COALESCE(pm.tvl_zig, 0) AS tvl_zig,
        COALESCE(pm.vol_buy_zig, 0) + COALESCE(pm.vol_sell_zig, 0) AS vol_zig,
        COALESCE(pm.tx_buy, 0) + COALESCE(pm.tx_sell, 0) AS tx,
        COALESCE(pm.unique_traders, 0) AS unique_traders,
        pr.price_in_zig
      FROM pools AS p
      INNER JOIN tokens AS b ON b.token_id = p.base_token_id
      INNER JOIN tokens AS q ON q.token_id = p.quote_token_id
      LEFT JOIN pool_matrix AS pm
        ON pm.pool_id = p.pool_id
       AND pm.bucket = ${bucketLit}
      LEFT JOIN (
        SELECT
          pool_id,
          token_id,
          argMax(price_in_zig, updated_at) AS price_in_zig
        FROM prices
        GROUP BY pool_id, token_id
      ) AS pr
        ON pr.pool_id = p.pool_id
       AND pr.token_id = p.base_token_id
      WHERE p.base_token_id = ${tid}
      ORDER BY p.created_at ASC
    `);

    const data = rows.rows.map(r => {
      const priceN = r.is_uzig_quote === 1 || r.is_uzig_quote === true ? toNum(r.price_in_zig) : null;
      const tvlN   = toNum(r.tvl_zig) || 0;
      const volN   = toNum(r.vol_zig) || 0;
      const mcapN  = includeCaps && priceN != null && circ != null ? priceN * circ : null;
      const fdvN   = includeCaps && priceN != null && max  != null ? priceN * max  : null;
      return {
        pairContract: r.pair_contract,
        base: { tokenId: r.base_token_id, symbol: r.base_symbol, denom: r.base_denom, exponent: toNum(r.base_exp) },
        quote:{ tokenId: r.quote_token_id, symbol: r.quote_symbol, denom: r.quote_denom, exponent: toNum(r.quote_exp) },
        isUzigQuote: r.is_uzig_quote === 1 || r.is_uzig_quote === true,
        createdAt: r.created_at,
        priceNative: priceN,
        priceUsd: priceN != null ? priceN * zigUsd : null,
        tvlNative: tvlN, tvlUsd: tvlN * zigUsd,
        volumeNative: volN, volumeUsd: volN * zigUsd,
        tx: toNum(r.tx) || 0,
        uniqueTraders: toNum(r.unique_traders) || 0,
        ...(includeCaps ? {
          mcapNative: mcapN, mcapUsd: mcapN != null ? mcapN * zigUsd : null,
          fdvNative: fdvN,   fdvUsd:  fdvN  != null ? fdvN  * zigUsd : null
        } : {})
      };
    });

    res.json({
      success: true,
      token: { tokenId: h.token_id, symbol: h.symbol, denom: h.denom, imageUri: h.image_uri },
      data,
      meta: { bucket, includeCaps: includeCaps ? 1 : 0 }
    });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/* =========================== HOLDERS: GET /tokens/:id/holders =========================== */
router.get('/:id/holders', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });
    const limit  = Math.max(1, Math.min(parseInt(req.query.limit || '200', 10), 500));
    const offset = Math.max(0, parseInt(req.query.offset || '0', 10));
    const tid = Number(tok.token_id);

    const sup = await DB.query(`
      SELECT max_supply_base, total_supply_base, exponent
      FROM tokens
      WHERE token_id = ${tid}
    `);
    const exp = sup.rows[0]?.exponent != null ? Number(sup.rows[0].exponent) : 6;
    const maxBase = Number(sup.rows[0]?.max_supply_base || 0);
    const totBase = Number(sup.rows[0]?.total_supply_base || 0);

    const totalRow = await DB.query(`
      SELECT toInt64(count()) AS total
      FROM holders
      WHERE token_id = ${tid}
        AND balance_base > 0
    `);
    const total = Number(totalRow.rows[0]?.total || 0);

    const rows = await DB.query(`
      SELECT
        address,
        balance_base AS bal
      FROM holders
      WHERE token_id = ${tid}
        AND balance_base > 0
      ORDER BY bal DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `);

    const top10 = rows.rows.slice(0, 10).reduce((a, r) => a + Number(r.bal), 0);
    const pctTop10Max = maxBase > 0 ? (top10 / maxBase) * 100 : null;

    const holders = rows.rows.map(r => {
      const balDisp = Number(r.bal) / (10 ** exp);
      const pctMax  = maxBase > 0 ? (Number(r.bal) / maxBase) * 100 : null;
      const pctTot  = totBase > 0 ? (Number(r.bal) / totBase) * 100 : null;
      return { address: r.address, balance: balDisp, pctOfMax: pctMax, pctOfTotal: pctTot };
    });

    res.json({ success: true, data: holders, meta: { limit, offset, totalHolders: total, top10PctOfMax: pctTop10Max } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/* =========================== SECURITY: GET /tokens/:id/security =========================== */
router.get('/:id/security', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });
    const tid = Number(tok.token_id);

    const sq = await DB.query(`
      SELECT
        token_id,
        denom,
        is_mintable,
        can_change_minting_cap,
        max_supply_base,
        total_supply_base,
        creator_address,
        creator_balance_base,
        creator_pct_of_max,
        top10_pct_of_max,
        holders_count,
        first_seen_at,
        checked_at
      FROM token_security
      WHERE token_id = ${tid}
      LIMIT 1
    `);
    const s = sq.rows[0] || null;

    const tq = await DB.query(`
      SELECT exponent, created_at
      FROM tokens
      WHERE token_id = ${tid}
    `);
    const exp = tq.rows[0]?.exponent != null ? Number(tq.rows[0]?.exponent) : 6;

    const toDisp = (v) => v == null ? null : Number(v) / 10 ** exp;
    const num = (v, d = 0) => v == null ? d : Number(v);

    const maxSupplyDisp   = toDisp(s?.max_supply_base);
    const totalSupplyDisp = toDisp(s?.total_supply_base);
    const creatorBalDisp  = toDisp(s?.creator_balance_base);

    const creatorPctOfMax = num(s?.creator_pct_of_max, 0);
    const top10PctOfMax   = num(s?.top10_pct_of_max, 0);
    const holdersCount    = num(s?.holders_count, 0);

    const penalties = [];
    const bonuses   = [];

    if (s?.is_mintable === true) penalties.push({k:'is_mintable', pts:12});
    else bonuses.push({k:'not_mintable', pts:4});

    if (s?.can_change_minting_cap === true) penalties.push({k:'can_change_minting_cap', pts:8});

    if (top10PctOfMax >= 75) penalties.push({k:'top10>=75%', pts:20});
    else if (top10PctOfMax >= 50) penalties.push({k:'top10>=50%', pts:12});
    else if (top10PctOfMax >= 30) penalties.push({k:'top10>=30%', pts:6});
    else bonuses.push({k:'top10<30%', pts:4});

    if (creatorPctOfMax >= 25) penalties.push({k:'creator>=25%', pts:18});
    else if (creatorPctOfMax >= 10) penalties.push({k:'creator>=10%', pts:10});
    else if (creatorPctOfMax > 0) bonuses.push({k:'creator<10%', pts:3});

    if (holdersCount < 100) penalties.push({k:'holders<100', pts:8});
    else if (holdersCount < 1000) penalties.push({k:'holders<1k', pts:4});
    else if (holdersCount >= 10000) bonuses.push({k:'holders>=10k', pts:5});
    else if (holdersCount >= 50000) bonuses.push({k:'holders>=50k', pts:10});

    if (s?.is_mintable === false && s?.max_supply_base != null && s?.total_supply_base != null) {
      if (String(s.max_supply_base) === String(s.total_supply_base)) {
        bonuses.push({k:'fully_minted_equals_max', pts:4});
      }
    }

    const firstSeen = s?.first_seen_at ? new Date(s.first_seen_at) : null;
    if (firstSeen) {
      const daysAlive = (Date.now() - firstSeen.getTime()) / (1000*60*60*24);
      if (daysAlive >= 180) bonuses.push({k:'age>=180d', pts:6});
      else if (daysAlive >= 90) bonuses.push({k:'age>=90d', pts:4});
      else if (daysAlive >= 30) bonuses.push({k:'age>=30d', pts:2});
    }

    let score = 100;
    for (const p of penalties) score -= p.pts;
    for (const b of bonuses)   score += b.pts;
    score = Math.max(1, Math.min(99, Math.round(score)));

    const checks = {
      isMintable: !!(s?.is_mintable),
      canChangeMintingCap: !!(s?.can_change_minting_cap),
      maxSupply: maxSupplyDisp,
      totalSupply: totalSupplyDisp,
      top10PctOfMax: Number(top10PctOfMax.toFixed(4)),
      creatorPctOfMax: Number(creatorPctOfMax.toFixed(4)),
      holdersCount: holdersCount
    };

    const dev = {
      tokenTotalSupply: totalSupplyDisp,
      creatorAddress: s?.creator_address || null,
      creatorBalance: creatorBalDisp,
      creatorPctOfMax: Number(creatorPctOfMax.toFixed(4)),
      topHoldersPctOfMax: Number(top10PctOfMax.toFixed(4)),
      holdersCount: holdersCount,
      firstSeenAt: s?.first_seen_at || null
    };

    const categories = {
      supply: {
        isMintable: !!(s?.is_mintable),
        canChangeMintingCap: !!(s?.can_change_minting_cap),
        maxSupply: maxSupplyDisp,
        totalSupply: totalSupplyDisp
      },
      distribution: {
        top10PctOfMax: Number(top10PctOfMax.toFixed(4)),
        creatorPctOfMax: Number(creatorPctOfMax.toFixed(4))
      },
      adoption: {
        holdersCount: holdersCount,
        firstSeenAt: s?.first_seen_at || null
      }
    };

    res.json({
      success: true,
      data: {
        score,
        penalties,
        bonuses,
        categories,
        checks,
        dev,
        lastUpdated: s?.checked_at || null,
        source: 'token_security'
      }
    });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});



/* =============================== OHLCV: GET /tokens/:id/ohlcv =============================== */
/* When priceSource=best, we use bestSellPool() (same as /swap). 'all' and explicit 'pool' still supported. */
function tfToSec(tf) {
  const m = { m:60, h:3600, d:86400, w:604800, M:2592000 };
  const map = {
    '1m':60, '5m':300, '15m':900, '30m':1800,
    '1h':3600, '2h':7200, '4h':14400, '8h':28800, '12h':43200,
    '1d':86400, '3d':259200, '5d':432000, '1w':604800,
    '1M':2592000, '3M':7776000
  };
  if (map[tf]) return map[tf];
  const g = /^(\d+)([mhdwM])$/.exec(tf || '');
  if (!g) return 60;
  return Number(g[1]) * (m[g[2]] || 60);
}

router.get('/:id/ohlcv', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success: false, error: 'token not found' });

    const tf = req.query.tf || '1m';
    const stepSec = tfToSec(tf);
    const mode = (req.query.mode || 'price').toLowerCase();
    const unit = (req.query.unit || 'native').toLowerCase();
    const priceSource = (req.query.priceSource || 'best').toLowerCase();
    const poolIdParam = req.query.poolId;
    const pairParam = req.query.pair;
    const fill = (req.query.fill || 'none').toLowerCase(); // prev|zero|none
    const minTvlZigBest = Number(req.query.minBestTvl || '0');
    const amtParam = req.query.amt ? Number(req.query.amt) : undefined;

    const now = new Date();
    let toIso = req.query.to || now.toISOString();
    let fromIso = req.query.from || null;

    if (!fromIso) {
      if (req.query.span) {
        const spanSec = tfToSec(req.query.span);
        const to = new Date(toIso);
        fromIso = new Date(to.getTime() - spanSec * 1000).toISOString();
      } else if (req.query.window) {
        const bars = Math.max(1, Math.min(parseInt(req.query.window, 10) || 300, 5000));
        const to = new Date(toIso);
        fromIso = new Date(to.getTime() - bars * stepSec * 1000).toISOString();
      } else {
        const bars = tf === '1m' ? 1440 : 300;
        fromIso = new Date(new Date(toIso).getTime() - bars * stepSec * 1000).toISOString();
      }
    }

    const zigUsd = await getZigUsd();

    // supply (for mcap)
    const ss = await DB.query(
      `SELECT total_supply_base, exponent FROM tokens WHERE token_id = $1`,
      [tok.token_id]
    );
    const exp =
      ss.rows[0]?.exponent != null ? Number(ss.rows[0].exponent) : 6;
    const circ =
      ss.rows[0]?.total_supply_base != null
        ? Number(ss.rows[0].total_supply_base) / 10 ** exp
        : null;

    // Determine pool set + seed prevClose
    let headerSQL = '';
    let params = [];
    let seedPrevClose = null;

    if (priceSource === 'all') {
      params = [tok.token_id, fromIso, toIso, stepSec];
      headerSQL = `
        WITH src AS (
          SELECT
            o.pool_id,
            o.bucket_start,
            o.open,
            o.high,
            o.low,
            o.close,
            o.volume_zig,
            o.trade_count
          FROM ohlcv_1m o
          JOIN pools p ON p.pool_id = o.pool_id
          WHERE p.base_token_id = $1
            AND p.is_uzig_quote = 1
            AND o.bucket_start >= parseDateTimeBestEffort($2)
            AND o.bucket_start <  parseDateTimeBestEffort($3)
        ),
      `;
      const q = await DB.query(
        `
        SELECT o.close
        FROM ohlcv_1m o
        JOIN pools p ON p.pool_id = o.pool_id
        WHERE p.base_token_id = $1
          AND p.is_uzig_quote = 1
          AND o.bucket_start < parseDateTimeBestEffort($2)
        ORDER BY o.bucket_start DESC
        LIMIT 1
        `,
        [tok.token_id, fromIso]
      );
      seedPrevClose =
        q.rows[0]?.close != null ? Number(q.rows[0].close) : null;
    } else if (priceSource === 'pool') {
      let poolRow = null;
      if (poolIdParam || pairParam) {
        const { rows } = await DB.query(
          `
          SELECT pool_id
          FROM pools
          WHERE (toString(pool_id) = $1 OR pair_contract = $1)
            AND base_token_id = $2
          LIMIT 1
          `,
          [poolIdParam || pairParam, tok.token_id]
        );
        poolRow = rows[0] || null;
      }
      if (!poolRow?.pool_id) {
        return res.json({
          success: true,
          data: [],
          meta: {
            tf,
            mode,
            unit,
            fill,
            priceSource: 'pool',
            poolId: null,
          },
        });
      }
      params = [poolRow.pool_id, fromIso, toIso, stepSec];
      headerSQL = `
        WITH src AS (
          SELECT
            o.pool_id,
            o.bucket_start,
            o.open,
            o.high,
            o.low,
            o.close,
            o.volume_zig,
            o.trade_count
          FROM ohlcv_1m o
          WHERE o.pool_id = $1
            AND o.bucket_start >= parseDateTimeBestEffort($2)
            AND o.bucket_start <  parseDateTimeBestEffort($3)
        ),
      `;
      const q = await DB.query(
        `
        SELECT close
        FROM ohlcv_1m
        WHERE pool_id = $1
          AND bucket_start < parseDateTimeBestEffort($2)
        ORDER BY bucket_start DESC
        LIMIT 1
        `,
        [poolRow.pool_id, fromIso]
      );
      seedPrevClose =
        q.rows[0]?.close != null ? Number(q.rows[0].close) : null;
    } else {
      // priceSource === 'best' (default): choose the same pool as /swap sell leg
      const best = await bestSellPool(tok.token_id, {
        amountIn: amtParam,
        minTvlZig: minTvlZigBest,
        zigUsd,
      });

      const bestPoolId =
        best && best.poolId != null && best.poolId !== 'undefined'
          ? best.poolId
          : null;

      if (!bestPoolId) {
        return res.json({
          success: true,
          data: [],
          meta: {
            tf,
            mode,
            unit,
            fill,
            priceSource: 'best',
            poolId: null,
          },
        });
      }

      params = [bestPoolId, fromIso, toIso, stepSec];
      headerSQL = `
        WITH src AS (
          SELECT
            o.pool_id,
            o.bucket_start,
            o.open,
            o.high,
            o.low,
            o.close,
            o.volume_zig,
            o.trade_count
          FROM ohlcv_1m o
          WHERE o.pool_id = $1
            AND o.bucket_start >= parseDateTimeBestEffort($2)
            AND o.bucket_start <  parseDateTimeBestEffort($3)
        ),
      `;
      const q = await DB.query(
        `
        SELECT close
        FROM ohlcv_1m
        WHERE pool_id = $1
          AND bucket_start < parseDateTimeBestEffort($2)
        ORDER BY bucket_start DESC
        LIMIT 1
        `,
        [bestPoolId, fromIso]
      );
      seedPrevClose =
        q.rows[0]?.close != null ? Number(q.rows[0].close) : null;
    }

    // Aggregate to requested TF (ClickHouse syntax)
    const { rows } = await DB.query(
      `
      ${headerSQL}
      tagged AS (
        SELECT
          bucket_start,
          open,
          high,
          low,
          close,
          volume_zig,
          trade_count,
          toDateTime(
            intDiv(
              toUnixTimestamp(bucket_start),
              CAST($4 AS Int64)
            ) * CAST($4 AS Int64)
          ) AS bucket_ts
        FROM src
      ),
      sums AS (
        SELECT
          bucket_ts,
          MIN(low)  AS low,
          MAX(high) AS high,
          SUM(volume_zig)  AS volume_native,
          SUM(trade_count) AS trades
        FROM tagged
        GROUP BY bucket_ts
      ),
      firsts AS (
        SELECT
          bucket_ts,
          argMin(open, bucket_start) AS open   -- ✅ earliest open in bucket
        FROM tagged
        GROUP BY bucket_ts
      ),
      lasts AS (
        SELECT
          bucket_ts,
          argMax(close, bucket_start) AS close -- ✅ latest close in bucket
        FROM tagged
        GROUP BY bucket_ts
      )
      SELECT
        toInt64(toUnixTimestamp(s.bucket_ts)) AS ts_sec,
        f.open,
        s.high,
        s.low,
        l.close,
        s.volume_native,
        s.trades
      FROM sums s
      LEFT JOIN firsts f USING (bucket_ts)
      LEFT JOIN lasts  l USING (bucket_ts)
      ORDER BY ts_sec ASC
      `,
      params
    );

    // JS gap fill
    const start =
      Math.floor(new Date(fromIso).getTime() / 1000 / stepSec) * stepSec;
    const end =
      Math.floor(new Date(toIso).getTime() / 1000 / stepSec) * stepSec;

    const bySec = new Map(
      rows.map((r) => [
        Number(r.ts_sec),
        {
          sec: Number(r.ts_sec),
          open: r.open != null ? Number(r.open) : null,
          high: r.high != null ? Number(r.high) : null,
          low: r.low != null ? Number(r.low) : null,
          close: r.close != null ? Number(r.close) : null,
          volume: r.volume_native != null ? Number(r.volume_native) : 0,
          trades: r.trades != null ? Number(r.trades) : 0,
        },
      ])
    );

    let prevClose =
      fill === 'prev' && Number.isFinite(seedPrevClose)
        ? Number(seedPrevClose)
        : null;
    const out = [];

    for (let ts = start; ts <= end; ts += stepSec) {
      const r = bySec.get(ts);
      if (r && r.open != null && r.high != null && r.low != null && r.close != null) {
        const openAdj = prevClose != null ? prevClose : r.open;
        const highAdj = Math.max(r.high, openAdj);
        const lowAdj = Math.min(r.low, openAdj);
        const base = {
          ts_sec: ts,
          open: openAdj,
          high: highAdj,
          low: lowAdj,
          close: r.close,
          volume: r.volume,
          trades: r.trades,
        };
        out.push(base);
        prevClose = base.close;
      } else if (fill !== 'none') {
        if (fill === 'prev' && prevClose != null) {
          out.push({
            ts_sec: ts,
            open: prevClose,
            high: prevClose,
            low: prevClose,
            close: prevClose,
            volume: 0,
            trades: 0,
          });
        } else if (fill === 'zero') {
          out.push({
            ts_sec: ts,
            open: 0,
            high: 0,
            low: 0,
            close: 0,
            volume: 0,
            trades: 0,
          });
          prevClose = 0;
        }
      }
    }

    const conv = out.map((b) => {
      let o = { ...b };
      if (mode === 'mcap' && circ != null) {
        o.open *= circ;
        o.high *= circ;
        o.low *= circ;
        o.close *= circ;
      }
      if (unit === 'usd') {
        o.open *= zigUsd;
        o.high *= zigUsd;
        o.low *= zigUsd;
        o.close *= zigUsd;
        o.volume *= zigUsd;
      }
      return o;
    });

    res.json({
      success: true,
      data: conv,
      meta: {
        tf,
        mode,
        unit,
        fill,
        priceSource,
        stepSec,
        alignedFromSec: start,
        alignedToSecExclusive: end + stepSec,
        prevCloseSeed: Number.isFinite(seedPrevClose) ? seedPrevClose : null,
      },
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ success: false, error: e.message });
  }
});


export default router;
