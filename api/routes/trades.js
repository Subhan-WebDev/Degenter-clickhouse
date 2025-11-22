// api/routes/trades.js
import express from 'express';
import { DB } from '../../lib/db.js';
import { getZigUsd, resolveTokenId } from '../util/resolve-token.js';

const router = express.Router();

/* ---------------- helpers ---------------- */

const VALID_DIR = new Set(['buy','sell','provide','withdraw']);
const VALID_CLASS = new Set(['shrimp','shark','whale']);
const VALID_LIMITS = new Set([100, 500, 1000]);

function normDir(d) {
  const x = String(d || '').toLowerCase();
  return VALID_DIR.has(x) ? x : null;
}

function clampInt(v, { min = 0, max = 1e9, def = 0 } = {}) {
  const n = Number.parseInt(v, 10);
  if (Number.isNaN(n)) return def;
  return Math.max(min, Math.min(max, n));
}

function parseLimit(q) {
  const n = Number(q);
  if (VALID_LIMITS.has(n)) return n;
  return 100; // default
}
function parsePage(q) {
  const n = Number(q);
  return Number.isFinite(n) && n >= 1 ? Math.floor(n) : 1;
}

/** Dynamic + extended TF (accepts Xd like 60d) */
function minutesForTf(tf) {
  const m = String(tf || '').toLowerCase();
  const mDays = m.match(/^(\d+)d$/);
  if (mDays) return Number(mDays[1]) * 1440;

  const map = {
    '30m': 30,
    '1h' : 60,
    '2h' : 120,
    '4h' : 240,
    '8h' : 480,
    '12h': 720,
    '24h': 1440,
    '1d' : 1440,
    '3d' : 4320,
    '5d' : 7200,
    '7d' : 10080,
    '14d': 20160,
    '30d': 43200,
    '60d': 86400
  };
  return map[m] || 1440; // default 24h
}

/** Build time window clause + params */
function buildWindow({ tf, from, to, days }, params, alias = 't') {
  const clauses = [];
  if (from && to) {
    clauses.push(`${alias}.created_at >= $${params.length + 1}::timestamptz`);
    params.push(from);
    clauses.push(`${alias}.created_at < $${params.length + 1}::timestamptz`);
    params.push(to);
    return { clause: clauses.join(' AND ') };
  }
  if (days) {
    const d = clampInt(days, { min: 1, max: 365, def: 1 });
    clauses.push(`${alias}.created_at >= now() - ($${params.length + 1} || ' days')::interval`);
    params.push(String(d));
    return { clause: clauses.join(' AND ') };
  }
  const mins = minutesForTf(tf);
  clauses.push(`${alias}.created_at >= now() - INTERVAL '${mins} minutes'`);
  return { clause: clauses.join(' AND ') };
}

/** FROM/JOIN block for trades with a chosen alias */
function tradesFromJoin(alias = 't') {
  return `
    FROM trades ${alias}
    JOIN pools  p ON p.pool_id = ${alias}.pool_id
    JOIN tokens q ON q.token_id = p.quote_token_id
    JOIN tokens b ON b.token_id = p.base_token_id
    LEFT JOIN tokens toff ON toff.denom = ${alias}.offer_asset_denom
    LEFT JOIN tokens task ON task.denom = ${alias}.ask_asset_denom
  `;
}

/** WHERE builder (without time) — scope/direction/includeLiquidity */
function buildWhereBase({ scope, scopeValue, direction, includeLiquidity }, params, alias = 't') {
  const where = [];
  if (includeLiquidity) where.push(`${alias}.action IN ('swap','provide','withdraw')`);
  else where.push(`${alias}.action = 'swap'`);

  if (direction) {
    where.push(`${alias}.direction = $${params.length + 1}`);
    params.push(direction);
  }

  if (scope === 'token') {
    where.push(`b.token_id = $${params.length + 1}`);
    params.push(scopeValue);
  } else if (scope === 'wallet') {
    where.push(`${alias}.signer = $${params.length + 1}`);
    params.push(scopeValue);
  } else if (scope === 'pool') {
    if (scopeValue.poolId) {
      where.push(`p.pool_id = $${params.length + 1}`);
      params.push(scopeValue.poolId);
    } else if (scopeValue.pairContract) {
      where.push(`p.pair_contract = $${params.length + 1}`);
      params.push(scopeValue.pairContract);
    }
  }
  return where;
}

/** scale helper for shaping (JS side) */
function scale(base, exp, fallback = 6) {
  if (base == null) return null;
  const e = (exp == null ? fallback : Number(exp));
  return Number(base) / 10 ** e;
}

/** shape one row into API response (keeps ZIG-leg + notional) */
function shapeRow(r, unit, zigUsd) {
  const offerScaled = scale(
    r.offer_amount_base,
    (r.offer_asset_denom === 'uzig') ? 6 : (r.offer_exp ?? 6),
    6
  );

  const askScaled = scale(
    r.ask_amount_base,
    (r.ask_asset_denom === 'uzig') ? 6 : (r.ask_exp ?? 6),
    6
  );

  const returnAsQuote = scale(r.return_amount_base, r.qexp ?? 6, 6);
  const returnAsBase  = scale(r.return_amount_base, r.bexp ?? 6, 6);

  // notional in ZIG (quote)
  let valueZig = null;
  if (r.is_uzig_quote) {
    valueZig = (r.direction === 'buy')
      ? scale(r.offer_amount_base, r.qexp ?? 6, 6)
      : scale(r.return_amount_base, r.qexp ?? 6, 6);
  } else if (r.pq_price_in_zig != null) {
    const rawQuote = (r.direction === 'buy')
      ? scale(r.offer_amount_base, r.qexp ?? 6, 6)
      : scale(r.return_amount_base, r.qexp ?? 6, 6);
    if (rawQuote != null) valueZig = rawQuote * Number(r.pq_price_in_zig);
  }
  const valueUsd = valueZig != null ? valueZig * zigUsd : null;

  // ZIG-leg preferred for class
  const zigLegAmount =
    (r.offer_asset_denom === 'uzig' && offerScaled != null) ? offerScaled :
    (r.ask_asset_denom   === 'uzig' && askScaled   != null) ? askScaled   :
    null;

  // price (ZIG per 1 BASE)
  let quoteAmtZig = null;
  if (r.is_uzig_quote) {
    quoteAmtZig = (r.direction === 'buy')
      ? scale(r.offer_amount_base, r.qexp ?? 6, 6)
      : scale(r.return_amount_base, r.qexp ?? 6, 6);
  } else if (r.pq_price_in_zig != null) {
    const rawQuote = (r.direction === 'buy')
      ? scale(r.offer_amount_base, r.qexp ?? 6, 6)
      : scale(r.return_amount_base, r.qexp ?? 6, 6);
    if (rawQuote != null) quoteAmtZig = rawQuote * Number(r.pq_price_in_zig);
  }
  const baseAmt = (r.direction === 'buy')
    ? returnAsBase
    : (r.direction === 'sell')
      ? scale(r.offer_amount_base, r.bexp ?? 6, 6)
      : null;

  const priceNative = (quoteAmtZig != null && baseAmt != null && baseAmt !== 0) ? (quoteAmtZig / baseAmt) : null;
  const priceUsd = priceNative != null ? priceNative * zigUsd : null;

  return {
    time: r.created_at,
    txHash: r.tx_hash,
    pairContract: r.pair_contract,
    signer: r.signer,
    direction: r.direction,
    is_router: r.is_router === true,

    offerDenom: r.offer_asset_denom,
    offerAmountBase: r.offer_amount_base,
    offerAmount: offerScaled,

    askDenom: r.ask_asset_denom,
    askAmountBase: r.ask_amount_base,
    askAmount: askScaled,

    returnAmountBase: r.return_amount_base,
    returnAmount: (r.direction === 'buy') ? returnAsBase : returnAsQuote,

    priceNative,
    priceUsd,

    valueNative: valueZig,
    valueUsd,

    zigLegAmount
  };
}

/** worth basis (ZIG-leg preferred, else notional), unit-aware */
function worthForClass(item, unit, zigUsd) {
  const zigBasis = (item.zigLegAmount != null) ? item.zigLegAmount : item.valueNative;
  if (zigBasis == null) return null;
  return unit === 'usd' ? zigBasis * zigUsd : zigBasis;
}
function classifyByThreshold(x) {
  if (x < 1000) return 'shrimp';
  if (x <= 10000) return 'shark';
  return 'whale';
}
function applyClassFilterJS(data, unit, klass, zigUsd) {
  if (!klass || !VALID_CLASS.has(klass)) return data;
  return data.filter(x => {
    const w = worthForClass(x, unit, zigUsd);
    if (w == null) return false;
    return classifyByThreshold(w) === klass;
  });
}

/** combine legs for router tx (for combineRouter modes) */
function buildRouterCombined(legs, unit, zigUsd) {
  const sorted = legs.slice().sort((a,b) => new Date(a.time) - new Date(b.time));
  const first = sorted[0];
  const last  = sorted[sorted.length - 1];
  const valueNative = sorted.reduce((s,x)=>s+(x.valueNative||0),0);
  const valueUsd    = sorted.reduce((s,x)=>s+(x.valueUsd||0),0);
  const zigLegSum   = sorted.reduce((s,x)=>s+(x.zigLegAmount||0),0);

  const toDenom  = (last.direction === 'buy') ? last.askDenom : (last.offerDenom || last.askDenom);
  const amountIn  = first.offerAmount ?? null;
  const amountOut = (last.direction === 'buy') ? last.returnAmount : (last.offerAmount ?? null);

  const out = {
    ...first,
    pairContract: 'router',
    is_router: true,
    time: first.time,
    direction: first.direction,
    fromDenom: first.offerDenom,
    toDenom,
    path: sorted.map(l => ({ pair: l.pairContract, offerDenom: l.offerDenom, askDenom: l.askDenom })),
    valueNative,
    valueUsd,
    amountIn,
    amountOut,
    legs: sorted,
    zigLegAmount: zigLegSum
  };
  const w = worthForClass(out, unit, zigUsd);
  out.class = w != null ? classifyByThreshold(w) : null;
  return out;
}
function combineRouterTradesShallow(items, unit, zigUsd) {
  const byTx = new Map();
  for (const t of items) {
    const cur = byTx.get(t.txHash);
    if (!cur) byTx.set(t.txHash, [t]); else cur.push(t);
  }
  return Array.from(byTx.values()).map(legs => {
    const anyRouter = legs.some(l => l.is_router);
    return anyRouter ? buildRouterCombined(legs, unit, zigUsd) : legs[0];
  });
}
async function fetchSiblingLegsByTx(txHashes, windowOpts) {
  if (!txHashes.length) return [];
  const params = [];
  const { clause } = buildWindow(windowOpts, params, 't');
  const inParams = txHashes.map((_, i) => `$${params.length + i + 1}`).join(',');
  params.push(...txHashes);

  const sql = `
    SELECT
      t.*,
      p.pair_contract,
      p.is_uzig_quote,
      q.exponent AS qexp,
      b.exponent AS bexp,
      b.denom    AS base_denom,
      (SELECT price_in_zig FROM prices WHERE token_id = p.quote_token_id ORDER BY updated_at DESC LIMIT 1) AS pq_price_in_zig,
      toff.exponent AS offer_exp,
      task.exponent AS ask_exp
    ${tradesFromJoin('t')}
    WHERE ${clause} AND t.tx_hash IN (${inParams})
    ORDER BY t.created_at ASC
  `;
  const { rows } = await DB.query(sql, params);
  return rows;
}
async function combineRouterTradesDeep(shaped, unit, windowOpts, zigUsd) {
  const txSet = new Set(shaped.map(x => x.txHash));
  if (!txSet.size) return shaped;
  const siblings = await fetchSiblingLegsByTx(Array.from(txSet), windowOpts);
  const shapedSiblings = siblings.map(r => shapeRow(r, unit, zigUsd));
  const merged = new Map();
  for (const row of [...shaped, ...shapedSiblings]) {
    const arr = merged.get(row.txHash);
    if (!arr) merged.set(row.txHash, [row]); else arr.push(row);
  }
  const out = [];
  for (const legs of merged.values()) {
    const anyRouter = legs.some(l => l.is_router);
    out.push(anyRouter ? buildRouterCombined(legs, unit, zigUsd) : legs[0]);
  }
  return out;
}

function paginateArray(data, page, limit) {
  const total = data.length;
  const pages = Math.max(1, Math.ceil(total / limit));
  const p = Math.min(page, pages);
  const start = (p - 1) * limit;
  const end = start + limit;
  return { items: data.slice(start, end), total, page: p, pages, limit };
}

/**
 * CPU-friendly builder:
 * - LATERAL join to get latest quote price once per row via index
 * - optional totals (includeTotal=false by default) to avoid COUNT(*) OVER()
 */
function buildWorthPagedSQL({
  scope, scopeValue, direction, includeLiquidity,
  windowOpts, page, limit, unit, klass, minValue, maxValue,
  extraWhere = [], includeTotal = false
}, params) {
  // Base WHERE
  const baseWhere = buildWhereBase({ scope, scopeValue, direction, includeLiquidity }, params, 't');

  // extras
  if (Array.isArray(extraWhere) && extraWhere.length) baseWhere.push(...extraWhere);

  // time window
  const { clause: timeClause } = buildWindow(windowOpts, params, 't');
  baseWhere.push(timeClause);

  // worthZig expression using base.*
  const worthZig = `
    COALESCE(
      CASE WHEN base.offer_asset_denom='uzig'
           THEN base.offer_amount_base / POWER(10, COALESCE(base.offer_exp,6))
           WHEN base.ask_asset_denom='uzig'
           THEN base.ask_amount_base   / POWER(10, COALESCE(base.ask_exp,6))
      END,
      CASE WHEN base.is_uzig_quote THEN
             CASE WHEN base.direction='buy'
                  THEN base.offer_amount_base  / POWER(10, COALESCE(base.qexp,6))
                  ELSE base.return_amount_base / POWER(10, COALESCE(base.qexp,6))
             END
           ELSE
             (CASE WHEN base.direction='buy'
                   THEN base.offer_amount_base  / POWER(10, COALESCE(base.qexp,6))
                   ELSE base.return_amount_base / POWER(10, COALESCE(base.qexp,6))
              END) * base.pq_price_in_zig
      END
    )
  `;

  const zigUsdIdx = params.length + 1;
  params.push(0); // placeholder to be filled by caller
  const worthUsd = `(${worthZig}) * $${zigUsdIdx}`;

  // filters on worth/class
  const filters = [];
  const k = String(klass || '').toLowerCase();
  if (VALID_CLASS.has(k)) {
    if (k === 'shrimp') filters.push(unit === 'zig' ? `${worthZig} < 1000` : `${worthUsd} < 1000`);
    if (k === 'shark')  filters.push(unit === 'zig' ? `${worthZig} >= 1000 AND ${worthZig} <= 10000`
                                                   : `${worthUsd} >= 1000 AND ${worthUsd} <= 10000`);
    if (k === 'whale')  filters.push(unit === 'zig' ? `${worthZig} > 10000` : `${worthUsd} > 10000`);
  }
  if (minValue != null) filters.push(unit === 'zig' ? `${worthZig} >= ${Number(minValue)}` : `${worthUsd} >= ${Number(minValue)}`);
  if (maxValue != null) filters.push(unit === 'zig' ? `${worthZig} <= ${Number(maxValue)}` : `${worthUsd} <= ${Number(maxValue)}`);

  const offset = (page - 1) * limit;

  const sql = `
    WITH base AS (
      SELECT
        t.*,
        p.pair_contract,
        p.is_uzig_quote,
        q.exponent AS qexp,
        b.exponent AS bexp,
        b.denom    AS base_denom,
        toff.exponent AS offer_exp,
        task.exponent AS ask_exp,
        pr.price_in_zig AS pq_price_in_zig
      FROM trades t
      JOIN pools  p ON p.pool_id = t.pool_id
      JOIN tokens q ON q.token_id = p.quote_token_id
      JOIN tokens b ON b.token_id = p.base_token_id
      LEFT JOIN tokens toff ON toff.denom = t.offer_asset_denom
      LEFT JOIN tokens task ON task.denom = t.ask_asset_denom
      LEFT JOIN LATERAL (
        SELECT price_in_zig
        FROM prices
        WHERE token_id = p.quote_token_id
        ORDER BY updated_at DESC
        LIMIT 1
      ) pr ON TRUE
      WHERE ${baseWhere.join(' AND ')}
    )
    ${includeTotal ? `
      , counted AS (
        SELECT base.*,
               ${worthZig} AS worth_zig,
               (${worthZig}) * $${zigUsdIdx} AS worth_usd
        FROM base
        ${filters.length ? `WHERE ${filters.join(' AND ')}` : ''}
      )
      SELECT *, (SELECT COUNT(*) FROM counted) AS total
      FROM counted
      ORDER BY created_at DESC
      LIMIT ${limit} OFFSET ${offset}
    ` : `
      , ranked AS (
        SELECT base.*,
               ${worthZig} AS worth_zig,
               (${worthZig}) * $${zigUsdIdx} AS worth_usd
        FROM base
        ${filters.length ? `WHERE ${filters.join(' AND ')}` : ''}
      )
      SELECT *
      FROM ranked
      ORDER BY created_at DESC
      LIMIT ${limit} OFFSET ${offset}
    `}
  `;
  return { sql, params, zigUsdIdx };
}

/* ---------------- ROUTES ---------------- */

/** GET /trades */
router.get('/', async (req, res) => {
  try {
    const unit   = String(req.query.unit || 'usd').toLowerCase();
    const page   = parsePage(req.query.page);
    const limit  = parseLimit(req.query.limit);
    const dir    = normDir(req.query.direction);
    const includeLiquidity = req.query.includeLiquidity === '1';
    const combine = String(req.query.combineRouter || '').toLowerCase();
    const klass  = String(req.query.class || '').toLowerCase();
    const minV   = req.query.minValue != null ? Number(req.query.minValue) : null;
    const maxV   = req.query.maxValue != null ? Number(req.query.maxValue) : null;
    const includeTotal = ['1','true','yes'].includes(String(req.query.includeTotal || '').toLowerCase());

    const zigUsd = await getZigUsd();
    const windowOpts = { tf:req.query.tf, from:req.query.from, to:req.query.to, days:req.query.days };

    if (combine === 'deep' || combine === '1' || combine === 'true') {
      // Combine path: oversample, shape, combine, filter, paginate in JS
      const sqlLimit = Math.min(limit * 20, 20000);
      const params = [];
      const where = buildWhereBase({ scope:'all', scopeValue:null, direction:dir, includeLiquidity }, params, 't');
      const { clause } = buildWindow(windowOpts, params, 't');
      where.push(clause);

      const sql = `
        SELECT
          t.*,
          p.pair_contract,
          p.is_uzig_quote,
          q.exponent AS qexp,
          b.exponent AS bexp,
          b.denom    AS base_denom,
          (SELECT price_in_zig FROM prices WHERE token_id = p.quote_token_id ORDER BY updated_at DESC LIMIT 1) AS pq_price_in_zig,
          toff.exponent AS offer_exp,
          task.exponent AS ask_exp
        ${tradesFromJoin('t')}
        WHERE ${where.join(' AND ')}
        ORDER BY t.created_at DESC
        LIMIT ${sqlLimit}
      `;
      const { rows } = await DB.query(sql, params);
      let shaped = rows.map(r => shapeRow(r, unit, zigUsd));
      if (combine === 'deep') shaped = await combineRouterTradesDeep(shaped, unit, windowOpts, zigUsd);
      else shaped = combineRouterTradesShallow(shaped, unit, zigUsd);
      if (klass) shaped = applyClassFilterJS(shaped, unit, klass, zigUsd);
      if (minV != null) shaped = shaped.filter(x => (worthForClass(x, unit, zigUsd) ?? -Infinity) >= minV);
      if (maxV != null) shaped = shaped.filter(x => (worthForClass(x, unit, zigUsd) ?? Infinity) <= maxV);

      const { items, total, pages, page: p } = paginateArray(shaped, page, limit);
      return res.json({ success:true, data: items, meta:{ unit, tf:req.query.tf||'24h', limit, page: p, pages, total } });
    }

    // DB-side worth/class/pagination
    const params = [];
    const { sql, params: p2, zigUsdIdx } = buildWorthPagedSQL({
      scope: 'all',
      scopeValue: null,
      direction: dir,
      includeLiquidity,
      windowOpts,
      page, limit,
      unit, klass,
      minValue: minV, maxValue: maxV,
      extraWhere: [],
      includeTotal
    }, params);
    p2[zigUsdIdx - 1] = zigUsd;

    const { rows } = await DB.query(sql, p2);
    const total = includeTotal ? (rows[0]?.total ? Number(rows[0].total) : 0) : undefined;
    const pages = total != null ? Math.max(1, Math.ceil(total / limit)) : undefined;

    const shaped = rows.map(r => {
      const s = shapeRow(r, unit, zigUsd);
      const w = worthForClass(s, unit, zigUsd);
      s.class = w != null ? classifyByThreshold(w) : null;
      return s;
    });

    res.json({ success:true, data: shaped, meta:{ unit, tf:req.query.tf || '24h', limit, page, pages, total } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/token/:id */
router.get('/token/:id', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const unit   = String(req.query.unit || 'usd').toLowerCase();
    const page   = parsePage(req.query.page);
    const limit  = parseLimit(req.query.limit);
    const dir    = normDir(req.query.direction);
    const includeLiquidity = req.query.includeLiquidity === '1';
    const combine = String(req.query.combineRouter || '').toLowerCase();
    const klass  = String(req.query.class || '').toLowerCase();
    const minV   = req.query.minValue != null ? Number(req.query.minValue) : null;
    const maxV   = req.query.maxValue != null ? Number(req.query.maxValue) : null;
    const includeTotal = ['1','true','yes'].includes(String(req.query.includeTotal || '').toLowerCase());

    const zigUsd = await getZigUsd();
    const windowOpts = { tf:req.query.tf, from:req.query.from, to:req.query.to, days:req.query.days };

    if (combine === 'deep' || combine === '1' || combine === 'true') {
      const sqlLimit = Math.min(limit * 20, 20000);
      const params = [];
      const where = buildWhereBase({ scope:'token', scopeValue: tok.token_id, direction:dir, includeLiquidity }, params, 't');
      const { clause } = buildWindow(windowOpts, params, 't');
      where.push(clause);

      const sql = `
        SELECT
          t.*,
          p.pair_contract,
          p.is_uzig_quote,
          q.exponent AS qexp,
          b.exponent AS bexp,
          b.denom    AS base_denom,
          (SELECT price_in_zig FROM prices WHERE token_id = p.quote_token_id ORDER BY updated_at DESC LIMIT 1) AS pq_price_in_zig,
          toff.exponent AS offer_exp,
          task.exponent AS ask_exp
        ${tradesFromJoin('t')}
        WHERE ${where.join(' AND ')}
        ORDER BY t.created_at DESC
        LIMIT ${sqlLimit}
      `;
      const { rows } = await DB.query(sql, params);
      let shaped = rows.map(r => shapeRow(r, unit, zigUsd));
      if (combine === 'deep') shaped = await combineRouterTradesDeep(shaped, unit, windowOpts, zigUsd);
      else shaped = combineRouterTradesShallow(shaped, unit, zigUsd);
      if (klass) shaped = applyClassFilterJS(shaped, unit, klass, zigUsd);
      if (minV != null) shaped = shaped.filter(x => (worthForClass(x, unit, zigUsd) ?? -Infinity) >= minV);
      if (maxV != null) shaped = shaped.filter(x => (worthForClass(x, unit, zigUsd) ?? Infinity) <= maxV);

      const { items, total, pages, page: p } = paginateArray(shaped, page, limit);
      return res.json({ success:true, data: items, meta:{ unit, tf:req.query.tf||'24h', limit, page: p, pages, total } });
    }

    const params = [];
    const { sql, params: p2, zigUsdIdx } = buildWorthPagedSQL({
      scope: 'token',
      scopeValue: tok.token_id,
      direction: dir,
      includeLiquidity,
      windowOpts,
      page, limit,
      unit, klass,
      minValue: minV, maxValue: maxV,
      extraWhere: [],
      includeTotal
    }, params);
    p2[zigUsdIdx - 1] = zigUsd;

    const { rows } = await DB.query(sql, p2);
    const total = includeTotal ? (rows[0]?.total ? Number(rows[0].total) : 0) : undefined;
    const pages = total != null ? Math.max(1, Math.ceil(total / limit)) : undefined;

    const shaped = rows.map(r => {
      const s = shapeRow(r, unit, zigUsd);
      const w = worthForClass(s, unit, zigUsd);
      s.class = w != null ? classifyByThreshold(w) : null;
      return s;
    });

    res.json({ success:true, data: shaped, meta:{ unit, tf:req.query.tf || '24h', limit, page, pages, total } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/pool/:ref  (ref = pool_id or pair contract) */
router.get('/pool/:ref', async (req, res) => {
  try {
    const ref = req.params.ref;
    const row = await DB.query(
      `SELECT pool_id, pair_contract FROM pools WHERE pair_contract=$1 OR pool_id::text=$1 LIMIT 1`,
      [ref]
    );
    if (!row.rows.length) return res.status(404).json({ success:false, error:'pool not found' });
    const poolId = row.rows[0].pool_id;

    const unit   = String(req.query.unit || 'usd').toLowerCase();
    const page   = parsePage(req.query.page);
    const limit  = parseLimit(req.query.limit);
    const dir    = normDir(req.query.direction);
    const includeLiquidity = req.query.includeLiquidity === '1';
    const combine = String(req.query.combineRouter || '').toLowerCase();
    const klass  = String(req.query.class || '').toLowerCase();
    const minV   = req.query.minValue != null ? Number(req.query.minValue) : null;
    const maxV   = req.query.maxValue != null ? Number(req.query.maxValue) : null;
    const includeTotal = ['1','true','yes'].includes(String(req.query.includeTotal || '').toLowerCase());

    const zigUsd = await getZigUsd();
    const windowOpts = { tf:req.query.tf, from:req.query.from, to:req.query.to, days:req.query.days };

    if (combine === 'deep' || combine === '1' || combine === 'true') {
      const sqlLimit = Math.min(limit * 20, 20000);
      const params = [];
      const where = buildWhereBase({ scope:'pool', scopeValue: { poolId }, direction:dir, includeLiquidity }, params, 't');
      const { clause } = buildWindow(windowOpts, params, 't');
      where.push(clause);

      const sql = `
        SELECT
          t.*,
          p.pair_contract,
          p.is_uzig_quote,
          q.exponent AS qexp,
          b.exponent AS bexp,
          b.denom    AS base_denom,
          (SELECT price_in_zig FROM prices WHERE token_id = p.quote_token_id ORDER BY updated_at DESC LIMIT 1) AS pq_price_in_zig,
          toff.exponent AS offer_exp,
          task.exponent AS ask_exp
        ${tradesFromJoin('t')}
        WHERE ${where.join(' AND ')}
        ORDER BY t.created_at DESC
        LIMIT ${sqlLimit}
      `;
      const { rows } = await DB.query(sql, params);
      let shaped = rows.map(r => shapeRow(r, unit, zigUsd));
      if (combine === 'deep') shaped = await combineRouterTradesDeep(shaped, unit, windowOpts, zigUsd);
      else shaped = combineRouterTradesShallow(shaped, unit, zigUsd);
      if (klass) shaped = applyClassFilterJS(shaped, unit, klass, zigUsd);
      if (minV != null) shaped = shaped.filter(x => (worthForClass(x, unit, zigUsd) ?? -Infinity) >= minV);
      if (maxV != null) shaped = shaped.filter(x => (worthForClass(x, unit, zigUsd) ?? Infinity) <= maxV);

      const { items, total, pages, page: p } = paginateArray(shaped, page, limit);
      return res.json({ success:true, data: items, meta:{ unit, tf:req.query.tf||'24h', limit, page: p, pages, total } });
    }

    const params = [];
    const { sql, params: p2, zigUsdIdx } = buildWorthPagedSQL({
      scope: 'pool',
      scopeValue: { poolId },
      direction: dir,
      includeLiquidity,
      windowOpts,
      page, limit,
      unit, klass,
      minValue: minV, maxValue: maxV,
      extraWhere: [],
      includeTotal
    }, params);
    p2[zigUsdIdx - 1] = zigUsd;

    const { rows } = await DB.query(sql, p2);
    const total = includeTotal ? (rows[0]?.total ? Number(rows[0].total) : 0) : undefined;
    const pages = total != null ? Math.max(1, Math.ceil(total / limit)) : undefined;

    const shaped = rows.map(r => {
      const s = shapeRow(r, unit, zigUsd);
      const w = worthForClass(s, unit, zigUsd);
      s.class = w != null ? classifyByThreshold(w) : null;
      return s;
    });

    res.json({ success:true, data: shaped, meta:{ unit, tf:req.query.tf || '24h', limit, page, pages, total } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/wallet/:address */
router.get('/wallet/:address', async (req, res) => {
  try {
    const address = req.params.address;
    const unit    = String(req.query.unit || 'usd').toLowerCase();
    const page    = parsePage(req.query.page);
    const limit   = parseLimit(req.query.limit);
    const dir     = normDir(req.query.direction);
    const includeLiquidity = req.query.includeLiquidity === '1';
    const combine = String(req.query.combineRouter || '').toLowerCase();
    const klass  = String(req.query.class || '').toLowerCase();
    const minV   = req.query.minValue != null ? Number(req.query.minValue) : null;
    const maxV   = req.query.maxValue != null ? Number(req.query.maxValue) : null;
    const includeTotal = ['1','true','yes'].includes(String(req.query.includeTotal || '').toLowerCase());

    const zigUsd = await getZigUsd();
    const windowOpts = { tf:req.query.tf, from:req.query.from, to:req.query.to, days:req.query.days };

    if (combine === 'deep' || combine === '1' || combine === 'true') {
      const sqlLimit = Math.min(limit * 20, 20000);
      const params = [];
      const where = buildWhereBase({ scope:'wallet', scopeValue: address, direction:dir, includeLiquidity }, params, 't');
      const { clause } = buildWindow(windowOpts, params, 't');
      where.push(clause);

      if (req.query.tokenId) {
        const tok = await resolveTokenId(req.query.tokenId);
        if (tok) { where.push(`b.token_id = $${params.length + 1}`); params.push(tok.token_id); }
      }
      if (req.query.pair) { where.push(`p.pair_contract = $${params.length + 1}`); params.push(String(req.query.pair)); }
      else if (req.query.poolId) { where.push(`p.pool_id = $${params.length + 1}`); params.push(String(req.query.poolId)); }

      const sql = `
        SELECT
          t.*,
          p.pair_contract,
          p.is_uzig_quote,
          q.exponent AS qexp,
          b.exponent AS bexp,
          b.denom    AS base_denom,
          (SELECT price_in_zig FROM prices WHERE token_id = p.quote_token_id ORDER BY updated_at DESC LIMIT 1) AS pq_price_in_zig,
          toff.exponent AS offer_exp,
          task.exponent AS ask_exp
        ${tradesFromJoin('t')}
        WHERE ${where.join(' AND ')}
        ORDER BY t.created_at DESC
        LIMIT ${sqlLimit}
      `;
      const { rows } = await DB.query(sql, params);
      let shaped = rows.map(r => shapeRow(r, unit, zigUsd));
      if (combine === 'deep') shaped = await combineRouterTradesDeep(shaped, unit, windowOpts, zigUsd);
      else shaped = combineRouterTradesShallow(shaped, unit, zigUsd);
      if (klass) shaped = applyClassFilterJS(shaped, unit, klass, zigUsd);
      if (minV != null) shaped = shaped.filter(x => (worthForClass(x, unit, zigUsd) ?? -Infinity) >= minV);
      if (maxV != null) shaped = shaped.filter(x => (worthForClass(x, unit, zigUsd) ?? Infinity) <= maxV);

      const { items, total, pages, page: p } = paginateArray(shaped, page, limit);
      return res.json({ success:true, data: items, meta:{ unit, tf:req.query.tf||'24h', limit, page: p, pages, total } });
    }

    // DB-side pagination path
    const extraWhere = [];
    if (req.query.tokenId) {
      const tok = await resolveTokenId(req.query.tokenId);
      if (tok) extraWhere.push(`b.token_id = ${tok.token_id}`);
    }
    if (req.query.pair) {
      extraWhere.push(`p.pair_contract = '${String(req.query.pair).replace(/'/g,"''")}'`);
    } else if (req.query.poolId) {
      extraWhere.push(`p.pool_id = ${Number(req.query.poolId)}`);
    }

    const params = [];
    const { sql, params: p2, zigUsdIdx } = buildWorthPagedSQL({
      scope: 'wallet',
      scopeValue: address,
      direction: dir,
      includeLiquidity,
      windowOpts,
      page, limit,
      unit, klass,
      minValue: minV, maxValue: maxV,
      extraWhere,
      includeTotal
    }, params);
    p2[zigUsdIdx - 1] = zigUsd;

    const { rows } = await DB.query(sql, p2);
    const total = includeTotal ? (rows[0]?.total ? Number(rows[0].total) : 0) : undefined;
    const pages = total != null ? Math.max(1, Math.ceil(total / limit)) : undefined;

    const shaped = rows.map(r => {
      const s = shapeRow(r, unit, zigUsd);
      const w = worthForClass(s, unit, zigUsd);
      s.class = w != null ? classifyByThreshold(w) : null;
      return s;
    });

    res.json({ success:true, data: shaped, meta:{ unit, tf:req.query.tf || '24h', limit, page, pages, total } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/large — unchanged except tiny fixes + includeTotal honored */
router.get('/large', async (req, res) => {
  try {
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const unit   = (req.query.unit || 'zig').toLowerCase();
    const page   = parsePage(req.query.page);
    const limit  = parseLimit(req.query.limit);
    const dir    = normDir(req.query.direction);
    const klass  = String(req.query.class || '').toLowerCase();
    const combine = String(req.query.combineRouter || '').toLowerCase();
    const minV   = req.query.minValue != null ? Number(req.query.minValue) : null;
    const maxV   = req.query.maxValue != null ? Number(req.query.maxValue) : null;

    const zigUsd = await getZigUsd();

    const params = [bucket];
    let dirClause = '';
    if (dir) { params.push(dir); dirClause = `AND lt.direction = $${params.length}`; }

    const offset = (page - 1) * limit;
    const zigUsdIdx = params.length + 1;
    params.push(zigUsd);

    const worthZig = `
      COALESCE(
        CASE WHEN t.offer_asset_denom='uzig'
             THEN t.offer_amount_base / POWER(10, COALESCE(toff.exponent,6))
             WHEN t.ask_asset_denom='uzig'
             THEN t.ask_amount_base   / POWER(10, COCOALESCE(task.exponent,6))
        END,
        CASE WHEN p.is_uzig_quote THEN
               CASE WHEN t.direction='buy'
                    THEN t.offer_amount_base  / POWER(10, COALESCE(q.exponent,6))
                    ELSE t.return_amount_base / POWER(10, COALESCE(q.exponent,6))
               END
             ELSE
               (CASE WHEN t.direction='buy'
                     THEN t.offer_amount_base  / POWER(10, COALESCE(q.exponent,6))
                     ELSE t.return_amount_base / POWER(10, COALESCE(q.exponent,6))
                END) * (SELECT price_in_zig FROM prices WHERE token_id=p.quote_token_id ORDER BY updated_at DESC LIMIT 1)
        END
      )
    `;
    const worthUsd = `(${worthZig}) * $${zigUsdIdx}`;

    const filters = [];
    if (VALID_CLASS.has(klass)) {
      if (klass === 'shrimp') filters.push(unit === 'zig' ? `${worthZig} < 1000` : `${worthUsd} < 1000`);
      if (klass === 'shark')  filters.push(unit === 'zig' ? `${worthZig} >= 1000 AND ${worthZig} <= 10000` : `${worthUsd} >= 1000 AND ${worthUsd} <= 10000`);
      if (klass === 'whale')  filters.push(unit === 'zig' ? `${worthZig} > 10000` : `${worthUsd} > 10000`);
    }
    if (minV != null) filters.push(unit === 'zig' ? `${worthZig} >= ${Number(minV)}` : `${worthUsd} >= ${Number(minV)}`);
    if (maxV != null) filters.push(unit === 'zig' ? `${worthZig} <= ${Number(maxV)}` : `${worthUsd} <= ${Number(maxV)}`);

    const sql = `
      WITH pick AS (
        SELECT DISTINCT ON (lt.tx_hash, lt.pool_id, lt.direction)
               lt.tx_hash, lt.pool_id, lt.direction, lt.value_zig, lt.created_at
        FROM large_trades lt
        WHERE lt.bucket = $1 ${dir ? dirClause : ''}
        ORDER BY lt.tx_hash, lt.pool_id, lt.direction, lt.created_at DESC
      ),
      base AS (
        SELECT
          t.*,
          p.pair_contract,
          p.is_uzig_quote,
          q.exponent AS qexp,
          b.exponent AS bexp,
          b.denom    AS base_denom,
          toff.exponent AS offer_exp,
          task.exponent AS ask_exp,
          (SELECT price_in_zig FROM prices WHERE token_id = p.quote_token_id ORDER BY updated_at DESC LIMIT 1) AS pq_price_in_zig
        FROM trades t
        JOIN pick k ON k.tx_hash = t.tx_hash AND k.pool_id = t.pool_id AND k.direction = t.direction
        JOIN pools  p ON p.pool_id = t.pool_id
        JOIN tokens q ON q.token_id = p.quote_token_id
        JOIN tokens b ON b.token_id = p.base_token_id
        LEFT JOIN tokens toff ON toff.denom = t.offer_asset_denom
        LEFT JOIN tokens task ON task.denom = t.ask_asset_denom
      ),
      ranked AS (
        SELECT base.*,
               ${worthZig.replaceAll('t.', 'base.').replaceAll('p.', 'p').replaceAll('q.', 'q') } AS worth_zig,
               (${worthZig.replaceAll('t.', 'base.').replaceAll('p.', 'p').replaceAll('q.', 'q')}) * $${zigUsdIdx} AS worth_usd,
               COUNT(*) OVER() AS total
        FROM base
        JOIN pools p ON p.pool_id = base.pool_id
        JOIN tokens q ON q.token_id = p.quote_token_id
        ${filters.length ? `WHERE ${filters.map(f => f.replaceAll('t.', 'base.')).join(' AND ')}` : ''}
        ORDER BY base.created_at DESC
        LIMIT ${limit} OFFSET ${offset}
      )
      SELECT * FROM ranked
    `;

    const { rows } = await DB.query(sql, params);

    let shaped = rows.map(r => shapeRow(r, unit, zigUsd));
    if (combine === 'deep' || combine === '1' || combine === 'true') {
      const windowOpts = { tf: bucket };
      if (combine === 'deep') shaped = await combineRouterTradesDeep(shaped, unit, windowOpts, zigUsd);
      else shaped = combineRouterTradesShallow(shaped, unit, zigUsd);
    }
    const total = rows[0]?.total ? Number(rows[0].total) : shaped.length;
    const pages = Math.max(1, Math.ceil(total / limit));

    shaped = shaped.map(s => {
      const w = worthForClass(s, unit, zigUsd);
      s.class = w != null ? classifyByThreshold(w) : null;
      return s;
    });

    res.json({ success:true, data: shaped, meta:{ unit, tf: bucket, limit, page, pages, total } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/recent — same optimization knobs as root */
router.get('/recent', async (req, res) => {
  try {
    const unit   = String(req.query.unit || 'usd').toLowerCase();
    const page   = parsePage(req.query.page);
    const limit  = parseLimit(req.query.limit);
    const dir    = normDir(req.query.direction);
    const includeLiquidity = req.query.includeLiquidity === '1';
    const combine = String(req.query.combineRouter || '').toLowerCase();
    const klass  = String(req.query.class || '').toLowerCase();
    const minV   = req.query.minValue != null ? Number(req.query.minValue) : null;
    const maxV   = req.query.maxValue != null ? Number(req.query.maxValue) : null;
    const includeTotal = ['1','true','yes'].includes(String(req.query.includeTotal || '').toLowerCase());

    const zigUsd = await getZigUsd();
    const windowOpts = { tf:req.query.tf, from:req.query.from, to:req.query.to, days:req.query.days };

    if (combine === 'deep' || combine === '1' || combine === 'true') {
      const sqlLimit = Math.min(limit * 20, 20000);
      const params = [];
      const where = buildWhereBase({ scope:'all', scopeValue:null, direction:dir, includeLiquidity }, params, 't');
      const { clause } = buildWindow(windowOpts, params, 't');
      where.push(clause);

      if (req.query.tokenId) {
        const tok = await resolveTokenId(req.query.tokenId);
        if (tok) { where.push(`b.token_id = $${params.length + 1}`); params.push(tok.token_id); }
      }
      if (req.query.pair) { where.push(`p.pair_contract = $${params.length + 1}`); params.push(String(req.query.pair)); }
      else if (req.query.poolId) { where.push(`p.pool_id = $${params.length + 1}`); params.push(String(req.query.poolId)); }

      const sql = `
        SELECT
          t.*,
          p.pair_contract,
          p.is_uzig_quote,
          q.exponent AS qexp,
          b.exponent AS bexp,
          b.denom    AS base_denom,
          (SELECT price_in_zig FROM prices WHERE token_id = p.quote_token_id ORDER BY updated_at DESC LIMIT 1) AS pq_price_in_zig,
          toff.exponent AS offer_exp,
          task.exponent AS ask_exp
        ${tradesFromJoin('t')}
        WHERE ${where.join(' AND ')}
        ORDER BY t.created_at DESC
        LIMIT ${sqlLimit}
      `;
      const { rows } = await DB.query(sql, params);
      let shaped = rows.map(r => shapeRow(r, unit, zigUsd));
      if (combine === 'deep') shaped = await combineRouterTradesDeep(shaped, unit, windowOpts, zigUsd);
      else shaped = combineRouterTradesShallow(shaped, unit, zigUsd);
      if (klass) shaped = applyClassFilterJS(shaped, unit, klass, zigUsd);
      if (minV != null) shaped = shaped.filter(x => (worthForClass(x, unit, zigUsd) ?? -Infinity) >= minV);
      if (maxV != null) shaped = shaped.filter(x => (worthForClass(x, unit, zigUsd) ?? Infinity) <= maxV);

      const { items, total, pages, page: p } = paginateArray(shaped, page, limit);
      return res.json({ success:true, data: items, meta:{ unit, limit, page: p, pages, total, tf: req.query.tf || '24h', minValue:minV ?? undefined, maxValue:maxV ?? undefined } });
    }

    // DB-side (stable totals optional)
    const extraWhere = [];
    if (req.query.tokenId) {
      const tok = await resolveTokenId(req.query.tokenId);
      if (tok) extraWhere.push(`b.token_id = ${tok.token_id}`);
    }
    if (req.query.pair) {
      extraWhere.push(`p.pair_contract = '${String(req.query.pair).replace(/'/g,"''")}'`);
    } else if (req.query.poolId) {
      extraWhere.push(`p.pool_id = ${Number(req.query.poolId)}`);
    }

    const params = [];
    const { sql, params: p2, zigUsdIdx } = buildWorthPagedSQL({
      scope: 'all',
      scopeValue: null,
      direction: dir,
      includeLiquidity,
      windowOpts,
      page, limit,
      unit, klass,
      minValue: minV, maxValue: maxV,
      extraWhere,
      includeTotal
    }, params);
    p2[zigUsdIdx - 1] = zigUsd;

    const { rows } = await DB.query(sql, p2);
    const total = includeTotal ? (rows[0]?.total ? Number(rows[0].total) : 0) : undefined;
    const pages = total != null ? Math.max(1, Math.ceil(total / limit)) : undefined;

    const shaped = rows.map(r => {
      const s = shapeRow(r, unit, zigUsd);
      const w = worthForClass(s, unit, zigUsd);
      s.class = w != null ? classifyByThreshold(w) : null;
      return s;
    });

    res.json({ success:true, data: shaped, meta:{ unit, limit, page, pages, total, tf: req.query.tf || '24h', minValue:minV ?? undefined, maxValue:maxV ?? undefined } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

export default router;
