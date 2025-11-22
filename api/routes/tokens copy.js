// api/routes/tokens.js
import express from 'express';
import { DB } from '../../lib/db.js';
import { resolveTokenId, getZigUsd } from '../util/resolve-token.js';
import { resolvePoolSelection, changePctForMinutes } from '../util/pool-select.js';
import { getCandles, ensureTf } from '../util/ohlcv-agg.js';
import e from 'express';
import log from '../../lib/log.js';

const router = express.Router();
const toNum = x => (x == null ? null : Number(x));
const disp = (base, exp) => (base == null ? null : Number(base) / (10 ** (exp || 0)));

/* =======================================================================
   GET /tokens  (pagination + images + cumulative metrics)
   ======================================================================= */
router.get('/', async (req, res) => {
  try {
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const priceSource = (req.query.priceSource || 'best').toLowerCase();
    const sort = (req.query.sort || 'mcap').toLowerCase();
    const dir  = (req.query.dir || 'desc').toLowerCase() === 'asc' ? 'ASC' : 'DESC';
    const includeChange = req.query.includeChange === '1';
    const limit  = Math.max(1, Math.min(parseInt(req.query.limit || '50', 10), 200));
    const offset = Math.max(0, parseInt(req.query.offset || '0', 10));
    const zigUsd = await getZigUsd();

    const rows = await DB.query(`
      WITH agg AS (
        SELECT p.base_token_id AS token_id,
               SUM(pm.vol_buy_zig + pm.vol_sell_zig) AS vol_zig,
               SUM(pm.tx_buy + pm.tx_sell) AS tx
        FROM pool_matrix pm
        JOIN pools p ON p.pool_id=pm.pool_id
        WHERE pm.bucket=$1
        GROUP BY p.base_token_id
      ),
      base AS (
        SELECT t.token_id, t.denom, t.symbol, t.name, t.image_uri, t.created_at,
               tm.price_in_zig, tm.mcap_zig, tm.fdv_zig, tm.holders,
               a.vol_zig, a.tx
        FROM tokens t
        LEFT JOIN token_matrix tm ON tm.token_id=t.token_id AND tm.bucket=$1
        LEFT JOIN agg a ON a.token_id=t.token_id
      ),
      ranked AS (
        SELECT b.*, COUNT(*) OVER() AS total
        FROM base b
      )
      SELECT * FROM ranked
      ORDER BY
        ${sort === 'created' ? `created_at ${dir}` :
          sort === 'volume'  ? `COALESCE(vol_zig,0) ${dir}` :
          sort === 'tx'      ? `COALESCE(tx,0) ${dir}` :
          sort === 'price'   ? `COALESCE(price_in_zig,0) ${dir}` :
          sort === 'traders' ? `COALESCE(holders,0) ${dir}` :
                               `COALESCE(mcap_zig,0) ${dir}`}
      LIMIT $2 OFFSET $3
    `, [bucket, limit, offset]);

    // optional 24h change using selected priceSource
    const changeMap = new Map();
    if (includeChange && rows.rows.length) {
      const pairs = await Promise.all(rows.rows.map(async r => {
        const sel = await resolvePoolSelection(r.token_id, { priceSource });
        return { id: r.token_id, poolId: sel.pool?.pool_id || null };
      }));
      for (const x of pairs) {
        if (!x.poolId) continue;
        const pct = await changePctForMinutes(x.poolId, 1440);
        changeMap.set(String(x.id), pct);
      }
    }

    const data = rows.rows.map(r => {
      const priceN = toNum(r.price_in_zig);
      const mcapN  = toNum(r.mcap_zig);
      const fdvN   = toNum(r.fdv_zig);
      const volN   = toNum(r.vol_zig) || 0;
      return {
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
    });

    const total = rows.rows[0]?.total ?? 0;
    res.json({ success: true, data, meta: { bucket, priceSource, sort, dir, limit, offset, total } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/* =======================================================================
   GET /tokens/swap-list  (image + hooky stats for dropdown)
   ======================================================================= */
router.get('/swap-list', async (req, res) => {
  try {
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const limit  = Math.max(1, Math.min(parseInt(req.query.limit || '200', 10), 500));
    const offset = Math.max(0, parseInt(req.query.offset || '0', 10));
    const zigUsd = await getZigUsd();

    const rows = await DB.query(`
      WITH agg AS (
        SELECT p.base_token_id AS token_id,
               SUM(pm.vol_buy_zig + pm.vol_sell_zig) AS vol_zig,
               SUM(pm.tx_buy + pm.tx_sell) AS tx,
               SUM(pm.tvl_zig) AS tvl_zig  
        FROM pool_matrix pm
        JOIN pools p ON p.pool_id=pm.pool_id
        WHERE pm.bucket=$1
        GROUP BY p.base_token_id
      )
      SELECT t.token_id, t.symbol, t.name, t.denom, t.image_uri, t.exponent,
             tm.price_in_zig, tm.mcap_zig, tm.fdv_zig,
             a.vol_zig, a.tx, a.tvl_zig
      FROM tokens t
      LEFT JOIN token_matrix tm ON tm.token_id=t.token_id AND tm.bucket=$1
      LEFT JOIN agg a ON a.token_id=t.token_id
      ORDER BY COALESCE(a.vol_zig,0) DESC NULLS LAST
      LIMIT $2 OFFSET $3
    `, [bucket, limit, offset]);

    const data = rows.rows.map(r => {
      const priceN = toNum(r.price_in_zig);
      const mcapN  = toNum(r.mcap_zig);
      const fdvN   = toNum(r.fdv_zig);
      const volN   = toNum(r.vol_zig) || 0;
      const tvlN   = toNum(r.tvl_zig) || 0;
      return {
        tokenId: r.token_id,
        symbol: r.symbol,
        exponent:r.exponent,
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
    });

    res.json({ success: true, data, meta: { bucket, limit, offset }});
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

router.get('/:id', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const zigUsd = await getZigUsd();
    const priceSource = (req.query.priceSource || 'best').toLowerCase();
    const sel = await resolvePoolSelection(tok.token_id, { priceSource, poolId: req.query.poolId });

    // current native price for selected pool (if any)
    const pr = sel.pool?.pool_id
      ? await DB.query(
          `SELECT price_in_zig 
             FROM prices 
            WHERE token_id=$1 AND pool_id=$2 
         ORDER BY updated_at DESC 
            LIMIT 1`,
          [tok.token_id, sel.pool.pool_id]
        )
      : { rows: [] };
    const priceNative = pr.rows[0]?.price_in_zig != null ? Number(pr.rows[0].price_in_zig) : null;

    // token static fields
    const srow = await DB.query(`
      SELECT total_supply_base, max_supply_base, exponent, image_uri, website, twitter, telegram, description
        FROM tokens WHERE token_id=$1
    `, [tok.token_id]);
    const s = srow.rows[0] || {};
    const exp = s.exponent != null ? Number(s.exponent) : 6;
    const circ = disp(s.total_supply_base, exp);
    const max  = disp(s.max_supply_base,   exp);

    // ==== LIVE TVL (sum across this token's UZIG-quoted pools) ===========
    // TVL_zig(pool) = token_reserve_disp * mid(price_in_zig) + zig_reserve_disp
    // TVL_usd       = TVL_zig * zigUsd
    const live = await DB.query(`
      SELECT
        p.pool_id,
        ps.reserve_base_base   AS res_base_base,
        ps.reserve_quote_base  AS res_quote_base,
        tb.exponent            AS base_exp,
        tq.exponent            AS quote_exp,
        (
          SELECT price_in_zig
            FROM prices pr
           WHERE pr.pool_id = p.pool_id
             AND pr.token_id = p.base_token_id
        ORDER BY pr.updated_at DESC
           LIMIT 1
        ) AS price_in_zig
      FROM pools p
      LEFT JOIN pool_state ps   ON ps.pool_id   = p.pool_id
      JOIN tokens tb            ON tb.token_id  = p.base_token_id
      JOIN tokens tq            ON tq.token_id  = p.quote_token_id
      WHERE p.base_token_id = $1
        AND p.is_uzig_quote = TRUE
    `, [tok.token_id]);

    let tvlZigSum = 0;
    for (const r of live.rows) {
      const Rt = Number(r.res_base_base  || 0) / Math.pow(10, Number(r.base_exp  ?? 0)); // token reserve (display)
      const Rz = Number(r.res_quote_base || 0) / Math.pow(10, Number(r.quote_exp ?? 0)); // ZIG reserve (display)
      const midZig = Number(r.price_in_zig || 0); // zig per token
      const tvlZigPool = (Rt * midZig) + Rz;
      if (Number.isFinite(tvlZigPool)) tvlZigSum += tvlZigPool;
    }
    const liquidityNativeZig = tvlZigSum;            // ZIG
    const liquidityUSD       = liquidityNativeZig * zigUsd; // USD
    // =====================================================================

    // stats aggregates across all pools (four buckets)
    const buckets = ['30m','1h','4h','24h'];
    const agg = await DB.query(`
      SELECT pm.bucket,
             COALESCE(SUM(pm.vol_buy_zig),0)    AS vbuy,
             COALESCE(SUM(pm.vol_sell_zig),0)   AS vsell,
             COALESCE(SUM(pm.tx_buy),0)         AS tbuy,
             COALESCE(SUM(pm.tx_sell),0)        AS tsell,
             COALESCE(SUM(pm.unique_traders),0) AS uniq,
             COALESCE(SUM(pm.tvl_zig),0)        AS tvl
        FROM pools p
        JOIN pool_matrix pm ON pm.pool_id=p.pool_id
       WHERE p.base_token_id=$1
         AND pm.bucket = ANY($2)
       GROUP BY pm.bucket
    `, [tok.token_id, buckets]);

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
      '30m': sel.pool?.pool_id ? await changePctForMinutes(sel.pool.pool_id, 30)   : 0,
      '1h' : sel.pool?.pool_id ? await changePctForMinutes(sel.pool.pool_id, 60)   : 0,
      '4h' : sel.pool?.pool_id ? await changePctForMinutes(sel.pool.pool_id, 240)  : 0,
      '24h': sel.pool?.pool_id ? await changePctForMinutes(sel.pool.pool_id, 1440) : 0,
    };

    const mcNative  = (priceNative != null && circ != null) ? circ * priceNative : null;
    const fdvNative = (priceNative != null && max  != null) ? max  * priceNative : null;

    // header meta counts
    const poolsCount = (await DB.query(
      `SELECT COUNT(*)::int AS c FROM pools WHERE base_token_id=$1`, [tok.token_id]
    )).rows[0]?.c || 0;

    const holders = (await DB.query(
      `SELECT holders_count FROM token_holders_stats WHERE token_id=$1`, [tok.token_id]
    )).rows[0]?.holders_count || 0;

    const creation = (await DB.query(
      `SELECT MIN(created_at) AS first_ts FROM pools WHERE base_token_id=$1`, [tok.token_id]
    )).rows[0]?.first_ts || null;

    // socials (twitter block)
    const tw = await DB.query(`
      SELECT handle, user_id, name, is_blue_verified, verified_type, profile_picture, cover_picture,
             followers, following, created_at_twitter, last_refreshed
        FROM token_twitter WHERE token_id=$1
    `, [tok.token_id]);

    // === RESPONSE =========================================================
    res.json({
      success: true,
      data: {
        // Meta / identity
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

        // Compact price object
        price: {
          source: priceSource,
          poolId: sel.pool?.pool_id ? String(sel.pool.pool_id) : null,
          native: priceNative,
          usd: priceNative != null ? priceNative * zigUsd : null,
          changePct: priceChange
        },

        // Supply + caps
        supply: { circulating: circ, max },
        mcap:   { native: mcNative, usd: mcNative != null ? mcNative * zigUsd : null },
        fdv:    { native: fdvNative, usd: fdvNative != null ? fdvNative * zigUsd : null },

        // ===== Original block (names preserved) =====
        priceInNative: priceNative,
        priceInUsd: priceNative != null ? priceNative * zigUsd : null,
        priceSource: priceSource,
        poolId: sel.pool?.pool_id ? String(sel.pool.pool_id) : null,
        pools: poolsCount,
        holder: holders,
        creationTime: creation,
        supply: max ?? circ, // original field
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

        // >>> fixed liquidity <<<
        liquidity: liquidityUSD,                 // USD
        liquidityNative: liquidityNativeZig      // ZIG (extra, non-breaking)
      }
    });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/* =======================================================================
   GET /tokens/:id/pools  (token header + image; pool metrics)
   ======================================================================= */
router.get('/:id/pools', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const includeCaps = req.query.includeCaps === '1';
    const zigUsd = await getZigUsd();

    const header = await DB.query(`SELECT token_id, symbol, denom, image_uri, total_supply_base, max_supply_base, exponent FROM tokens WHERE token_id=$1`, [tok.token_id]);
    const h = header.rows[0];
    const exp = h.exponent != null ? Number(h.exponent) : 6;
    const circ = disp(h.total_supply_base, exp);
    const max  = disp(h.max_supply_base, exp);

    const rows = await DB.query(`
      SELECT
        p.pool_id, p.pair_contract, p.base_token_id, p.quote_token_id, p.is_uzig_quote, p.created_at,
        b.symbol AS base_symbol, b.denom AS base_denom, b.exponent AS base_exp,
        q.symbol AS quote_symbol, q.denom AS quote_denom, q.exponent AS quote_exp,
        COALESCE(pm.tvl_zig,0) AS tvl_zig,
        COALESCE(pm.vol_buy_zig,0) + COALESCE(pm.vol_sell_zig,0) AS vol_zig,
        COALESCE(pm.tx_buy,0) + COALESCE(pm.tx_sell,0) AS tx,
        COALESCE(pm.unique_traders,0) AS unique_traders,
        pr.price_in_zig
      FROM pools p
      JOIN tokens b ON b.token_id=p.base_token_id
      JOIN tokens q ON q.token_id=p.quote_token_id
      LEFT JOIN pool_matrix pm ON pm.pool_id=p.pool_id AND pm.bucket=$2
      LEFT JOIN LATERAL (
        SELECT price_in_zig FROM prices WHERE pool_id=p.pool_id AND token_id=p.base_token_id
        ORDER BY updated_at DESC LIMIT 1
      ) pr ON TRUE
      WHERE p.base_token_id=$1
      ORDER BY p.created_at ASC
    `, [tok.token_id, bucket]);

    const data = rows.rows.map(r => {
      const priceN = r.is_uzig_quote ? toNum(r.price_in_zig) : null;
      const tvlN   = toNum(r.tvl_zig) || 0;
      const volN   = toNum(r.vol_zig) || 0;
      const mcapN  = includeCaps && priceN != null && circ != null ? priceN * circ : null;
      const fdvN   = includeCaps && priceN != null && max  != null ? priceN * max  : null;
      return {
        pairContract: r.pair_contract,
        base: { tokenId: r.base_token_id, symbol: r.base_symbol, denom: r.base_denom, exponent: toNum(r.base_exp) },
        quote:{ tokenId: r.quote_token_id, symbol: r.quote_symbol, denom: r.quote_denom, exponent: toNum(r.quote_exp) },
        isUzigQuote: r.is_uzig_quote === true,
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

/* =======================================================================
   GET /tokens/:id/holders (with total count)
   ======================================================================= */
router.get('/:id/holders', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });
    const limit  = Math.max(1, Math.min(parseInt(req.query.limit || '200', 10), 500));
    const offset = Math.max(0, parseInt(req.query.offset || '0', 10));

    const sup = await DB.query(`SELECT max_supply_base, total_supply_base, exponent FROM tokens WHERE token_id=$1`, [tok.token_id]);
    const exp = sup.exponent != null ? Number(sup.exponent) : 6;
    const maxBase = Number(sup.rows[0]?.max_supply_base || 0);
    const totBase = Number(sup.rows[0]?.total_supply_base || 0);

    const totalRow = await DB.query(`SELECT COUNT(*)::bigint AS total FROM holders WHERE token_id=$1 AND balance_base::numeric > 0`, [tok.token_id]);
    const total = Number(totalRow.rows[0]?.total || 0);

    const { rows } = await DB.query(`
      SELECT address, balance_base::numeric AS bal
      FROM holders
      WHERE token_id=$1 AND balance_base::numeric > 0
      ORDER BY bal DESC
      LIMIT $2 OFFSET $3
    `, [tok.token_id, limit, offset]);

    const top10 = rows.slice(0, 10).reduce((a, r) => a + Number(r.bal), 0);
    const pctTop10Max = maxBase > 0 ? (top10 / maxBase) * 100 : null;

    const holders = rows.map(r => {
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

/* =======================================================================
   GET /tokens/:id/security  (use public.token_security)
   ======================================================================= */
/** GET /tokens/:id/security
 * Uses only columns from your token_security schema.
 * Returns:
 *  - score (0..100) with transparent penalties/bonuses
 *  - checks  (stable, flat keys)
 *  - dev     (birdeye-like details you track)
 */
router.get('/:id/security', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    // Pull security row
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
      FROM public.token_security
      WHERE token_id=$1
      LIMIT 1
    `, [tok.token_id]);
    const s = sq.rows[0] || null;

    // Always get exponent from tokens for display scaling
    const tq = await DB.query(
      `SELECT exponent, created_at FROM public.tokens WHERE token_id=$1`,
      [tok.token_id]
    );
    const exp = tq.rows[0]?.exponent != null ? Number(tq.rows[0]?.exponent) : 6;
    // Helpers
    const toDisp = (v) => v == null ? null : Number(v) / 10 ** exp;
    const num = (v, d = 0) => v == null ? d : Number(v);

    // Derived display values
    const maxSupplyDisp   = toDisp(s?.max_supply_base);
    const totalSupplyDisp = toDisp(s?.total_supply_base);
    const creatorBalDisp  = toDisp(s?.creator_balance_base);

    const creatorPctOfMax = num(s?.creator_pct_of_max, 0); // %
    const top10PctOfMax   = num(s?.top10_pct_of_max, 0);   // %
    const holdersCount    = num(s?.holders_count, 0);

    // --------- Scoring (only what you actually track) ----------
    // Start at 100, subtract penalties, add bonuses, clamp 1..99
    const penalties = [];
    const bonuses   = [];

    // Mintability / supply mutability
    if (s?.is_mintable === true) {
      penalties.push({k:'is_mintable', pts:12});
    } else {
      bonuses.push({k:'not_mintable', pts:4});
    }

    if (s?.can_change_minting_cap === true) {
      penalties.push({k:'can_change_minting_cap', pts:8});
    }

    // Distribution concentration (top10 of max supply)
    if (top10PctOfMax >= 75) penalties.push({k:'top10>=75%', pts:20});
    else if (top10PctOfMax >= 50) penalties.push({k:'top10>=50%', pts:12});
    else if (top10PctOfMax >= 30) penalties.push({k:'top10>=30%', pts:6});
    else bonuses.push({k:'top10<30%', pts:4});

    // Creator concentration (of max supply)
    if (creatorPctOfMax >= 25) penalties.push({k:'creator>=25%', pts:18});
    else if (creatorPctOfMax >= 10) penalties.push({k:'creator>=10%', pts:10});
    else if (creatorPctOfMax > 0) bonuses.push({k:'creator<10%', pts:3});

    // Holder base
    if (holdersCount < 100) penalties.push({k:'holders<100', pts:8});
    else if (holdersCount < 1000) penalties.push({k:'holders<1k', pts:4});
    else if (holdersCount >= 10000) bonuses.push({k:'holders>=10k', pts:5});
    else if (holdersCount >= 50000) bonuses.push({k:'holders>=50k', pts:10}); // rare bonus

    // Supply fully minted bonus
    if (s?.is_mintable === false && s?.max_supply_base != null && s?.total_supply_base != null) {
      if (String(s.max_supply_base) === String(s.total_supply_base)) {
        bonuses.push({k:'fully_minted_equals_max', pts:4});
      }
    }

    // Age bonus (first_seen_at older than 30d)
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

    // ---------- Response shape ----------
    const checks = {
      // flat & explicit (good for UI badges)
      isMintable: !!(s?.is_mintable),
      canChangeMintingCap: !!(s?.can_change_minting_cap),
      maxSupply: maxSupplyDisp,
      totalSupply: totalSupplyDisp,
      top10PctOfMax: Number(top10PctOfMax.toFixed(4)),
      creatorPctOfMax: Number(creatorPctOfMax.toFixed(4)),
      holdersCount: holdersCount
    };

    const dev = {
      // birdeye-like details you actually store
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
        checks,   // backwards-friendly, simple keys
        dev,      // for a “Dev / Ownership” panel
        lastUpdated: s?.checked_at || null,
        source: 'token_security'
      }
    });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});


/* =======================================================================
   GET /tokens/:id/ohlcv  (delegates to util; supports fill; no GROUP BY errors)
   ======================================================================= */
// inside api/routes/tokens.js

// helper: parse timeframe to seconds (supports many)
function tfToSec(tf) {
  const m = { m:60, h:3600, d:86400, w:604800, M:2592000 }; // 30d month for charts
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
/* =======================================================================
   GET /tokens/:id/ohlcv  (timezone-safe, numeric bucket keys)
   ======================================================================= */

// Helpers (no SQL change)
function toEpochSecUtc(v) {
  if (v == null) return null;
  if (typeof v === 'number') return v > 1e12 ? Math.floor(v / 1000) : Math.floor(v);
  if (v instanceof Date) return Math.floor(v.getTime() / 1000);
  const s = String(v);
  const ms = Date.parse(s.endsWith('Z') ? s : s + 'Z'); // treat as UTC
  return Number.isFinite(ms) ? Math.floor(ms / 1000) : null;
}
function alignFloorSec(sec, stepSec) {
  return Math.floor(sec / stepSec) * stepSec;
}

router.get('/:id/ohlcv', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const tf = (req.query.tf || '1m');
    const stepSec = tfToSec(tf);
    const mode = (req.query.mode || 'price').toLowerCase();                 // price|mcap
    const unit = (req.query.unit || 'native').toLowerCase();                // native|usd
    const priceSource = (req.query.priceSource || 'best').toLowerCase();    // best|first|pool|all
    const poolIdParam = req.query.poolId;
    const pairParam   = req.query.pair;
    const fill = (req.query.fill || 'none').toLowerCase();                  // prev|zero|none

    // resolve time window:
    const now = new Date();
    let toIso   = req.query.to || now.toISOString();
    let fromIso = req.query.from || null;

    if (!fromIso) {
      if (req.query.span) {
        const spanSec = tfToSec(req.query.span);
        const to = new Date(toIso);
        fromIso = new Date(to.getTime() - spanSec*1000).toISOString();
      } else if (req.query.window) {
        const bars = Math.max(1, Math.min(parseInt(req.query.window,10) || 300, 5000));
        const to = new Date(toIso);
        fromIso = new Date(to.getTime() - bars*stepSec*1000).toISOString();
      } else {
        const bars = tf === '1m' ? 1440 : 300;
        fromIso = new Date(new Date(toIso).getTime() - bars*stepSec*1000).toISOString();
      }
    }

    const zigUsd = await getZigUsd();

    // supply (for mcap conversion)
    const ss = await DB.query(`SELECT total_supply_base, exponent FROM tokens WHERE token_id=$1`, [tok.token_id]);
    const exp = ss.rows[0]?.exponent!= null ? Number(ss.rows[0]?.exponent) : 6;
    const circ = ss.rows[0]?.total_supply_base != null ? Number(ss.rows[0].total_supply_base) / 10**exp : null;

    // resolve pool set:
    let headerSQL = ``;
    let params = [];
    if (priceSource === 'all') {
      params = [tok.token_id, fromIso, toIso, stepSec];
      headerSQL = `
        WITH src AS (
          SELECT o.pool_id, o.bucket_start, o.open, o.high, o.low, o.close, o.volume_zig, o.trade_count
          FROM ohlcv_1m o
          JOIN pools p ON p.pool_id=o.pool_id
          WHERE p.base_token_id=$1 AND p.is_uzig_quote=TRUE
            AND o.bucket_start >= $2::timestamptz AND o.bucket_start < $3::timestamptz
        ),
      `;
    } else {
      let poolRow = null;
      if (priceSource === 'pool' && (poolIdParam || pairParam)) {
        const { rows } = await DB.query(
          `SELECT pool_id FROM pools WHERE (pool_id::text=$1 OR pair_contract=$1) AND base_token_id=$2 LIMIT 1`,
          [poolIdParam || pairParam, tok.token_id]
        );
        poolRow = rows[0] || null;
      }
      if (!poolRow) {
        const { pool } = await resolvePoolSelection(tok.token_id, { priceSource, poolId: poolIdParam });
        poolRow = pool;
      }
      if (!poolRow?.pool_id) {
        return res.json({ success:true, data: [], meta:{ tf, mode, unit, fill, priceSource, poolId:null } });
      }
      params = [poolRow.pool_id, fromIso, toIso, stepSec];
      headerSQL = `
        WITH src AS (
          SELECT o.pool_id, o.bucket_start, o.open, o.high, o.low, o.close, o.volume_zig, o.trade_count
          FROM ohlcv_1m o
          WHERE o.pool_id=$1
            AND o.bucket_start >= $2::timestamptz AND o.bucket_start < $3::timestamptz
        ),
      `;
    }

    // aggregate 1m into requested tf (unchanged SQL)
    const { rows } = await DB.query(`
      ${headerSQL}
      tagged AS (
        SELECT
          bucket_start, open, high, low, close, volume_zig, trade_count,
          to_timestamp(floor(extract(epoch from bucket_start)/$4)*$4) AT TIME ZONE 'UTC' AS bucket_ts
        FROM src
      ),
      sums AS (
        SELECT bucket_ts,
               MIN(low)  AS low,
               MAX(high) AS high,
               SUM(volume_zig)  AS volume_native,
               SUM(trade_count) AS trades
        FROM tagged
        GROUP BY bucket_ts
      ),
      firsts AS (
        SELECT DISTINCT ON (bucket_ts) bucket_ts, open
        FROM tagged
        ORDER BY bucket_ts, bucket_start ASC
      ),
      lasts AS (
        SELECT DISTINCT ON (bucket_ts) bucket_ts, close
        FROM tagged
        ORDER BY bucket_ts, bucket_start DESC
      )
      SELECT s.bucket_ts AS ts, f.open, s.high, s.low, l.close, s.volume_native, s.trades
      FROM sums s
      LEFT JOIN firsts f USING (bucket_ts)
      LEFT JOIN lasts  l USING (bucket_ts)
      ORDER BY ts ASC
    `, params);

    // ---- JS gap fill using numeric seconds (no Date-string keys) ----
   // ---------- JS gap fill with smooth opens (no SQL / endpoint changes) ----------
const out = [];

// bucket range in *seconds*
const start = Math.floor(new Date(fromIso).getTime() / 1000 / stepSec) * stepSec;
const end   = Math.floor(new Date(toIso).getTime()   / 1000 / stepSec) * stepSec;

// Normalize any timestamp to UTC epoch seconds (avoid timezone drift)
const toSec = (v) => {
  if (v == null) return null;
  if (typeof v === 'number') return v > 1e12 ? Math.floor(v / 1000) : Math.floor(v);
  const s = String(v);
  const ms = Date.parse(s.endsWith('Z') ? s : s + 'Z');
  return Number.isFinite(ms) ? Math.floor(ms / 1000) : null;
};

// Build a map keyed by bucket second
const bySec = new Map(
  rows
    .map(r => ({
      sec: toSec(r.ts),
      open: Number(r.open),
      high: Number(r.high),
      low:  Number(r.low),
      close: Number(r.close),
      volume: Number(r.volume_native),
      trades: Number(r.trades)
    }))
    .filter(r => r.sec != null)
    .map(r => [r.sec, r])
);

// We will (a) fill missing buckets flat at prevClose
//       and (b) force the *first real bucket after a gap* to open at prevClose
let prevClose = null;

for (let ts = start; ts <= end; ts += stepSec) {
  const r = bySec.get(ts);

  if (r) {
    // Smooth open: use previous close if we have one
    const openAdj = (prevClose != null) ? prevClose : r.open;
    const highAdj = Math.max(r.high, openAdj);
    const lowAdj  = Math.min(r.low,  openAdj);

    const base = {
      ts: new Date(ts * 1000).toISOString(),
      open: openAdj,
      high: highAdj,
      low:  lowAdj,
      close: r.close,
      volume: r.volume,
      trades: r.trades
    };
    out.push(base);
    prevClose = base.close; // advance
  } else if (fill !== 'none') {
    if (fill === 'prev' && prevClose != null) {
      out.push({
        ts: new Date(ts * 1000).toISOString(),
        open: prevClose,
        high: prevClose,
        low:  prevClose,
        close: prevClose,
        volume: 0,
        trades: 0
      });
      // prevClose unchanged
    } else if (fill === 'zero') {
      out.push({
        ts: new Date(ts * 1000).toISOString(),
        open: 0, high: 0, low: 0, close: 0,
        volume: 0, trades: 0
      });
      prevClose = 0;
    }
  }
}

// ---------- optional conversions (unchanged behavior) ----------
const conv = out.map(b => {
  let o = { ...b };
  if (mode === 'mcap' && circ != null) {
    o.open  = o.open  * circ;
    o.high  = o.high  * circ;
    o.low   = o.low   * circ;
    o.close = o.close * circ;
  }
  if (unit === 'usd') {
    o.open  *= zigUsd; o.high *= zigUsd; o.low *= zigUsd; o.close *= zigUsd;
    o.volume*= zigUsd;
  }
  return o;
});

// ---------- response ----------
res.json({
  success: true,
  data: conv,
  meta: {
    tf, mode, unit, fill, priceSource,
    poolId: (params[0] && priceSource !== 'all') ? String(params[0]) : null
  }
});

  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});



export default router;