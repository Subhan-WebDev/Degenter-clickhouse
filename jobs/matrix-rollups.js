// jobs/matrix-rollups.js — ClickHouse-safe version (no correlated subqueries)
import { DB } from '../lib/db.js';
import log from '../lib/log.js';

const LOOP_SEC = parseInt(process.env.MATRIX_ROLLUP_SEC || '60', 10);
const BUCKETS = [
  ['30m', 30],
  ['1h', 60],
  ['4h', 240],
  ['24h', 1440],
];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * UNIT RULES
 * - prices.price_in_zig: ZIG per 1 DISPLAY unit of that token.
 * - ohlcv_1m.close: price in ZIG per DISPLAY base unit.
 * - *_base columns in trades/pool_state are RAW; convert RAW→DISPLAY by /10^exp.
 * - UZIG RAW exponent is always 6.
 */

/* ------------------------------------------------------------------------- */
/* POOL MATRIX: volumes + TVL                                                */
/* ------------------------------------------------------------------------- */

async function rollPoolMatrix(label, mins) {
  const sql = `
    /* 1) Trade-derived volumes for this bucket */
    WITH q AS (
      SELECT
        t.pool_id                                                     AS pool_id,
        sumIf(t.offer_amount_base, t.direction = 'buy')               AS buy_quote_base,
        sumIf(t.return_amount_base, t.direction = 'sell')             AS sell_quote_base,
        sumIf(1, t.direction = 'buy')                                 AS tx_buy,
        sumIf(1, t.direction = 'sell')                                AS tx_sell,
        uniqExact(t.signer)                                           AS uniq
      FROM trades AS t
      WHERE t.action = 'swap'
        AND t.created_at >= (now() - toIntervalMinute(${mins}))
      GROUP BY t.pool_id
    ),

    /* latest quote price per pool (for non-uzig quotes in volumes) */
    latest_prices AS (
      SELECT
        pool_id,
        toFloat64(argMax(price_in_zig, updated_at)) AS price_in_zig
      FROM prices
      GROUP BY pool_id
    ),

    vol_enriched AS (
      SELECT
        q.pool_id                                   AS pool_id,
        q.tx_buy,
        q.tx_sell,
        q.uniq,
        p.is_uzig_quote,
        qtk.exponent                                AS qexp,

        /* Quote volume in DISPLAY units */
        multiIf(
          p.is_uzig_quote = 1,
          q.buy_quote_base  / 1000000.,
          q.buy_quote_base  / pow(10, coalesce(qtk.exponent, 6))
        )                                           AS vol_buy_quote,
        multiIf(
          p.is_uzig_quote = 1,
          q.sell_quote_base / 1000000.,
          q.sell_quote_base / pow(10, coalesce(qtk.exponent, 6))
        )                                           AS vol_sell_quote,

        /* Value in ZIG (DISPLAY quote * DISPLAY price) */
        multiIf(
          p.is_uzig_quote = 1,
          q.buy_quote_base  / 1000000.,
          (q.buy_quote_base / pow(10, coalesce(qtk.exponent, 6))) *
            coalesce(lp.price_in_zig, 0.)
        )                                           AS vol_buy_zig,
        multiIf(
          p.is_uzig_quote = 1,
          q.sell_quote_base / 1000000.,
          (q.sell_quote_base / pow(10, coalesce(qtk.exponent, 6))) *
            coalesce(lp.price_in_zig, 0.)
        )                                           AS vol_sell_zig
      FROM q
      INNER JOIN pools  AS p   ON p.pool_id    = q.pool_id
      INNER JOIN tokens AS qtk ON qtk.token_id = p.quote_token_id
      LEFT  JOIN latest_prices AS lp ON lp.pool_id = q.pool_id
    ),

    /* 2a) latest base price per (token_id, pool_id) from prices */
    base_direct_price AS (
      SELECT
        pr.token_id  AS token_id,
        pr.pool_id   AS pool_id,
        toFloat64(argMax(pr.price_in_zig, pr.updated_at)) AS price
      FROM prices AS pr
      GROUP BY pr.token_id, pr.pool_id
    ),

    /* 2b) latest uzig-quoted price per token_id (any pool) */
    token_uzig_price AS (
      SELECT
        pr.token_id  AS token_id,
        toFloat64(argMax(pr.price_in_zig, pr.updated_at)) AS price
      FROM prices AS pr
      INNER JOIN pools AS px ON px.pool_id = pr.pool_id
      WHERE px.is_uzig_quote = 1
      GROUP BY pr.token_id
    ),

    /* 2c) last OHLCV close per pool_id */
    pool_ohlcv_price AS (
      SELECT
        o.pool_id    AS pool_id,
        toFloat64(argMax(o.close, o.bucket_start)) AS close_price
      FROM ohlcv_1m AS o
      GROUP BY o.pool_id
    ),

    /* 3) Pool TVL in ZIG + reserves DISPLAY (inline prices instead of latest_price_disp CTE) */
    pool_tvl AS (
      SELECT
        s.pool_id                                                  AS pool_id,
        (s.reserve_base_base / pow(10, coalesce(b.exponent, 6)))   AS reserve_base_disp,
        multiIf(
          p.is_uzig_quote = 1,
          s.reserve_quote_base / 1000000.,
          s.reserve_quote_base / pow(10, coalesce(q.exponent, 6))
        )                                                          AS reserve_quote_disp,

        /* base & quote price in ZIG (DISPLAY) */
        coalesce(
          bdp.price,          -- base price for this pool/base
          bup.price,          -- any uzig-quoted pool for base
          ohp.close_price,    -- last OHLCV close for this pool
          0.
        )                                                          AS base_px_disp_zig,
        multiIf(
          p.is_uzig_quote = 1,
          1.,
          coalesce(qp.price, 0.)
        )                                                          AS quote_px_disp_zig,

        /* TVL in ZIG */
        (
          coalesce(
            multiIf(
              p.is_uzig_quote = 1,
              s.reserve_quote_base / 1000000.,
              s.reserve_quote_base / pow(10, coalesce(q.exponent, 6))
            ),
            0.
          ) * multiIf(
                p.is_uzig_quote = 1,
                1.,
                coalesce(qp.price, 0.)
              )
        ) +
        (
          coalesce(
            (s.reserve_base_base / pow(10, coalesce(b.exponent, 6))),
            0.
          ) * coalesce(
                coalesce(
                  bdp.price,
                  bup.price,
                  ohp.close_price,
                  0.
                ),
                0.
              )
        )                                                          AS tvl_zig
      FROM pool_state AS s
      INNER JOIN pools  AS p  ON p.pool_id  = s.pool_id
      INNER JOIN tokens AS b  ON b.token_id = p.base_token_id
      INNER JOIN tokens AS q  ON q.token_id = p.quote_token_id
      LEFT  JOIN base_direct_price AS bdp
             ON bdp.token_id = p.base_token_id
            AND bdp.pool_id  = p.pool_id
      LEFT  JOIN token_uzig_price AS bup
             ON bup.token_id = p.base_token_id
      LEFT  JOIN pool_ohlcv_price AS ohp
             ON ohp.pool_id = p.pool_id
      LEFT  JOIN token_uzig_price AS qp   -- quote uzig price
             ON qp.token_id = p.quote_token_id
    ),

    /* 4) Final merged view per pool */
    final AS (
      SELECT
        v.pool_id,
        v.vol_buy_quote,
        v.vol_sell_quote,
        v.vol_buy_zig,
        v.vol_sell_zig,
        v.tx_buy,
        v.tx_sell,
        v.uniq                AS unique_traders,
        pt.tvl_zig,
        pt.reserve_base_disp,
        pt.reserve_quote_disp
      FROM vol_enriched AS v
      LEFT JOIN pool_tvl AS pt ON pt.pool_id = v.pool_id
    )

    INSERT INTO pool_matrix (
      pool_id,
      bucket,
      vol_buy_quote, vol_sell_quote,
      vol_buy_zig,  vol_sell_zig,
      tx_buy, tx_sell, unique_traders,
      tvl_zig, reserve_base_disp, reserve_quote_disp,
      updated_at
    )
    SELECT
      pool_id,
      '${label}' AS bucket,
      vol_buy_quote,
      vol_sell_quote,
      vol_buy_zig,
      vol_sell_zig,
      tx_buy,
      tx_sell,
      unique_traders,
      tvl_zig,
      reserve_base_disp,
      reserve_quote_disp,
      now() AS updated_at
    FROM final
  `;

  await DB.query(sql);
}

/* ------------------------------------------------------------------------- */
/* TOKEN MATRIX: price + mcap + FDV + holders                                */
/* ------------------------------------------------------------------------- */

async function rollTokenMatrix(label) {
  const sql = `
    /* 1) Price from PRICES (DISPLAY, uzig-quoted pools only) */
    WITH token_price_prices AS (
      SELECT
        pr.token_id AS token_id,
        toFloat64(argMax(pr.price_in_zig, pr.updated_at)) AS price_disp_zig_prices
      FROM prices AS pr
      INNER JOIN pools AS p2 ON p2.pool_id = pr.pool_id
      WHERE p2.is_uzig_quote = 1
      GROUP BY pr.token_id
    ),

    /* 2) Price from OHLCV (DISPLAY, uzig pools, last 60m) */
    token_price_ohlcv AS (
      SELECT
        p3.base_token_id AS token_id,
        toFloat64(avg(o.close)) AS price_disp_zig_ohlcv
      FROM ohlcv_1m AS o
      INNER JOIN pools AS p3 ON p3.pool_id = o.pool_id
      WHERE p3.is_uzig_quote = 1
        AND o.bucket_start >= (now() - toIntervalMinute(60))
      GROUP BY p3.base_token_id
    ),

    /* 3) Merge prices with special rebasing fix */
    token_price_disp AS (
      SELECT
        t.token_id AS token_id,
        t.exponent AS texp,
        multiIf(
          /* special glue case for 6-decimal rebasing tokens */
          pp.price_disp_zig_prices IS NOT NULL
            AND po.price_disp_zig_ohlcv IS NOT NULL
            AND po.price_disp_zig_ohlcv > 0
            AND (pp.price_disp_zig_prices / po.price_disp_zig_ohlcv) BETWEEN 100000 AND 10000000
            AND coalesce(t.exponent, 6) = 6,
          pp.price_disp_zig_prices / 1000000.,

          pp.price_disp_zig_prices IS NOT NULL,
          pp.price_disp_zig_prices,

          po.price_disp_zig_ohlcv IS NOT NULL,
          po.price_disp_zig_ohlcv,

          0.
        ) AS price_disp_zig
      FROM tokens AS t
      LEFT JOIN token_price_prices AS pp ON pp.token_id = t.token_id
      LEFT JOIN token_price_ohlcv  AS po ON po.token_id = t.token_id
    ),

    /* 4) Holders count per token */
    holders AS (
      SELECT
        h.token_id AS token_id,
        count() AS holders
      FROM holders AS h
      WHERE h.balance_base > 0
      GROUP BY h.token_id
    ),

    /* 5) Final scaled metrics */
    scaled AS (
      SELECT
        t.token_id                                        AS token_id,
        tpd.price_disp_zig                                AS price_in_zig,
        (t.total_supply_base / pow(10, coalesce(toInt32(t.exponent), 6))) AS circ_disp,
        (t.max_supply_base   / pow(10, coalesce(toInt32(t.exponent), 6))) AS max_disp,
        coalesce(h.holders, 0)                            AS holders
      FROM tokens AS t
      LEFT JOIN token_price_disp AS tpd
        ON tpd.token_id = t.token_id
      LEFT JOIN holders AS h
        ON h.token_id = t.token_id
    )

    INSERT INTO token_matrix(
      token_id, bucket,
      price_in_zig, mcap_zig, fdv_zig,
      holders, updated_at
    )
    SELECT
      token_id,
      '${label}' AS bucket,
      price_in_zig,
      (circ_disp * price_in_zig) AS mcap_zig,
      (max_disp  * price_in_zig) AS fdv_zig,
      holders,
      now()
    FROM scaled
  `;

  await DB.query(sql);
}

/* ------------------------------------------------------------------------- */
/* LOOP + EXPORTS                                                            */
/* ------------------------------------------------------------------------- */

async function once() {
  for (const [label, mins] of BUCKETS) {
    await rollPoolMatrix(label, mins);
    await rollTokenMatrix(label);
  }
  log.debug('[matrix] pools & tokens rollups done');
}

async function start() {
  log.info(`[matrix] starting loop (every ${LOOP_SEC}s)`);
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      await once();
    } catch (e) {
      log.warn('[matrix]', e.message || e);
    }
    await sleep(LOOP_SEC * 1000);
  }
}

/** One-shots for fast-track (in CH mode they just recompute all) */
export async function refreshPoolMatrixOnce(_poolId) {
  for (const [label, mins] of BUCKETS) {
    await rollPoolMatrix(label, mins);
  }
  log.info('[matrix/once] pool matrix recomputed for all pools');
}

export async function refreshTokenMatrixOnce(_tokenId) {
  for (const [label] of BUCKETS) {
    await rollTokenMatrix(label);
  }
  log.info('[matrix/once] token matrix recomputed for all tokens');
}

export default { start, once, refreshPoolMatrixOnce, refreshTokenMatrixOnce };
