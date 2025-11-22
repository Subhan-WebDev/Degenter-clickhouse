// jobs/leaderboards.js
import { DB } from '../lib/db.js';
import { warn, debug } from '../lib/log.js';

const LEADERBOARD_SEC = parseInt(process.env.LEADERBOARD_SEC || '60', 10);
const LARGE_TRADE_MIN_ZIG = Number(process.env.LARGE_TRADE_MIN_ZIG || '1000');

export function startLeaderboards() {
  const BUCKETS = [
    ['30m', 30],
    ['1h', 60],
    ['4h', 240],
    ['24h', 1440],
  ];

  (async function loop () {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        for (const [label, mins] of BUCKETS) {
          /* ───────────── traders leaderboard ───────────── */
          await DB.query(`
            WITH base AS (
              SELECT
                t.signer                           AS signer,
                t.pool_id                          AS pool_id,
                p.is_uzig_quote                    AS is_uzig_quote,
                qtk.exponent                       AS qexp,
                t.direction                        AS direction,
                t.offer_amount_base                AS offer_base,
                t.return_amount_base               AS return_base
              FROM trades AS t
              INNER JOIN pools  AS p   ON p.pool_id      = t.pool_id
              INNER JOIN tokens AS qtk ON qtk.token_id   = p.quote_token_id
              WHERE t.action = 'swap'
                AND t.created_at >= (now() - toIntervalMinute(${mins}))
                AND t.signer != ''
            ),
            latest_prices AS (
              SELECT
                pool_id,
                argMax(price_in_zig, updated_at) AS price_in_zig
              FROM prices
              GROUP BY pool_id
            ),
            priced AS (
              SELECT
                b.signer,
                b.direction,
                CASE
                  WHEN b.is_uzig_quote = 1
                    THEN b.offer_base  / pow(10, coalesce(b.qexp, 6))
                  ELSE (b.offer_base  / pow(10, coalesce(b.qexp, 6))) * coalesce(lp.price_in_zig, 0)
                END AS offer_zig,
                CASE
                  WHEN b.is_uzig_quote = 1
                    THEN b.return_base / pow(10, coalesce(b.qexp, 6))
                  ELSE (b.return_base / pow(10, coalesce(b.qexp, 6))) * coalesce(lp.price_in_zig, 0)
                END AS return_zig
              FROM base AS b
              LEFT JOIN latest_prices AS lp ON lp.pool_id = b.pool_id
            ),
            agg AS (
              SELECT
                signer,
                count()                                 AS trades_count,
                sum(offer_zig + return_zig)            AS volume_zig,
                sum(return_zig - offer_zig)            AS gross_pnl_zig
              FROM priced
              GROUP BY signer
            )
            INSERT INTO leaderboard_traders(
              bucket, address, trades_count, volume_zig, gross_pnl_zig, updated_at
            )
            SELECT
              '${label}'       AS bucket,
              signer           AS address,
              trades_count,
              volume_zig,
              gross_pnl_zig,
              now()            AS updated_at
            FROM agg
          `);

          /* ───────────── large trades table ───────────── */
          await DB.query(`
            WITH recent AS (
              SELECT
                t.pool_id                           AS pool_id,
                t.tx_hash                           AS tx_hash,
                t.signer                            AS signer,
                t.direction                         AS direction,
                t.created_at                        AS created_at,
                p.is_uzig_quote                     AS is_uzig_quote,
                qtk.exponent                        AS qexp,
                CASE
                  WHEN t.direction = 'buy'  THEN t.offer_amount_base
                  WHEN t.direction = 'sell' THEN t.return_amount_base
                  ELSE 0
                END                                 AS quote_leg_base
              FROM trades AS t
              INNER JOIN pools  AS p   ON p.pool_id    = t.pool_id
              INNER JOIN tokens AS qtk ON qtk.token_id = p.quote_token_id
              WHERE t.action = 'swap'
                AND t.created_at >= (now() - toIntervalMinute(${mins}))
            ),
            latest_prices AS (
              SELECT
                pool_id,
                argMax(price_in_zig, updated_at) AS price_in_zig
              FROM prices
              GROUP BY pool_id
            ),
            valued AS (
              SELECT
                r.pool_id,
                r.tx_hash,
                r.signer,
                r.direction,
                r.created_at,
                CASE
                  WHEN r.is_uzig_quote = 1
                    THEN (r.quote_leg_base / pow(10, coalesce(r.qexp, 6)))
                  ELSE (r.quote_leg_base / pow(10, coalesce(r.qexp, 6))) * coalesce(lp.price_in_zig, 0)
                END AS value_zig
              FROM recent AS r
              LEFT JOIN latest_prices AS lp ON lp.pool_id = r.pool_id
            )
            INSERT INTO large_trades(
              bucket, pool_id, tx_hash, signer, direction, value_zig, created_at
            )
            SELECT
              '${label}'   AS bucket,
              pool_id,
              tx_hash,
              signer,
              direction,
              value_zig,
              created_at
            FROM valued
            WHERE value_zig >= ${LARGE_TRADE_MIN_ZIG}
          `);
        }

        debug('[leaderboard] updated');
      } catch (e) {
        warn('[leaderboard]', e.message);
      }

      await new Promise((r) => setTimeout(r, LEADERBOARD_SEC * 1000));
    }
  })().catch(() => {});
}
