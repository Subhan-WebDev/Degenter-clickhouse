// api/util/ohlcv.js
import { DB } from '../../lib/db.js';

/**
 * Aggregate token-wide OHLCV from per-pool 1m bars (only pools where token is base).
 * Returns bars in native; caller can convert to USD.
 */
export async function getTokenOhlcv({
  tokenId,
  fromIso, toIso,
  timeframe = '1m' // supports '1m','5m','15m','1h','4h','1d'
}) {
  const tf = timeframe.toLowerCase();
  const valid = new Set(['1m','5m','15m','1h','4h','1d']);
  if (!valid.has(tf)) throw new Error(`bad timeframe: ${tf}`);

  const grpExpr = (() => {
    if (tf === '1m')  return `date_trunc('minute', o.bucket_start)`;
    if (tf === '5m')  return `date_trunc('minute', o.bucket_start) - MOD(EXTRACT(MINUTE FROM o.bucket_start)::int,5) * INTERVAL '1 minute'`;
    if (tf === '15m') return `date_trunc('minute', o.bucket_start) - MOD(EXTRACT(MINUTE FROM o.bucket_start)::int,15) * INTERVAL '1 minute'`;
    if (tf === '1h')  return `date_trunc('hour', o.bucket_start)`;
    if (tf === '4h')  return `date_trunc('hour', o.bucket_start) - MOD(EXTRACT(HOUR FROM o.bucket_start)::int,4) * INTERVAL '1 hour'`;
    if (tf === '1d')  return `date_trunc('day', o.bucket_start)`;
  })();

  // sum across all pools for the token where it is base
  const { rows } = await DB.query(`
    WITH tp AS (
      SELECT pool_id FROM pools WHERE base_token_id = $1
    ),
    agg AS (
      SELECT
        ${grpExpr} AS ts,
        first_value(open)  OVER w AS open,
        max(high)          AS high,
        min(low)           AS low,
        last_value(close)  OVER w AS close,
        sum(volume_zig)    AS volume_zig,
        sum(trade_count)   AS trade_count
      FROM ohlcv_1m o
      WHERE o.pool_id IN (SELECT pool_id FROM tp)
        AND o.bucket_start >= $2::timestamptz
        AND o.bucket_start <  $3::timestamptz
      WINDOW w AS (PARTITION BY ${grpExpr} ORDER BY o.bucket_start
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
      GROUP BY ts
      ORDER BY ts ASC
    )
    SELECT * FROM agg
  `, [tokenId, fromIso, toIso]);

  return rows.map(r => ({
    ts: r.ts,
    open: Number(r.open),
    high: Number(r.high),
    low: Number(r.low),
    close: Number(r.close),
    volume_native: Number(r.volume_zig),
    trades: Number(r.trade_count)
  }));
}
