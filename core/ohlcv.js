// core/ohlcv.js – ClickHouse implementation
import { DB } from '../lib/db.js';
import BatchQueue from '../lib/batch.js';

// Remember last close per pool so next bucket's open = previous close
// Key: String(pool_id), Value: { bucket_start: string, close: number }
const lastCloseByPool = new Map();

// ClickHouse DateTime only supports seconds.
// Keep just "YYYY-MM-DDTHH:MM:SS" from whatever we got.
function normalizeBucketStart(bucket_start) {
  if (!bucket_start) return null;
  if (bucket_start instanceof Date) {
    // toISOString() -> "YYYY-MM-DDTHH:MM:SS.sssZ"
    return bucket_start.toISOString().slice(0, 19);
  }
  const s = String(bucket_start);
  if (s.length >= 19) return s.slice(0, 19);
  return s;
}

// Format decimals safely for ClickHouse Decimal
// scale=18 for prices, scale=8 for volumes/liquidity
function decOrZero(v, scale) {
  const n = Number(v || 0);
  if (!Number.isFinite(n)) return '0';
  if (scale != null) return n.toFixed(scale);
  return String(n);
}

// Aggregate items in the batch to 1 row per (pool_id, bucket_start)
function aggregateBatch(items) {
  const map = new Map();
  for (const it of items) {
    const key = `${it.pool_id}__${it.bucket_start}`;
    let row = map.get(key);
    if (!row) {
      // first trade in this candle
      row = {
        pool_id: it.pool_id,
        bucket_start: it.bucket_start,
        open: it.price,
        high: it.price,
        low: it.price,
        close: it.price,
        volume_zig: it.vol_zig || 0,
        trade_count: it.trade_inc || 0,
        liquidity_zig: it.liquidity_zig ?? null,
      };
      map.set(key, row);
    } else {
      // update OHLCV
      if (it.price > row.high) row.high = it.price;
      if (it.price < row.low) row.low = it.price;
      row.close = it.price; // last price wins
      row.volume_zig += it.vol_zig || 0;
      row.trade_count += it.trade_inc || 0;
      if (it.liquidity_zig != null) row.liquidity_zig = it.liquidity_zig;
    }
  }
  return Array.from(map.values());
}

function buildInsertSQL(rows) {
  if (!rows.length) return { sql: null, args: [] };

  const cols = [
    'pool_id',
    'bucket_start',
    'open',
    'high',
    'low',
    'close',
    'volume_zig',
    'trade_count',
    'liquidity_zig',
  ];

  const placeholders = [];
  const args = [];
  let p = 1;

  for (const r of rows) {
    placeholders.push(
      `($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`,
    );

    args.push(
      r.pool_id ?? 0,
      r.bucket_start,                            // normalized string
      decOrZero(r.open, 18),                    // Decimal(38,18)
      decOrZero(r.high, 18),
      decOrZero(r.low, 18),
      decOrZero(r.close, 18),
      decOrZero(r.volume_zig, 8),               // Decimal(38,8)
      r.trade_count ?? 0,                       // Int32
      decOrZero(r.liquidity_zig ?? 0, 8),       // Decimal(38,8)
    );
  }

  const sql = `
    INSERT INTO ohlcv_1m
      (${cols.join(',')})
    VALUES
      ${placeholders.join(',')}
  `;

  return { sql, args };
}

const ohlcvQueue = new BatchQueue({
  maxItems: Number(process.env.OHLCV_BATCH_MAX || 600),
  maxWaitMs: Number(process.env.OHLCV_BATCH_WAIT_MS || 120),
  flushFn: async (items) => {
    if (!items.length) return;

    // 1) aggregate inside this batch to 1 row per (pool_id, bucket_start)
    const agg = aggregateBatch(items);

    // 1.5) ensure cross-bucket continuity:
    // for each pool, sort by bucket_start and make
    // next bucket's open = previous bucket's close.
    if (agg.length) {
      agg.sort((a, b) => {
        const ap = String(a.pool_id ?? '');
        const bp = String(b.pool_id ?? '');
        if (ap < bp) return -1;
        if (ap > bp) return 1;
        const as = String(a.bucket_start ?? '');
        const bs = String(b.bucket_start ?? '');
        if (as < bs) return -1;
        if (as > bs) return 1;
        return 0;
      });

      for (const r of agg) {
        const key = String(r.pool_id ?? '');
        const prev = lastCloseByPool.get(key);

        const curTs = String(r.bucket_start ?? '');
        if (prev && curTs > prev.bucket_start) {
          // Make this bucket's open = previous bucket's close
          r.open = prev.close;
          // We leave high/low as actual trade extremes inside this bucket.
          // It's fine if open is slightly outside [low, high].
        }

        // Update last close for this pool
        lastCloseByPool.set(key, {
          bucket_start: curTs,
          close: r.close,
        });
      }
    }

    // 2) build and run INSERT
    const { sql, args } = buildInsertSQL(agg);
    if (!sql) return;
    await DB.query(sql, args);
  },
});

/**
 * Public API — same signature you had before.
 * The indexer calls this for each trade-tick.
 */
export async function upsertOHLCV1m({
  pool_id,
  bucket_start,
  price,
  vol_zig,
  trade_inc,
  liquidity_zig = null,
}) {
  const bs = normalizeBucketStart(bucket_start);
  ohlcvQueue.push({
    pool_id,
    bucket_start: bs,
    price,
    vol_zig: vol_zig || 0,
    trade_inc: trade_inc || 0,
    liquidity_zig,
  });
}

export async function drainOHLCV() {
  await ohlcvQueue.drain();
}
