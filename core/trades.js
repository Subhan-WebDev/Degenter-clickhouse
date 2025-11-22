// core/trades.js
import { DB } from '../lib/db.js';
import BatchQueue from '../lib/batch.js';

const INSERT_SQL = `
  INSERT INTO trades
   (pool_id, pair_contract, action, direction,
    offer_asset_denom, offer_amount_base,
    ask_asset_denom, ask_amount_base,
    return_amount_base, is_router,
    reserve_asset1_denom, reserve_asset1_amount_base,
    reserve_asset2_denom, reserve_asset2_amount_base,
    height, tx_hash, signer, msg_index, created_at)
  VALUES %VALUES%
`;

// ClickHouse DateTime only supports seconds.
// Keep just "YYYY-MM-DDTHH:MM:SS" from whatever we got.
function normalizeCreatedAt(createdAt) {
  if (!createdAt) return null;
  const s = String(createdAt);
  if (s.length >= 19) {
    return s.slice(0, 19); // e.g. "2025-09-27T21:31:14"
  }
  return s;
}

// Ensure decimals never see '' or undefined; use 0 instead.
function decOrZero(v) {
  if (v === null || v === undefined) return 0;
  const s = String(v).trim();
  if (!s) return 0;
  return s; // still a string of digits, ClickHouse will cast
}

function sqlValues(rows) {
  const vals = [];
  const args = [];
  let i = 1;
  for (const t of rows) {
    vals.push(
      `($${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++})`,
    );

    args.push(
      // pool + id stuff
      t.pool_id ?? 0,
      t.pair_contract || '',
      t.action || 'swap',      // should be one of 'swap','provide','withdraw'
      t.direction || 'buy',    // 'buy','sell','provide','withdraw'

      // offer / ask
      t.offer_asset_denom || '',
      decOrZero(t.offer_amount_base),
      t.ask_asset_denom || '',
      decOrZero(t.ask_amount_base),
      decOrZero(t.return_amount_base),

      // router flag
      t.is_router ? 1 : 0,

      // reserves
      t.reserve_asset1_denom || '',
      decOrZero(t.reserve_asset1_amount_base),
      t.reserve_asset2_denom || '',
      decOrZero(t.reserve_asset2_amount_base),

      // chain info
      t.height ?? 0,
      t.tx_hash || '',
      t.signer || '',
      t.msg_index ?? 0,

      // created_at
      normalizeCreatedAt(t.created_at),
    );
  }
  return { text: INSERT_SQL.replace('%VALUES%', vals.join(',')), args };
}

const tradesQueue = new BatchQueue({
  maxItems: Number(process.env.TRADES_BATCH_MAX || 800),
  maxWaitMs: Number(process.env.TRADES_BATCH_WAIT_MS || 120),
  flushFn: async (items) => {
    if (!items.length) return;
    const { text, args } = sqlValues(items);
    await DB.query(text, args);
  },
});

export async function insertTrade(t) {
  tradesQueue.push(t);
}

export async function drainTrades() {
  await tradesQueue.drain();
}
