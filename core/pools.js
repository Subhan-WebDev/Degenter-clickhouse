// core/pools.js
import { DB } from '../lib/db.js';
import { upsertTokenMinimal } from './tokens.js';
import { info } from '../lib/log.js';

async function getOrCreatePoolId(pairContract) {
  // 1) try existing
  const existing = await DB.query(
    `SELECT pool_id FROM pools WHERE pair_contract = $1 LIMIT 1`,
    [pairContract],
  );
  if (existing.rows[0]) return existing.rows[0].pool_id;

  // 2) compute deterministic id via ClickHouse hash
  const { rows } = await DB.query(
    `SELECT cityHash64($1) AS pool_id`,
    [pairContract],
  );
  return rows[0].pool_id;
}

// ClickHouse DateTime only supports seconds (no nanoseconds).
// We keep just "YYYY-MM-DDTHH:MM:SS" from whatever we got.
function normalizeCreatedAt(createdAt) {
  if (!createdAt) return null;
  const s = String(createdAt);
  if (s.length >= 19) {
    return s.slice(0, 19); // e.g. "2025-09-27T21:31:14"
  }
  return s;
}

export async function upsertPool({
  pairContract,
  baseDenom,
  quoteDenom,
  pairType,
  createdAt,
  height,
  txHash,
  signer,
}) {
  const baseId = await upsertTokenMinimal(baseDenom);
  const quoteId = await upsertTokenMinimal(quoteDenom);
  const isUzig = quoteDenom === 'uzig';

  const poolId = await getOrCreatePoolId(pairContract);
  const createdAtDb = normalizeCreatedAt(createdAt);

  // Insert row if not already present for this pair_contract
  await DB.query(
    `
    INSERT INTO pools (
      pool_id, pair_contract,
      base_token_id, quote_token_id,
      pair_type, is_uzig_quote,
      created_at, created_height, created_tx_hash,
      signer
    )
    SELECT
      $1, $2,
      $3, $4,
      $5, $6,
      $7, $8, $9,
      $10
    WHERE NOT EXISTS (
      SELECT 1 FROM pools WHERE pair_contract = $2
    )
  `,
    [
      poolId,
      pairContract,
      baseId,
      quoteId,
      String(pairType),
      isUzig ? 1 : 0,
      createdAtDb,
      height,
      txHash,
      signer,
    ],
  );

  info(
    'POOL UPSERT:',
    pairContract,
    `${baseDenom}/${quoteDenom}`,
    pairType,
    'pool_id=',
    poolId,
  );
  return poolId;
}

export async function poolWithTokens(pairContract) {
  const { rows } = await DB.query(
    `
    SELECT
      p.pool_id,
      p.is_uzig_quote,
      b.token_id AS base_id,
      b.denom AS base_denom,
      COALESCE(b.exponent, 6) AS base_exp,
      q.token_id AS quote_id,
      q.denom AS quote_denom,
      COALESCE(q.exponent, 6) AS quote_exp
    FROM pools p
    JOIN tokens b ON b.token_id = p.base_token_id
    JOIN tokens q ON q.token_id = p.quote_token_id
    WHERE p.pair_contract = $1
    LIMIT 1
  `,
    [pairContract],
  );
  return rows[0] || null;
}
