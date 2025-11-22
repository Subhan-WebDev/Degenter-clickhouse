// api/util/pool-select.js
import { DB } from '../../lib/db.js';

/** Earliest-created UZIG-quoted pool for this token. */
export async function firstUzigPool(tokenId) {
  const { rows } = await DB.query(`
    SELECT p.pool_id, p.pair_contract
    FROM pools p
    WHERE p.base_token_id=$1 AND p.is_uzig_quote=TRUE
    ORDER BY p.created_at ASC
    LIMIT 1
  `, [tokenId]);
  if (!rows.length) return null;
  return { pool_id: rows[0].pool_id, pair_contract: rows[0].pair_contract };
}

/** Best executable price pool (lowest price_in_zig, tie-break by highest 24h TVL). */
export async function bestUzigPool(tokenId) {
  const prices = await DB.query(`
    SELECT pr.pool_id, pr.price_in_zig, pr.updated_at, p.pair_contract
    FROM prices pr
    JOIN pools p ON p.pool_id=pr.pool_id
    WHERE pr.token_id=$1 AND p.is_uzig_quote=TRUE
    ORDER BY pr.updated_at DESC
    LIMIT 16
  `, [tokenId]);

  if (!prices.rows.length) return null;

  const ids = prices.rows.map(r => r.pool_id);
  const tvls = await DB.query(
    `SELECT pool_id, tvl_zig FROM pool_matrix WHERE bucket='24h' AND pool_id = ANY($1)`,
    [ids]
  );
  const tvlMap = new Map(tvls.rows.map(r => [String(r.pool_id), Number(r.tvl_zig || 0)]));

  const sorted = prices.rows.sort((a, b) => {
    const pa = Number(a.price_in_zig), pb = Number(b.price_in_zig);
    if (pa !== pb) return pa - pb;                       // lower price better
    const ta = tvlMap.get(String(a.pool_id)) || 0;
    const tb = tvlMap.get(String(b.pool_id)) || 0;
    return tb - ta;                                      // tie-break by higher TVL
  });

  const top = sorted[0];
  return {
    pool_id: top.pool_id,
    pair_contract: top.pair_contract,
    price_in_zig: Number(top.price_in_zig)
  };
}

/**
 * Resolve pool according to requested source.
 * @returns {{ mode:'pool'|'best'|'first', pool:{pool_id:number, pair_contract?:string}|null }}
 */
export async function resolvePoolSelection(tokenId, { priceSource = 'best', poolId } = {}) {
  const src = String(priceSource || 'best').toLowerCase();

  if (src === 'pool') {
    const pid = Number(poolId || 0);
    if (!Number.isFinite(pid) || pid <= 0) return { mode: 'pool', pool: null };
    const { rows } = await DB.query(
      `SELECT pool_id, pair_contract FROM pools WHERE pool_id=$1 AND base_token_id=$2`,
      [pid, tokenId]
    );
    return { mode: 'pool', pool: rows[0] ? { pool_id: rows[0].pool_id, pair_contract: rows[0].pair_contract } : null };
  }

  if (src === 'first') {
    return { mode: 'first', pool: await firstUzigPool(tokenId) };
  }

  return { mode: 'best', pool: await bestUzigPool(tokenId) };
}

/** % change over N minutes for one pool’s OHLCV stream. */
export async function changePctForMinutes(poolId, minutes) {
  const { rows } = await DB.query(`
    WITH last AS (
      SELECT close FROM ohlcv_1m WHERE pool_id=$1 ORDER BY bucket_start DESC LIMIT 1
    ),
    prev AS (
      SELECT close FROM ohlcv_1m
      WHERE pool_id=$1 AND bucket_start <= now() - ($2 || ' minutes')::interval
      ORDER BY bucket_start DESC LIMIT 1
    )
    SELECT CASE WHEN prev.close IS NOT NULL AND prev.close > 0
                THEN ((last.close - prev.close)/prev.close)*100 END AS pct
    FROM last, prev
  `, [poolId, minutes]);
  return rows[0]?.pct != null ? Number(rows[0].pct) : null;
}
