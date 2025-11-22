// api/util/prices.js
import { DB } from '../../lib/db.js';

/** best native price for a token (UZIG-quoted pools preferred) */
export async function getBestNativePriceForToken(tokenId) {
  const { rows } = await DB.query(`
    SELECT pr.price_in_zig, pr.pool_id, pr.updated_at
    FROM prices pr
    JOIN pools p ON p.pool_id = pr.pool_id
    WHERE pr.token_id = $1 AND p.is_uzig_quote = TRUE
    ORDER BY pr.updated_at DESC
    LIMIT 1
  `, [tokenId]);
  if (!rows[0]) return null;
  return { price: Number(rows[0].price_in_zig), pool_id: rows[0].pool_id };
}

/** list pools for a token (as base) with last metrics */
export async function listTokenPoolsWithMetrics(tokenId, bucket = '24h') {
  const { rows } = await DB.query(`
    SELECT
      p.pool_id, p.pair_contract, p.is_uzig_quote,
      b.denom AS base_denom, q.denom AS quote_denom,
      pm.vol_buy_zig, pm.vol_sell_zig, pm.tx_buy, pm.tx_sell, pm.unique_traders, pm.tvl_zig
    FROM pools p
    JOIN tokens b ON b.token_id = p.base_token_id
    JOIN tokens q ON q.token_id = p.quote_token_id
    LEFT JOIN pool_matrix pm ON pm.pool_id = p.pool_id AND pm.bucket = $2
    WHERE p.base_token_id = $1
    ORDER BY COALESCE(pm.tvl_zig,0) DESC NULLS LAST, p.pool_id DESC
  `, [tokenId, bucket]);
  return rows;
}
