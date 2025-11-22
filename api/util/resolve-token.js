// api/util/resolve-token.js
import { DB } from '../../lib/db.js';

export async function resolveTokenId(idOrSymbolOrDenom) {
  // try denom, exact symbol, case-insens symbol, name (ILIKE), then id
  const q = `
    WITH inp AS (SELECT $1::text AS q)
    SELECT token_id, denom, symbol, name, exponent
    FROM tokens t
    WHERE t.denom = (SELECT q FROM inp)
       OR t.symbol = (SELECT q FROM inp)
       OR lower(t.symbol) = lower((SELECT q FROM inp))
       OR t.name ILIKE (SELECT q FROM inp)
       OR t.token_id::text = (SELECT q FROM inp)
    ORDER BY
      CASE WHEN t.denom = (SELECT q FROM inp) THEN 0 ELSE 1 END,
      CASE WHEN lower(t.symbol) = lower((SELECT q FROM inp)) THEN 0 ELSE 1 END,
      t.token_id DESC
    LIMIT 1`;
  const { rows } = await DB.query(q, [idOrSymbolOrDenom]);
  return rows[0] || null;
}

export async function getZigUsd() {
  const { rows } = await DB.query(
    `SELECT zig_usd FROM exchange_rates ORDER BY ts DESC LIMIT 1`
  );
  return rows[0]?.zig_usd ? Number(rows[0].zig_usd) : 0;
}
