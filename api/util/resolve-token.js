// api/util/resolve-token.js
import { DB } from '../../lib/db.js';

const esc = v => `'${String(v).replace(/'/g, "''")}'`;

export async function resolveTokenId(idOrSymbolOrDenom) {
  const q = esc(idOrSymbolOrDenom);

  // try denom, exact symbol, case-insens symbol, name (case-insens), then id-as-string
  const sql = `
    SELECT
      token_id,
      denom,
      symbol,
      name,
      exponent
    FROM tokens AS t
    WHERE t.denom = ${q}
       OR t.symbol = ${q}
       OR lower(t.symbol) = lower(${q})
       OR lower(t.name)   = lower(${q})
       OR toString(t.token_id) = ${q}
    ORDER BY
      CASE WHEN t.denom = ${q} THEN 0 ELSE 1 END,
      CASE WHEN lower(t.symbol) = lower(${q}) THEN 0 ELSE 1 END,
      t.token_id DESC
    LIMIT 1
  `;

  const { rows } = await DB.query(sql);
  return rows[0] || null;
}

export async function getZigUsd() {
  const r = await DB.query(`
    SELECT zig_usd
    FROM exchange_rates
    ORDER BY ts DESC
    LIMIT 1
  `);
  return r.rows[0]?.zig_usd ? Number(r.rows[0].zig_usd) : 0;
}
