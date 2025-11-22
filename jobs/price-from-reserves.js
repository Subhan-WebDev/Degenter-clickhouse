// jobs/price-from-reserves.js
import { DB } from '../lib/db.js';
import { fetchPoolReserves, priceFromReserves_UZIGQuote, upsertPrice } from '../core/prices.js';
import { warn, debug } from '../lib/log.js';

const PRICE_SIM_SEC = parseInt(process.env.PRICE_SIM_SEC || '8', 10);
const CONCURRENCY   = parseInt(process.env.PRICE_JOB_CONCURRENCY || '8', 10);

async function runBounded(items, limit, fn) {
  let i = 0;
  const workers = Array(Math.min(limit, items.length))
    .fill(0)
    .map(async () => {
      while (true) {
        const idx = i++;
        if (idx >= items.length) break;
        try {
          await fn(items[idx]);
        } catch {
          // already logged
        }
      }
    });
  await Promise.all(workers);
}

export function startPriceFromReserves() {
  (async function loop() {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        // Only UZIG-quoted pools here; non-uzig handled via fx-chain
        const { rows } = await DB.query(`
          SELECT
            p.pool_id,
            p.pair_contract,
            b.token_id AS base_token_id,
            b.denom    AS base_denom,
            b.exponent AS base_exp
          FROM pools p
          INNER JOIN tokens b ON b.token_id = p.base_token_id
          WHERE p.is_uzig_quote = 1
          ORDER BY p.pool_id DESC
        `);

        await runBounded(rows, CONCURRENCY, async (r) => {
          try {
            // wait-for-meta: need exponent present
            if (r.base_exp == null) {
              debug('[price/job skip] meta not ready', {
                pool_id: r.pool_id,
                denom: r.base_denom,
              });
              return;
            }

            const reserves = await fetchPoolReserves(r.pair_contract);
            const price = priceFromReserves_UZIGQuote(
              { base_denom: r.base_denom, base_exp: Number(r.base_exp) },
              reserves,
            );
            if (price != null && Number.isFinite(price) && price > 0) {
              await upsertPrice(r.base_token_id, r.pool_id, price, true);
              debug('[price/reserves]', r.pair_contract, r.base_denom, 'px_zig=', price);
            }
          } catch (e) {
            warn('[price/reserves]', r.pair_contract, e.message);
          }
        });
      } catch (e) {
        warn('[priceFromReserves loop]', e.message);
      }

      await new Promise((r) => setTimeout(r, PRICE_SIM_SEC * 1000));
    }
  })().catch(() => {});
}
