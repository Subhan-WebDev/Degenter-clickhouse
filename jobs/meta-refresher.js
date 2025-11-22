// jobs/meta-refresher.js
import { DB } from '../lib/db.js';
import { setTokenMetaFromLCD } from '../core/tokens.js';
import { info, warn, debug } from '../lib/log.js';

const META_REFRESH_SEC       = parseInt(process.env.META_REFRESH_SEC || '60', 10);

// One-off backfill toggles
const META_BACKFILL          = process.env.META_BACKFILL === '1';
const META_BACKFILL_BATCH    = parseInt(process.env.META_BACKFILL_BATCH || '250', 10);
const META_BACKFILL_SLEEP_MS = parseInt(process.env.META_BACKFILL_SLEEP_MS || '250', 10);
const META_CONCURRENCY       = parseInt(process.env.META_CONCURRENCY || '4', 10);

function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }

async function runBounded(items, limit, fn) {
  let i = 0;
  const workers = Array(Math.min(limit, items.length)).fill(0).map(async () => {
    while (true) {
      const idx = i++;
      if (idx >= items.length) break;
      const it = items[idx];
      try { await fn(it); }
      catch (e) { warn('[meta/batch]', it?.denom || it, e.message); }
    }
  });
  await Promise.all(workers);
}

/** ➕ One-shot for fast-track */
export async function refreshMetaOnce(denom) {
  if (!denom) return;
  try {
    await setTokenMetaFromLCD(denom);
    info('[meta/once] refreshed', denom);
  } catch (e) {
    warn('[meta/once]', denom, e.message);
  }
}

async function backfillAllTokensOnce() {
  if (!META_BACKFILL) return;
  info(
    `[meta/backfill] starting one-off full metadata refresh (batch=${META_BACKFILL_BATCH}, conc=${META_CONCURRENCY})`,
  );

  let lastId = 0;
  let total  = 0;

  for (;;) {
    // ClickHouse: LIMIT must be a numeric literal, not a String param
    const { rows } = await DB.query(
      `
        SELECT token_id, denom
          FROM tokens
         WHERE token_id > $1
         ORDER BY token_id ASC
         LIMIT ${META_BACKFILL_BATCH}
      `,
      [lastId],
    );

    if (!rows.length) break;

    const first = rows[0]?.token_id;
    const last  = rows.at(-1)?.token_id;
    debug('[meta/backfill] batch', { first, last, count: rows.length });

    await runBounded(rows, META_CONCURRENCY, (r) => setTokenMetaFromLCD(r.denom));
    total += rows.length;
    lastId = last;

    if (META_BACKFILL_SLEEP_MS > 0) await sleep(META_BACKFILL_SLEEP_MS);
  }

  info(`[meta/backfill] completed. refreshed ~${total} tokens`);
}

export function startMetaRefresher() {
  (async function loop () {
    try {
      await backfillAllTokensOnce();
    } catch (e) {
      warn('[meta/backfill]', e.message);
    }

    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        const { rows } = await DB.query(`
          SELECT denom FROM tokens
          WHERE name IS NULL
             OR symbol IS NULL
             OR display IS NULL
             OR exponent IS NULL
          ORDER BY token_id DESC
          LIMIT 3
        `);
        if (rows.length) info('[meta] refreshing', rows.map(r => r.denom));
        await runBounded(rows, Math.min(3, META_CONCURRENCY), (r) => setTokenMetaFromLCD(r.denom));
      } catch (e) {
        warn('[meta]', e.message);
      }
      await sleep(META_REFRESH_SEC * 1000);
    }
  })().catch(()=>{});
}

export function startIbcMetaRefresher() {
  (async function loop () {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        const { rows } = await DB.query(`
          SELECT denom FROM tokens
          WHERE (name IS NULL OR symbol IS NULL OR display IS NULL OR exponent IS 6)
          ORDER BY (denom LIKE 'ibc/%') DESC, token_id DESC
          LIMIT 5
        `);
        if (rows.length) info('[meta/ibc] refreshing', rows.map(r => r.denom));
        await runBounded(rows, Math.min(4, META_CONCURRENCY), (r) => setTokenMetaFromLCD(r.denom));
      } catch (e) { warn('[meta/ibc]', e.message); }
      await sleep(META_REFRESH_SEC * 1000);
    }
  })().catch(()=>{});
}
