// jobs/holders-refresher.js — parallel sweeps + IBC opt-out + fairness (ClickHouse version)
import { DB } from '../lib/db.js';
import { lcdDenomOwners } from '../lib/lcd.js';
import { info, warn } from '../lib/log.js';

const HOLDERS_REFRESH_SEC = parseInt(process.env.HOLDERS_REFRESH_SEC || '180', 10);
// how many tokens to sweep per cycle (choose based on LCD headroom)
const HOLDERS_BATCH_SIZE = parseInt(process.env.HOLDERS_BATCH_SIZE || '4', 10);
// limit how many LCD pages we pull PER TOKEN in one sweep
const MAX_HOLDER_PAGES_PER_CYCLE = parseInt(process.env.MAX_HOLDER_PAGES_PER_CYCLE || '30', 10);
// limit how many LCD page fetches run concurrently across the batch
const LCD_PAGE_CONCURRENCY = parseInt(process.env.LCD_PAGE_CONCURRENCY || '4', 10);

function digitsOrNull(x) {
  const s = String(x ?? '');
  return /^\d+$/.test(s) ? s : null;
}

function isIbcDenom(d) {
  return typeof d === 'string' && d.startsWith('ibc/');
}

/**
 * For IBC or error cases: just mark stats "touched" without a real count.
 * ClickHouse version: use DELETE + INSERT instead of ON CONFLICT.
 */
async function bumpStatsTimestampOnly(token_id) {
  try {
    // Remove old stats row (if any)
    await DB.query(
      `ALTER TABLE token_holders_stats DELETE WHERE token_id = $1`,
      [token_id],
    );
  } catch (e) {
    // Not fatal; table might be empty or DELETE may be eventually consistent
    warn('[holders/bumpStats] delete stats', token_id, e.message);
  }

  // Insert a fresh row with NULL count
  await DB.query(
    `
      INSERT INTO token_holders_stats(token_id, holders_count, updated_at)
      VALUES ($1, NULL, now())
    `,
    [token_id],
  );
}

/* ───────────── simple semaphore to throttle LCD page fetches ───────────── */
class Semaphore {
  constructor(n) { this.n = n; this.q = []; }
  async acquire() {
    if (this.n > 0) { this.n--; return; }
    await new Promise(res => this.q.push(res));
  }
  release() {
    const next = this.q.shift();
    if (next) next(); else this.n++;
  }
}
const pageSem = new Semaphore(LCD_PAGE_CONCURRENCY);

async function fetchOwnersPageThrottled(denom, nextKey) {
  await pageSem.acquire();
  try {
    return await lcdDenomOwners(denom, nextKey);
  } finally {
    pageSem.release();
  }
}

/**
 * Fully sweep holders for a single token (skips IBC denoms).
 *
 * ClickHouse version notes:
 * - We DO NOT use transactions or ON CONFLICT.
 * - We build a full in-memory snapshot of holders, then:
 *   1) DELETE existing holders for that token_id
 *   2) INSERT the fresh snapshot
 *   3) Recompute holders_count and write a single stats row
 */
export async function refreshHoldersOnce(token_id, denom, maxPages = MAX_HOLDER_PAGES_PER_CYCLE) {
  if (!token_id || !denom) return;

  if (isIbcDenom(denom)) {
    info('[holders/once] skip IBC denom', denom);
    await bumpStatsTimestampOnly(token_id);
    return;
  }

  // address -> balance_base (string)
  const addrMap = new Map();
  let nextKey = null;

  for (let i = 0; i < maxPages; i++) {
    let page;
    try {
      page = await fetchOwnersPageThrottled(denom, nextKey);
    } catch (e) {
      const msg = String(e?.message || '');
      if (msg.includes('501')) {
        warn('[holders/owners 501]', denom, 'skipping this cycle');
        await bumpStatsTimestampOnly(token_id);
        return;
      }
      warn('[holders/owners]', denom, msg);
      break; // transient error → end this token’s sweep; try later
    }

    const items = page?.denom_owners || [];
    if (!items.length && !page?.pagination?.next_key) {
      // empty & no more pages → done
    }

    for (const it of items) {
      const addr = it.address;
      const amtRaw = it.balance?.amount || '0';
      const amt = digitsOrNull(amtRaw) || '0';
      addrMap.set(addr, amt); // last value wins if duplicates
    }

    nextKey = page?.pagination?.next_key || null;
    if (!nextKey) break; // finished all pages
  }

  const holdersRows = Array.from(addrMap.entries()).map(([address, balance_base]) => ({
    address,
    balance_base,
  }));

  try {
    // 1) Remove old holders snapshot for this token
    await DB.query(
      `ALTER TABLE holders DELETE WHERE token_id = $1`,
      [token_id],
    );
  } catch (e) {
    warn('[holders/once]', denom, 'delete holders failed:', e.message);
    // we still try to insert new data – CH deletes are eventually consistent
  }

  // 2) Insert new snapshot (if any)
  if (holdersRows.length > 0) {
    const params = [];
    let i = 1;
    const values = holdersRows.map((row) => {
      params.push(token_id, row.address, row.balance_base);
      return `($${i++},$${i++},$${i++}, now())`;
    });

    await DB.query(
      `
        INSERT INTO holders(token_id, address, balance_base, updated_at)
        VALUES ${values.join(',')}
      `,
      params,
    );
  }

  // 3) Recompute holders_count (balance_base > 0)
  let count = 0;
  try {
    const { rows: hc } = await DB.query(
      `
        SELECT count() AS c
        FROM holders
        WHERE token_id = $1 AND balance_base > 0
      `,
      [token_id],
    );
    count = Number(hc?.[0]?.c || 0);
  } catch (e) {
    warn('[holders/once]', denom, 'count query failed:', e.message);
  }

  // 4) Replace stats row
  try {
    await DB.query(
      `ALTER TABLE token_holders_stats DELETE WHERE token_id = $1`,
      [token_id],
    );
  } catch (e) {
    warn('[holders/once]', denom, 'delete stats failed:', e.message);
  }

  await DB.query(
    `
      INSERT INTO token_holders_stats(token_id, holders_count, updated_at)
      VALUES ($1, $2, now())
    `,
    [token_id, count],
  );

  info('[holders/once] updated', denom, 'count=', count);
}

/**
 * Periodic refresher:
 * - pick the K stalest non-IBC, non-uzig tokens this cycle
 * - sweep them in parallel with LCD page concurrency limits
 *
 * ClickHouse version:
 * - uses toDateTime('1970-01-01 00:00:00') instead of TIMESTAMPTZ 'epoch'
 * - LIMIT must be a numeric literal, not a String param
 */
export function startHoldersRefresher() {
  (async function loop() {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        const { rows } = await DB.query(`
          WITH cand AS (
            SELECT
              t.token_id,
              t.denom,
              coalesce(s.updated_at, toDateTime('1970-01-01 00:00:00')) AS last_h_upd
            FROM tokens AS t
            LEFT JOIN token_holders_stats AS s ON s.token_id = t.token_id
            WHERE t.denom != 'uzig'
              AND t.denom NOT LIKE 'ibc/%'
          )
          SELECT token_id, denom
          FROM cand
          ORDER BY last_h_upd ASC
          LIMIT ${HOLDERS_BATCH_SIZE}
        `);

        if (rows.length > 0) {
          await Promise.allSettled(
            rows.map(({ token_id, denom }) =>
              refreshHoldersOnce(token_id, denom),
            ),
          );
        }
      } catch (e) {
        warn('[holders]', e.message);
      }

      await new Promise((r) => setTimeout(r, HOLDERS_REFRESH_SEC * 1000));
    }
  })().catch(() => {});
}
