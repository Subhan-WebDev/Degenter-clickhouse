// bin/start-indexer.js
import 'dotenv/config';
import { init, close } from '../lib/db.js';
import { getStatus } from '../lib/rpc.js';
import { info, err } from '../lib/log.js';
import { readCheckpoint, writeCheckpoint } from '../core/checkpoint.js';
import { processHeight } from '../core/block-processor.js';
import { drainTrades } from '../core/trades.js';
import { drainOHLCV } from '../core/ohlcv.js';
import { drainPoolState } from '../core/pool_state.js';

const ENV_MAX_BLOCKS   = parseInt(process.env.MAX_BLOCKS || '0', 10);      // <=0 => infinite
const POLL_SLEEP_MS    = parseInt(process.env.POLL_SLEEP_MS || '400', 10);
const PIPELINE_DEPTH   = parseInt(process.env.PIPELINE_DEPTH || '3', 10);
const BLOCK_CAP        = Number.isFinite(ENV_MAX_BLOCKS) && ENV_MAX_BLOCKS > 0 ? ENV_MAX_BLOCKS : Infinity;
const IS_INFINITE_MODE = !Number.isFinite(BLOCK_CAP);

function sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }

// Local helper (avoid fragile import)
const unwrapStatus = (j) => {
  const h = j?.result?.sync_info?.latest_block_height
         ?? j?.sync_info?.latest_block_height
         ?? null;
  const n = Number(h);
  return Number.isFinite(n) ? n : null;
};

async function drainAll() {
  await Promise.all([drainTrades(), drainOHLCV(), drainPoolState()]);
}

async function main() {
  await init();

  const tip0 = unwrapStatus(await getStatus());
  if (!tip0) throw new Error('status: no latest_block_height');
  const saved = await readCheckpoint();
  let current = (saved !== null && saved !== undefined) ? Number(saved) : tip0;
  info('startup heights:', { tip: tip0, saved, start: current, cap: IS_INFINITE_MODE ? 'infinite' : BLOCK_CAP });

  let processed = 0;
  const inflight = new Map(); // height -> Promise

  const commitInOrder = async () => {
    const keys = Array.from(inflight.keys()).sort((a,b)=>a-b);
    for (const h of keys) {
      const p = inflight.get(h);
      if (!p) continue;
      const r = await p.catch(e => ({ ok:false, error:e }));
      inflight.delete(h);
      await writeCheckpoint(h);
      processed++;
      const progress = IS_INFINITE_MODE ? `(${processed})` : `(${processed}/${BLOCK_CAP})`;
      info('done height', h, progress);
      await drainAll();
      if (r && r.ok === false && r.error) {
        // keep going, but show error
        err(`height ${h} error:`, r.error.stack || r.error);
      }
    }
  };

  while (processed < BLOCK_CAP) {
    const tipNow = unwrapStatus(await getStatus()) ?? tip0;

    // fill pipeline
    while (inflight.size < PIPELINE_DEPTH && current <= tipNow && processed + inflight.size < BLOCK_CAP) {
      const h = current++;
      inflight.set(h, (async () => {
        try { await processHeight(h); return { ok: true }; }
        catch (e) { return { ok:false, error:e }; }
      })());
    }

    await commitInOrder();

    if (current > tipNow) await sleep(POLL_SLEEP_MS);
  }

  await drainAll();
}

process.on('SIGINT', async () => { await drainAll().catch(()=>{}); await close(); process.exit(0); });
process.on('SIGTERM', async () => { await drainAll().catch(()=>{}); await close(); process.exit(0); });

main()
  .then(async () => { await close(); })
  .catch(async (e) => { err(e); await close(); process.exit(1); });
