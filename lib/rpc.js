// lib/rpc.js
import { fetch } from 'undici';
import { setTimeout as sleep } from 'node:timers/promises';
import { debug, warn } from './log.js';

const RPCS = [process.env.RPC_PRIMARY, process.env.RPC_BACKUP].filter(Boolean);
if (!RPCS.length) warn('RPC endpoints are empty; set RPC_PRIMARY in .env');

let rpcIndex = 0;

async function httpJSON(baseList, idxRef, path) {
  if (!baseList.length) throw new Error('no RPC endpoints configured');
  for (let attempt = 0;; attempt++) {
    const base = baseList[(idxRef + attempt) % baseList.length];
    try {
      const url = `${base}${path}`;
      debug('RPC →', url);
      const r = await fetch(url, { headers: { accept: 'application/json' } });
      if (r.status === 429 || r.status >= 500) throw new Error(`HTTP ${r.status}`);
      return await r.json();
    } catch (e) {
      const backoff = Math.min(1000 * Math.pow(1.5, attempt), 10_000) + Math.floor(Math.random()*250);
      warn(`rpc ${e.message} ${base}${path} → retry in ${backoff}ms`);
      await sleep(backoff);
    }
  }
}

export const rpc = (path) => httpJSON(RPCS, rpcIndex++, path);

export const getStatus       = () => rpc('/status');
export const getBlock        = (h) => rpc(`/block?height=${h}`);
export const getBlockResults = (h) => rpc(`/block_results?height=${h}`);

export const unwrapStatus = j =>
  j?.result?.sync_info?.latest_block_height ? Number(j.result.sync_info.latest_block_height) : null;

export const unwrapBlock = j =>
  (j?.result?.block ? { header: j.result.block.header, txs: j.result.block.data?.txs || [] } : null);

export const unwrapBlockResults = j =>
  ({ txs_results: j?.result?.txs_results || [] });

export default {
  rpc, getStatus, getBlock, getBlockResults,
  unwrapStatus, unwrapBlock, unwrapBlockResults
};
