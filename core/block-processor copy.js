// core/block-processor.js
import { getBlock, getBlockResults, unwrapBlock, unwrapBlockResults } from '../lib/rpc.js';
import { info, warn, debug } from '../lib/log.js';
import { upsertPool, poolWithTokens } from './pools.js';
import { setTokenMetaFromLCD } from './tokens.js';
import { insertTrade } from './trades.js';
import { upsertPoolState } from './pool_state.js';
import { upsertOHLCV1m } from './ohlcv.js';
import { pgNotify } from '../lib/pg_notify.js';
import { startFasttrackListener } from '../jobs/fasttrack-listener.js';
import { DB } from '../lib/db.js'; // ⬅️ added so we can check meta readiness
import {
  digitsOrNull, wasmByAction, byType, buildMsgSenderMap, normalizePair,
  classifyDirection, parseReservesKV, parseAssetsList, sha256hex
} from './parse.js';
import { BlockTimer } from './timing.js';

// ⬇️ price helpers (use same code path as the price job)
import { upsertPrice, fetchPoolReserves, priceFromReserves_UZIGQuote } from './prices.js';

// ✅ Start fast-track ONCE, at process boot (not per-pair)
startFasttrackListener();

const FACTORY_ADDR = process.env.FACTORY_ADDR || '';
const ROUTER_ADDR = process.env.ROUTER_ADDR || null;

const BLOCK_PROC_CONCURRENCY = Number(process.env.BLOCK_PROC_CONCURRENCY || 12);
const MAX_PENDING_TASKS = Number(process.env.BLOCK_PROC_MAX_TASKS || 5000);

async function runWithConcurrency(tasks, limit, T, labelPrefix) {
  const results = [];
  let i = 0;
  const workers = Array(Math.min(limit, tasks.length)).fill(0).map(async () => {
    while (true) {
      const idx = i++;
      if (idx >= tasks.length) break;
      const label = `${labelPrefix}#${idx}`;
      results[idx] = await T.track(label, tasks[idx]);
    }
  });
  await Promise.all(workers);
  return results;
}

// caches (safe, small)
const poolsByContract = new Map(); // pairContract -> pool row
const metaFetched = new Set();

async function getPoolCached(pairContract) {
  if (!pairContract) return null;
  const c = poolsByContract.get(pairContract);
  if (c) return c;
  const p = await poolWithTokens(pairContract);
  if (p) poolsByContract.set(pairContract, p);
  return p;
}
function rememberMeta(denom, lowPrioTasks) {
  if (!denom || metaFetched.has(denom)) return;
  metaFetched.add(denom);
  lowPrioTasks.push(() => setTokenMetaFromLCD(denom));
}

export async function processHeight(h) {
  const T = new BlockTimer(h, debug);
  info('PROCESS BLOCK →', h);

  T.mark('rpc');
  const [blkJson, resJson] = await Promise.all([getBlock(h), getBlockResults(h)]);
  T.endMark('rpc');

  T.mark('unwrap');
  const blk = unwrapBlock(blkJson);
  const res = unwrapBlockResults(resJson);
  T.endMark('unwrap');

  if (!blk || !blk.header) throw new Error('block: missing header');

  const txs = blk.txs || [];
  const hashes = txs.map(sha256hex);
  const txResults = res.txs_results || [];
  const timestamp = blk.header.time;

  const poolTasks = [];           // phase 1: create_pair → ensure pools exist
  const tasks = [];               // phase 2: swaps/liquidity/etc
  const lowPrioTasks = [];
  const prefetchSet = new Set();
  let nCreatePair = 0, nSwap = 0, nLiq = 0;

  T.mark('scan');
  const N = Math.max(txResults.length, hashes.length);
  for (let i = 0; i < N; i++) {
    const txr = txResults[i] || { events: [] };
    const tx_hash = hashes[i] || null;

    const wasms = byType(txr.events, 'wasm');
    const insts = byType(txr.events, 'instantiate');
    const executes = byType(txr.events, 'execute');
    const msgs = byType(txr.events, 'message');
    const msgSenderByIndex = buildMsgSenderMap(msgs);

    // create_pair
    const cps = wasmByAction(wasms, 'create_pair');
    for (const cp of cps) {
      if ((cp.m.get('_contract_address') || '').trim() !== FACTORY_ADDR) continue;
      nCreatePair++;
      const pairType = String(cp.m.get('pair_type') || 'xyk');
      const { base, quote } = normalizePair(cp.m.get('pair'));
      const reg = wasms.find(w => w.m.get('action') === 'register' && w.m.get('_contract_address') === FACTORY_ADDR);
      const poolAddr = reg?.m.get('pair_contract_addr') || insts.at(-1)?.m.get('_contract_address');
      if (!poolAddr) { warn('create_pair: could not find pool addr'); continue; }
      const signer = msgSenderByIndex.get(Number(cp.m.get('msg_index'))) || null;

      poolTasks.push(async () => {
        await upsertPool({
          pairContract: poolAddr, baseDenom: base, quoteDenom: quote, pairType,
          createdAt: timestamp, height: h, txHash: tx_hash, signer
        });

        // refresh our cache
        const p = await poolWithTokens(poolAddr);
        if (p) poolsByContract.set(poolAddr, p);

        // 🔔 Fast-track notify for the new pair
        if (p) {
          await pgNotify('pair_created', {
            pool_id: p.pool_id,
            pair_contract: poolAddr,
            base_denom: p.base_denom,
            quote_denom: p.quote_denom,
            base_token_id: p.base_id,
            quote_token_id: p.quote_id,
            is_uzig_quote: p.is_uzig_quote === true
          });
          debug('[notify] pair_created', poolAddr);
        }
      });

      rememberMeta(base, lowPrioTasks);
      rememberMeta(quote, lowPrioTasks);
    }

    // swaps
    const swaps = wasmByAction(wasms, 'swap');
    for (let idx = 0; idx < swaps.length; idx++) {
      const s = swaps[idx];
      const pairContract = s.m.get('_contract_address');
      if (!pairContract) continue;
      nSwap++;
      prefetchSet.add(pairContract);

      const offer = s.m.get('offer_asset') || s.m.get('offer_asset_denom');
      const ask = s.m.get('ask_asset') || s.m.get('ask_asset_denom');
      const offerAmt = digitsOrNull(s.m.get('offer_amount'));
      const askAmt = digitsOrNull(s.m.get('ask_amount'));
      const retAmt = digitsOrNull(s.m.get('return_amount'));

      let res1d = s.m.get('reserve_asset1_denom') || s.m.get('asset1_denom') || null;
      let res1a = digitsOrNull(s.m.get('reserve_asset1_amount') || s.m.get('asset1_amount'));
      let res2d = s.m.get('reserve_asset2_denom') || s.m.get('asset2_denom') || null;
      let res2a = digitsOrNull(s.m.get('reserve_asset2_amount') || s.m.get('asset2_amount'));
      const reservesStr = s.m.get('reserves');
      if ((!res1d || !res1a || !res2d || !res2a) && reservesStr) {
        const kv = parseReservesKV(reservesStr);
        if (kv?.[0]) { res1d = res1d ?? kv[0].denom; res1a = res1a ?? digitsOrNull(kv[0].amount_base); }
        if (kv?.[1]) { res2d = res2d ?? kv[1].denom; res2a = res2a ?? digitsOrNull(kv[1].amount_base); }
      }

      const msgIndex = Number(s.m.get('msg_index') ?? idx);
      const signerEOA = msgSenderByIndex.get(msgIndex) || null;

      const poolSwapSender = s.m.get('sender') || null;
      const routerExec = !!ROUTER_ADDR && executes.some(e => e.m.get('_contract_address') === ROUTER_ADDR && Number(e.m.get('msg_index') || -1) === msgIndex);
      const isRouter = !!(ROUTER_ADDR && (poolSwapSender === ROUTER_ADDR || routerExec));

      tasks.push(async () => {
        const pool = await getPoolCached(pairContract);
        if (!pool) { warn(`[swap] unknown pool ${pairContract}`); return; }

        await insertTrade({
          pool_id: pool.pool_id, pair_contract: pairContract,
          action: 'swap', direction: classifyDirection(offer, pool.quote_denom),
          offer_asset_denom: offer, offer_amount_base: offerAmt,
          ask_asset_denom: ask, ask_amount_base: askAmt,
          return_amount_base: retAmt, is_router: isRouter,
          reserve_asset1_denom: res1d, reserve_asset1_amount_base: res1a,
          reserve_asset2_denom: res2d, reserve_asset2_amount_base: res2a,
          height: h, tx_hash, signer: signerEOA, msg_index: msgIndex, created_at: timestamp
        });

        await upsertPoolState(
          pool.pool_id, pool.base_denom, pool.quote_denom, res1d, res1a, res2d, res2a
        );

        // OHLCV & live price — compute from LCD reserves
        if (pool.is_uzig_quote) {
          try {
            // 🔒 WAIT FOR META: require exponent/decimals to be known
            // WAIT FOR META check (both in swap & liquidity branches)
            const { rows: rExp } = await DB.query(
              'SELECT exponent AS exp FROM tokens WHERE token_id = $1',
              [pool.base_id]
            );
            const baseExp = rExp?.[0]?.exp;
            if (baseExp == null) {
              debug('[price/skip] meta not ready for base token', { token_id: pool.base_id, denom: pool.base_denom });
              return;
            }

            const reserves = await fetchPoolReserves(pairContract);
            const price = priceFromReserves_UZIGQuote(
              { base_denom: pool.base_denom, base_exp: Number(baseExp) },
              reserves
            );
            if (price != null && Number.isFinite(price) && price > 0) {
              const quoteRaw = (offer === pool.quote_denom)
                ? Number(offerAmt || 0)
                : Number(retAmt || 0);
              const volZig = quoteRaw / 1e6; // UZIG always 6 decimals

              const bucket = new Date(Math.floor(new Date(timestamp).getTime() / 60000) * 60000);
              await upsertOHLCV1m({
                pool_id: pool.pool_id,
                bucket_start: bucket,
                price,
                vol_zig: volZig,
                trade_inc: 1
              });

              await upsertPrice(pool.base_id, pool.pool_id, price, true);
            }
          } catch (e) {
            warn('[swap price/lcd]', pairContract, e.message);
          }
        }
      });
    }

    // liquidity (provide/withdraw)
    const provides = wasmByAction(wasms, 'provide_liquidity');
    const withdraws = wasmByAction(wasms, 'withdraw_liquidity');
    const liqs = [...provides, ...withdraws];

    for (let li = 0; li < liqs.length; li++) {
      const le = liqs[li];
      const pairContract = le.m.get('_contract_address');
      if (!pairContract) continue;
      nLiq++;
      prefetchSet.add(pairContract);

      let res1d = le.m.get('reserve_asset1_denom');
      let res1a = digitsOrNull(le.m.get('reserve_asset1_amount'));
      let res2d = le.m.get('reserve_asset2_denom');
      let res2a = digitsOrNull(le.m.get('reserve_asset2_amount'));
      if ((!res1d || !res1a || !res2d || !res2a) && le.m.get('assets')) {
        const parsed = parseAssetsList(le.m.get('assets'));
        if (parsed?.a1) { res1d = res1d ?? parsed.a1.denom; res1a = res1a ?? digitsOrNull(parsed.a1.amount_base); }
        if (parsed?.a2) { res2d = res2d ?? parsed.a2.denom; res2a = res2a ?? digitsOrNull(parsed.a2.amount_base); }
      }

      const msgIndex = Number(le.m.get('msg_index') ?? li);
      const signerEOA = msgSenderByIndex.get(msgIndex) || null;

      tasks.push(async () => {
        const pool = await getPoolCached(pairContract);
        if (!pool) return;
        const action = (le.m.get('action') === 'provide_liquidity' ? 'provide' : 'withdraw');

        await insertTrade({
          pool_id: pool.pool_id, pair_contract: pairContract,
          action, direction: action,
          offer_asset_denom: null, offer_amount_base: null,
          ask_asset_denom: null, ask_amount_base: null,
          return_amount_base: digitsOrNull(le.m.get('share')),
          is_router: false,
          reserve_asset1_denom: res1d, reserve_asset1_amount_base: res1a,
          reserve_asset2_denom: res2d, reserve_asset2_amount_base: res2a,
          height: h, tx_hash, signer: signerEOA, msg_index: msgIndex, created_at: timestamp
        });

        await upsertPoolState(
          pool.pool_id, pool.base_denom, pool.quote_denom, res1d, res1a, res2d, res2a
        );

        // Live price on liquidity tx — compute from LCD reserves (no OHLCV here)
        if (pool.is_uzig_quote) {
          try {
            // 🔒 WAIT FOR META
            // WAIT FOR META check (both in swap & liquidity branches)
            const { rows: rExp } = await DB.query(
              'SELECT exponent AS exp FROM tokens WHERE token_id = $1',
              [pool.base_id]
            );
            const baseExp = rExp?.[0]?.exp;
            if (baseExp == null) {
              debug('[price/skip] meta not ready for base token', { token_id: pool.base_id, denom: pool.base_denom });
              return;
            }


            const reserves = await fetchPoolReserves(pairContract);
            const price = priceFromReserves_UZIGQuote(
              { base_denom: pool.base_denom, base_exp: Number(baseExp) },
              reserves
            );
            if (price != null && Number.isFinite(price) && price > 0) {
              await upsertPrice(pool.base_id, pool.pool_id, price, true);
            }
          } catch (e) {
            warn('[liq price/lcd]', pairContract, e.message);
          }
        }
      });
    }

    if (tasks.length >= MAX_PENDING_TASKS) {
      T.count('flushes');
      await runWithConcurrency(tasks.splice(0), BLOCK_PROC_CONCURRENCY, T, 'core');
    }
  }
  T.endMark('scan');

  // === PHASE 1: ensure pools are written before liq/swaps from same tx
  if (poolTasks.length > 0) {
    await runWithConcurrency(poolTasks, BLOCK_PROC_CONCURRENCY, T, 'pools');
  }

  // Prefetch pools after creation
  T.mark('prefetch');
  if (prefetchSet.size > 0) {
    await runWithConcurrency(
      Array.from(prefetchSet, (pc) => async () => {
        if (!poolsByContract.has(pc)) {
          const p = await poolWithTokens(pc);
          if (p) poolsByContract.set(pc, p);
        }
      }),
      Math.min(BLOCK_PROC_CONCURRENCY, 24),
      T,
      'prefetch'
    );
  }
  T.endMark('prefetch');

  // === PHASE 2: swaps/liquidity
  T.mark('core_tasks');
  if (tasks.length) await runWithConcurrency(tasks, BLOCK_PROC_CONCURRENCY, T, 'core');
  T.endMark('core_tasks');

  // low priority (LCD metadata) — keep small
  T.mark('lowprio');
  if (lowPrioTasks.length) await runWithConcurrency(lowPrioTasks, Math.min(4, BLOCK_PROC_CONCURRENCY), T, 'meta');
  T.endMark('lowprio');

  // counters
  T.count('create_pair', nCreatePair);
  T.count('swaps', nSwap);
  T.count('liquidity', nLiq);

  const S = T.summary();
  debug(`[block ${h}] metrics`, JSON.stringify(S));
  info('done height', h, `(+${nSwap} swaps, ${nLiq} liq)`, `${S.total_ms} ms`);
}
