// bin/import-pools-csv.js
//
// Imports pools from a CSV (your old DB export) into the current DB,
// creating ONLY base/quote tokens and NEVER inserting LP token denoms
// into the tokens table.
//
// Usage:
//   node bin/import-pools-csv.js /absolute/path/to/pools.csv [--dry]
//
// CSV is expected to have headers:
//   id,tx_hash,pair_contract_address,factory_contract_address,timestamp,
//   asset_0_denom,asset_1_denom,lp_token_denom,pool_type,created_by,pair_type,source
//
// Notes:
// - If one of asset denoms is 'uzig', we force it to be the QUOTE side.
// - pair_type falls back to pool_type when missing.
// - Upserts on (pair_contract) and doesn’t touch core code.
// - Does NOT insert lp_token_denom into tokens. Ever.

import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { parse } from 'csv-parse/sync';
import { DB, init, close } from '../lib/db.js';
import { upsertTokenMinimal } from '../core/tokens.js';
import { info, warn, err } from '../lib/log.js';

function pickPair(base0, base1) {
  // Force 'uzig' to be QUOTE if present. Else keep order as in CSV (asset_0 = base, asset_1 = quote).
  const a0 = String(base0 || '').trim();
  const a1 = String(base1 || '').trim();
  if (a0 === 'uzig' && a1) return { base: a1, quote: 'uzig' };
  if (a1 === 'uzig' && a0) return { base: a0, quote: 'uzig' };
  return { base: a0, quote: a1 };
}

function mapPairType(rowPairType, rowPoolType) {
  const p = String(rowPairType || rowPoolType || '').toLowerCase();
  // your enum is: 'xyk' | 'concentrated' | 'custom-concentrated'
  if (p.includes('conc')) return 'concentrated';
  if (p.includes('xyk') || !p) return 'xyk';
  return 'xyk';
}

function toIso(ts) {
  if (!ts) return null;
  // accept unix or ISO-ish; try Date constructor
  const n = Number(ts);
  if (Number.isFinite(n)) {
    // if looks like seconds, convert; if ms, use as-is
    return new Date(n < 1e12 ? n * 1000 : n).toISOString();
  }
  const d = new Date(ts);
  return isNaN(+d) ? null : d.toISOString();
}

async function upsertPoolDirect({
  pairContract,
  baseTokenId,
  quoteTokenId,
  pairType,
  isUzig,
  lpTokenDenom,
  factoryContract,
  createdAt,
  txHash,
  signer,
}) {
  // Insert directly to pools table (don’t change core)
  await DB.query(
    `
    INSERT INTO pools(
      pair_contract, base_token_id, quote_token_id,
      lp_token_denom, pair_type, is_uzig_quote,
      factory_contract, created_at, created_tx_hash, signer
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    ON CONFLICT (pair_contract) DO UPDATE SET
      base_token_id   = EXCLUDED.base_token_id,
      quote_token_id  = EXCLUDED.quote_token_id,
      pair_type       = EXCLUDED.pair_type,
      is_uzig_quote   = EXCLUDED.is_uzig_quote,
      lp_token_denom  = EXCLUDED.lp_token_denom,
      factory_contract= EXCLUDED.factory_contract,
      -- keep original creation if already present; otherwise set it
      created_at      = COALESCE(pools.created_at, EXCLUDED.created_at),
      created_tx_hash = COALESCE(pools.created_tx_hash, EXCLUDED.created_tx_hash),
      signer          = COALESCE(pools.signer, EXCLUDED.signer)
    `,
    [
      pairContract,
      baseTokenId,
      quoteTokenId,
      lpTokenDenom || null,
      pairType,
      isUzig,
      factoryContract || null,
      createdAt || null,
      txHash || null,
      signer || null,
    ]
  );
}

async function main() {
  const csvPath = process.argv[2];
  const DRY     = process.argv.includes('--dry');

  if (!csvPath) {
    console.error('Usage: node bin/import-pools-csv.js /path/to/pools.csv [--dry]');
    process.exit(2);
  }
  const abs = path.resolve(csvPath);
  if (!fs.existsSync(abs)) {
    console.error('File not found:', abs);
    process.exit(2);
  }

  await init();

  const raw = fs.readFileSync(abs, 'utf8');
  const rows = parse(raw, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });

  info(`[import] parsed ${rows.length} rows from ${abs}`);

  let ok = 0, skip = 0, fail = 0;
  for (const r of rows) {
    try {
      const pairContract = (r.pair_contract_address || r.pair_contract || '').trim();
      if (!pairContract) { skip++; continue; }

      const asset0 = r.asset_0_denom || r.asset0 || '';
      const asset1 = r.asset_1_denom || r.asset1 || '';
      const { base, quote } = pickPair(asset0, asset1);

      if (!base || !quote) {
        warn('[import] missing base/quote for', pairContract);
        skip++;
        continue;
      }

      // NEVER insert lp_token_denom into tokens table
      const baseId  = await upsertTokenMinimal(base);
      const quoteId = await upsertTokenMinimal(quote);

      if (!baseId || !quoteId) {
        warn('[import] could not resolve token ids for', pairContract, base, quote);
        skip++;
        continue;
      }

      const pairType = mapPairType(r.pair_type, r.pool_type);
      const isUzig   = (quote === 'uzig');
      const lpDenom  = r.lp_token_denom || null;
      const factory  = r.factory_contract_address || r.factory_contract || null;
      const createdAt = toIso(r.timestamp);
      const txHash    = r.tx_hash || null;
      const signer    = r.created_by || null;

      if (DRY) {
        info('[dry-run] would upsert pool', { pairContract, base, quote, pairType, isUzig, lpDenom, factory, createdAt, txHash, signer });
        ok++;
        continue;
      }

      await upsertPoolDirect({
        pairContract,
        baseTokenId: baseId,
        quoteTokenId: quoteId,
        pairType,
        isUzig,
        lpTokenDenom: lpDenom,
        factoryContract: factory,
        createdAt,
        txHash,
        signer,
      });

      ok++;
    } catch (e) {
      fail++;
      warn('[import/pool]', e.message);
    }
  }

  info(`[import] done. ok=${ok} skip=${skip} fail=${fail}`);
  await close();
}

main().catch(async (e) => { err(e); await close(); process.exit(1); });
