// jobs/fx-zig.js
import { fetch } from 'undici';
import { DB } from '../lib/db.js';
import { warn, info } from '../lib/log.js';

const FX_SEC      = parseInt(process.env.FX_SEC || '36', 10);
const CMC_KEY     = process.env.CMC_API_KEY; // required
const CMC_SYMBOL  = process.env.CMC_SYMBOL || 'ZIG';
const CMC_CONVERT = process.env.CMC_CONVERT || 'USD';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function getZigUsd() {
  if (!CMC_KEY) throw new Error('CMC_API_KEY not set');

  const url = `https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol=${encodeURIComponent(
    CMC_SYMBOL,
  )}&convert=${encodeURIComponent(CMC_CONVERT)}`;

  let backoff = 1500;
  for (let attempt = 0; attempt < 4; attempt++) {
    const res = await fetch(url, {
      headers: {
        accept: 'application/json',
        'X-CMC_PRO_API_KEY': CMC_KEY,
      },
    });

    if (res.status === 200) {
      const j = await res.json();
      const price = j?.data?.[CMC_SYMBOL]?.quote?.[CMC_CONVERT]?.price;
      if (price == null || !Number.isFinite(Number(price))) {
        throw new Error('CMC returned no price');
      }
      return Number(price);
    }

    if (res.status === 429 || res.status >= 500) {
      warn(`[fx] CMC ${res.status} → retry in ${backoff}ms`);
      await sleep(backoff);
      backoff = Math.min(backoff * 2, 15000);
      continue;
    }

    const text = await res.text();
    throw new Error(`CMC ${res.status}: ${text.slice(0, 200)}`);
  }

  throw new Error('CMC retries exhausted');
}

async function onceFx() {
  const px = await getZigUsd();
  await DB.query(
    `
    INSERT INTO exchange_rates (ts, zig_usd)
    VALUES (toStartOfMinute(now()), $1)
  `,
    [px],
  );
  info('[fx] zig_usd =', px);
}

export function startFx() {
  (async function loop() {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        await onceFx();
      } catch (e) {
        warn('[fx]', e.message || e);
      }
      await sleep(FX_SEC * 1000);
    }
  })().catch(() => {});
}

// optional single-run export if you want to invoke it manually
export { onceFx as fxOnce };
