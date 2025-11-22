// bin/smoke-lib.js
import 'dotenv/config';
import { init, close, DB } from '../lib/db.js';
import { getStatus, unwrapStatus } from '../lib/rpc.js';
import { lcdFactoryDenom } from '../lib/lcd.js';
import { info, warn, err } from '../lib/log.js';

async function main() {
  await init();

  // DB ping
  const { rows } = await DB.query('select 1 as ok');
  info('db ok =', rows[0].ok);

  // RPC ping
  try {
    const st = await getStatus();
    const tip = unwrapStatus(st);
    info('rpc tip height =', tip);
  } catch (e) {
    warn('rpc smoke failed:', e.message);
  }

  // LCD ping (try a harmless endpoint; adjust denom if needed)
  try {
    const j = await lcdFactoryDenom('uzig'); // if your chain doesn't have this, swap to any known denom
    info('lcd sample keys =', Object.keys(j || {}));
  } catch (e) {
    warn('lcd smoke failed:', e.message);
  }

  await close();
}

main().catch(e => { err(e); process.exit(1); });
