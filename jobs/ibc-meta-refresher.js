// jobs/meta-refresher.js
import { DB } from '../lib/db.js';
import { setTokenMetaFromLCD } from '../core/tokens.js';
import { info, warn } from '../lib/log.js';

const META_REFRESH_SEC = parseInt(process.env.META_REFRESH_SEC || '60', 10);

export function startIbcMetaRefresher() {
  (async function loop () {
    while (true) {
      try {
        // Prefer IBC tokens whose meta fields are missing, then others
        const { rows } = await DB.query(`
          SELECT denom FROM tokens
          WHERE (name IS NULL OR symbol IS NULL OR display IS NULL OR exponent IS NULL)
          ORDER BY (denom LIKE 'ibc/%') DESC, token_id DESC
          LIMIT 5
        `);
        if (rows.length) info('[meta] refreshing', rows.map(r => r.denom));
        await Promise.all(rows.map(r => setTokenMetaFromLCD(r.denom)));
      } catch (e) { warn('[meta]', e.message); }
      await new Promise(r => setTimeout(r, META_REFRESH_SEC * 1000));
    }
  })().catch(()=>{});
}
