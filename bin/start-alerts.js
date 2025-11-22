// bin/start-alerts.js
import 'dotenv/config';
import { init } from '../lib/db.js';
import { info } from '../lib/log.js';
import { startAlertsEngine } from '../jobs/alerts.js';

async function main() {
  await init();
  info('alerts: starting…');
  startAlertsEngine();
  setInterval(()=>{}, 1<<30);
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
