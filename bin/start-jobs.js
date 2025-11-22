// bin/start-jobs.js
import 'dotenv/config';
import { init } from '../lib/db.js';
import { info } from '../lib/log.js';

import { startMetaRefresher } from '../jobs/meta-refresher.js';
import { startHoldersRefresher } from '../jobs/holders-refresher.js';
import { startPriceFromReserves } from '../jobs/price-from-reserves.js';
import { startLeaderboards } from '../jobs/leaderboards.js';
import { startPartitionsMaintainer } from '../jobs/partitions.js';
import { startTokenSecurityScanner } from '../jobs/token-security.js';
import { startFx } from '../jobs/fx-zig.js';
import matrix from '../jobs/matrix-rollups.js';
import { startIbcMetaRefresher } from '../jobs/ibc-meta-refresher.js';
import { startFasttrackListener } from '../jobs/fasttrack-listener.js';

async function main() {
  console.log('start-jobs from:', import.meta.url);
  await init();
  info('jobs: starting…');

  // periodic jobs
  matrix.start();
  startMetaRefresher();
  startHoldersRefresher();
  startPriceFromReserves();
  startLeaderboards();
  startPartitionsMaintainer();
  startTokenSecurityScanner();
  startFx();
  startIbcMetaRefresher();

  // 🔔 fast-track listener
  startFasttrackListener();

  // keep process alive
  setInterval(()=>{}, 1<<30);
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
