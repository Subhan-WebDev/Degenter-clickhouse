// jobs/partitions.js
import { debug, warn } from '../lib/log.js';

const PARTITIONS_SEC = parseInt(process.env.PARTITIONS_SEC || '1800', 10); // 30m

export function startPartitionsMaintainer() {
  (async function loop() {
    debug('[partitions] disabled (ClickHouse backend) – no action needed');
    // keep process alive but do nothing, so existing wiring doesn't break
    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        // nothing
      } catch (e) {
        warn('[partitions]', e.message);
      }
      await new Promise((r) => setTimeout(r, PARTITIONS_SEC * 1000));
    }
  })().catch(() => {});
}
