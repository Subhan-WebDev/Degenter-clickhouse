// lib/pg_notify.js
import { info, warn } from './log.js';

// allow only simple channel names (identifiers) to avoid silly mistakes
const CHAN_RX = /^[a-z_][a-z0-9_]*$/i;

/**
 * In the old Postgres setup this would send NOTIFY.
 * With ClickHouse backend we don't have LISTEN/NOTIFY, so this is a safe no-op.
 */
export async function pgNotify(channel, payload) {
  if (!CHAN_RX.test(channel)) {
    throw new Error(`invalid channel: ${channel}`);
  }
  // You can log if you want to see where it's called:
  // warn('[pgNotify] no-op (ClickHouse backend)', { channel, payload });
}

/**
 * In the old Postgres setup this would open a dedicated connection and LISTEN.
 * With ClickHouse backend we just disable it so jobs that depend on it
 * don't crash the whole indexer. Fasttrack features will be re-done later.
 */
export async function pgListen(channel, onMessage) {
  if (!CHAN_RX.test(channel)) {
    throw new Error(`invalid channel: ${channel}`);
  }
  info(`[pgListen] disabled (ClickHouse backend); fasttrack notifications are no-op for channel "${channel}"`);
  // Return a dummy object so callers that expect "a client" don't explode.
  return {
    close() {},
    release() {},
  };
}
