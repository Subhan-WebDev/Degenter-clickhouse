// lib/pg_notify.js
import { EventEmitter } from 'events';
import { info, warn } from './log.js';

// allow only simple channel names (identifiers) to avoid silly mistakes
const CHAN_RX = /^[a-z_][a-z0-9_]*$/i;

// simple in-memory event bus
const bus = new EventEmitter();
// just in case we ever have many listeners
bus.setMaxListeners(100);

/**
 * pgNotify(channel, payload)
 *
 * In the old Postgres setup this would send NOTIFY via LISTEN/NOTIFY.
 * For the ClickHouse backend we don't have Postgres, so we provide a
 * local in-process event bus.
 *
 * IMPORTANT:
 * - This works when the *notifier* and the *listener* are in the same
 *   Node process (e.g. your indexer process).
 * - If you run `start:indexer` and `start:jobs` as two separate OS
 *   processes, notifications won't cross between them with this
 *   implementation. In that case, rely on the listener started from
 *   `core/block-processor.js` (which is in the indexer process).
 */
export async function pgNotify(channel, payload) {
  if (!CHAN_RX.test(channel)) {
    throw new Error(`invalid channel: ${channel}`);
  }

  try {
    // fire and forget — handlers run asynchronously
    bus.emit(channel, payload);
    info('[pgNotify]', { channel });
  } catch (e) {
    warn('[pgNotify error]', e.message);
  }
}

/**
 * pgListen(channel, onMessage)
 *
 * Register an async handler for a channel on the local bus.
 * onMessage gets whatever object was passed to pgNotify.
 */
export async function pgListen(channel, onMessage) {
  if (!CHAN_RX.test(channel)) {
    throw new Error(`invalid channel: ${channel}`);
  }

  const handler = (payload) => {
    Promise.resolve(onMessage(payload)).catch((e) => {
      warn('[pgListen handler error]', e.message);
    });
  };

  bus.on(channel, handler);
  info(`[pgListen] registered in-memory listener for "${channel}"`);

  // mimic the old Postgres client API a bit
  return {
    close() {
      bus.off(channel, handler);
      info(`[pgListen] removed listener for "${channel}"`);
    },
    release() {
      bus.off(channel, handler);
      info(`[pgListen] released listener for "${channel}"`);
    },
  };
}
