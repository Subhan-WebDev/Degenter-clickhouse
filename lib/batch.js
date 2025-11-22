// lib/batch.js
// Generic batch queue -> flush function(items) with size/time thresholds.

export class BatchQueue {
  constructor({ maxItems = 500, maxWaitMs = 500, flushFn }) {
    if (typeof flushFn !== 'function') throw new Error('flushFn required');
    this.maxItems = maxItems;
    this.maxWaitMs = maxWaitMs;
    this.flushFn = flushFn;

    this.queue = [];
    this.timer = null;
    this.flushing = false;
  }

  push(item) {
    this.queue.push(item);
    if (this.queue.length >= this.maxItems) {
      this._scheduleImmediate();
    } else if (!this.timer) {
      this.timer = setTimeout(() => this._flush().catch(()=>{}), this.maxWaitMs);
    }
  }

  async drain() {
    if (this.timer) { clearTimeout(this.timer); this.timer = null; }
    if (this.queue.length) await this._flush();
  }

  _scheduleImmediate() {
    if (this.timer) { clearTimeout(this.timer); this.timer = null; }
    // microtask to coalesce multiple push() bursts
    queueMicrotask(() => this._flush().catch(()=>{}));
  }

  async _flush() {
    if (this.flushing) return;
    if (!this.queue.length) return;
    this.flushing = true;
    const items = this.queue;
    this.queue = [];
    try {
      await this.flushFn(items);
    } finally {
      this.flushing = false;
    }
  }
}
export default BatchQueue;
