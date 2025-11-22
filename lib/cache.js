// lib/cache.js
// tiny TTL cache (Map-based), good enough for LCD/RPC metadata bursts

export class TTLCache {
  constructor({ max = 500, ttlMs = 30_000 } = {}) {
    this.max = max;
    this.ttlMs = ttlMs;
    this.map = new Map();
  }
  _now() { return Date.now(); }
  _purge() {
    if (this.map.size <= this.max) return;
    // drop oldest half
    const n = Math.floor(this.map.size / 2);
    const keys = Array.from(this.map.keys()).slice(0, n);
    for (const k of keys) this.map.delete(k);
  }
  set(key, value, ttlMs = this.ttlMs) {
    const exp = this._now() + ttlMs;
    this.map.set(key, { value, exp });
    this._purge();
  }
  get(key) {
    const e = this.map.get(key);
    if (!e) return undefined;
    if (e.exp < this._now()) {
      this.map.delete(key);
      return undefined;
    }
    return e.value;
  }
  has(key) { return this.get(key) !== undefined; }
  delete(key) { return this.map.delete(key); }
  clear() { this.map.clear(); }
}
export default TTLCache;
