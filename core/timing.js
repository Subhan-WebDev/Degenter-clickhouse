// core/timing.js
const hr = () => {
  const [s, ns] = process.hrtime();
  return s * 1e3 + ns / 1e6;
};

export class BlockTimer {
  constructor(height, log = console.log) {
    this.h = height;
    this.log = log;
    this.t0 = hr();
    this.spans = [];
    this.marks = new Map();
    this.counters = Object.create(null);
    this.slowest = [];
  }
  mark(label) { this.marks.set(label, hr()); }
  endMark(label) {
    const t = this.marks.get(label);
    if (!t) return 0;
    const ms = hr() - t;
    this.spans.push([label, ms]);
    this.marks.delete(label);
    return ms;
  }
  count(k, n = 1) { this.counters[k] = (this.counters[k] || 0) + n; }
  track(label, fn) {
    const t = hr();
    const fin = (ok, val) => { this.slowest.push({ label, ms: hr() - t, ok }); return val; };
    try {
      const p = fn();
      return p && typeof p.then === 'function'
        ? p.then(v => fin(true, v)).catch(e => { fin(false); throw e; })
        : fin(true, p);
    } catch (e) { fin(false); throw e; }
  }
  flushSlowest(topN = 5) { this.slowest.sort((a,b)=>b.ms-a.ms); return this.slowest.slice(0, topN); }
  summary() {
    const total = hr() - this.t0;
    const spans = Object.fromEntries(this.spans.map(([k,v]) => [k, Number(v.toFixed(1))]));
    return {
      height: this.h,
      total_ms: Number(total.toFixed(1)),
      spans,
      counters: this.counters,
      slowest: this.flushSlowest(5).map(x => ({ label: x.label, ms: Number(x.ms.toFixed(1)), ok: x.ok })),
    };
  }
}
