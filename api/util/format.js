// api/util/format.js
export function num(x) { return x == null ? null : Number(x); }
export function safeInt(x) { const n = parseInt(x, 10); return Number.isFinite(n) ? n : null; }
export function bool(x) { return x === true || x === 'true' || x === '1'; }
