// core/parse.js
import crypto from 'node:crypto';

export function safeB64DecodeMaybe(s) {
  if (typeof s !== 'string') return s;
  if (!/^[A-Za-z0-9+/=]+$/.test(s)) return s;
  if (/[{}:_\-.]/.test(s)) return s;
  try {
    const buf = Buffer.from(s, 'base64');
    const text = buf.toString('utf8');
    const round = Buffer.from(text, 'utf8').toString('base64').replace(/=+$/,'');
    const orig  = s.replace(/=+$/,'');
    if (round !== orig) return s;
    if (!/^[\x20-\x7E]+$/.test(text)) return s;
    return text;
  } catch { return s; }
}

export function digitsOrNull(x){ const s=String(x??''); return /^\d+$/.test(s)?s:null; }
export const sha256hex = b64 => crypto.createHash('sha256').update(Buffer.from(b64, 'base64')).digest('hex').toUpperCase();

function kvmap(ev) {
  const m = new Map();
  for (const a of ev.attributes || []) {
    const k = safeB64DecodeMaybe(a.key);
    const v = safeB64DecodeMaybe(a.value);
    m.set(k, v);
  }
  return { type: ev.type, m };
}
export const byType = (evs, t) => (evs || []).filter(e => e.type === t).map(kvmap);
export const wasmByAction = (wasms, act) => wasms.filter(w => w.m.get('action') === act);

export const splitPairString = pair => { const i = (pair||'').indexOf('-'); return i===-1?[pair,'']:[pair.slice(0,i), pair.slice(i+1)]; };
export const classifyDirection = (offerDenom, quoteDenom) => offerDenom === quoteDenom ? 'buy' : 'sell';
export const toDisp = (amountBase, exp) => Number(amountBase || 0) / Math.pow(10, exp || 0);

export function parseAssetsList(assetsStr) {
  if (!assetsStr || typeof assetsStr !== 'string') return null;
  const parts = assetsStr.split(',').map(s => s.trim());
  const out = [];
  for (const p of parts) {
    const m = p.match(/^(\d+)([a-zA-Z0-9.\-_]+)$/);
    if (m) out.push({ amount_base: m[1], denom: m[2] });
  }
  return out.length ? { a1: out[0], a2: out[1] || null } : null;
}
export function parseReservesKV(reservesStr) {
  if (!reservesStr || typeof reservesStr !== 'string') return null;
  const parts = reservesStr.split(',').map(s => s.trim()).filter(Boolean);
  const out = [];
  for (const p of parts) {
    const i = p.indexOf(':'); if (i<=0) continue;
    const denom = p.slice(0, i);
    const amt   = p.slice(i+1);
    if (!denom || !/^\d+$/.test(amt)) continue;
    out.push({ denom, amount_base: amt });
  }
  return out.length ? out : null;
}

export function buildMsgSenderMap(messageEvents) {
  const map = new Map();
  for (const e of messageEvents || []) {
    const idx = e.m.get('msg_index');
    const sender = e.m.get('sender');
    if (idx !== undefined && sender) map.set(Number(idx), sender);
  }
  return map;
}
export function normalizePair(pairStr) {
  const [x, y] = splitPairString(pairStr || '');
  if (x === 'uzig' && y) return { base: y, quote: 'uzig' };
  if (y === 'uzig' && x) return { base: x, quote: 'uzig' };
  return { base: x, quote: y };
}
