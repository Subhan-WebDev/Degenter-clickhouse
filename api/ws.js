// api/ws.js
import { WebSocketServer } from 'ws';

/**
 * WebSocket streams:
 *  - ohlcv: sends snapshot.ohlcv (200 bars) + ohlcv.delta upserts.
 *           For tf>1m, the forming bucket is synthesized from 1m bars.
 *  - trades (optional): sends snapshot.trades + trades.append.
 *
 * Requires Node 18+ (global fetch). If older, install node-fetch and import it.
 */

// ---------- helpers ----------
const TF_STEP = { '1m':60, '5m':300, '15m':900, '30m':1800, '1h':3600, '4h':14400, '1d':86400 };
const validTf = (x='1m') => TF_STEP[String(x).toLowerCase()] ? String(x).toLowerCase() : '1m';
const nowSec = () => Math.floor(Date.now()/1000);
const floor  = (t, step) => Math.floor(t/step)*step;
const toIso  = (s) => new Date(s*1000).toISOString();
const isObj  = (v) => v && typeof v === 'object' && !Array.isArray(v);

function send(ws, msg) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(msg));
  }
}
function safeParse(s) { try { return JSON.parse(s); } catch { return null; } }

let NEXT_STREAM_ID = 1;
const newStreamId = () => `s${NEXT_STREAM_ID++}`;

// ---------- resolve token via REST (lightweight) ----------
async function resolveToken(base, tokenKey) {
  try {
    const r = await fetch(`${base}/tokens/${encodeURIComponent(tokenKey)}`, { cache: 'no-store' });
    if (r.ok) {
      const j = await r.json();
      if (j?.success !== false) return { tokenId: tokenKey };
    }
  } catch {}
  // Let downstream fail if truly unknown
  return { tokenId: tokenKey };
}

// ---------- OHLCV stream ----------
class OhlcvStream {
  constructor({ ws, streamId, tokenId, tf='1m', mode='price', unit='usd', priceSource='best', baseUrl }) {
    this.ws = ws; this.id = streamId;
    this.tokenId = tokenId;
    this.tf = validTf(tf); this.step = TF_STEP[this.tf];
    this.mode = mode; this.unit = unit; this.priceSource = priceSource;
    this.baseUrl = baseUrl;

    this.seq = 0;
    this.timer = null;
    this.lockSec = null; // last fully-closed TF bucket start
  }

  async fetchOhlcvRange(fromSec, toSec, tf = this.tf) {
    const url = `${this.baseUrl}/tokens/${encodeURIComponent(this.tokenId)}/ohlcv`
      + `?tf=${tf}&from=${encodeURIComponent(toIso(fromSec))}&to=${encodeURIComponent(toIso(toSec))}`
      + `&mode=${this.mode}&unit=${this.unit}&priceSource=${this.priceSource}&fill=prev`;

    const res = await fetch(url, { cache: 'no-store' });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);

    const json = await res.json();
    const raw = Array.isArray(json?.data) ? json.data : [];
    return raw.map(b => ({
      tsSec: Number(b.ts_sec ?? b.ts ?? b.time ?? b.bucket_ts ?? b.bucket ?? Math.floor(Date.parse(String(b.ts||b.time))/1000)),
      open: Number(b.open), high: Number(b.high), low: Number(b.low), close: Number(b.close),
      volume: Number(b.volume ?? b.volume_native ?? 0),
      trades: Number(b.trades ?? b.trade_count ?? 0),
    })).filter(x => x.tsSec && [x.open,x.high,x.low,x.close].every(Number.isFinite))
      .sort((a,b)=>a.tsSec-b.tsSec);
  }

  async fetch1mRange(fromSec, toSec) {
    return this.fetchOhlcvRange(fromSec, toSec, '1m');
  }

  aggregateFormingFrom1m(bucketStart, mins) {
    if (!mins.length) return null;
    const open = mins[0].open;
    const highs = [open, ...mins.map(m => m.high)];
    const lows  = [open, ...mins.map(m => m.low)];
    const close = mins.at(-1).close;
    const volume = mins.reduce((s,m)=>s+(m.volume||0),0);
    const trades = mins.reduce((s,m)=>s+(m.trades||0),0);
    return { tsSec: bucketStart, open, high: Math.max(...highs), low: Math.min(...lows), close, volume, trades };
  }

  async snapshot() {
    const end = nowSec();
    const start = end - 200*this.step;

    let data = [];
    try {
      data = await this.fetchOhlcvRange(start, end);
    } catch (e) {
      console.error(`[ohlcv ${this.tokenId} ${this.tf}] snapshot fetch failed:`, e.message);
      data = [];
    }

    const lastClosedStart = floor(end, this.step) - this.step;
    this.lockSec = lastClosedStart;

    this.seq += 1;
    send(this.ws, {
      type: 'snapshot.ohlcv',
      streamId: this.id,
      meta: { stepSec: this.step, priceSource: this.priceSource, lockSec: this.lockSec },
      bars: data,
      seq: this.seq,
    });
    console.log(`[ohlcv ${this.tokenId} ${this.tf}] snapshot ${data.length} bars, lockSec=${this.lockSec}`);
  }

  async tick() {
    try {
      const end = nowSec();
      const lastClosedStart = floor(end, this.step) - this.step;
      if (this.lockSec == null || lastClosedStart > this.lockSec) this.lockSec = lastClosedStart;

      // recent closed TF tail (overlap)
      const tfTailFrom = end - 3*this.step;
      let tfTail = [];
      try {
        tfTail = await this.fetchOhlcvRange(tfTailFrom, end);
      } catch (e) {
        console.error(`[ohlcv ${this.tokenId} ${this.tf}] tick tf fetch failed:`, e.message);
      }

      // forming synthesized from 1m for tf>1m
      let forming = null;
      if (this.step > 60) {
        const bucketStart = floor(end, this.step);
        const formingEnd  = end - 1;
        try {
          const mins = await this.fetch1mRange(bucketStart, formingEnd);
          forming = this.aggregateFormingFrom1m(bucketStart, mins);
        } catch (e) {
          console.error(`[ohlcv ${this.tokenId} ${this.tf}] 1m forming fetch failed:`, e.message);
        }
      }

      const upserts = [...tfTail];
      if (forming) {
        const i = upserts.findIndex(b => b.tsSec === forming.tsSec);
        if (i >= 0) upserts[i] = forming; else upserts.push(forming);
      }

      if (upserts.length) {
        this.seq += 1;
        send(this.ws, {
          type: 'ohlcv.delta',
          streamId: this.id,
          upserts,
          lockSec: this.lockSec,
          seq: this.seq,
        });
      }
    } catch (e) {
      console.error(`[ohlcv ${this.tokenId} ${this.tf}] tick error:`, e);
    } finally {
      const ms = 900 + Math.floor(Math.random()*400);
      clearTimeout(this.timer);
      this.timer = setTimeout(()=>this.tick(), ms);
    }
  }

  async start() {
    try {
      console.log(`[ohlcv ${this.tokenId} ${this.tf}] start`);
      await this.snapshot();
    } catch (e) {
      console.error(`[ohlcv ${this.tokenId} ${this.tf}] snapshot fatal:`, e);
      const end = nowSec();
      const lastClosedStart = floor(end, this.step) - this.step;
      this.lockSec = lastClosedStart;
      this.seq += 1;
      send(this.ws, {
        type: 'snapshot.ohlcv',
        streamId: this.id,
        meta: { stepSec: this.step, priceSource: this.priceSource, lockSec: this.lockSec },
        bars: [],
        seq: this.seq,
      });
    } finally {
      this.tick();
    }
  }

  stop() { clearTimeout(this.timer); }
}

// ---------- Trades stream (optional) ----------
class TradesStream {
  constructor({ ws, streamId, tokenId, unit='usd', baseUrl }) {
    this.ws = ws; this.id = streamId; this.tokenId = tokenId;
    this.unit = unit; this.baseUrl = baseUrl;
    this.seq = 0;
    this.timer = null;
    this.lastIso = null;
  }

  async fetchRecent(limit=200, startIso, endIso) {
    const qs = new URLSearchParams();
    qs.set('tokenId', this.tokenId);
    qs.set('unit', this.unit);
    qs.set('limit', String(limit));
    if (startIso) qs.set('startTime', startIso);
    if (endIso)   qs.set('endTime', endIso);
    const url = `${this.baseUrl}/trades/recent?${qs.toString()}`;
    const r = await fetch(url, { cache: 'no-store' });
    if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
    const j = await r.json();
    const arr = Array.isArray(j?.data) ? j.data : [];
    arr.sort((a,b)=>Date.parse(a.time)-Date.parse(b.time));
    return arr;
  }

  async snapshot() {
    let items = [];
    try {
      const now = new Date();
      const past = new Date(now.getTime()-24*3600*1000);
      items = await this.fetchRecent(200, past.toISOString(), now.toISOString());
    } catch (e) {
      console.error(`[trades ${this.tokenId}] snapshot failed:`, e.message);
    }
    this.lastIso = items.at(-1)?.time ?? null;
    this.seq += 1;
    send(this.ws, { type:'snapshot.trades', streamId:this.id, items, seq:this.seq });
  }

  async tick() {
    try {
      let items = [];
      try {
        items = await this.fetchRecent(200, this.lastIso || undefined, new Date().toISOString());
      } catch (e) {
        console.error(`[trades ${this.tokenId}] tick fetch failed:`, e.message);
      }
      if (items.length) {
        this.lastIso = items.at(-1)?.time || this.lastIso;
        this.seq += 1;
        send(this.ws, { type:'trades.append', streamId:this.id, items, seq:this.seq });
      }
    } catch (e) {
      console.error(`[trades ${this.tokenId}] tick error:`, e);
    } finally {
      clearTimeout(this.timer);
      this.timer = setTimeout(()=>this.tick(), 2000);
    }
  }

  async start() { await this.snapshot(); this.tick(); }
  stop() { clearTimeout(this.timer); }
}

// ---------- bootstrap ----------
export function startWS(httpServer, { path='/ws' } = {}) {
  const wss = new WebSocketServer({ server: httpServer, path });

  wss.on('connection', async (ws, req) => {
    const proto = (req.headers['x-forwarded-proto'] || '').toString().toLowerCase();
    const scheme = proto === 'https' ? 'https' : 'http';
    const host = req.headers['x-forwarded-host'] || req.headers.host;
    const baseUrl = `${scheme}://${host}`;

    console.log('[ws] client connected from', req.socket.remoteAddress, 'base=', baseUrl);
    const streams = new Map(); // id -> stream

    send(ws, { type:'hello', serverTime: new Date().toISOString() });

    ws.on('message', async raw => {
      const msg = safeParse(String(raw));
      if (!isObj(msg)) return;

      if (msg.type === 'ping') { send(ws, { type:'pong', ts: msg.ts || Date.now() }); return; }

      if (msg.type === 'subscribe' && Array.isArray(msg.streams)) {
        const ack = [];
        for (const spec of msg.streams) {
          if (!isObj(spec)) continue;
          const kind = String(spec.kind || '').toLowerCase();
          const tokenKey = spec.tokenId || spec.token || spec.id;
          if (!tokenKey) { send(ws, { type:'error', error:'missing tokenId' }); continue; }

          let tokenRes;
          try { tokenRes = await resolveToken(baseUrl, tokenKey); }
          catch (e) { console.error('[ws] resolveToken failed:', e); send(ws, { type:'error', error:`cannot resolve token: ${tokenKey}` }); continue; }

          const id = newStreamId();
          let inst = null;
          if (kind === 'ohlcv') {
            inst = new OhlcvStream({
              ws, streamId:id, tokenId: tokenRes.tokenId,
              tf: spec.tf || '1m',
              mode: spec.mode || 'price',
              unit: spec.unit || 'usd',
              priceSource: spec.priceSource || 'best',
              baseUrl
            });
          } else if (kind === 'trades') {
            inst = new TradesStream({
              ws, streamId:id, tokenId: tokenRes.tokenId,
              unit: spec.unit || 'usd',
              baseUrl
            });
          } else {
            send(ws, { type:'error', error:`unknown stream kind: ${kind}` });
            continue;
          }

          streams.set(id, inst);
          ack.push({ id, kind });
          inst.start().catch(e => console.error(`[${kind}] start failed:`, e));
        }
        send(ws, { type:'subscribed', streams: ack });
        return;
      }

      if (msg.type === 'unsubscribe' && Array.isArray(msg.streamIds)) {
        for (const id of msg.streamIds) {
          const inst = streams.get(id);
          if (inst) { inst.stop(); streams.delete(id); }
        }
        send(ws, { type:'unsubscribed', streamIds: msg.streamIds });
        return;
      }
    });

    ws.on('error', (e) => console.error('[ws] socket error:', e?.message || e));
    ws.on('close', () => {
      for (const [,inst] of streams) inst.stop();
      streams.clear();
      console.log('[ws] client closed');
    });
  });

  console.log(`[ws] ready on ${path}`);
  return wss;
}
