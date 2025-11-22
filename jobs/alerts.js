// jobs/alerts.js
import { DB } from '../lib/db.js';
import { info, warn } from '../lib/log.js';

const ALERTS_SEC = parseInt(process.env.ALERTS_SEC || '10', 10);
const LARGE_TRADE_MIN_ZIG = Number(process.env.LARGE_TRADE_MIN_ZIG || '1000');

function throttled(lastTriggered, throttleSec){
  if (!lastTriggered) return false;
  const next = new Date(new Date(lastTriggered).getTime() + throttleSec*1000);
  return new Date() < next;
}

async function evalAlert(a) {
  const p = a.params || {};
  if (throttled(a.last_triggered, a.throttle_sec)) return null;

  if (a.alert_type === 'price_cross') {
    // prefer pool-specific price; else any UZIG-quoted pool for token
    let priceRow = null;
    if (p.pool_id) {
      const { rows } = await DB.query(
        `SELECT price_in_zig FROM prices WHERE pool_id=$1 ORDER BY updated_at DESC LIMIT 1`,
        [p.pool_id]
      );
      priceRow = rows[0] || null;
    } else if (p.token_id) {
      const { rows } = await DB.query(`
        SELECT pr.price_in_zig
        FROM prices pr
        JOIN pools po ON po.pool_id=pr.pool_id
        WHERE pr.token_id=$1 AND po.is_uzig_quote=TRUE
        ORDER BY pr.updated_at DESC LIMIT 1
      `,[p.token_id]);
      priceRow = rows[0] || null;
    }
    const px = priceRow?.price_in_zig ? Number(priceRow.price_in_zig) : null;
    if (px == null) return null;
    const aboveHit = (p.above != null && px >= Number(p.above));
    const belowHit = (p.below != null && px <= Number(p.below));
    if (aboveHit || belowHit) {
      return { triggered: true, kind: 'price_cross', payload: { price_in_zig: px, params: p } };
    }
    return null;
  }

  if (a.alert_type === 'wallet_trade') {
    const sinceMin = Number(p.since_min || 10);
    const args = [p.address, sinceMin];
    let where = `t.signer=$1 AND t.created_at >= now() - ($2 || ' minutes')::interval`;
    if (p.direction) { where += ` AND t.direction='${p.direction}'`; }
    if (p.pool_id) { args.push(p.pool_id); where += ` AND t.pool_id=$${args.length}`; }
    if (p.token_id) {
      args.push(p.token_id);
      where += ` AND EXISTS (SELECT 1 FROM pools px WHERE px.pool_id=t.pool_id AND px.base_token_id=$${args.length})`;
    }
    const { rows } = await DB.query(`SELECT count(*)::int AS c FROM trades t WHERE ${where}`, args);
    if ((rows[0]?.c || 0) > 0) {
      return { triggered: true, kind: 'wallet_trade', payload: { count: rows[0].c, params: p } };
    }
    return null;
  }

  if (a.alert_type === 'large_trade') {
    const sinceMin = Number(p.since_min || 10);
    const minZig = Number(p.min_zig || LARGE_TRADE_MIN_ZIG);
    const args = [sinceMin, minZig];
    let where = `created_at >= now() - ($1 || ' minutes')::interval AND value_zig >= $2`;
    if (p.pool_id) { args.push(p.pool_id); where += ` AND pool_id=$${args.length}`; }
    const { rows } = await DB.query(`SELECT count(*)::int AS c FROM large_trades WHERE ${where}`, args);
    if ((rows[0]?.c || 0) > 0) {
      return { triggered: true, kind: 'large_trade', payload: { count: rows[0].c, params: p } };
    }
    return null;
  }

  if (a.alert_type === 'tvl_change') {
    const poolId = Number(p.pool_id);
    const windowMin = Number(p.window_min || 60);
    const deltaPct = Number(p.delta_pct || 10);

    const { rows: nowRows } = await DB.query(`
      SELECT tvl_zig FROM pool_matrix WHERE pool_id=$1 AND bucket='1h' ORDER BY updated_at DESC LIMIT 1
    `, [poolId]);
    const tvlNow = nowRows[0]?.tvl_zig ? Number(nowRows[0].tvl_zig) : null;

    const { rows: pastRows } = await DB.query(`
      SELECT tvl_zig FROM pool_matrix
      WHERE pool_id=$1 AND bucket='1h' AND updated_at <= now() - ($2 || ' minutes')::interval
      ORDER BY updated_at DESC LIMIT 1
    `, [poolId, windowMin]);
    const tvlPast = pastRows[0]?.tvl_zig ? Number(pastRows[0].tvl_zig) : null;

    if (tvlNow == null || tvlPast == null || tvlPast === 0) return null;
    const chg = ((tvlNow - tvlPast) / Math.abs(tvlPast)) * 100;
    if (Math.abs(chg) >= deltaPct) {
      return { triggered: true, kind: 'tvl_change', payload: { tvl_now: tvlNow, tvl_past: tvlPast, delta_pct: Number(chg.toFixed(4)), params: p } };
    }
    return null;
  }

  return null;
}

export function startAlertsEngine() {
  (async function loop(){
    while (true) {
      try {
        const { rows: alerts } = await DB.query(`
          SELECT a.alert_id, a.wallet_id, a.alert_type, a.params, a.throttle_sec, a.last_triggered
          FROM alerts a
          WHERE a.is_active = TRUE
        `);
        for (const a of alerts) {
          try {
            const ok = await evalAlert(a);
            if (ok?.triggered) {
              await DB.query(`UPDATE alerts SET last_triggered = now() WHERE alert_id=$1`, [a.alert_id]);
              await DB.query(`
                INSERT INTO alert_events(alert_id, wallet_id, kind, payload)
                VALUES ($1,$2,$3,$4)
              `, [a.alert_id, a.wallet_id, ok.kind, ok.payload]);
              info('[alert]', a.alert_id, ok.kind);
            }
          } catch (e) {
            warn('[alert-eval]', a.alert_id, e.message);
          }
        }
      } catch (e) {
        warn('[alerts]', e.message);
      }
      await new Promise(r => setTimeout(r, ALERTS_SEC * 1000));
    }
  })().catch(()=>{});
}
