// api/routes/alerts.js
import express from 'express';
import { DB } from '../../lib/db.js';

const router = express.Router();

/** GET /alerts/:walletId */
router.get('/:walletId', async (req, res) => {
  try {
    const wid = parseInt(req.params.walletId,10);
    const { rows } = await DB.query(`
      SELECT alert_id, alert_type, params, is_active, throttle_sec, last_triggered, created_at
      FROM alerts
      WHERE wallet_id=$1
      ORDER BY created_at DESC
    `, [wid]);
    res.json({ success:true, data: rows });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** POST /alerts  { walletId, type, params, throttleSec? } */
router.post('/', async (req, res) => {
  try {
    const { walletId, type, params, throttleSec } = req.body;
    if (!walletId || !type || !params) return res.status(400).json({ success:false, error:'walletId, type, params required' });
    const { rows } = await DB.query(`
      INSERT INTO alerts(wallet_id, alert_type, params, throttle_sec, is_active)
      VALUES ($1,$2,$3,$4,TRUE)
      RETURNING *
    `, [walletId, type, params, throttleSec || 300]);
    res.json({ success:true, data: rows[0] });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** PATCH /alerts/:id  { is_active? , params? , throttle_sec? } */
router.patch('/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id,10);
    const { is_active, params, throttle_sec } = req.body;
    const { rows } = await DB.query(`
      UPDATE alerts
      SET is_active = COALESCE($2, is_active),
          params = COALESCE($3, params),
          throttle_sec = COALESCE($4, throttle_sec)
      WHERE alert_id=$1
      RETURNING *
    `, [id, is_active, params, throttle_sec]);
    res.json({ success:true, data: rows[0] || null });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** DELETE /alerts/:id */
router.delete('/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id,10);
    await DB.query(`DELETE FROM alerts WHERE alert_id=$1`, [id]);
    res.json({ success:true });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

export default router;
