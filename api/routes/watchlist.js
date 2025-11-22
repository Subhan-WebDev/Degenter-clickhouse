// api/routes/watchlist.js
import express from 'express';
import { DB } from '../../lib/db.js';
import { resolveTokenId } from '../util/resolve-token.js';

const router = express.Router();

/** GET /watchlist/:walletId */
router.get('/:walletId', async (req, res) => {
  try {
    const wid = parseInt(req.params.walletId,10);
    const { rows } = await DB.query(`
      SELECT w.id, w.token_id, t.denom, t.symbol, w.pool_id, w.note, w.created_at
      FROM watchlist w
      LEFT JOIN tokens t ON t.token_id=w.token_id
      WHERE w.wallet_id=$1
      ORDER BY w.created_at DESC
    `, [wid]);
    res.json({ success:true, data: rows });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** POST /watchlist  { walletId, token?:idOrSymbolOrDenom, poolId?, note? } */
router.post('/', async (req, res) => {
  try {
    const { walletId, token, poolId, note } = req.body;
    if (!walletId || (!token && !poolId)) return res.status(400).json({ success:false, error:'walletId and token or poolId required' });
    let tokenId = null;
    if (token) {
      const tok = await resolveTokenId(token);
      if (!tok) return res.status(404).json({ success:false, error:'token not found' });
      tokenId = tok.token_id;
    }
    const { rows } = await DB.query(`
      INSERT INTO watchlist(wallet_id, token_id, pool_id, note)
      VALUES ($1,$2,$3,$4)
      ON CONFLICT (wallet_id, token_id) DO NOTHING
      RETURNING *
    `, [walletId, tokenId, poolId || null, note || null]);
    res.json({ success:true, data: rows[0] || null });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** DELETE /watchlist/:id */
router.delete('/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id,10);
    await DB.query(`DELETE FROM watchlist WHERE id=$1`, [id]);
    res.json({ success:true });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

export default router;
