// core/checkpoint.js
import { DB } from '../lib/db.js';

export async function readCheckpoint() {
  const { rows } = await DB.query(`
    SELECT last_height
    FROM index_state
    WHERE id = 'block'
    ORDER BY updated_at DESC
    LIMIT 1
  `);
  return rows[0]?.last_height ?? null;
}

export async function writeCheckpoint(h) {
  await DB.query(
    `
    INSERT INTO index_state (id, last_height, updated_at)
    VALUES ('block', $1, now())
  `,
    [h],
  );
}
