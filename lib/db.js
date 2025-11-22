// lib/db.js
import 'dotenv/config';
import { createClient } from '@clickhouse/client';
import { info, warn, err } from './log.js';

const CLICKHOUSE_HOST = process.env.CLICKHOUSE_HOST || 'http://localhost:8123';
const CLICKHOUSE_DB   = process.env.CLICKHOUSE_DB   || 'degenter';
const CLICKHOUSE_USER = process.env.CLICKHOUSE_USER || 'default';
const CLICKHOUSE_PASS = process.env.CLICKHOUSE_PASSWORD || '';

const client = createClient({
  host: CLICKHOUSE_HOST,
  database: CLICKHOUSE_DB,
  username: CLICKHOUSE_USER,
  password: CLICKHOUSE_PASS,
  application: 'degenter-indexer',
});

/**
 * Convert Postgres-style $1, $2, ... into ClickHouse named params {p1:String}
 * and build query_params map.
 */
function prepareQuery(sql, params = []) {
  if (!params.length) {
    return { text: sql, query_params: undefined };
  }

  const query_params = {};
  const text = sql.replace(/\$(\d+)/g, (_, numStr) => {
    const idx = Number(numStr) - 1;
    const key = `p${numStr}`;
    const v = params[idx];

    // store as string; ClickHouse will cast to target column type
    if (!(key in query_params)) {
      if (v === null || v === undefined) {
        query_params[key] = null;
      } else if (typeof v === 'boolean') {
        // represent booleans as 1/0; ClickHouse can cast where needed
        query_params[key] = v ? '1' : '0';
      } else {
        query_params[key] = String(v);
      }
    }
    return `{${key}:String}`;
  });

  return { text, query_params };
}

export const DB = {
  /**
   * DB.query(sql, params) → { rows, rowCount }
   * - SELECT/WITH → client.query(...).json()
   * - everything else → client.command(...)
   */
  async query(sql, params = []) {
    const trimmed = sql.trim();
    const isSelect = /^(\(?\s*SELECT|\(?\s*WITH)\b/i.test(trimmed);
    const { text, query_params } = prepareQuery(sql, params);

    try {
      if (isSelect) {
        const resultSet = await client.query({
          query: text,
          format: 'JSONEachRow',
          query_params,
        });
        const rows = await resultSet.json();
        return { rows, rowCount: rows.length };
      } else {
        await client.command({ query: text, query_params });
        return { rows: [], rowCount: 0 };
      }
    } catch (e) {
      err('[clickhouse] query error:', e.message);
      err('SQL:', text);
      throw e;
    }
  },
};

export async function init() {
  try {
    const r = await DB.query('SELECT now() AS now');
    info('[clickhouse] connected @', r.rows[0]?.now);
  } catch (e) {
    err('[clickhouse] connect failed:', e.message);
    throw e;
  }
}

/**
 * ClickHouse doesn’t have BEGIN/COMMIT like Postgres.
 * We run fn with DB directly. Any idempotency must be handled at app level.
 */
export async function tx(fn) {
  return fn(DB);
}

/** simple helper that retries transient failures a few times */
export async function queryRetry(sql, args = [], attempts = 3) {
  for (let i = 0; i < attempts; i++) {
    try {
      return await DB.query(sql, args);
    } catch (e) {
      const last = i === attempts - 1;
      if (last) throw e;
      warn('[clickhouse] db retry', i + 1, e.message);
      await new Promise((r) => setTimeout(r, 150 * (i + 1)));
    }
  }
}

export async function close() {
  try {
    await client.close();
    info('[clickhouse] client closed');
  } catch (e) {
    warn('[clickhouse] close error:', e.message);
  }
}
