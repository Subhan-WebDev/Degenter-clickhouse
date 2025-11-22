// core/tokens.js
import { DB } from '../lib/db.js';
import { lcdDenomsMetadata, lcdFactoryDenom, lcdIbcDenomTrace } from '../lib/lcd.js';
import { warn } from '../lib/log.js';
import { fetch } from 'undici';

// --- Minimal upsert (unchanged) ---
export async function upsertTokenMinimal(denom) {
  const { rows } = await DB.query(
    `INSERT INTO tokens(denom, exponent) VALUES ($1, 0)
     ON CONFLICT (denom) DO NOTHING
     RETURNING token_id`,
    [denom]
  );
  if (rows[0]) return rows[0].token_id;
  const r2 = await DB.query(`SELECT token_id FROM tokens WHERE denom=$1`, [denom]);
  return r2.rows[0]?.token_id || null;
}

// --- Fallback when no LCD metadata/display ---
function deriveFromBaseDenom(base) {
  if (typeof base !== 'string') return null;
  const m = base.match(/^u([a-z0-9]+)$/i);
  if (m) {
    const core = m[1];
    return { symbol: core.toUpperCase(), display: core.toLowerCase(), exponent: 0 };
  }
  return { symbol: base.toUpperCase(), display: base.toLowerCase(), exponent: 0 };
}

// --- Exponent from denom unit matching `display` (or alias) ---
function expFromDisplay(meta) {
  if (!meta || !meta.display || !Array.isArray(meta.denom_units)) return null;
  const dus = meta.denom_units;
  const byDenom = dus.find(u => u?.denom === meta.display && typeof u.exponent === 'number');
  if (byDenom) return byDenom.exponent;
  const byAlias = dus.find(
    u => Array.isArray(u.aliases) && u.aliases.includes(meta.display) && typeof u.exponent === 'number'
  );
  if (byAlias) return byAlias.exponent;
  return null;
}

// ---------- URI helpers (handle image vs JSON on IPFS or HTTP) ----------
function looksLikeJsonUrl(u = '') {
  try { return /\.json$/i.test(new URL(u).pathname); } catch { return false; }
}
function pickIcon(obj) { return obj?.icon || obj?.image || obj?.logo || null; }
function normString(x) { if (typeof x !== 'string') return null; const s = x.trim(); return s || null; }
function normUrl(x) { return normString(x); }

async function resolveUriPayload(uri) {
  if (!uri) return { image_uri: null, website: null, twitter: null, telegram: null, description: null, kind: null };
  try {
    const r = await fetch(uri, { headers: { accept: 'application/json, image/*;q=0.9, */*;q=0.5' } });
    const ct = String(r.headers.get('content-type') || '').toLowerCase();

    if (ct.startsWith('image/')) {
      return { image_uri: uri, website: null, twitter: null, telegram: null, description: null, kind: 'image' };
    }
    if (ct.includes('application/json') || looksLikeJsonUrl(uri)) {
      const j = await r.json().catch(() => null);
      if (j && typeof j === 'object') {
        return {
          image_uri: normUrl(pickIcon(j)),
          website: normUrl(j.website),
          twitter: normUrl(j.twitter),
          telegram: normUrl(j.telegram),
          description: normString(j.description),
          kind: 'json'
        };
      }
    }
    return { image_uri: null, website: null, twitter: null, telegram: null, description: null, kind: 'other' };
  } catch {
    return { image_uri: null, website: null, twitter: null, telegram: null, description: null, kind: 'error' };
  }
}

/**
 * setTokenMetaFromLCD:
 * - IBC trace resolution
 * - Exponent: from denom unit == display (or alias).
 *   • For IBC: if missing → default to 6 (stable UI/display).
 *   • For non-IBC: fallback deriveFromBaseDenom (→ 0 for u-xxx), else 0.
 * - Resolve metadata.uri:
 *    * direct image → image_uri
 *    * JSON → icon→image_uri, website/twitter/telegram/description from JSON
 * - Update tokens with any values we have (no overwrites with nulls).
 * - Update supply from factory if available.
 */
export async function setTokenMetaFromLCD(denom) {
  try {
    // Ensure description column exists (idempotent)
    await DB.query(`ALTER TABLE IF EXISTS tokens ADD COLUMN IF NOT EXISTS description TEXT`).catch(() => {});

    let lookupDenom = denom;
    let isIbc = false;
    let baseFromTrace = null;

    // IBC handling: resolve trace & mark type
    if (typeof denom === 'string' && denom.startsWith('ibc/')) {
      isIbc = true;
      const trace = await lcdIbcDenomTrace(denom).catch(() => null);
      baseFromTrace = trace?.denom?.base || null;
      if (baseFromTrace) lookupDenom = baseFromTrace;
      await DB.query(`UPDATE tokens SET type='ibc' WHERE denom=$1`, [denom]).catch(() => {});
    }

    // Pull LCD metadata for the lookup denom (may be base for IBC)
    const meta = await lcdDenomsMetadata(lookupDenom).catch(() => null);
    const m = meta?.metadata;

    let name     = m?.name ?? null;
    let symbol   = m?.symbol ?? null;
    let display  = m?.display ?? null;
    let lcdDesc  = m?.description ?? null;
    let uri      = m?.uri ?? null;

    // Decide exponent exactly once
    let exponent = expFromDisplay(m); // number | null

    if (isIbc) {
      // IBC default: 6 if metadata doesn't give an explicit exponent
      if (typeof exponent !== 'number') exponent = 6;
    } else {
      // Non-IBC fallback heuristics (u-xxx → 0)
      if (exponent == null) {
        const baseForDerive = baseFromTrace || lookupDenom;
        const d = deriveFromBaseDenom(baseForDerive);
        if (d) {
          if (!symbol)  symbol  = d.symbol;
          if (!display) display = d.display;
          exponent = d.exponent; // usually 0 for u-xxx
        }
      }
      if (exponent == null) exponent = 0;
    }

    // For IBC with no display from metadata, fall back to the traced base for transparency
    if (!display && isIbc && baseFromTrace) display = baseFromTrace;

    // Resolve URI payload (image or JSON w/ icon & socials)
    let imageFromUri = null, siteFromUri = null, twFromUri = null, tgFromUri = null, descFromUri = null;
    if (uri) {
      const r = await resolveUriPayload(uri);
      imageFromUri = r.image_uri || (r.kind === 'image' ? uri : null);
      siteFromUri  = r.website;
      twFromUri    = r.twitter;
      tgFromUri    = r.telegram;
      descFromUri  = r.description;
    }

    const finalDesc = normString(descFromUri) || normString(lcdDesc) || null;

    // Final update: only set when we have values; keep existing otherwise
    await DB.query(`
      UPDATE tokens
      SET name        = COALESCE($2,  name),
          symbol      = COALESCE($3,  symbol),
          display     = COALESCE($4,  display),
          exponent    = COALESCE($5,  exponent),
          image_uri   = COALESCE($6,  image_uri),
          description = COALESCE($7,  description),
          website     = COALESCE($8,  website),
          twitter     = COALESCE($9,  twitter),
          telegram    = COALESCE($10, telegram),
          type        = CASE WHEN $11::boolean THEN 'ibc' ELSE type END
      WHERE denom=$1
    `, [
      denom,
      name,
      symbol,
      display,
      exponent,                 // IBC → default 6 if unknown; non-IBC → derived or 0
      imageFromUri || null,
      finalDesc,
      siteFromUri || null,
      twFromUri || null,
      tgFromUri || null,
      isIbc
    ]);

    // Factory supply (when available)
    const fact = await lcdFactoryDenom(lookupDenom).catch(() => null);
    if (fact && (fact.total_supply || fact.total_minted)) {
      await DB.query(
        `UPDATE tokens
           SET max_supply_base   = $2::NUMERIC,
               total_supply_base = $3::NUMERIC
         WHERE denom=$1`,
        [denom, fact.max_supply || fact.minting_cap || null, fact.total_supply || fact.total_minted || null]
      );
    }
  } catch (e) {
    warn(`[tokenMeta] ${denom} → ${e.message}`);
  }
}
