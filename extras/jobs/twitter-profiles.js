// extras/jobs/twitter-profiles.js
// Refresh Twitter (X) profiles for tokens that need it (not updated in 24h).

import { setTimeout as wait } from "timers/promises";
import { DB, init as dbInit, queryRetry } from "../../lib/db.js";
import * as log from "../../lib/log.js";

const API_BASE = "https://api.twitterapi.io";
const API_KEY = process.env.TWITTERAPI_IO_KEY;
if (!API_KEY) {
  throw new Error("Missing TWITTERAPI_IO_KEY env var");
}
const HEADERS = { "X-API-Key": API_KEY };

function mapUser(u) {
  const d = u?.data ?? u;
  return {
    user_id:             d?.id ?? null,
    userName:            d?.userName ?? null,
    profile_url:         d?.url ?? (d?.userName ? `https://x.com/${d.userName}` : null),
    name:                d?.name ?? null,
    is_blue_verified:    d?.isBlueVerified ?? null,
    verified_type:       d?.verifiedType ?? null,
    profile_picture:     d?.profilePicture ?? null,
    cover_picture:       d?.coverPicture ?? null,
    description:         d?.description ?? d?.profile_bio?.description ?? null,
    location:            d?.location ?? null,
    followers:           d?.followers ?? null,
    following:           d?.following ?? null,
    favourites_count:    d?.favouritesCount ?? null,
    statuses_count:      d?.statusesCount ?? null,
    media_count:         d?.mediaCount ?? null,
    can_dm:              d?.canDm ?? null,
    created_at_twitter:  d?.createdAt ? new Date(d.createdAt).toISOString() : null,
    possibly_sensitive:  d?.possiblySensitive ?? null,
    is_automated:        d?.isAutomated ?? null,
    automated_by:        d?.automatedBy ?? null,
    pinned_tweet_ids:    Array.isArray(d?.pinnedTweetIds) ? d.pinnedTweetIds : null,
    unavailable:         d?.unavailable ?? false,
    unavailable_message: d?.message ?? null,
    unavailable_reason:  d?.unavailableReason ?? null,
    raw:                 d ?? null,
  };
}

async function upsertProfile(token_id, handle, m) {
  const sql = `
    INSERT INTO public.token_twitter (
      token_id, handle, user_id, profile_url, name, is_blue_verified, verified_type,
      profile_picture, cover_picture, description, location,
      followers, following, favourites_count, statuses_count, media_count, can_dm,
      created_at_twitter, possibly_sensitive, is_automated, automated_by,
      pinned_tweet_ids, unavailable, unavailable_message, unavailable_reason,
      raw, last_refreshed, last_error, last_error_at
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,
      $8,$9,$10,$11,
      $12,$13,$14,$15,$16,$17,
      $18,$19,$20,$21,
      $22,$23,$24,$25,
      $26, now(), NULL, NULL
    )
    ON CONFLICT (token_id) DO UPDATE SET
      handle=$2,user_id=$3,profile_url=$4,name=$5,is_blue_verified=$6,verified_type=$7,
      profile_picture=$8,cover_picture=$9,description=$10,location=$11,
      followers=$12,following=$13,favourites_count=$14,statuses_count=$15,media_count=$16,can_dm=$17,
      created_at_twitter=$18,possibly_sensitive=$19,is_automated=$20,automated_by=$21,
      pinned_tweet_ids=$22,unavailable=$23,unavailable_message=$24,unavailable_reason=$25,
      raw=$26,last_refreshed=now(),last_error=NULL,last_error_at=NULL;
  `;
  const v = [
    token_id, handle, m.user_id, m.profile_url, m.name, m.is_blue_verified, m.verified_type,
    m.profile_picture, m.cover_picture, m.description, m.location,
    m.followers, m.following, m.favourites_count, m.statuses_count, m.media_count, m.can_dm,
    m.created_at_twitter, m.possibly_sensitive, m.is_automated, m.automated_by,
    m.pinned_tweet_ids, m.unavailable, m.unavailable_message, m.unavailable_reason,
    m.raw
  ];
  await queryRetry(sql, v);
}

async function selectDue() {
  const { rows } = await DB.query(`
    WITH handles AS (
      SELECT t.token_id, public.norm_twitter_handle(t.twitter) AS handle
      FROM public.tokens t
      WHERE t.twitter IS NOT NULL AND length(trim(t.twitter)) > 0
    )
    SELECT h.token_id, h.handle, tt.user_id
    FROM handles h
    LEFT JOIN public.token_twitter tt USING (token_id)
    WHERE tt.token_id IS NULL
       OR tt.last_refreshed < now() - interval '24 hours'
  `);
  return rows;
}

async function runOnce() {
  const due = await selectDue();
  if (due.length === 0) {
    log.info("twitter-profiles: nothing due");
    return 0;
  }
  log.info(`twitter-profiles: due=${due.length}`);

  const withId = due.filter(d => !!d.user_id);
  const withoutId = due.filter(d => !d.user_id);

  // Batch by user_id (faster/cheaper)
  const BATCH = Number(process.env.TWITTER_BATCH_SIZE || 100);
  for (let i = 0; i < withId.length; i += BATCH) {
    const chunk = withId.slice(i, i + BATCH);
    const qs = new URLSearchParams({ userIds: chunk.map(r => r.user_id).join(",") });
    const res = await fetch(`${API_BASE}/twitter/user/batch_info_by_ids?${qs}`, { headers: HEADERS });
    const json = await res.json();
    const users = json?.users || [];
    const byId = new Map();
    for (const u of users) byId.set(u?.id ?? u?.data?.id, u);

    for (const row of chunk) {
      try {
        const u = byId.get(row.user_id);
        if (!u) continue;
        const m = mapUser(u);
        await upsertProfile(row.token_id, row.handle, m);
      } catch (e) {
        await queryRetry(
          `INSERT INTO public.token_twitter(token_id, handle, last_refreshed, last_error, last_error_at)
           VALUES ($1,$2, now(), $3, now())
           ON CONFLICT (token_id) DO UPDATE SET last_error = EXCLUDED.last_error, last_error_at = now();`,
          [row.token_id, row.handle, String(e)]
        );
      }
    }
  }

  // First-time fetches by username
  const CONCURRENCY = Number(process.env.TWITTER_USERNAME_CONCURRENCY || 5);
  let idx = 0;
  await Promise.all(
    Array.from({ length: CONCURRENCY }, async () => {
      while (idx < withoutId.length) {
        const row = withoutId[idx++];
        try {
          const qs = new URLSearchParams({ userName: row.handle });
          const res = await fetch(`${API_BASE}/twitter/user/info?${qs}`, { headers: HEADERS });
          const json = await res.json();
          const m = mapUser(json);
          await upsertProfile(row.token_id, row.handle, m);
          await wait(50);
        } catch (e) {
          await queryRetry(
            `INSERT INTO public.token_twitter(token_id, handle, last_refreshed, last_error, last_error_at)
             VALUES ($1,$2, now(), $3, now())
             ON CONFLICT (token_id) DO UPDATE SET last_error = EXCLUDED.last_error, last_error_at = now();`,
            [row.token_id, row.handle, String(e)]
          );
        }
      }
    })
  );

  log.info("twitter-profiles: refresh complete");
  return due.length;
}

export async function main() {
  // Ensures the pool is live (your init() logs a nice connect message)
  await dbInit();
  await runOnce();
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}
