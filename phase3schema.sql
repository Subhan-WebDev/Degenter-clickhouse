-- ====================================================================
-- ENUMS (idempotent)
-- ====================================================================
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname='token_type') THEN
    CREATE TYPE token_type AS ENUM ('native','factory','ibc','cw20');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname='pair_type') THEN
    CREATE TYPE pair_type AS ENUM ('xyk','concentrated','custom-concentrated');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname='trade_action') THEN
    CREATE TYPE trade_action AS ENUM ('swap','provide','withdraw');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname='trade_direction') THEN
    CREATE TYPE trade_direction AS ENUM ('buy','sell','provide','withdraw');
  END IF;
END$$;

-- ====================================================================
-- BASE TABLES
-- ====================================================================

-- TOKENS
CREATE TABLE IF NOT EXISTS public.tokens (
  token_id           BIGSERIAL PRIMARY KEY,
  denom              TEXT NOT NULL UNIQUE,
  type               token_type NOT NULL DEFAULT 'factory',
  name               TEXT,
  symbol             TEXT,
  display            TEXT,
  exponent           SMALLINT NOT NULL DEFAULT 6,
  image_uri          TEXT,
  website            TEXT,
  twitter            TEXT,
  telegram           TEXT,
  max_supply_base    NUMERIC(78,0),
  total_supply_base  NUMERIC(78,0),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_tokens_created_at ON public.tokens(created_at);

-- POOLS
CREATE TABLE IF NOT EXISTS public.pools (
  pool_id            BIGSERIAL PRIMARY KEY,
  pair_contract      TEXT NOT NULL UNIQUE,
  base_token_id      BIGINT NOT NULL REFERENCES public.tokens(token_id),
  quote_token_id     BIGINT NOT NULL REFERENCES public.tokens(token_id),
  lp_token_denom     TEXT,
  pair_type          TEXT NOT NULL,
  is_uzig_quote      BOOLEAN NOT NULL DEFAULT FALSE,
  factory_contract   TEXT,
  router_contract    TEXT,
  created_at         TIMESTAMPTZ,
  created_height     BIGINT,
  created_tx_hash    TEXT,
  signer             TEXT
);
CREATE INDEX IF NOT EXISTS idx_pools_created_at      ON public.pools(created_at);
CREATE INDEX IF NOT EXISTS idx_pools_pair_contract   ON public.pools(pair_contract);
CREATE INDEX IF NOT EXISTS idx_pools_base_token_id   ON public.pools(base_token_id);
CREATE INDEX IF NOT EXISTS idx_pools_quote_token_id  ON public.pools(quote_token_id);
CREATE INDEX IF NOT EXISTS idx_pools_base_quote      ON public.pools(base_token_id, quote_token_id);
CREATE INDEX IF NOT EXISTS idx_pools_pair_type       ON public.pools(pair_type);

-- ====================================================================
-- TRADES  (PARTITIONED by created_at)
-- ====================================================================
-- Parent
CREATE TABLE IF NOT EXISTS public.trades (
  trade_id                    BIGSERIAL,
  pool_id                     BIGINT NOT NULL REFERENCES public.pools(pool_id),
  pair_contract               TEXT NOT NULL,
  action                      trade_action NOT NULL,
  direction                   trade_direction NOT NULL,
  offer_asset_denom           TEXT,
  offer_amount_base           NUMERIC(78,0),
  ask_asset_denom             TEXT,
  ask_amount_base             NUMERIC(78,0),
  return_amount_base          NUMERIC(78,0),
  is_router                   BOOLEAN NOT NULL DEFAULT FALSE,
  reserve_asset1_denom        TEXT,
  reserve_asset1_amount_base  NUMERIC(78,0),
  reserve_asset2_denom        TEXT,
  reserve_asset2_amount_base  NUMERIC(78,0),
  height                      BIGINT,
  tx_hash                     TEXT,
  signer                      TEXT,
  msg_index                   INT,
  created_at                  TIMESTAMPTZ NOT NULL
) PARTITION BY RANGE (created_at);

-- Primary key on parent not required; use a surrogate on partitions if desired.
-- Create the **correct** UNIQUE index on parent (includes partition key)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='public' AND tablename='trades' AND indexname='uq_trades_tx_pool_msg_time'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uq_trades_tx_pool_msg_time
             ON public.trades (tx_hash, pool_id, msg_index, created_at)';
  END IF;
END$$;

-- Helpful secondary indexes (on parent)
CREATE INDEX IF NOT EXISTS idx_trades_time               ON public.trades(created_at);
CREATE INDEX IF NOT EXISTS idx_trades_signer             ON public.trades(signer);
CREATE INDEX IF NOT EXISTS idx_trades_action_signer_time ON public.trades(action, signer, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_signer_time        ON public.trades(signer, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_pool_time          ON public.trades(pool_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_height             ON public.trades(height);
CREATE INDEX IF NOT EXISTS idx_trades_tx                 ON public.trades(tx_hash);
-- BRINs (cheap, good for time/height)
CREATE INDEX IF NOT EXISTS brin_trades_created_at        ON public.trades USING brin(created_at);
CREATE INDEX IF NOT EXISTS brin_trades_height            ON public.trades USING brin(height);

-- ====================================================================
-- HOLDERS + HOLDER STATS
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.holders (
  token_id         BIGINT NOT NULL REFERENCES public.tokens(token_id),
  address          TEXT   NOT NULL,
  balance_base     NUMERIC(78,0) NOT NULL,
  updated_at       TIMESTAMPTZ NOT NULL,
  last_seen_height BIGINT,
  PRIMARY KEY (token_id, address)
);
CREATE INDEX IF NOT EXISTS idx_holders_token_time ON public.holders(token_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_holders_address    ON public.holders(address);

CREATE TABLE IF NOT EXISTS public.token_holders_stats (
  token_id       BIGINT PRIMARY KEY REFERENCES public.tokens(token_id),
  holders_count  BIGINT NOT NULL,
  updated_at     TIMESTAMPTZ NOT NULL
);

-- ====================================================================
-- PRICES / PRICE TICKS (ticks partitioned by ts)
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.prices (
  price_id       BIGSERIAL PRIMARY KEY,
  token_id       BIGINT NOT NULL REFERENCES public.tokens(token_id),
  pool_id        BIGINT NOT NULL REFERENCES public.pools(pool_id),
  price_in_zig   NUMERIC(38,18) NOT NULL,
  is_pair_native BOOLEAN NOT NULL,
  updated_at     TIMESTAMPTZ NOT NULL,
  UNIQUE (token_id, pool_id)
);
CREATE INDEX IF NOT EXISTS idx_prices_token_time ON public.prices(token_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_prices_pool_time  ON public.prices(pool_id,  updated_at DESC);

-- PRICE TICKS parent
CREATE TABLE IF NOT EXISTS public.price_ticks (
  pool_id        BIGINT NOT NULL REFERENCES public.pools(pool_id),
  token_id       BIGINT NOT NULL REFERENCES public.tokens(token_id),
  price_in_zig   NUMERIC(38,18) NOT NULL,
  ts             TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (pool_id, ts)
) PARTITION BY RANGE (ts);

-- ====================================================================
-- OHLCV 1m (partitioned by bucket_start)
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.ohlcv_1m (
  pool_id        BIGINT NOT NULL REFERENCES public.pools(pool_id),
  bucket_start   TIMESTAMPTZ NOT NULL,
  open           NUMERIC(38,18) NOT NULL,
  high           NUMERIC(38,18) NOT NULL,
  low            NUMERIC(38,18) NOT NULL,
  close          NUMERIC(38,18) NOT NULL,
  volume_zig     NUMERIC(38,8)  NOT NULL DEFAULT 0,
  trade_count    INTEGER        NOT NULL DEFAULT 0,
  liquidity_zig  NUMERIC(38,8),
  PRIMARY KEY (pool_id, bucket_start)
) PARTITION BY RANGE (bucket_start);
CREATE INDEX IF NOT EXISTS idx_ohlcv_pool_time ON ONLY public.ohlcv_1m(pool_id, bucket_start DESC);
CREATE INDEX IF NOT EXISTS brin_ohlcv_bucket   ON ONLY public.ohlcv_1m USING brin(bucket_start);

-- ====================================================================
-- LIVE POOL STATE (reserves snapshot)
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.pool_state (
  pool_id            BIGINT PRIMARY KEY REFERENCES public.pools(pool_id),
  reserve_base_base  NUMERIC(78,0),
  reserve_quote_base NUMERIC(78,0),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ====================================================================
-- MATRIX TABLES (not partitioned to keep it simple)
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.pool_matrix (
  pool_id            BIGINT NOT NULL REFERENCES public.pools(pool_id),
  bucket             TEXT   NOT NULL CHECK (bucket IN ('30m','1h','4h','24h')),
  vol_buy_quote      NUMERIC(38,8) NOT NULL DEFAULT 0,
  vol_sell_quote     NUMERIC(38,8) NOT NULL DEFAULT 0,
  vol_buy_zig        NUMERIC(38,8) NOT NULL DEFAULT 0,
  vol_sell_zig       NUMERIC(38,8) NOT NULL DEFAULT 0,
  tx_buy             INTEGER       NOT NULL DEFAULT 0,
  tx_sell            INTEGER       NOT NULL DEFAULT 0,
  unique_traders     INTEGER       NOT NULL DEFAULT 0,
  tvl_zig            NUMERIC(38,8),
  reserve_base_disp  NUMERIC(38,18),
  reserve_quote_disp NUMERIC(38,18),
  updated_at         TIMESTAMPTZ   NOT NULL,
  PRIMARY KEY (pool_id, bucket)
);
CREATE INDEX IF NOT EXISTS idx_pool_matrix_updated ON public.pool_matrix(updated_at DESC);

CREATE TABLE IF NOT EXISTS public.token_matrix (
  token_id     BIGINT NOT NULL REFERENCES public.tokens(token_id),
  bucket       TEXT   NOT NULL CHECK (bucket IN ('30m','1h','4h','24h')),
  price_in_zig NUMERIC(38,18),
  mcap_zig     NUMERIC(38,8),
  fdv_zig      NUMERIC(38,8),
  holders      BIGINT,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (token_id, bucket)
);
CREATE INDEX IF NOT EXISTS idx_token_matrix_bucket ON public.token_matrix(bucket, updated_at DESC);

-- ====================================================================
-- LEADERBOARD & OUTLIERS (kept non-partitioned for simplicity)
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.leaderboard_traders (
  bucket        TEXT   NOT NULL CHECK (bucket IN ('30m','1h','4h','24h')),
  address       TEXT   NOT NULL,
  trades_count  INT    NOT NULL,
  volume_zig    NUMERIC(38,8) NOT NULL,
  gross_pnl_zig NUMERIC(38,8) NOT NULL,
  updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
  PRIMARY KEY (bucket, address)
);
CREATE INDEX IF NOT EXISTS idx_leaderboard_updated ON public.leaderboard_traders(updated_at DESC);

CREATE TABLE IF NOT EXISTS public.large_trades (
  id              BIGSERIAL PRIMARY KEY,
  bucket          TEXT NOT NULL CHECK (bucket IN ('30m','1h','4h','24h')),
  pool_id         BIGINT NOT NULL REFERENCES public.pools(pool_id),
  tx_hash         TEXT NOT NULL,
  signer          TEXT,
  value_zig       NUMERIC(38,8) NOT NULL,
  direction       trade_direction NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL,
  inserted_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_large_trades_bucket_time ON public.large_trades(bucket, created_at DESC);

-- ====================================================================
-- FX
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.exchange_rates (
  ts       TIMESTAMPTZ PRIMARY KEY,
  zig_usd  NUMERIC(38,8) NOT NULL
);

-- ====================================================================
-- INDEXER PROGRESS
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.index_state (
  id TEXT PRIMARY KEY,
  last_height BIGINT NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ====================================================================
-- PHASE 3: WALLETS / WATCHLIST / ALERTS
-- ====================================================================

-- WALLET directory (keep both last_seen + last_seen_at for code compatibility)
CREATE TABLE IF NOT EXISTS public.wallets (
  wallet_id     BIGSERIAL PRIMARY KEY,
  address       TEXT NOT NULL UNIQUE,
  display_name  TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen     TIMESTAMPTZ,      -- used by upsertWallet()
  last_seen_at  TIMESTAMPTZ       -- used by touchWallet()
);
CREATE INDEX IF NOT EXISTS idx_wallets_last_seen ON public.wallets(last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_wallets_last_seen_at ON public.wallets(last_seen_at DESC);

-- WATCHLIST
CREATE TABLE IF NOT EXISTS public.watchlist (
  id          BIGSERIAL PRIMARY KEY,
  wallet_id   BIGINT NOT NULL REFERENCES public.wallets(wallet_id) ON DELETE CASCADE,
  token_id    BIGINT REFERENCES public.tokens(token_id),
  pool_id     BIGINT REFERENCES public.pools(pool_id),
  note        TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_watchlist_wallet_token UNIQUE (wallet_id, token_id),
  CONSTRAINT uq_watchlist_wallet_pool  UNIQUE (wallet_id, pool_id)
);

-- ALERTS
CREATE TABLE IF NOT EXISTS public.alerts (
  alert_id        BIGSERIAL PRIMARY KEY,
  wallet_id       BIGINT NOT NULL REFERENCES public.wallets(wallet_id) ON DELETE CASCADE,
  alert_type      TEXT NOT NULL CHECK (alert_type IN ('price_cross','wallet_trade','large_trade','tvl_change')),
  params          JSONB NOT NULL,
  is_active       BOOLEAN NOT NULL DEFAULT TRUE,
  throttle_sec    INT NOT NULL DEFAULT 300,
  last_triggered  TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ALERT EVENTS (log)
CREATE TABLE IF NOT EXISTS public.alert_events (
  id            BIGSERIAL PRIMARY KEY,
  alert_id      BIGINT NOT NULL REFERENCES public.alerts(alert_id) ON DELETE CASCADE,
  wallet_id     BIGINT NOT NULL REFERENCES public.wallets(wallet_id) ON DELETE CASCADE,
  kind          TEXT NOT NULL,
  payload       JSONB NOT NULL,
  triggered_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_alert_events_alert_time ON public.alert_events(alert_id, triggered_at DESC);

-- ====================================================================
-- OPTIONAL HELPERS: create monthly partitions (now + N months ahead)
--   Use only if you want to pre-create partitions from SQL (manual test).
--   Otherwise, you can create them by hand or via your app job.
-- ====================================================================

-- Helper function to create a single month partition
CREATE OR REPLACE FUNCTION public.ensure_month_partition(
  parent TEXT,
  colname TEXT,
  year INT,
  month INT
) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
  from_ts TIMESTAMPTZ := make_timestamptz(year, month, 1, 0, 0, 0, 'UTC');
  to_ts   TIMESTAMPTZ := (from_ts + INTERVAL '1 month');
  child   TEXT        := format('%I_%s', parent, to_char(from_ts, 'YYYY_MM'));
  ddl     TEXT;
BEGIN
  -- if parent is not partitioned, this will fail; trap and ignore
  IF to_regclass(child) IS NULL THEN
    ddl := format(
      'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
      child, parent, from_ts, to_ts
    );
    EXECUTE ddl;
  END IF;
EXCEPTION
  WHEN undefined_table THEN
    -- parent not partitioned; ignore
    NULL;
END;
$$;

-- Convenience DO block: create partitions for current month + next 3 months
DO $$
DECLARE
  k INT;
  base TIMESTAMPTZ := date_trunc('month', now() AT TIME ZONE 'UTC');
  y INT;
  m INT;
BEGIN
  FOR k IN 0..3 LOOP
    y := EXTRACT(YEAR FROM (base + (k || ' month')::interval))::INT;
    m := EXTRACT(MONTH FROM (base + (k || ' month')::interval))::INT;

    PERFORM public.ensure_month_partition('trades',     'created_at',  y, m);
    PERFORM public.ensure_month_partition('price_ticks','ts',          y, m);
    PERFORM public.ensure_month_partition('ohlcv_1m',   'bucket_start',y, m);
  END LOOP;
END$$;

-- ====================================================================
-- DONE
-- ====================================================================
 CREATE UNIQUE INDEX IF NOT EXISTS uq_trades_tx_pool_msg
 ON trades (created_at, tx_hash, pool_id, msg_index);

-- Delete older duplicates, keep latest created_at per (tx_hash, pool_id, direction)
WITH ranked AS (
  SELECT ctid, tx_hash, pool_id, direction, created_at,
         ROW_NUMBER() OVER (
           PARTITION BY tx_hash, pool_id, direction
           ORDER BY created_at DESC
         ) AS rn
  FROM large_trades
)
DELETE FROM large_trades lt
USING ranked r
WHERE lt.ctid = r.ctid
  AND r.rn > 1;

-- Now create the unique index
CREATE UNIQUE INDEX IF NOT EXISTS ux_large_trades_tx_pool_dir
ON large_trades (tx_hash, pool_id, direction);
-- Create September 2025 monthly partition
CREATE TABLE IF NOT EXISTS ohlcv_1m_2025_09
  PARTITION OF ohlcv_1m
  FOR VALUES FROM ('2025-09-01 00:00:00+00')
               TO   ('2025-10-01 00:00:00+00');

-- Create October 2025 too (so you’re covered)
CREATE TABLE IF NOT EXISTS ohlcv_1m_2025_10
  PARTITION OF ohlcv_1m
  FOR VALUES FROM ('2025-10-01 00:00:00+00')
               TO   ('2025-11-01 00:00:00+00');

-- (Recommended) add a DEFAULT partition so you never hard-fail again.
-- If you want a default catch-all:
CREATE TABLE IF NOT EXISTS ohlcv_1m_default
  PARTITION OF ohlcv_1m DEFAULT;

ALTER TABLE public.tokens
  ADD COLUMN IF NOT EXISTS description TEXT;


-- 0) Handle normalizer (idempotent)
CREATE OR REPLACE FUNCTION public.norm_twitter_handle(in_raw TEXT)
RETURNS TEXT
LANGUAGE sql IMMUTABLE STRICT AS $$
SELECT lower(
    regexp_replace(
        regexp_replace(
            regexp_replace(
                coalesce(in_raw, ''),
                '^(https?://)?(www\.)?(x|twitter)\.com/', 
                '', 'i'
            ), 
            '^@', '', 'i'
        ), 
        '[/\?\#].*$', '', 'g'
    )
);
$$;

-- 1) Optional: index on normalized handle inside tokens (fast lookups)
CREATE INDEX IF NOT EXISTS idx_tokens_twitter_handle 
ON public.tokens (public.norm_twitter_handle(twitter));

-- 2) TOKEN TWITTER table (1 row per token)
CREATE TABLE IF NOT EXISTS public.token_twitter (
    token_id BIGINT PRIMARY KEY 
        REFERENCES public.tokens(token_id) 
        ON DELETE CASCADE,
    handle TEXT NOT NULL, -- normalized @username
    user_id TEXT, -- Twitter user id
    profile_url TEXT,
    name TEXT,
    is_blue_verified BOOLEAN,
    verified_type TEXT,
    profile_picture TEXT,
    cover_picture TEXT,
    description TEXT,
    location TEXT,
    followers BIGINT,
    following BIGINT,
    favourites_count BIGINT,
    statuses_count BIGINT,
    media_count BIGINT,
    can_dm BOOLEAN,
    created_at_twitter TIMESTAMPTZ,
    possibly_sensitive BOOLEAN,
    is_automated BOOLEAN,
    automated_by TEXT,
    pinned_tweet_ids TEXT[], -- Array of pinned tweet IDs
    unavailable BOOLEAN,
    unavailable_message TEXT,
    unavailable_reason TEXT,
    raw JSONB, -- full payload (future-proofing)
    last_refreshed TIMESTAMPTZ NOT NULL DEFAULT now(), -- last successful refresh
    last_error TEXT,
    last_error_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_token_twitter_handle 
ON public.token_twitter(handle);

CREATE INDEX IF NOT EXISTS idx_token_twitter_last_refreshed 
ON public.token_twitter(last_refreshed DESC);
