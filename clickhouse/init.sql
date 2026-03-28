-- ClickHouse initialization script
-- Creates the crypto database and all data mart tables

CREATE DATABASE IF NOT EXISTS crypto;

-- ─── mart_hourly_ohlcv ────────────────────────────────────────────────────────
-- OHLCV (Open, High, Low, Close, Volume) per coin per hour
-- ReplacingMergeTree deduplicates on (coin_id, hour) using updated_at version
CREATE TABLE IF NOT EXISTS crypto.mart_hourly_ohlcv
(
    coin_id       String,
    hour          DateTime,
    open          Float64,
    high          Float64,
    low           Float64,
    close         Float64,
    volume        Float64,
    updated_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(hour)
ORDER BY (coin_id, hour)
SETTINGS index_granularity = 8192;

-- ─── mart_top_movers ──────────────────────────────────────────────────────────
-- Top 10 gainers and losers by 24h price change, computed daily
CREATE TABLE IF NOT EXISTS crypto.mart_top_movers
(
    date                  Date,
    coin_id               String,
    symbol                String,
    name                  String,
    price_change_24h_pct  Float64,
    direction             String,   -- 'gainer' | 'loser'
    rank                  UInt8,
    updated_at            DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, direction, rank)
SETTINGS index_granularity = 8192;

-- ─── mart_market_overview ─────────────────────────────────────────────────────
-- Aggregate market stats per hour: total market cap, dominance, total volume
CREATE TABLE IF NOT EXISTS crypto.mart_market_overview
(
    hour              DateTime,
    total_market_cap  Float64,
    btc_dominance     Float64,   -- percentage 0-100
    eth_dominance     Float64,   -- percentage 0-100
    total_volume      Float64,
    updated_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(hour)
ORDER BY hour
SETTINGS index_granularity = 8192;

-- ─── mart_coin_stats ──────────────────────────────────────────────────────────
-- Per-coin statistics: rolling averages, volatility, max drawdown
CREATE TABLE IF NOT EXISTS crypto.mart_coin_stats
(
    coin_id          String,
    symbol           String,
    name             String,
    avg_price_7d     Float64,
    avg_price_30d    Float64,
    volatility_30d   Float64,   -- standard deviation of daily returns
    max_drawdown_30d Float64,   -- maximum percentage drop from peak
    last_price       Float64,
    updated_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY coin_id
SETTINGS index_granularity = 8192;
