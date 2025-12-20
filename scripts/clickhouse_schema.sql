-- =============================================================================
-- Quantitative Trading System - ClickHouse Schema
-- Optimized for high-throughput tick data and analytics queries
-- =============================================================================

-- =============================================================================
-- DATABASE SETUP
-- =============================================================================

CREATE DATABASE IF NOT EXISTS ticks ENGINE = Atomic;
CREATE DATABASE IF NOT EXISTS ohlcv ENGINE = Atomic;
CREATE DATABASE IF NOT EXISTS orderbook ENGINE = Atomic;
CREATE DATABASE IF NOT EXISTS analytics ENGINE = Atomic;
CREATE DATABASE IF NOT EXISTS monitoring ENGINE = Atomic;

-- =============================================================================
-- TICK DATA TABLES
-- =============================================================================

-- Trade ticks - main tick storage
-- Optimized for: time-series queries, symbol lookups, aggregations
CREATE TABLE IF NOT EXISTS ticks.trades
(
    -- Time partitioning (primary)
    timestamp DateTime64(3, 'UTC'),
    date Date DEFAULT toDate(timestamp),
    
    -- Symbol identification
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    
    -- Trade data
    price Decimal64(8),
    volume Decimal64(8),
    side Enum8('buy' = 1, 'sell' = 2, 'unknown' = 0),
    
    -- Trade identification
    trade_id String,
    
    -- Additional fields
    conditions Array(LowCardinality(String)),
    
    -- Ingestion metadata
    received_at DateTime64(3, 'UTC') DEFAULT now64(3),
    source LowCardinality(String) DEFAULT 'unknown'
)
ENGINE = MergeTree()
PARTITION BY (toYYYYMM(date), exchange)
ORDER BY (symbol, timestamp, trade_id)
TTL date + INTERVAL 5 YEAR
SETTINGS 
    index_granularity = 8192,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

-- Quote ticks (bid/ask)
CREATE TABLE IF NOT EXISTS ticks.quotes
(
    timestamp DateTime64(3, 'UTC'),
    date Date DEFAULT toDate(timestamp),
    
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    
    -- Best bid/ask
    bid_price Decimal64(8),
    bid_size Decimal64(8),
    ask_price Decimal64(8),
    ask_size Decimal64(8),
    
    -- Derived
    spread Decimal64(8) DEFAULT ask_price - bid_price,
    mid_price Decimal64(8) DEFAULT (bid_price + ask_price) / 2,
    
    -- Metadata
    received_at DateTime64(3, 'UTC') DEFAULT now64(3),
    source LowCardinality(String) DEFAULT 'unknown'
)
ENGINE = MergeTree()
PARTITION BY (toYYYYMM(date), exchange)
ORDER BY (symbol, timestamp)
TTL date + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

-- =============================================================================
-- OHLCV BARS (Aggregated from ticks)
-- =============================================================================

-- 1-minute bars
CREATE TABLE IF NOT EXISTS ohlcv.bars_1m
(
    timestamp DateTime('UTC'),
    date Date DEFAULT toDate(timestamp),
    
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    
    open Decimal64(8),
    high Decimal64(8),
    low Decimal64(8),
    close Decimal64(8),
    volume Decimal64(8),
    
    trades UInt32,
    vwap Decimal64(8),
    
    -- Additional metrics
    buy_volume Decimal64(8) DEFAULT 0,
    sell_volume Decimal64(8) DEFAULT 0,
    
    -- Source tracking
    tick_count UInt32 DEFAULT 0
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (symbol, timestamp)
TTL date + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- 5-minute bars
CREATE TABLE IF NOT EXISTS ohlcv.bars_5m
(
    timestamp DateTime('UTC'),
    date Date DEFAULT toDate(timestamp),
    
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    
    open Decimal64(8),
    high Decimal64(8),
    low Decimal64(8),
    close Decimal64(8),
    volume Decimal64(8),
    
    trades UInt32,
    vwap Decimal64(8)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (symbol, timestamp)
TTL date + INTERVAL 3 YEAR
SETTINGS index_granularity = 8192;

-- 1-hour bars
CREATE TABLE IF NOT EXISTS ohlcv.bars_1h
(
    timestamp DateTime('UTC'),
    date Date DEFAULT toDate(timestamp),
    
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    
    open Decimal64(8),
    high Decimal64(8),
    low Decimal64(8),
    close Decimal64(8),
    volume Decimal64(8),
    
    trades UInt32,
    vwap Decimal64(8)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (symbol, timestamp)
TTL date + INTERVAL 5 YEAR
SETTINGS index_granularity = 8192;

-- Daily bars
CREATE TABLE IF NOT EXISTS ohlcv.bars_daily
(
    timestamp DateTime('UTC'),
    date Date DEFAULT toDate(timestamp),
    
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    
    open Decimal64(8),
    high Decimal64(8),
    low Decimal64(8),
    close Decimal64(8),
    volume Decimal64(8),
    
    trades UInt32,
    vwap Decimal64(8),
    
    -- Daily-specific
    prev_close Decimal64(8),
    change Decimal64(8),
    change_pct Decimal64(6)
)
ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (symbol, date)
SETTINGS index_granularity = 8192;

-- =============================================================================
-- ORDER BOOK DATA
-- =============================================================================

-- Order book snapshots
CREATE TABLE IF NOT EXISTS orderbook.snapshots
(
    timestamp DateTime64(3, 'UTC'),
    date Date DEFAULT toDate(timestamp),
    
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    
    -- Top of book
    best_bid Decimal64(8),
    best_ask Decimal64(8),
    best_bid_size Decimal64(8),
    best_ask_size Decimal64(8),
    
    -- Depth levels (nested arrays for efficiency)
    bid_prices Array(Decimal64(8)),
    bid_sizes Array(Decimal64(8)),
    ask_prices Array(Decimal64(8)),
    ask_sizes Array(Decimal64(8)),
    
    -- Aggregated metrics
    total_bid_depth Decimal64(8),
    total_ask_depth Decimal64(8),
    imbalance Decimal64(8),
    
    -- Metadata
    levels UInt8,
    sequence_num UInt64
)
ENGINE = MergeTree()
PARTITION BY (toYYYYMM(date), exchange)
ORDER BY (symbol, timestamp)
TTL date + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- =============================================================================
-- MATERIALIZED VIEWS FOR REAL-TIME AGGREGATION
-- =============================================================================

-- Real-time 1-minute bar aggregation from ticks
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv.bars_1m_mv TO ohlcv.bars_1m AS
SELECT
    toStartOfMinute(timestamp) AS timestamp,
    toDate(timestamp) AS date,
    exchange,
    symbol,
    argMin(price, timestamp) AS open,
    max(price) AS high,
    min(price) AS low,
    argMax(price, timestamp) AS close,
    sum(volume) AS volume,
    count() AS trades,
    sum(price * volume) / sum(volume) AS vwap,
    sumIf(volume, side = 'buy') AS buy_volume,
    sumIf(volume, side = 'sell') AS sell_volume,
    count() AS tick_count
FROM ticks.trades
GROUP BY
    toStartOfMinute(timestamp),
    toDate(timestamp),
    exchange,
    symbol;

-- Real-time 5-minute bar aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv.bars_5m_mv TO ohlcv.bars_5m AS
SELECT
    toStartOfFiveMinutes(timestamp) AS timestamp,
    toDate(timestamp) AS date,
    exchange,
    symbol,
    argMin(price, timestamp) AS open,
    max(price) AS high,
    min(price) AS low,
    argMax(price, timestamp) AS close,
    sum(volume) AS volume,
    count() AS trades,
    sum(price * volume) / sum(volume) AS vwap
FROM ticks.trades
GROUP BY
    toStartOfFiveMinutes(timestamp),
    toDate(timestamp),
    exchange,
    symbol;

-- Real-time hourly bar aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv.bars_1h_mv TO ohlcv.bars_1h AS
SELECT
    toStartOfHour(timestamp) AS timestamp,
    toDate(timestamp) AS date,
    exchange,
    symbol,
    argMin(price, timestamp) AS open,
    max(price) AS high,
    min(price) AS low,
    argMax(price, timestamp) AS close,
    sum(volume) AS volume,
    count() AS trades,
    sum(price * volume) / sum(volume) AS vwap
FROM ticks.trades
GROUP BY
    toStartOfHour(timestamp),
    toDate(timestamp),
    exchange,
    symbol;

-- =============================================================================
-- ANALYTICS TABLES
-- =============================================================================

-- Intraday statistics
CREATE TABLE IF NOT EXISTS analytics.intraday_stats
(
    date Date,
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    
    -- Volume statistics
    total_volume Decimal64(8),
    total_trades UInt64,
    avg_trade_size Decimal64(8),
    
    -- Price statistics
    open Decimal64(8),
    high Decimal64(8),
    low Decimal64(8),
    close Decimal64(8),
    vwap Decimal64(8),
    
    -- Volatility
    realized_volatility Decimal64(8),
    intraday_range_pct Decimal64(6),
    
    -- Microstructure
    avg_spread Decimal64(8),
    avg_spread_bps Decimal64(4),
    
    -- Order flow
    buy_volume Decimal64(8),
    sell_volume Decimal64(8),
    net_flow Decimal64(8),
    vpin Decimal64(6)  -- Volume-synchronized probability of informed trading
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (symbol, date)
SETTINGS index_granularity = 8192;

-- Market microstructure metrics
CREATE TABLE IF NOT EXISTS analytics.microstructure
(
    timestamp DateTime64(3, 'UTC'),
    date Date DEFAULT toDate(timestamp),
    
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    
    -- Spread metrics
    quoted_spread Decimal64(8),
    effective_spread Decimal64(8),
    realized_spread Decimal64(8),
    price_impact Decimal64(8),
    
    -- Depth metrics
    depth_imbalance Decimal64(6),
    order_flow_imbalance Decimal64(6),
    
    -- Volatility
    realized_volatility_5m Decimal64(8),
    realized_volatility_30m Decimal64(8),
    
    -- Kyle's lambda (price impact coefficient)
    kyle_lambda Decimal64(8)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (symbol, timestamp)
TTL date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- =============================================================================
-- MONITORING TABLES
-- =============================================================================

-- Data ingestion metrics
CREATE TABLE IF NOT EXISTS monitoring.ingestion_metrics
(
    timestamp DateTime DEFAULT now(),
    
    source LowCardinality(String),
    table_name LowCardinality(String),
    
    rows_inserted UInt64,
    bytes_inserted UInt64,
    insert_time_ms UInt32,
    
    errors UInt32,
    duplicates UInt32,
    
    latency_p50_ms Float32,
    latency_p99_ms Float32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (source, timestamp)
TTL timestamp + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- Data quality alerts
CREATE TABLE IF NOT EXISTS monitoring.data_quality
(
    timestamp DateTime DEFAULT now(),
    date Date DEFAULT toDate(timestamp),
    
    source LowCardinality(String),
    symbol LowCardinality(String),
    
    completeness_score Float32,
    gap_count UInt32,
    max_gap_seconds UInt32,
    outlier_count UInt32,
    
    details String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (source, symbol, timestamp)
TTL date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- =============================================================================
-- USEFUL QUERIES AND FUNCTIONS
-- =============================================================================

-- Function to calculate VWAP for a time period
CREATE FUNCTION IF NOT EXISTS vwap AS (prices, volumes) ->
    arraySum(arrayMap((p, v) -> p * v, prices, volumes)) / arraySum(volumes);

-- Function to calculate realized volatility (Parkinson)
CREATE FUNCTION IF NOT EXISTS parkinson_vol AS (high, low) ->
    sqrt(sum(pow(log(high / low), 2)) / (4 * count() * log(2)));

-- =============================================================================
-- SAMPLE QUERIES FOR COMMON USE CASES
-- =============================================================================

/*
-- Get OHLCV bars with volume profile
SELECT
    toStartOfFiveMinutes(timestamp) AS time,
    symbol,
    argMin(price, timestamp) AS open,
    max(price) AS high,
    min(price) AS low,
    argMax(price, timestamp) AS close,
    sum(volume) AS volume,
    sum(price * volume) / sum(volume) AS vwap,
    sumIf(volume, side = 'buy') AS buy_volume,
    sumIf(volume, side = 'sell') AS sell_volume
FROM ticks.trades
WHERE symbol = 'AAPL'
    AND timestamp >= now() - INTERVAL 1 DAY
GROUP BY time, symbol
ORDER BY time;

-- Calculate intraday statistics
SELECT
    symbol,
    count() AS trade_count,
    sum(volume) AS total_volume,
    max(price) - min(price) AS price_range,
    (max(price) - min(price)) / avg(price) * 100 AS range_pct,
    sum(price * volume) / sum(volume) AS vwap
FROM ticks.trades
WHERE date = today()
GROUP BY symbol
ORDER BY total_volume DESC
LIMIT 20;

-- Market microstructure analysis
SELECT
    toStartOfMinute(timestamp) AS minute,
    symbol,
    avg(ask_price - bid_price) AS avg_spread,
    avg((ask_price - bid_price) / mid_price * 10000) AS spread_bps,
    sum(bid_size) AS total_bid_depth,
    sum(ask_size) AS total_ask_depth,
    (sum(bid_size) - sum(ask_size)) / (sum(bid_size) + sum(ask_size)) AS depth_imbalance
FROM ticks.quotes
WHERE symbol = 'AAPL'
    AND timestamp >= now() - INTERVAL 1 HOUR
GROUP BY minute, symbol
ORDER BY minute;
*/

-- =============================================================================
-- GRANTS AND PERMISSIONS
-- =============================================================================

-- Create user for application
-- CREATE USER IF NOT EXISTS quant IDENTIFIED BY 'quant123';
-- GRANT ALL ON ticks.* TO quant;
-- GRANT ALL ON ohlcv.* TO quant;
-- GRANT ALL ON orderbook.* TO quant;
-- GRANT ALL ON analytics.* TO quant;
-- GRANT ALL ON monitoring.* TO quant;