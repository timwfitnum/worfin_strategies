-- =============================================================================
-- ClickHouse Database Schema for Quantitative Trading System
-- Optimized for high-throughput tick data and analytics
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Database Creation
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS ticks ENGINE = Atomic;
CREATE DATABASE IF NOT EXISTS ohlcv ENGINE = Atomic;
CREATE DATABASE IF NOT EXISTS orderbook ENGINE = Atomic;
CREATE DATABASE IF NOT EXISTS analytics ENGINE = Atomic;
CREATE DATABASE IF NOT EXISTS monitoring ENGINE = Atomic;

-- =============================================================================
-- TICK DATA TABLES (ticks database)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Trade Ticks - Primary tick storage
-- Partitioned by date, ordered by symbol and timestamp for efficient queries
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ticks.trades
(
    -- Identifiers
    exchange          LowCardinality(String),
    symbol            LowCardinality(String),
    trade_id          String,
    
    -- Timing
    timestamp         DateTime64(6, 'UTC'),  -- Microsecond precision
    exchange_timestamp DateTime64(6, 'UTC'),
    received_timestamp DateTime64(6, 'UTC') DEFAULT now64(6),
    
    -- Price and Volume
    price             Float64,
    volume            Float64,
    
    -- Trade details
    side              Enum8('buy' = 1, 'sell' = 2, 'unknown' = 0),
    trade_type        LowCardinality(String) DEFAULT 'regular',
    
    -- Conditions and flags
    conditions        Array(String) DEFAULT [],
    is_odd_lot        Bool DEFAULT false,
    
    -- Data quality
    data_source       LowCardinality(String),
    sequence_number   UInt64 DEFAULT 0,
    
    -- Derived fields (for faster queries)
    date              Date MATERIALIZED toDate(timestamp),
    hour              UInt8 MATERIALIZED toHour(timestamp)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp, trade_id)
TTL timestamp + INTERVAL 5 YEAR
SETTINGS index_granularity = 8192,
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;

-- Create index for common query patterns
ALTER TABLE ticks.trades ADD INDEX idx_exchange (exchange) TYPE bloom_filter GRANULARITY 4;
ALTER TABLE ticks.trades ADD INDEX idx_date (date) TYPE minmax GRANULARITY 1;

-- -----------------------------------------------------------------------------
-- Quote Ticks - Bid/Ask data
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ticks.quotes
(
    exchange          LowCardinality(String),
    symbol            LowCardinality(String),
    
    timestamp         DateTime64(6, 'UTC'),
    exchange_timestamp DateTime64(6, 'UTC'),
    received_timestamp DateTime64(6, 'UTC') DEFAULT now64(6),
    
    -- Bid side
    bid_price         Float64,
    bid_size          Float64,
    bid_exchange      LowCardinality(String) DEFAULT '',
    
    -- Ask side
    ask_price         Float64,
    ask_size          Float64,
    ask_exchange      LowCardinality(String) DEFAULT '',
    
    -- Derived
    spread            Float64 MATERIALIZED ask_price - bid_price,
    spread_pct        Float64 MATERIALIZED (ask_price - bid_price) / ((ask_price + bid_price) / 2) * 10000,  -- bps
    mid_price         Float64 MATERIALIZED (bid_price + ask_price) / 2,
    
    -- Metadata
    data_source       LowCardinality(String),
    sequence_number   UInt64 DEFAULT 0,
    
    date              Date MATERIALIZED toDate(timestamp)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp)
TTL timestamp + INTERVAL 1 YEAR  -- Quotes retained shorter than trades
SETTINGS index_granularity = 8192;

-- =============================================================================
-- OHLCV TABLES (ohlcv database)
-- Pre-aggregated bars at multiple timeframes
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1-Minute Bars
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ohlcv.bars_1m
(
    exchange          LowCardinality(String),
    symbol            LowCardinality(String),
    timestamp         DateTime('UTC'),  -- Bar open time
    
    -- OHLCV
    open              Float64,
    high              Float64,
    low               Float64,
    close             Float64,
    volume            Float64,
    
    -- Extended metrics
    trades            UInt32 DEFAULT 0,
    vwap              Float64 DEFAULT 0,
    
    -- Buy/Sell breakdown
    buy_volume        Float64 DEFAULT 0,
    sell_volume       Float64 DEFAULT 0,
    
    -- Additional statistics
    typical_price     Float64 MATERIALIZED (high + low + close) / 3,
    range_pct         Float64 MATERIALIZED (high - low) / open * 100,
    
    -- Metadata
    data_source       LowCardinality(String),
    is_complete       Bool DEFAULT true,
    
    date              Date MATERIALIZED toDate(timestamp)
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp)
TTL timestamp + INTERVAL 3 YEAR
SETTINGS index_granularity = 8192;

-- -----------------------------------------------------------------------------
-- 5-Minute Bars
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ohlcv.bars_5m
(
    exchange          LowCardinality(String),
    symbol            LowCardinality(String),
    timestamp         DateTime('UTC'),
    
    open              Float64,
    high              Float64,
    low               Float64,
    close             Float64,
    volume            Float64,
    trades            UInt32 DEFAULT 0,
    vwap              Float64 DEFAULT 0,
    buy_volume        Float64 DEFAULT 0,
    sell_volume       Float64 DEFAULT 0,
    
    data_source       LowCardinality(String),
    is_complete       Bool DEFAULT true,
    date              Date MATERIALIZED toDate(timestamp)
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp)
TTL timestamp + INTERVAL 5 YEAR
SETTINGS index_granularity = 8192;

-- -----------------------------------------------------------------------------
-- 1-Hour Bars
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ohlcv.bars_1h
(
    exchange          LowCardinality(String),
    symbol            LowCardinality(String),
    timestamp         DateTime('UTC'),
    
    open              Float64,
    high              Float64,
    low               Float64,
    close             Float64,
    volume            Float64,
    trades            UInt32 DEFAULT 0,
    vwap              Float64 DEFAULT 0,
    buy_volume        Float64 DEFAULT 0,
    sell_volume       Float64 DEFAULT 0,
    
    data_source       LowCardinality(String),
    is_complete       Bool DEFAULT true,
    date              Date MATERIALIZED toDate(timestamp)
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp)
TTL timestamp + INTERVAL 10 YEAR
SETTINGS index_granularity = 8192;

-- -----------------------------------------------------------------------------
-- Daily Bars
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ohlcv.bars_daily
(
    exchange          LowCardinality(String),
    symbol            LowCardinality(String),
    date              Date,
    
    open              Float64,
    high              Float64,
    low               Float64,
    close             Float64,
    volume            Float64,
    
    -- Adjusted prices (for splits/dividends)
    adj_open          Float64 DEFAULT 0,
    adj_high          Float64 DEFAULT 0,
    adj_low           Float64 DEFAULT 0,
    adj_close         Float64 DEFAULT 0,
    adj_volume        Float64 DEFAULT 0,
    
    -- Extended
    trades            UInt64 DEFAULT 0,
    vwap              Float64 DEFAULT 0,
    
    -- Market session prices
    pre_market_high   Float64 DEFAULT 0,
    pre_market_low    Float64 DEFAULT 0,
    after_hours_high  Float64 DEFAULT 0,
    after_hours_low   Float64 DEFAULT 0,
    
    data_source       LowCardinality(String),
    is_complete       Bool DEFAULT true
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYear(date)
ORDER BY (symbol, date)
SETTINGS index_granularity = 8192;

-- =============================================================================
-- MATERIALIZED VIEWS FOR AGGREGATION
-- Automatically aggregate ticks into OHLCV bars
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1-Minute Bar Aggregation from Trades
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv.mv_bars_1m TO ohlcv.bars_1m AS
SELECT
    exchange,
    symbol,
    toStartOfMinute(timestamp) AS timestamp,
    argMin(price, timestamp) AS open,
    max(price) AS high,
    min(price) AS low,
    argMax(price, timestamp) AS close,
    sum(volume) AS volume,
    count() AS trades,
    sum(price * volume) / sum(volume) AS vwap,
    sumIf(volume, side = 'buy') AS buy_volume,
    sumIf(volume, side = 'sell') AS sell_volume,
    any(data_source) AS data_source,
    true AS is_complete
FROM ticks.trades
GROUP BY exchange, symbol, toStartOfMinute(timestamp);

-- -----------------------------------------------------------------------------
-- 5-Minute Bar Aggregation from 1-Minute Bars
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv.mv_bars_5m TO ohlcv.bars_5m AS
SELECT
    exchange,
    symbol,
    toStartOfFiveMinutes(timestamp) AS timestamp,
    argMin(open, timestamp) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, timestamp) AS close,
    sum(volume) AS volume,
    sum(trades) AS trades,
    sum(vwap * volume) / sum(volume) AS vwap,
    sum(buy_volume) AS buy_volume,
    sum(sell_volume) AS sell_volume,
    any(data_source) AS data_source,
    true AS is_complete
FROM ohlcv.bars_1m
GROUP BY exchange, symbol, toStartOfFiveMinutes(timestamp);

-- -----------------------------------------------------------------------------
-- 1-Hour Bar Aggregation from 5-Minute Bars
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv.mv_bars_1h TO ohlcv.bars_1h AS
SELECT
    exchange,
    symbol,
    toStartOfHour(timestamp) AS timestamp,
    argMin(open, timestamp) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, timestamp) AS close,
    sum(volume) AS volume,
    sum(trades) AS trades,
    sum(vwap * volume) / sum(volume) AS vwap,
    sum(buy_volume) AS buy_volume,
    sum(sell_volume) AS sell_volume,
    any(data_source) AS data_source,
    true AS is_complete
FROM ohlcv.bars_5m
GROUP BY exchange, symbol, toStartOfHour(timestamp);

-- =============================================================================
-- ORDER BOOK TABLES (orderbook database)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Order Book Snapshots (L2)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orderbook.snapshots_l2
(
    exchange          LowCardinality(String),
    symbol            LowCardinality(String),
    timestamp         DateTime64(6, 'UTC'),
    
    -- Nested arrays for bid/ask levels
    bid_prices        Array(Float64),
    bid_sizes         Array(Float64),
    ask_prices        Array(Float64),
    ask_sizes         Array(Float64),
    
    -- Derived metrics
    spread            Float64 MATERIALIZED if(length(ask_prices) > 0 AND length(bid_prices) > 0, 
                                              ask_prices[1] - bid_prices[1], 0),
    mid_price         Float64 MATERIALIZED if(length(ask_prices) > 0 AND length(bid_prices) > 0,
                                              (ask_prices[1] + bid_prices[1]) / 2, 0),
    imbalance         Float64 MATERIALIZED if(length(ask_sizes) > 0 AND length(bid_sizes) > 0,
                                              (bid_sizes[1] - ask_sizes[1]) / (bid_sizes[1] + ask_sizes[1]), 0),
    
    -- Total depth
    total_bid_depth   Float64 MATERIALIZED arraySum(bid_sizes),
    total_ask_depth   Float64 MATERIALIZED arraySum(ask_sizes),
    
    data_source       LowCardinality(String),
    depth_levels      UInt8 MATERIALIZED length(bid_prices),
    
    date              Date MATERIALIZED toDate(timestamp)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp)
TTL timestamp + INTERVAL 30 DAY  -- Order book data retained 30 days
SETTINGS index_granularity = 8192;

-- =============================================================================
-- ANALYTICS TABLES (analytics database)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Strategy Signals
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.signals
(
    signal_id         UUID DEFAULT generateUUIDv4(),
    timestamp         DateTime64(6, 'UTC'),
    
    strategy_id       UInt16,
    strategy_name     LowCardinality(String),
    
    symbol            LowCardinality(String),
    signal_type       Enum8('buy' = 1, 'sell' = 2, 'hold' = 0),
    strength          Float64,  -- -1 to 1
    confidence        Float64,  -- 0 to 1
    
    -- Signal components
    features          Map(String, Float64),
    model_version     String DEFAULT '',
    
    -- Target and horizon
    target_price      Nullable(Float64),
    stop_loss         Nullable(Float64),
    take_profit       Nullable(Float64),
    horizon_minutes   UInt32 DEFAULT 0,
    
    -- Outcome tracking
    entry_price       Nullable(Float64),
    exit_price        Nullable(Float64),
    pnl               Nullable(Float64),
    pnl_pct           Nullable(Float64),
    
    date              Date MATERIALIZED toDate(timestamp)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (strategy_id, symbol, timestamp)
SETTINGS index_granularity = 8192;

-- -----------------------------------------------------------------------------
-- Execution Analytics
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.executions
(
    execution_id      UUID DEFAULT generateUUIDv4(),
    order_id          String,
    
    timestamp         DateTime64(6, 'UTC'),
    fill_timestamp    DateTime64(6, 'UTC'),
    
    strategy_id       UInt16,
    symbol            LowCardinality(String),
    exchange          LowCardinality(String),
    
    -- Order details
    side              Enum8('buy' = 1, 'sell' = 2),
    order_type        LowCardinality(String),
    
    -- Quantities
    requested_qty     Float64,
    filled_qty        Float64,
    remaining_qty     Float64,
    
    -- Prices
    limit_price       Nullable(Float64),
    avg_fill_price    Float64,
    
    -- Benchmarks
    decision_price    Float64,  -- Price at signal generation
    arrival_price     Float64,  -- Price at order submission
    vwap_price        Float64,  -- VWAP during execution
    
    -- Slippage analysis (in basis points)
    slippage_bps      Float64 MATERIALIZED (avg_fill_price - decision_price) / decision_price * 10000 * 
                                           if(side = 'buy', 1, -1),
    implementation_shortfall Float64 MATERIALIZED (avg_fill_price - arrival_price) / arrival_price * 10000 *
                                                   if(side = 'buy', 1, -1),
    
    -- Costs
    commission        Float64 DEFAULT 0,
    fees              Float64 DEFAULT 0,
    total_cost        Float64 MATERIALIZED commission + fees,
    
    -- Timing
    time_to_fill_ms   UInt64,
    
    date              Date MATERIALIZED toDate(timestamp)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (strategy_id, symbol, timestamp)
SETTINGS index_granularity = 8192;

-- -----------------------------------------------------------------------------
-- Performance Snapshots
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.performance_snapshots
(
    timestamp         DateTime('UTC'),
    
    strategy_id       UInt16,
    strategy_name     LowCardinality(String),
    
    -- PnL
    daily_pnl         Float64,
    mtd_pnl           Float64,
    ytd_pnl           Float64,
    total_pnl         Float64,
    
    -- Returns
    daily_return      Float64,
    mtd_return        Float64,
    ytd_return        Float64,
    
    -- Risk metrics
    sharpe_ratio      Float64,
    sortino_ratio     Float64,
    max_drawdown      Float64,
    current_drawdown  Float64,
    
    -- Risk measures
    daily_var_95      Float64,
    daily_var_99      Float64,
    expected_shortfall Float64,
    
    -- Position metrics
    position_count    UInt32,
    gross_exposure    Float64,
    net_exposure      Float64,
    
    -- Trading activity
    trade_count       UInt32,
    win_rate          Float64,
    profit_factor     Float64,
    
    date              Date MATERIALIZED toDate(timestamp)
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (strategy_id, timestamp)
SETTINGS index_granularity = 8192;

-- =============================================================================
-- MONITORING TABLES (monitoring database)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Data Quality Metrics
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS monitoring.data_quality
(
    timestamp         DateTime('UTC'),
    date              Date,
    
    symbol            LowCardinality(String),
    data_source       LowCardinality(String),
    
    -- Completeness
    expected_ticks    UInt64,
    actual_ticks      UInt64,
    completeness_pct  Float64,
    
    -- Gaps
    gap_count         UInt32,
    max_gap_seconds   Float64,
    total_gap_seconds Float64,
    
    -- Outliers
    outlier_count     UInt32,
    outlier_pct       Float64,
    
    -- Latency
    avg_latency_ms    Float64,
    max_latency_ms    Float64,
    p99_latency_ms    Float64,
    
    -- Quality score (0-100)
    quality_score     Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- -----------------------------------------------------------------------------
-- System Metrics
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS monitoring.system_metrics
(
    timestamp         DateTime('UTC'),
    
    -- CPU
    cpu_usage_pct     Float64,
    
    -- Memory
    memory_used_mb    Float64,
    memory_total_mb   Float64,
    memory_pct        Float64,
    
    -- Disk
    disk_used_gb      Float64,
    disk_total_gb     Float64,
    
    -- Network
    network_recv_mb   Float64,
    network_sent_mb   Float64,
    
    -- Database specific
    clickhouse_queries_per_sec  Float64,
    clickhouse_inserts_per_sec  Float64,
    redis_ops_per_sec           Float64,
    redis_memory_mb             Float64,
    
    -- Application
    active_connections UInt32,
    pending_orders     UInt32,
    open_positions     UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY timestamp
TTL timestamp + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- =============================================================================
-- USEFUL VIEWS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Latest quotes per symbol
-- -----------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS ticks.v_latest_quotes AS
SELECT
    symbol,
    argMax(bid_price, timestamp) AS bid_price,
    argMax(bid_size, timestamp) AS bid_size,
    argMax(ask_price, timestamp) AS ask_price,
    argMax(ask_size, timestamp) AS ask_size,
    argMax(spread, timestamp) AS spread,
    argMax(mid_price, timestamp) AS mid_price,
    max(timestamp) AS last_update
FROM ticks.quotes
WHERE timestamp > now() - INTERVAL 1 HOUR
GROUP BY symbol;

-- -----------------------------------------------------------------------------
-- Market microstructure summary
-- -----------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS analytics.v_market_microstructure AS
SELECT
    symbol,
    toDate(timestamp) AS date,
    count() AS trade_count,
    sum(volume) AS total_volume,
    sum(price * volume) / sum(volume) AS vwap,
    max(price) - min(price) AS price_range,
    avg(price) AS avg_price,
    stddevPop(price) AS price_std,
    avg(spread) AS avg_spread
FROM ticks.trades t
LEFT JOIN ticks.quotes q ON t.symbol = q.symbol 
    AND toStartOfSecond(t.timestamp) = toStartOfSecond(q.timestamp)
WHERE t.timestamp > now() - INTERVAL 7 DAY
GROUP BY symbol, toDate(t.timestamp);

-- =============================================================================
-- DICTIONARY FOR SYMBOL MAPPING
-- =============================================================================

CREATE DICTIONARY IF NOT EXISTS analytics.symbol_mapping
(
    internal_symbol String,
    ib_symbol       String,
    polygon_symbol  String,
    binance_symbol  String,
    coinbase_symbol String,
    yahoo_symbol    String,
    asset_class     String,
    currency        String,
    exchange        String,
    is_active       UInt8
)
PRIMARY KEY internal_symbol
SOURCE(CLICKHOUSE(
    HOST 'localhost'
    PORT 9000
    USER 'default'
    TABLE 'symbol_reference'
    DB 'analytics'
))
LIFETIME(MIN 3600 MAX 86400)
LAYOUT(HASHED());