-- =============================================================================
-- Quantitative Trading System - TimescaleDB Schema
-- PostgreSQL with TimescaleDB extension for time-series data
-- =============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- SCHEMA ORGANIZATION
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS market_data;
CREATE SCHEMA IF NOT EXISTS reference;
CREATE SCHEMA IF NOT EXISTS trading;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- =============================================================================
-- REFERENCE DATA TABLES
-- =============================================================================

-- Asset universe table
CREATE TABLE IF NOT EXISTS reference.assets (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(32) NOT NULL UNIQUE,
    name VARCHAR(255),
    asset_class VARCHAR(32) NOT NULL CHECK (asset_class IN ('equity', 'option', 'future', 'crypto', 'forex', 'bond', 'etf')),
    exchange VARCHAR(32),
    currency VARCHAR(8) DEFAULT 'USD',
    sector VARCHAR(64),
    industry VARCHAR(128),
    market_cap_category VARCHAR(16) CHECK (market_cap_category IN ('mega', 'large', 'mid', 'small', 'micro', 'nano')),
    
    -- Trading characteristics
    lot_size INTEGER DEFAULT 1,
    tick_size DECIMAL(18, 10),
    min_price_increment DECIMAL(18, 10),
    tradeable BOOLEAN DEFAULT TRUE,
    marginable BOOLEAN DEFAULT TRUE,
    shortable BOOLEAN DEFAULT TRUE,
    
    -- Identifiers
    isin VARCHAR(12),
    cusip VARCHAR(9),
    sedol VARCHAR(7),
    figi VARCHAR(12),
    cik VARCHAR(10),
    
    -- Metadata
    active BOOLEAN DEFAULT TRUE,
    listing_date DATE,
    delisting_date DATE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_assets_symbol ON reference.assets(symbol);
CREATE INDEX idx_assets_asset_class ON reference.assets(asset_class);
CREATE INDEX idx_assets_exchange ON reference.assets(exchange);
CREATE INDEX idx_assets_active ON reference.assets(active) WHERE active = TRUE;

-- Exchange reference table
CREATE TABLE IF NOT EXISTS reference.exchanges (
    id SERIAL PRIMARY KEY,
    code VARCHAR(16) NOT NULL UNIQUE,
    name VARCHAR(128) NOT NULL,
    country VARCHAR(64),
    timezone VARCHAR(64) NOT NULL,
    currency VARCHAR(8),
    
    -- Trading hours (in local time)
    open_time TIME,
    close_time TIME,
    pre_market_open TIME,
    after_hours_close TIME,
    
    -- Status
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Trading calendar
CREATE TABLE IF NOT EXISTS reference.trading_calendar (
    id SERIAL PRIMARY KEY,
    exchange_code VARCHAR(16) NOT NULL REFERENCES reference.exchanges(code),
    date DATE NOT NULL,
    is_trading_day BOOLEAN DEFAULT TRUE,
    is_half_day BOOLEAN DEFAULT FALSE,
    open_time TIMESTAMPTZ,
    close_time TIMESTAMPTZ,
    description VARCHAR(128),
    
    UNIQUE(exchange_code, date)
);

CREATE INDEX idx_calendar_exchange_date ON reference.trading_calendar(exchange_code, date);

-- Corporate actions
CREATE TABLE IF NOT EXISTS reference.corporate_actions (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(32) NOT NULL REFERENCES reference.assets(symbol),
    action_type VARCHAR(32) NOT NULL CHECK (action_type IN (
        'split', 'reverse_split', 'dividend', 'special_dividend',
        'spinoff', 'merger', 'acquisition', 'name_change', 'delisting'
    )),
    ex_date DATE NOT NULL,
    record_date DATE,
    payment_date DATE,
    
    -- Split/dividend details
    factor DECIMAL(18, 10),  -- For splits: new/old ratio, for dividends: amount per share
    currency VARCHAR(8) DEFAULT 'USD',
    
    -- Additional info
    description TEXT,
    adjusted BOOLEAN DEFAULT FALSE,  -- Whether historical data has been adjusted
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_corp_actions_symbol_date ON reference.corporate_actions(symbol, ex_date);

-- =============================================================================
-- MARKET DATA TABLES (TimescaleDB Hypertables)
-- =============================================================================

-- OHLCV Daily bars
CREATE TABLE IF NOT EXISTS market_data.ohlcv_daily (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    open DECIMAL(18, 8) NOT NULL,
    high DECIMAL(18, 8) NOT NULL,
    low DECIMAL(18, 8) NOT NULL,
    close DECIMAL(18, 8) NOT NULL,
    volume BIGINT,
    vwap DECIMAL(18, 8),
    trades INTEGER,
    
    -- Adjusted prices (for splits/dividends)
    adj_open DECIMAL(18, 8),
    adj_high DECIMAL(18, 8),
    adj_low DECIMAL(18, 8),
    adj_close DECIMAL(18, 8),
    adj_volume BIGINT,
    
    PRIMARY KEY (time, symbol)
);

SELECT create_hypertable('market_data.ohlcv_daily', 'time', 
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- Add compression policy
ALTER TABLE market_data.ohlcv_daily SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('market_data.ohlcv_daily', INTERVAL '7 days', if_not_exists => TRUE);

-- OHLCV Intraday bars (1-minute)
CREATE TABLE IF NOT EXISTS market_data.ohlcv_1min (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    open DECIMAL(18, 8) NOT NULL,
    high DECIMAL(18, 8) NOT NULL,
    low DECIMAL(18, 8) NOT NULL,
    close DECIMAL(18, 8) NOT NULL,
    volume BIGINT,
    vwap DECIMAL(18, 8),
    trades INTEGER,
    
    PRIMARY KEY (time, symbol)
);

SELECT create_hypertable('market_data.ohlcv_1min', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

ALTER TABLE market_data.ohlcv_1min SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('market_data.ohlcv_1min', INTERVAL '3 days', if_not_exists => TRUE);

-- Retention policy for intraday data (keep 90 days)
SELECT add_retention_policy('market_data.ohlcv_1min', INTERVAL '90 days', if_not_exists => TRUE);

-- =============================================================================
-- TRADING TABLES
-- =============================================================================

-- Strategies registry
CREATE TABLE IF NOT EXISTS trading.strategies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(64) NOT NULL UNIQUE,
    description TEXT,
    strategy_type VARCHAR(32) CHECK (strategy_type IN (
        'momentum', 'mean_reversion', 'arbitrage', 'volatility', 
        'event_driven', 'factor', 'ml_enhanced', 'hybrid'
    )),
    asset_classes VARCHAR(128)[],  -- Array of supported asset classes
    
    -- Risk parameters
    max_position_pct DECIMAL(5, 4) DEFAULT 0.02,
    max_allocation_pct DECIMAL(5, 4) DEFAULT 0.15,
    max_drawdown_pct DECIMAL(5, 4) DEFAULT 0.15,
    
    -- Status
    active BOOLEAN DEFAULT TRUE,
    paper_trading BOOLEAN DEFAULT TRUE,
    live_trading BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    config JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Signals table
CREATE TABLE IF NOT EXISTS trading.signals (
    id BIGSERIAL,
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    strategy_id INTEGER NOT NULL REFERENCES trading.strategies(id),
    symbol VARCHAR(32) NOT NULL,
    
    signal_type VARCHAR(16) NOT NULL CHECK (signal_type IN ('buy', 'sell', 'hold', 'close')),
    strength DECIMAL(5, 4) CHECK (strength BETWEEN -1 AND 1),
    confidence DECIMAL(5, 4) CHECK (confidence BETWEEN 0 AND 1),
    
    -- Signal details
    entry_price DECIMAL(18, 8),
    target_price DECIMAL(18, 8),
    stop_loss DECIMAL(18, 8),
    expected_return DECIMAL(8, 6),
    expected_volatility DECIMAL(8, 6),
    
    -- Features that generated the signal
    features JSONB,
    model_version VARCHAR(32),
    
    -- Status
    status VARCHAR(16) DEFAULT 'pending' CHECK (status IN ('pending', 'executed', 'expired', 'cancelled')),
    executed_at TIMESTAMPTZ,
    
    PRIMARY KEY (time, id)
);

SELECT create_hypertable('trading.signals', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX idx_signals_strategy ON trading.signals(strategy_id, time DESC);
CREATE INDEX idx_signals_symbol ON trading.signals(symbol, time DESC);
CREATE INDEX idx_signals_status ON trading.signals(status) WHERE status = 'pending';

-- Positions table
CREATE TABLE IF NOT EXISTS trading.positions (
    id SERIAL PRIMARY KEY,
    strategy_id INTEGER REFERENCES trading.strategies(id),
    symbol VARCHAR(32) NOT NULL,
    
    -- Position details
    side VARCHAR(8) NOT NULL CHECK (side IN ('long', 'short')),
    quantity DECIMAL(18, 8) NOT NULL,
    entry_price DECIMAL(18, 8) NOT NULL,
    current_price DECIMAL(18, 8),
    
    -- Cost basis
    cost_basis DECIMAL(18, 8),
    market_value DECIMAL(18, 8),
    unrealized_pnl DECIMAL(18, 8),
    realized_pnl DECIMAL(18, 8) DEFAULT 0,
    
    -- Risk metrics
    stop_loss DECIMAL(18, 8),
    take_profit DECIMAL(18, 8),
    max_loss DECIMAL(18, 8),
    
    -- Status
    status VARCHAR(16) DEFAULT 'open' CHECK (status IN ('open', 'closed', 'partial')),
    opened_at TIMESTAMPTZ DEFAULT NOW(),
    closed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_positions_strategy ON trading.positions(strategy_id);
CREATE INDEX idx_positions_symbol ON trading.positions(symbol);
CREATE INDEX idx_positions_status ON trading.positions(status) WHERE status = 'open';

-- Orders table
CREATE TABLE IF NOT EXISTS trading.orders (
    id BIGSERIAL,
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Order identification
    client_order_id UUID DEFAULT uuid_generate_v4(),
    broker_order_id VARCHAR(64),
    parent_order_id BIGINT,  -- For child orders in algos
    
    -- Order details
    strategy_id INTEGER REFERENCES trading.strategies(id),
    position_id INTEGER REFERENCES trading.positions(id),
    symbol VARCHAR(32) NOT NULL,
    side VARCHAR(8) NOT NULL CHECK (side IN ('buy', 'sell')),
    order_type VARCHAR(16) NOT NULL CHECK (order_type IN (
        'market', 'limit', 'stop', 'stop_limit', 'trailing_stop'
    )),
    
    -- Quantities and prices
    quantity DECIMAL(18, 8) NOT NULL,
    filled_quantity DECIMAL(18, 8) DEFAULT 0,
    limit_price DECIMAL(18, 8),
    stop_price DECIMAL(18, 8),
    avg_fill_price DECIMAL(18, 8),
    
    -- Execution details
    time_in_force VARCHAR(8) DEFAULT 'DAY' CHECK (time_in_force IN ('DAY', 'GTC', 'IOC', 'FOK', 'OPG', 'CLO')),
    algo_type VARCHAR(16),  -- VWAP, TWAP, POV, etc.
    
    -- Status
    status VARCHAR(16) DEFAULT 'pending' CHECK (status IN (
        'pending', 'submitted', 'accepted', 'partial', 'filled', 
        'cancelled', 'rejected', 'expired'
    )),
    
    -- Timestamps
    submitted_at TIMESTAMPTZ,
    filled_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    
    -- Costs
    commission DECIMAL(12, 4),
    fees DECIMAL(12, 4),
    
    PRIMARY KEY (time, id)
);

SELECT create_hypertable('trading.orders', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX idx_orders_client_id ON trading.orders(client_order_id);
CREATE INDEX idx_orders_broker_id ON trading.orders(broker_order_id);
CREATE INDEX idx_orders_strategy ON trading.orders(strategy_id, time DESC);
CREATE INDEX idx_orders_status ON trading.orders(status) WHERE status IN ('pending', 'submitted', 'accepted', 'partial');

-- Fills/Executions table
CREATE TABLE IF NOT EXISTS trading.fills (
    id BIGSERIAL,
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    order_id BIGINT NOT NULL,
    execution_id VARCHAR(64),
    
    symbol VARCHAR(32) NOT NULL,
    side VARCHAR(8) NOT NULL,
    quantity DECIMAL(18, 8) NOT NULL,
    price DECIMAL(18, 8) NOT NULL,
    
    -- Costs
    commission DECIMAL(12, 4),
    exchange_fee DECIMAL(12, 4),
    
    -- Exchange details
    exchange VARCHAR(16),
    liquidity VARCHAR(8) CHECK (liquidity IN ('maker', 'taker')),
    
    PRIMARY KEY (time, id)
);

SELECT create_hypertable('trading.fills', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- =============================================================================
-- ANALYTICS TABLES
-- =============================================================================

-- Strategy performance metrics (daily)
CREATE TABLE IF NOT EXISTS analytics.strategy_performance (
    time TIMESTAMPTZ NOT NULL,
    strategy_id INTEGER NOT NULL REFERENCES trading.strategies(id),
    
    -- Returns
    daily_return DECIMAL(12, 8),
    cumulative_return DECIMAL(12, 8),
    
    -- Risk metrics
    volatility_20d DECIMAL(12, 8),
    sharpe_ratio_20d DECIMAL(12, 8),
    sortino_ratio_20d DECIMAL(12, 8),
    max_drawdown DECIMAL(12, 8),
    current_drawdown DECIMAL(12, 8),
    
    -- Portfolio metrics
    nav DECIMAL(18, 4),
    gross_exposure DECIMAL(18, 4),
    net_exposure DECIMAL(18, 4),
    leverage DECIMAL(8, 4),
    
    -- Trading activity
    trade_count INTEGER,
    win_rate DECIMAL(5, 4),
    avg_win DECIMAL(12, 8),
    avg_loss DECIMAL(12, 8),
    profit_factor DECIMAL(8, 4),
    
    -- Costs
    total_commission DECIMAL(12, 4),
    total_slippage DECIMAL(12, 4),
    
    PRIMARY KEY (time, strategy_id)
);

SELECT create_hypertable('analytics.strategy_performance', 'time',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- Risk metrics table
CREATE TABLE IF NOT EXISTS analytics.risk_metrics (
    time TIMESTAMPTZ NOT NULL,
    strategy_id INTEGER REFERENCES trading.strategies(id),
    
    -- VaR metrics
    var_95 DECIMAL(12, 8),
    var_99 DECIMAL(12, 8),
    expected_shortfall_95 DECIMAL(12, 8),
    expected_shortfall_99 DECIMAL(12, 8),
    
    -- Exposure metrics
    beta DECIMAL(8, 4),
    correlation_spy DECIMAL(8, 4),
    
    -- Concentration metrics
    herfindahl_index DECIMAL(8, 4),
    max_position_pct DECIMAL(8, 4),
    
    -- Status
    breach BOOLEAN DEFAULT FALSE,
    breach_type VARCHAR(32),
    severity VARCHAR(16) CHECK (severity IN ('info', 'warning', 'critical')),
    
    PRIMARY KEY (time, strategy_id)
);

SELECT create_hypertable('analytics.risk_metrics', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- =============================================================================
-- MONITORING TABLES
-- =============================================================================

-- System health metrics
CREATE TABLE IF NOT EXISTS monitoring.system_health (
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    component VARCHAR(32) NOT NULL,
    
    status VARCHAR(16) CHECK (status IN ('healthy', 'degraded', 'unhealthy')),
    latency_ms DECIMAL(12, 4),
    cpu_percent DECIMAL(5, 2),
    memory_percent DECIMAL(5, 2),
    disk_percent DECIMAL(5, 2),
    
    -- Counts
    active_connections INTEGER,
    queue_depth INTEGER,
    error_count INTEGER,
    
    details JSONB,
    
    PRIMARY KEY (time, component)
);

SELECT create_hypertable('monitoring.system_health', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

SELECT add_retention_policy('monitoring.system_health', INTERVAL '30 days', if_not_exists => TRUE);

-- Data quality metrics
CREATE TABLE IF NOT EXISTS monitoring.data_quality (
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source VARCHAR(32) NOT NULL,
    symbol VARCHAR(32),
    
    -- Quality metrics
    completeness_score DECIMAL(5, 4),
    gap_count INTEGER,
    max_gap_seconds INTEGER,
    outlier_count INTEGER,
    stale_data BOOLEAN DEFAULT FALSE,
    
    details JSONB,
    
    PRIMARY KEY (time, source, symbol)
);

SELECT create_hypertable('monitoring.data_quality', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Alerts table
CREATE TABLE IF NOT EXISTS monitoring.alerts (
    id SERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    alert_type VARCHAR(32) NOT NULL,
    severity VARCHAR(16) CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    component VARCHAR(32),
    strategy_id INTEGER REFERENCES trading.strategies(id),
    symbol VARCHAR(32),
    
    title VARCHAR(255) NOT NULL,
    message TEXT,
    details JSONB,
    
    -- Status
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_by VARCHAR(64),
    acknowledged_at TIMESTAMPTZ,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMPTZ
);

CREATE INDEX idx_alerts_time ON monitoring.alerts(time DESC);
CREATE INDEX idx_alerts_unresolved ON monitoring.alerts(resolved, severity) WHERE resolved = FALSE;

-- =============================================================================
-- CONTINUOUS AGGREGATES
-- =============================================================================

-- 5-minute OHLCV from 1-minute
CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.ohlcv_5min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time) AS time,
    symbol,
    first(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, time) AS close,
    sum(volume) AS volume,
    sum(volume * vwap) / NULLIF(sum(volume), 0) AS vwap,
    sum(trades) AS trades
FROM market_data.ohlcv_1min
GROUP BY time_bucket('5 minutes', time), symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('market_data.ohlcv_5min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

-- Hourly OHLCV from 1-minute
CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.ohlcv_1hour
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS time,
    symbol,
    first(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, time) AS close,
    sum(volume) AS volume,
    sum(volume * vwap) / NULLIF(sum(volume), 0) AS vwap,
    sum(trades) AS trades
FROM market_data.ohlcv_1min
GROUP BY time_bucket('1 hour', time), symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('market_data.ohlcv_1hour',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- =============================================================================
-- FUNCTIONS AND TRIGGERS
-- =============================================================================

-- Update timestamp trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply to relevant tables
CREATE TRIGGER update_assets_updated_at
    BEFORE UPDATE ON reference.assets
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_strategies_updated_at
    BEFORE UPDATE ON trading.strategies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_positions_updated_at
    BEFORE UPDATE ON trading.positions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Function to calculate position P&L
CREATE OR REPLACE FUNCTION trading.calculate_position_pnl(
    p_position_id INTEGER,
    p_current_price DECIMAL(18, 8)
)
RETURNS TABLE(unrealized_pnl DECIMAL(18, 8), pnl_pct DECIMAL(12, 8)) AS $$
DECLARE
    v_side VARCHAR(8);
    v_quantity DECIMAL(18, 8);
    v_entry_price DECIMAL(18, 8);
BEGIN
    SELECT side, quantity, entry_price
    INTO v_side, v_quantity, v_entry_price
    FROM trading.positions
    WHERE id = p_position_id;
    
    IF v_side = 'long' THEN
        unrealized_pnl := (p_current_price - v_entry_price) * v_quantity;
    ELSE
        unrealized_pnl := (v_entry_price - p_current_price) * v_quantity;
    END IF;
    
    pnl_pct := unrealized_pnl / (v_entry_price * v_quantity);
    
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- INITIAL DATA
-- =============================================================================

-- Insert default exchanges
INSERT INTO reference.exchanges (code, name, country, timezone, currency, open_time, close_time)
VALUES 
    ('NYSE', 'New York Stock Exchange', 'USA', 'America/New_York', 'USD', '09:30', '16:00'),
    ('NASDAQ', 'NASDAQ', 'USA', 'America/New_York', 'USD', '09:30', '16:00'),
    ('LSE', 'London Stock Exchange', 'UK', 'Europe/London', 'GBP', '08:00', '16:30'),
    ('TSE', 'Tokyo Stock Exchange', 'Japan', 'Asia/Tokyo', 'JPY', '09:00', '15:00'),
    ('BINANCE', 'Binance', 'Global', 'UTC', 'USD', '00:00', '00:00'),
    ('COINBASE', 'Coinbase', 'USA', 'UTC', 'USD', '00:00', '00:00')
ON CONFLICT (code) DO NOTHING;

-- Grant permissions
GRANT USAGE ON SCHEMA market_data, reference, trading, analytics, monitoring TO quant;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA market_data, reference, trading, analytics, monitoring TO quant;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA market_data, reference, trading, analytics, monitoring TO quant;

-- =============================================================================
-- VERIFICATION
-- =============================================================================

-- Verify hypertables created
SELECT hypertable_name, num_chunks, compression_enabled
FROM timescaledb_information.hypertables
ORDER BY hypertable_name;

-- Show compression status
SELECT * FROM timescaledb_information.compression_settings;

-- Show continuous aggregates
SELECT view_name, materialization_hypertable_name
FROM timescaledb_information.continuous_aggregates;