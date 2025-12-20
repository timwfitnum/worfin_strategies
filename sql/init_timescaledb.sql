-- =============================================================================
-- TimescaleDB Schema for Quantitative Trading System
-- Handles positions, orders, reference data, and transactional operations
-- =============================================================================

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS trading;
CREATE SCHEMA IF NOT EXISTS reference;
CREATE SCHEMA IF NOT EXISTS risk;
CREATE SCHEMA IF NOT EXISTS performance;

-- =============================================================================
-- REFERENCE DATA TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Assets / Instruments Universe
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.assets (
    id                  SERIAL PRIMARY KEY,
    symbol              VARCHAR(50) NOT NULL UNIQUE,
    name                VARCHAR(255),
    asset_class         VARCHAR(50) NOT NULL,  -- equity, option, future, crypto, forex
    asset_subclass      VARCHAR(50),           -- stock, etf, index_option, perpetual, etc.
    
    -- Exchange info
    exchange            VARCHAR(50),
    primary_exchange    VARCHAR(50),
    
    -- Contract specifications
    currency            VARCHAR(10) DEFAULT 'USD',
    multiplier          DECIMAL(10, 4) DEFAULT 1,
    tick_size           DECIMAL(20, 10),
    lot_size            DECIMAL(20, 10) DEFAULT 1,
    
    -- Trading constraints
    min_order_size      DECIMAL(20, 10) DEFAULT 1,
    max_order_size      DECIMAL(20, 10),
    margin_requirement  DECIMAL(10, 4),
    
    -- Options/Futures specific
    underlying_symbol   VARCHAR(50),
    expiration_date     DATE,
    strike_price        DECIMAL(20, 4),
    option_type         VARCHAR(10),  -- call, put
    
    -- Status
    is_active           BOOLEAN DEFAULT TRUE,
    is_tradeable        BOOLEAN DEFAULT TRUE,
    
    -- Provider mappings
    ib_con_id           INTEGER,
    ib_symbol           VARCHAR(50),
    polygon_ticker      VARCHAR(50),
    binance_symbol      VARCHAR(50),
    coinbase_product_id VARCHAR(50),
    yahoo_symbol        VARCHAR(50),
    
    -- Metadata
    sector              VARCHAR(100),
    industry            VARCHAR(100),
    market_cap          DECIMAL(20, 2),
    
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_assets_symbol ON reference.assets(symbol);
CREATE INDEX idx_assets_class ON reference.assets(asset_class);
CREATE INDEX idx_assets_active ON reference.assets(is_active) WHERE is_active = TRUE;

-- -----------------------------------------------------------------------------
-- Corporate Actions
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.corporate_actions (
    id                  SERIAL PRIMARY KEY,
    symbol              VARCHAR(50) NOT NULL REFERENCES reference.assets(symbol),
    
    action_type         VARCHAR(50) NOT NULL,  -- split, dividend, spinoff, merger, etc.
    ex_date             DATE NOT NULL,
    record_date         DATE,
    payment_date        DATE,
    
    -- Split info
    split_from          DECIMAL(10, 4),
    split_to            DECIMAL(10, 4),
    
    -- Dividend info
    dividend_amount     DECIMAL(20, 6),
    dividend_type       VARCHAR(50),  -- cash, stock, special
    
    -- Merger/Spinoff info
    new_symbol          VARCHAR(50),
    conversion_ratio    DECIMAL(20, 10),
    cash_amount         DECIMAL(20, 6),
    
    -- Metadata
    announcement_date   DATE,
    description         TEXT,
    data_source         VARCHAR(50),
    
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_corp_actions_symbol ON reference.corporate_actions(symbol);
CREATE INDEX idx_corp_actions_date ON reference.corporate_actions(ex_date);

-- -----------------------------------------------------------------------------
-- Strategies
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.strategies (
    id                  SERIAL PRIMARY KEY,
    name                VARCHAR(100) NOT NULL UNIQUE,
    description         TEXT,
    strategy_type       VARCHAR(50),  -- momentum, mean_reversion, arbitrage, volatility, event
    
    -- Configuration
    config              JSONB DEFAULT '{}',
    parameters          JSONB DEFAULT '{}',
    
    -- Risk limits
    max_position_pct    DECIMAL(5, 4) DEFAULT 0.02,
    max_allocation_pct  DECIMAL(5, 4) DEFAULT 0.15,
    max_drawdown_pct    DECIMAL(5, 4) DEFAULT 0.15,
    
    -- Universe
    asset_classes       VARCHAR(50)[] DEFAULT ARRAY['equity'],
    symbol_universe     VARCHAR(50)[] DEFAULT '{}',
    
    -- Status
    is_active           BOOLEAN DEFAULT FALSE,
    is_paper_trading    BOOLEAN DEFAULT TRUE,
    
    -- Performance tracking
    inception_date      DATE,
    last_signal_at      TIMESTAMPTZ,
    last_trade_at       TIMESTAMPTZ,
    
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- TRADING TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Orders
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trading.orders (
    id                  SERIAL PRIMARY KEY,
    order_id            UUID DEFAULT gen_random_uuid() UNIQUE,
    client_order_id     VARCHAR(100),
    broker_order_id     VARCHAR(100),
    
    -- Timing
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    submitted_at        TIMESTAMPTZ,
    filled_at           TIMESTAMPTZ,
    cancelled_at        TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    
    -- Strategy and Symbol
    strategy_id         INTEGER REFERENCES reference.strategies(id),
    symbol              VARCHAR(50) NOT NULL REFERENCES reference.assets(symbol),
    
    -- Order details
    side                VARCHAR(10) NOT NULL,  -- buy, sell
    order_type          VARCHAR(20) NOT NULL,  -- market, limit, stop, stop_limit
    time_in_force       VARCHAR(10) DEFAULT 'DAY',  -- DAY, GTC, IOC, FOK
    
    -- Quantities
    quantity            DECIMAL(20, 8) NOT NULL,
    filled_quantity     DECIMAL(20, 8) DEFAULT 0,
    remaining_quantity  DECIMAL(20, 8),
    
    -- Prices
    limit_price         DECIMAL(20, 8),
    stop_price          DECIMAL(20, 8),
    avg_fill_price      DECIMAL(20, 8),
    
    -- Execution details
    broker              VARCHAR(50),
    exchange            VARCHAR(50),
    algo_type           VARCHAR(50),  -- VWAP, TWAP, POV, IS
    
    -- Status
    status              VARCHAR(20) DEFAULT 'pending',  -- pending, submitted, partial, filled, cancelled, rejected
    reject_reason       TEXT,
    
    -- Costs
    commission          DECIMAL(20, 8) DEFAULT 0,
    fees                DECIMAL(20, 8) DEFAULT 0,
    
    -- Benchmarks (for TCA)
    decision_price      DECIMAL(20, 8),  -- Price at signal generation
    arrival_price       DECIMAL(20, 8),  -- Price at order submission
    
    -- Metadata
    notes               TEXT,
    tags                VARCHAR(50)[] DEFAULT '{}'
);

-- Convert to hypertable for time-series optimization
SELECT create_hypertable('trading.orders', 'created_at', if_not_exists => TRUE);

CREATE INDEX idx_orders_strategy ON trading.orders(strategy_id);
CREATE INDEX idx_orders_symbol ON trading.orders(symbol);
CREATE INDEX idx_orders_status ON trading.orders(status);

-- -----------------------------------------------------------------------------
-- Positions
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trading.positions (
    id                  SERIAL PRIMARY KEY,
    position_id         UUID DEFAULT gen_random_uuid() UNIQUE,
    
    -- Strategy and Symbol
    strategy_id         INTEGER REFERENCES reference.strategies(id),
    symbol              VARCHAR(50) NOT NULL REFERENCES reference.assets(symbol),
    
    -- Position details
    side                VARCHAR(10) NOT NULL,  -- long, short
    quantity            DECIMAL(20, 8) NOT NULL,
    
    -- Cost basis
    avg_entry_price     DECIMAL(20, 8) NOT NULL,
    total_cost_basis    DECIMAL(20, 8) NOT NULL,
    
    -- Current valuation
    current_price       DECIMAL(20, 8),
    market_value        DECIMAL(20, 8),
    
    -- PnL
    unrealized_pnl      DECIMAL(20, 8) DEFAULT 0,
    unrealized_pnl_pct  DECIMAL(10, 6) DEFAULT 0,
    realized_pnl        DECIMAL(20, 8) DEFAULT 0,
    
    -- Risk metrics
    beta                DECIMAL(10, 6),
    delta               DECIMAL(10, 6),  -- For options
    gamma               DECIMAL(10, 6),
    vega                DECIMAL(10, 6),
    theta               DECIMAL(10, 6),
    
    -- Position management
    target_price        DECIMAL(20, 8),
    stop_loss_price     DECIMAL(20, 8),
    take_profit_price   DECIMAL(20, 8),
    
    -- Status
    status              VARCHAR(20) DEFAULT 'open',  -- open, closed, pending_close
    opened_at           TIMESTAMPTZ DEFAULT NOW(),
    closed_at           TIMESTAMPTZ,
    
    -- Metadata
    notes               TEXT,
    tags                VARCHAR(50)[] DEFAULT '{}',
    
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT unique_open_position UNIQUE (strategy_id, symbol, status)
);

CREATE INDEX idx_positions_strategy ON trading.positions(strategy_id);
CREATE INDEX idx_positions_symbol ON trading.positions(symbol);
CREATE INDEX idx_positions_status ON trading.positions(status) WHERE status = 'open';

-- -----------------------------------------------------------------------------
-- Trades (Fill History)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trading.trades (
    id                  SERIAL PRIMARY KEY,
    trade_id            UUID DEFAULT gen_random_uuid() UNIQUE,
    
    -- References
    order_id            UUID REFERENCES trading.orders(order_id),
    position_id         UUID REFERENCES trading.positions(position_id),
    strategy_id         INTEGER REFERENCES reference.strategies(id),
    
    -- Trade details
    symbol              VARCHAR(50) NOT NULL REFERENCES reference.assets(symbol),
    side                VARCHAR(10) NOT NULL,
    quantity            DECIMAL(20, 8) NOT NULL,
    price               DECIMAL(20, 8) NOT NULL,
    
    -- Value
    notional_value      DECIMAL(20, 8) NOT NULL,
    
    -- Costs
    commission          DECIMAL(20, 8) DEFAULT 0,
    fees                DECIMAL(20, 8) DEFAULT 0,
    
    -- Execution details
    broker              VARCHAR(50),
    exchange            VARCHAR(50),
    broker_trade_id     VARCHAR(100),
    
    -- Timing
    executed_at         TIMESTAMPTZ NOT NULL,
    settlement_date     DATE,
    
    -- PnL (for closing trades)
    realized_pnl        DECIMAL(20, 8),
    
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Convert to hypertable
SELECT create_hypertable('trading.trades', 'executed_at', if_not_exists => TRUE);

CREATE INDEX idx_trades_strategy ON trading.trades(strategy_id);
CREATE INDEX idx_trades_symbol ON trading.trades(symbol);
CREATE INDEX idx_trades_order ON trading.trades(order_id);

-- =============================================================================
-- SIGNALS TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS trading.signals (
    id                  SERIAL PRIMARY KEY,
    signal_id           UUID DEFAULT gen_random_uuid() UNIQUE,
    
    -- Timing
    timestamp           TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    
    -- Strategy and Symbol
    strategy_id         INTEGER REFERENCES reference.strategies(id),
    symbol              VARCHAR(50) NOT NULL REFERENCES reference.assets(symbol),
    
    -- Signal details
    signal_type         VARCHAR(10) NOT NULL,  -- buy, sell, hold
    strength            DECIMAL(5, 4) NOT NULL,  -- -1 to 1
    confidence          DECIMAL(5, 4),  -- 0 to 1
    
    -- Target
    target_price        DECIMAL(20, 8),
    stop_loss           DECIMAL(20, 8),
    take_profit         DECIMAL(20, 8),
    horizon_minutes     INTEGER,
    
    -- Features and reasoning
    features            JSONB DEFAULT '{}',
    reasoning           TEXT,
    model_version       VARCHAR(50),
    
    -- Outcome tracking
    was_traded          BOOLEAN DEFAULT FALSE,
    order_id            UUID REFERENCES trading.orders(order_id),
    entry_price         DECIMAL(20, 8),
    exit_price          DECIMAL(20, 8),
    actual_pnl          DECIMAL(20, 8),
    actual_pnl_pct      DECIMAL(10, 6)
);

-- Convert to hypertable
SELECT create_hypertable('trading.signals', 'timestamp', if_not_exists => TRUE);

CREATE INDEX idx_signals_strategy ON trading.signals(strategy_id);
CREATE INDEX idx_signals_symbol ON trading.signals(symbol);

-- =============================================================================
-- RISK TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Risk Metrics (Time-series)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS risk.metrics (
    timestamp           TIMESTAMPTZ NOT NULL,
    
    -- Scope
    strategy_id         INTEGER REFERENCES reference.strategies(id),
    level               VARCHAR(20) NOT NULL,  -- portfolio, strategy, position
    
    -- VaR and ES
    var_95              DECIMAL(20, 8),
    var_99              DECIMAL(20, 8),
    expected_shortfall  DECIMAL(20, 8),
    
    -- Exposure
    gross_exposure      DECIMAL(20, 8),
    net_exposure        DECIMAL(20, 8),
    leverage            DECIMAL(10, 4),
    
    -- Concentration
    largest_position_pct DECIMAL(10, 6),
    top_5_concentration  DECIMAL(10, 6),
    sector_concentration JSONB,
    
    -- Greeks (for options portfolios)
    portfolio_delta     DECIMAL(20, 8),
    portfolio_gamma     DECIMAL(20, 8),
    portfolio_vega      DECIMAL(20, 8),
    portfolio_theta     DECIMAL(20, 8),
    
    -- Correlations
    strategy_correlations JSONB,
    
    -- Factor exposures
    factor_exposures    JSONB,
    
    -- Drawdown
    current_drawdown    DECIMAL(10, 6),
    max_drawdown        DECIMAL(10, 6),
    
    -- Stress tests
    stress_scenarios    JSONB,
    
    -- Breach flags
    var_breach          BOOLEAN DEFAULT FALSE,
    drawdown_breach     BOOLEAN DEFAULT FALSE,
    concentration_breach BOOLEAN DEFAULT FALSE,
    
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Convert to hypertable
SELECT create_hypertable('risk.metrics', 'timestamp', if_not_exists => TRUE);

CREATE INDEX idx_risk_strategy ON risk.metrics(strategy_id);

-- -----------------------------------------------------------------------------
-- Risk Limits
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS risk.limits (
    id                  SERIAL PRIMARY KEY,
    
    -- Scope
    strategy_id         INTEGER REFERENCES reference.strategies(id),
    symbol              VARCHAR(50) REFERENCES reference.assets(symbol),
    level               VARCHAR(20) NOT NULL,  -- portfolio, strategy, symbol
    
    -- Limit definition
    metric              VARCHAR(50) NOT NULL,
    limit_type          VARCHAR(20) NOT NULL,  -- hard, soft, warning
    limit_value         DECIMAL(20, 8) NOT NULL,
    
    -- Status
    is_active           BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    description         TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT unique_limit UNIQUE (strategy_id, symbol, level, metric, limit_type)
);

-- -----------------------------------------------------------------------------
-- Risk Events / Breaches
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS risk.events (
    id                  SERIAL PRIMARY KEY,
    event_id            UUID DEFAULT gen_random_uuid() UNIQUE,
    
    timestamp           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Scope
    strategy_id         INTEGER REFERENCES reference.strategies(id),
    symbol              VARCHAR(50) REFERENCES reference.assets(symbol),
    
    -- Event details
    event_type          VARCHAR(50) NOT NULL,  -- limit_breach, warning, kill_switch, manual_override
    severity            VARCHAR(20) NOT NULL,  -- info, warning, critical
    
    metric              VARCHAR(50),
    current_value       DECIMAL(20, 8),
    limit_value         DECIMAL(20, 8),
    
    -- Response
    action_taken        VARCHAR(50),  -- none, alert, reduce, close_all
    resolved_at         TIMESTAMPTZ,
    resolved_by         VARCHAR(100),
    
    -- Details
    description         TEXT,
    details             JSONB,
    
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

SELECT create_hypertable('risk.events', 'timestamp', if_not_exists => TRUE);

-- =============================================================================
-- PERFORMANCE TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Daily Performance Snapshots
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS performance.daily_snapshots (
    date                DATE NOT NULL,
    strategy_id         INTEGER REFERENCES reference.strategies(id),
    
    -- Capital
    starting_capital    DECIMAL(20, 8),
    ending_capital      DECIMAL(20, 8),
    
    -- PnL
    gross_pnl           DECIMAL(20, 8),
    net_pnl             DECIMAL(20, 8),
    commission_paid     DECIMAL(20, 8),
    fees_paid           DECIMAL(20, 8),
    
    -- Returns
    gross_return        DECIMAL(10, 6),
    net_return          DECIMAL(10, 6),
    
    -- Cumulative
    cumulative_return   DECIMAL(10, 6),
    high_water_mark     DECIMAL(20, 8),
    
    -- Drawdown
    drawdown            DECIMAL(10, 6),
    drawdown_duration   INTEGER,  -- Days in drawdown
    
    -- Risk metrics
    daily_var_95        DECIMAL(10, 6),
    realized_vol        DECIMAL(10, 6),
    
    -- Trading activity
    trade_count         INTEGER DEFAULT 0,
    win_count           INTEGER DEFAULT 0,
    loss_count          INTEGER DEFAULT 0,
    
    -- Ratios (rolling calculations)
    sharpe_ratio_30d    DECIMAL(10, 6),
    sharpe_ratio_90d    DECIMAL(10, 6),
    sortino_ratio_30d   DECIMAL(10, 6),
    
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (date, strategy_id)
);

-- =============================================================================
-- FUNCTIONS AND TRIGGERS
-- =============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply to relevant tables
CREATE TRIGGER update_assets_updated_at
    BEFORE UPDATE ON reference.assets
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_strategies_updated_at
    BEFORE UPDATE ON reference.strategies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_positions_updated_at
    BEFORE UPDATE ON trading.positions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at
    BEFORE UPDATE ON trading.orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Function to calculate position PnL
CREATE OR REPLACE FUNCTION calculate_position_pnl()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.current_price IS NOT NULL THEN
        NEW.market_value = NEW.quantity * NEW.current_price;
        NEW.unrealized_pnl = NEW.market_value - NEW.total_cost_basis;
        NEW.unrealized_pnl_pct = NEW.unrealized_pnl / NEW.total_cost_basis;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER calculate_position_pnl_trigger
    BEFORE INSERT OR UPDATE OF current_price ON trading.positions
    FOR EACH ROW EXECUTE FUNCTION calculate_position_pnl();

-- =============================================================================
-- CONTINUOUS AGGREGATES (TimescaleDB specific)
-- =============================================================================

-- Hourly PnL summary
CREATE MATERIALIZED VIEW IF NOT EXISTS performance.hourly_pnl
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', executed_at) AS hour,
    strategy_id,
    symbol,
    COUNT(*) AS trade_count,
    SUM(quantity) AS total_quantity,
    SUM(notional_value) AS total_notional,
    SUM(realized_pnl) AS total_pnl,
    SUM(commission + fees) AS total_costs
FROM trading.trades
GROUP BY hour, strategy_id, symbol
WITH NO DATA;

-- Refresh policy
SELECT add_continuous_aggregate_policy('performance.hourly_pnl',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- =============================================================================
-- DATA RETENTION POLICIES
-- =============================================================================

-- Keep detailed order data for 2 years
SELECT add_retention_policy('trading.orders', INTERVAL '2 years', if_not_exists => TRUE);

-- Keep detailed trade data for 5 years
SELECT add_retention_policy('trading.trades', INTERVAL '5 years', if_not_exists => TRUE);

-- Keep signals for 1 year
SELECT add_retention_policy('trading.signals', INTERVAL '1 year', if_not_exists => TRUE);

-- Keep risk metrics for 2 years
SELECT add_retention_policy('risk.metrics', INTERVAL '2 years', if_not_exists => TRUE);

-- Keep risk events for 5 years
SELECT add_retention_policy('risk.events', INTERVAL '5 years', if_not_exists => TRUE);

-- =============================================================================
-- SEED DATA - Default Strategies
-- =============================================================================

INSERT INTO reference.strategies (name, description, strategy_type, asset_classes) VALUES
('ML_Momentum', 'ML-Enhanced Momentum Factor Strategy', 'momentum', ARRAY['equity']),
('Cross_Asset_Momentum', 'Cross-Asset Momentum with Risk Parity', 'momentum', ARRAY['equity', 'future', 'crypto']),
('Variance_Risk_Premium', 'Short Volatility via Iron Condors', 'volatility', ARRAY['option']),
('Smart_Beta_Rotation', 'Regime-Based Factor Allocation', 'momentum', ARRAY['equity']),
('Overnight_Premium', 'Systematic Close-to-Open Strategy', 'structural', ARRAY['equity']),
('Merger_Arbitrage', 'M&A Spread Trading', 'event', ARRAY['equity']),
('Sector_Rotation_Options', 'ETF Options Momentum', 'momentum', ARRAY['option']),
('Crypto_Stat_Arb', 'Crypto Pairs Trading', 'arbitrage', ARRAY['crypto']),
('ADR_Arbitrage', 'ADR Geographic Arbitrage', 'arbitrage', ARRAY['equity']),
('VIX_Calendar', 'VIX Term Structure Trading', 'volatility', ARRAY['future', 'option']),
('Earnings_Volatility', 'Earnings Vol Compression', 'volatility', ARRAY['option']),
('Crypto_Funding_Arb', 'Perpetual vs Spot Basis Trading', 'arbitrage', ARRAY['crypto'])
ON CONFLICT (name) DO NOTHING;

-- =============================================================================
-- COMMENTS
-- =============================================================================

COMMENT ON TABLE reference.assets IS 'Master table of all tradeable instruments';
COMMENT ON TABLE reference.strategies IS 'Trading strategy definitions and configurations';
COMMENT ON TABLE trading.orders IS 'Order lifecycle tracking with full audit trail';
COMMENT ON TABLE trading.positions IS 'Current and historical position tracking';
COMMENT ON TABLE trading.trades IS 'Execution fill history for TCA';
COMMENT ON TABLE trading.signals IS 'Strategy signals with outcome tracking';
COMMENT ON TABLE risk.metrics IS 'Time-series risk metrics for monitoring';
COMMENT ON TABLE risk.events IS 'Risk breach events and responses';
COMMENT ON TABLE performance.daily_snapshots IS 'Daily performance attribution and metrics';