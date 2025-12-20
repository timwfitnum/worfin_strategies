"""
Configuration Management for Quantitative Trading System
Using Pydantic for validation and environment-specific settings
"""

import os
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import (
    BaseModel,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict


# =============================================================================
# Enums
# =============================================================================


class Environment(str, Enum):
    """Deployment environment"""

    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TESTING = "testing"


class LogLevel(str, Enum):
    """Logging levels"""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class BrokerType(str, Enum):
    """Supported brokers"""

    INTERACTIVE_BROKERS = "interactive_brokers"
    ALPACA = "alpaca"
    BINANCE = "binance"
    COINBASE = "coinbase"


class DatabaseType(str, Enum):
    """Database types"""

    CLICKHOUSE = "clickhouse"
    TIMESCALEDB = "timescaledb"
    REDIS = "redis"


# =============================================================================
# Database Configurations
# =============================================================================


class ClickHouseSettings(BaseModel):
    """ClickHouse database settings"""

    host: str = Field(default="localhost", description="ClickHouse host")
    port: int = Field(default=9000, ge=1, le=65535, description="Native protocol port")
    http_port: int = Field(default=8123, ge=1, le=65535, description="HTTP interface port")
    user: str = Field(default="default", description="Database user")
    password: SecretStr = Field(default=SecretStr(""), description="Database password")
    database: str = Field(default="quant_analytics", description="Default database")
    pool_size: int = Field(default=10, ge=1, le=100, description="Connection pool size")
    connect_timeout: int = Field(default=10, ge=1, description="Connection timeout in seconds")
    send_receive_timeout: int = Field(default=300, ge=1, description="Query timeout in seconds")
    compression: bool = Field(default=True, description="Enable compression")

    # Performance settings
    max_threads: int = Field(default=8, ge=1, description="Max threads per query")
    max_memory_usage: int = Field(
        default=10_000_000_000, description="Max memory per query (bytes)"
    )
    use_uncompressed_cache: bool = Field(default=True, description="Use uncompressed cache")

    @property
    def connection_string(self) -> str:
        """Generate connection string"""
        pwd = self.password.get_secret_value()
        return f"clickhouse://{self.user}:{pwd}@{self.host}:{self.port}/{self.database}"


class TimescaleDBSettings(BaseModel):
    """TimescaleDB (PostgreSQL) settings"""

    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, ge=1, le=65535, description="Database port")
    user: str = Field(default="quant", description="Database user")
    password: SecretStr = Field(default=SecretStr("quant123"), description="Database password")
    database: str = Field(default="quant_trading", description="Database name")
    pool_size: int = Field(default=20, ge=1, le=100, description="Connection pool size")
    max_overflow: int = Field(default=40, ge=0, description="Max overflow connections")
    pool_recycle: int = Field(default=3600, ge=0, description="Connection recycle time (seconds)")
    pool_pre_ping: bool = Field(default=True, description="Ping connections before use")
    echo: bool = Field(default=False, description="Echo SQL statements")

    # SSL settings
    ssl_mode: str = Field(default="prefer", description="SSL mode")
    ssl_cert: Optional[Path] = Field(default=None, description="SSL certificate path")

    @property
    def sync_url(self) -> str:
        """Synchronous connection URL"""
        pwd = self.password.get_secret_value()
        return f"postgresql://{self.user}:{pwd}@{self.host}:{self.port}/{self.database}"

    @property
    def async_url(self) -> str:
        """Asynchronous connection URL"""
        pwd = self.password.get_secret_value()
        return f"postgresql+asyncpg://{self.user}:{pwd}@{self.host}:{self.port}/{self.database}"


class RedisSettings(BaseModel):
    """Redis cache settings"""

    host: str = Field(default="localhost", description="Redis host")
    port: int = Field(default=6379, ge=1, le=65535, description="Redis port")
    password: Optional[SecretStr] = Field(default=None, description="Redis password")
    db: int = Field(default=0, ge=0, le=15, description="Redis database number")
    pool_size: int = Field(default=50, ge=1, le=500, description="Connection pool size")
    socket_timeout: int = Field(default=5, ge=1, description="Socket timeout (seconds)")
    socket_connect_timeout: int = Field(default=5, ge=1, description="Connect timeout (seconds)")
    retry_on_timeout: bool = Field(default=True, description="Retry on timeout")
    decode_responses: bool = Field(default=True, description="Decode responses to strings")

    # Streams settings (for real-time data pipeline)
    stream_max_len: int = Field(default=100_000, description="Max stream length")
    consumer_group: str = Field(default="quant_consumers", description="Consumer group name")

    @property
    def url(self) -> str:
        """Redis URL"""
        if self.password:
            pwd = self.password.get_secret_value()
            return f"redis://:{pwd}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


# =============================================================================
# Broker Configurations
# =============================================================================


class InteractiveBrokersSettings(BaseModel):
    """Interactive Brokers connection settings"""

    host: str = Field(default="127.0.0.1", description="IB Gateway/TWS host")
    port: int = Field(default=4002, description="IB Gateway port (4001=live, 4002=paper)")
    client_id: int = Field(default=1, ge=1, le=32, description="Client ID")
    timeout: int = Field(default=60, ge=1, description="Connection timeout (seconds)")
    readonly: bool = Field(default=False, description="Read-only mode")
    account: Optional[str] = Field(default=None, description="Account ID")

    # API settings
    max_requests_per_second: float = Field(default=45.0, description="Max API requests/second")
    max_messages_per_second: int = Field(default=50, description="Max messages/second")

    # Market data settings
    market_data_type: int = Field(
        default=1, ge=1, le=4, description="1=Live, 2=Frozen, 3=Delayed, 4=Delayed-Frozen"
    )

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        """Validate IB port"""
        valid_ports = {4001, 4002, 7496, 7497}  # Live/Paper for Gateway/TWS
        if v not in valid_ports:
            raise ValueError(f"Port must be one of {valid_ports}")
        return v


class PolygonSettings(BaseModel):
    """Polygon.io API settings"""

    api_key: SecretStr = Field(..., description="Polygon API key")
    base_url: str = Field(default="https://api.polygon.io", description="API base URL")
    ws_url: str = Field(default="wss://socket.polygon.io", description="WebSocket URL")

    # Rate limiting
    requests_per_minute: int = Field(default=5, description="Free tier: 5/min")
    max_retries: int = Field(default=3, description="Max retry attempts")
    retry_delay: float = Field(default=1.0, description="Retry delay (seconds)")

    # Data settings
    adjusted: bool = Field(default=True, description="Use adjusted prices")
    limit: int = Field(default=50000, description="Max results per request")


class BinanceSettings(BaseModel):
    """Binance exchange settings"""

    api_key: SecretStr = Field(..., description="Binance API key")
    api_secret: SecretStr = Field(..., description="Binance API secret")
    testnet: bool = Field(default=False, description="Use testnet")

    # URLs
    base_url: str = Field(default="https://api.binance.com", description="REST API URL")
    ws_url: str = Field(default="wss://stream.binance.com:9443", description="WebSocket URL")
    testnet_url: str = Field(
        default="https://testnet.binance.vision", description="Testnet REST URL"
    )
    testnet_ws_url: str = Field(
        default="wss://testnet.binance.vision", description="Testnet WebSocket URL"
    )

    # Rate limiting
    weight_limit: int = Field(default=1200, description="Weight limit per minute")
    order_limit: int = Field(default=10, description="Order limit per second")

    @property
    def effective_base_url(self) -> str:
        """Get effective base URL based on testnet setting"""
        return self.testnet_url if self.testnet else self.base_url

    @property
    def effective_ws_url(self) -> str:
        """Get effective WebSocket URL based on testnet setting"""
        return self.testnet_ws_url if self.testnet else self.ws_url


class CoinbaseSettings(BaseModel):
    """Coinbase exchange settings"""

    api_key: SecretStr = Field(..., description="Coinbase API key")
    api_secret: SecretStr = Field(..., description="Coinbase API secret")
    passphrase: SecretStr = Field(..., description="Coinbase API passphrase")
    sandbox: bool = Field(default=False, description="Use sandbox environment")

    # URLs
    base_url: str = Field(default="https://api.exchange.coinbase.com", description="REST API URL")
    ws_url: str = Field(default="wss://ws-feed.exchange.coinbase.com", description="WebSocket URL")
    sandbox_url: str = Field(
        default="https://api-public.sandbox.exchange.coinbase.com", description="Sandbox REST URL"
    )
    sandbox_ws_url: str = Field(
        default="wss://ws-feed-public.sandbox.exchange.coinbase.com",
        description="Sandbox WebSocket URL",
    )


class AlpacaSettings(BaseModel):
    """Alpaca broker settings"""

    api_key: SecretStr = Field(..., description="Alpaca API key")
    api_secret: SecretStr = Field(..., description="Alpaca API secret")
    paper: bool = Field(default=True, description="Use paper trading")

    # URLs
    base_url: str = Field(default="https://api.alpaca.markets", description="Live REST URL")
    paper_url: str = Field(default="https://paper-api.alpaca.markets", description="Paper REST URL")
    data_url: str = Field(default="https://data.alpaca.markets", description="Market data URL")
    stream_url: str = Field(default="wss://stream.data.alpaca.markets", description="Stream URL")

    @property
    def effective_base_url(self) -> str:
        """Get effective base URL based on paper setting"""
        return self.paper_url if self.paper else self.base_url


# =============================================================================
# Risk Management Settings
# =============================================================================


class RiskSettings(BaseModel):
    """Risk management configuration"""

    # Position limits
    max_position_pct: float = Field(
        default=0.02, ge=0.001, le=0.1, description="Max position size (% of capital)"
    )
    max_strategy_pct: float = Field(
        default=0.15, ge=0.01, le=0.5, description="Max allocation per strategy"
    )
    max_sector_pct: float = Field(default=0.25, ge=0.05, le=0.5, description="Max sector exposure")

    # Drawdown limits
    max_daily_drawdown_pct: float = Field(
        default=0.025, ge=0.005, le=0.1, description="Max daily drawdown"
    )
    max_portfolio_drawdown_pct: float = Field(
        default=0.20, ge=0.05, le=0.5, description="Max portfolio drawdown"
    )
    strategy_stop_drawdown_pct: float = Field(
        default=0.15, ge=0.05, le=0.3, description="Strategy stop loss drawdown"
    )

    # VaR settings
    var_confidence: float = Field(default=0.95, ge=0.9, le=0.999, description="VaR confidence")
    var_horizon_days: int = Field(default=1, ge=1, le=30, description="VaR horizon")
    max_daily_var_pct: float = Field(default=0.025, ge=0.01, le=0.1, description="Max daily VaR")

    # Correlation limits
    max_strategy_correlation: float = Field(
        default=0.7, ge=0.3, le=1.0, description="Max correlation between strategies"
    )

    # Kelly criterion
    kelly_fraction: float = Field(
        default=0.25, ge=0.1, le=0.5, description="Fractional Kelly multiplier"
    )

    # Kill switch thresholds
    kill_switch_drawdown_pct: float = Field(
        default=0.15, ge=0.05, le=0.3, description="Auto kill switch threshold"
    )
    kill_switch_var_breach_count: int = Field(
        default=3, ge=1, le=10, description="VaR breaches before kill"
    )


# =============================================================================
# Execution Settings
# =============================================================================


class ExecutionSettings(BaseModel):
    """Order execution configuration"""

    # Slippage assumptions (basis points)
    slippage_large_cap_bps: float = Field(default=5.0, description="Large cap slippage (bps)")
    slippage_mid_cap_bps: float = Field(default=15.0, description="Mid cap slippage (bps)")
    slippage_small_cap_bps: float = Field(default=50.0, description="Small cap slippage (bps)")
    slippage_crypto_bps: float = Field(default=25.0, description="Crypto slippage (bps)")
    slippage_options_pct: float = Field(default=1.0, description="Options slippage (% of premium)")

    # Commission assumptions
    commission_per_share: float = Field(default=0.005, description="Per share commission")
    commission_minimum: float = Field(default=1.0, description="Minimum commission")
    commission_crypto_pct: float = Field(default=0.001, description="Crypto commission rate")

    # Execution algorithm settings
    default_algo: str = Field(default="VWAP", description="Default execution algorithm")
    twap_interval_seconds: int = Field(default=60, description="TWAP slice interval")
    vwap_participation_rate: float = Field(
        default=0.1, ge=0.01, le=0.5, description="VWAP participation rate"
    )
    pov_target_rate: float = Field(default=0.15, ge=0.01, le=0.5, description="POV target rate")

    # Order management
    max_order_age_seconds: int = Field(default=300, description="Max order age before cancel")
    partial_fill_threshold: float = Field(
        default=0.9, ge=0.5, le=1.0, description="Min fill ratio to accept"
    )


# =============================================================================
# Backtesting Settings
# =============================================================================


class BacktestSettings(BaseModel):
    """Backtesting configuration"""

    # Data settings
    default_start_date: str = Field(default="2020-01-01", description="Default backtest start")
    default_end_date: str = Field(default="2024-12-31", description="Default backtest end")

    # Validation
    use_cpcv: bool = Field(default=True, description="Use Combinatorial Purged CV")
    cpcv_n_splits: int = Field(default=5, ge=3, le=20, description="CPCV number of splits")
    cpcv_purge_gap: int = Field(default=5, ge=0, le=30, description="CPCV purge gap (days)")
    cpcv_embargo_pct: float = Field(
        default=0.01, ge=0.0, le=0.1, description="CPCV embargo percentage"
    )

    # Walk-forward
    walk_forward_train_pct: float = Field(
        default=0.75, ge=0.5, le=0.9, description="Walk-forward training %"
    )
    walk_forward_step_days: int = Field(
        default=30, ge=1, le=365, description="Walk-forward step size"
    )

    # Statistical validation
    min_sharpe_ratio: float = Field(default=1.0, ge=0.0, description="Minimum Sharpe ratio")
    min_deflated_sharpe: float = Field(
        default=0.95, ge=0.5, le=1.0, description="Minimum DSR confidence"
    )
    max_trials_for_dsr: int = Field(default=100, ge=1, description="Trials for DSR calculation")

    # Transaction costs
    include_transaction_costs: bool = Field(default=True, description="Include transaction costs")
    cost_buffer_multiplier: float = Field(
        default=2.0, ge=1.0, le=5.0, description="Cost buffer multiplier"
    )


# =============================================================================
# Monitoring Settings
# =============================================================================


class MonitoringSettings(BaseModel):
    """Monitoring and alerting configuration"""

    # Prometheus
    prometheus_enabled: bool = Field(default=True, description="Enable Prometheus metrics")
    prometheus_port: int = Field(default=9090, description="Prometheus port")
    metrics_prefix: str = Field(default="quant_", description="Metrics prefix")

    # Grafana
    grafana_enabled: bool = Field(default=True, description="Enable Grafana")
    grafana_port: int = Field(default=3000, description="Grafana port")
    grafana_admin_user: str = Field(default="admin", description="Grafana admin user")
    grafana_admin_password: SecretStr = Field(
        default=SecretStr("admin123"), description="Grafana admin password"
    )

    # Alerting
    alerting_enabled: bool = Field(default=True, description="Enable alerting")
    alert_email: Optional[str] = Field(default=None, description="Alert email address")
    slack_webhook_url: Optional[SecretStr] = Field(default=None, description="Slack webhook URL")

    # Alert thresholds
    alert_drawdown_warning_pct: float = Field(default=0.10, description="Drawdown warning")
    alert_drawdown_critical_pct: float = Field(default=0.15, description="Drawdown critical")
    alert_sharpe_degradation_pct: float = Field(default=0.30, description="Sharpe degradation")
    alert_data_gap_seconds: int = Field(default=60, description="Data gap threshold")
    alert_api_error_count: int = Field(default=5, description="API errors per minute")


# =============================================================================
# Main Settings Class
# =============================================================================


class Settings(BaseSettings):
    """
    Main application settings.
    Loads from environment variables with QUANT_ prefix.
    """

    model_config = SettingsConfigDict(
        env_prefix="QUANT_",
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
    )

    # Environment
    environment: Environment = Field(default=Environment.DEVELOPMENT)
    debug: bool = Field(default=False)
    log_level: LogLevel = Field(default=LogLevel.INFO)

    # Application
    app_name: str = Field(default="Quantitative Trading System")
    version: str = Field(default="1.0.0")
    secret_key: SecretStr = Field(default=SecretStr("change-me-in-production"))

    # Paths
    base_dir: Path = Field(default=Path(__file__).parent.parent.parent)
    data_dir: Path = Field(default=Path("data"))
    logs_dir: Path = Field(default=Path("logs"))
    models_dir: Path = Field(default=Path("models"))

    # API
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    api_workers: int = Field(default=4)
    api_reload: bool = Field(default=True)

    # Databases
    clickhouse: ClickHouseSettings = Field(default_factory=ClickHouseSettings)
    timescaledb: TimescaleDBSettings = Field(default_factory=TimescaleDBSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)

    # Brokers (optional - set via environment)
    interactive_brokers: Optional[InteractiveBrokersSettings] = Field(default=None)
    polygon: Optional[PolygonSettings] = Field(default=None)
    binance: Optional[BinanceSettings] = Field(default=None)
    coinbase: Optional[CoinbaseSettings] = Field(default=None)
    alpaca: Optional[AlpacaSettings] = Field(default=None)

    # Trading configuration
    initial_capital: float = Field(default=100_000.0, ge=10_000)
    paper_trading: bool = Field(default=True)
    trading_enabled: bool = Field(default=False)

    # Component settings
    risk: RiskSettings = Field(default_factory=RiskSettings)
    execution: ExecutionSettings = Field(default_factory=ExecutionSettings)
    backtest: BacktestSettings = Field(default_factory=BacktestSettings)
    monitoring: MonitoringSettings = Field(default_factory=MonitoringSettings)

    @model_validator(mode="after")
    def validate_production_settings(self) -> "Settings":
        """Validate critical settings for production"""
        if self.environment == Environment.PRODUCTION:
            # Ensure proper secret key
            if self.secret_key.get_secret_value() == "change-me-in-production":
                raise ValueError("Must set SECRET_KEY in production")

            # Ensure paper trading is disabled
            if self.paper_trading:
                raise ValueError("Paper trading should be disabled in production")

            # Ensure debug is off
            if self.debug:
                raise ValueError("Debug should be disabled in production")

        return self

    @property
    def is_production(self) -> bool:
        """Check if running in production"""
        return self.environment == Environment.PRODUCTION

    @property
    def is_development(self) -> bool:
        """Check if running in development"""
        return self.environment == Environment.DEVELOPMENT

    def get_database_urls(self) -> Dict[str, str]:
        """Get all database URLs"""
        return {
            "clickhouse": self.clickhouse.connection_string,
            "timescaledb_sync": self.timescaledb.sync_url,
            "timescaledb_async": self.timescaledb.async_url,
            "redis": self.redis.url,
        }


# =============================================================================
# Settings Factory
# =============================================================================


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.
    Uses environment variables and .env file.
    """
    return Settings()


def get_settings_for_environment(env: Environment) -> Settings:
    """Get settings for a specific environment"""
    os.environ["QUANT_ENVIRONMENT"] = env.value
    # Clear cache to reload with new environment
    get_settings.cache_clear()
    return get_settings()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Main settings
    "Settings",
    "get_settings",
    "get_settings_for_environment",
    # Enums
    "Environment",
    "LogLevel",
    "BrokerType",
    "DatabaseType",
    # Database settings
    "ClickHouseSettings",
    "TimescaleDBSettings",
    "RedisSettings",
    # Broker settings
    "InteractiveBrokersSettings",
    "PolygonSettings",
    "BinanceSettings",
    "CoinbaseSettings",
    "AlpacaSettings",
    # Component settings
    "RiskSettings",
    "ExecutionSettings",
    "BacktestSettings",
    "MonitoringSettings",
]
