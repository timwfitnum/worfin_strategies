"""
Pydantic Configuration Management for Quantitative Trading System.
Supports dev/staging/prod environments with validation and type safety.
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
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(str, Enum):
    """Deployment environment enumeration."""

    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TESTING = "testing"


class LogLevel(str, Enum):
    """Log level enumeration."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


# =============================================================================
# Database Configuration
# =============================================================================


class ClickHouseConfig(BaseModel):
    """ClickHouse database configuration."""

    host: str = "localhost"
    port: int = 9000
    http_port: int = 8123
    user: str = "default"
    password: SecretStr = SecretStr("")
    database: str = "quant_analytics"
    pool_size: int = Field(default=10, ge=1, le=100)
    max_threads: int = Field(default=8, ge=1, le=64)
    max_memory_usage: int = Field(default=10_000_000_000, description="Max memory in bytes")
    connect_timeout: int = Field(default=10, ge=1, le=60)
    send_receive_timeout: int = Field(default=300, ge=1, le=3600)
    compression: bool = True

    @property
    def connection_string(self) -> str:
        """Generate ClickHouse connection string."""
        return f"clickhouse://{self.user}:{self.password.get_secret_value()}@{self.host}:{self.port}/{self.database}"


class TimescaleDBConfig(BaseModel):
    """TimescaleDB/PostgreSQL configuration."""

    host: str = "localhost"
    port: int = 5432
    user: str = "quant"
    password: SecretStr = SecretStr("quant123")
    database: str = "quant_trading"
    pool_size: int = Field(default=20, ge=1, le=100)
    max_overflow: int = Field(default=40, ge=0, le=200)
    pool_recycle: int = Field(default=3600, ge=60, le=86400)
    pool_pre_ping: bool = True
    echo: bool = False
    ssl_mode: str = "prefer"

    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        return (
            f"postgresql://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def async_connection_string(self) -> str:
        """Generate async PostgreSQL connection string."""
        return (
            f"postgresql+asyncpg://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.database}"
        )


class RedisConfig(BaseModel):
    """Redis configuration."""

    host: str = "localhost"
    port: int = 6379
    password: Optional[SecretStr] = None
    db: int = Field(default=0, ge=0, le=15)
    pool_size: int = Field(default=50, ge=1, le=500)
    decode_responses: bool = True
    socket_timeout: float = Field(default=5.0, ge=0.1, le=60.0)
    socket_connect_timeout: float = Field(default=5.0, ge=0.1, le=60.0)
    retry_on_timeout: bool = True

    @property
    def connection_string(self) -> str:
        """Generate Redis connection string."""
        if self.password:
            return f"redis://:{self.password.get_secret_value()}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


class DatabaseConfig(BaseModel):
    """Unified database configuration."""

    clickhouse: ClickHouseConfig = Field(default_factory=ClickHouseConfig)
    timescale: TimescaleDBConfig = Field(default_factory=TimescaleDBConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)


# =============================================================================
# Broker Configuration
# =============================================================================


class InteractiveBrokersConfig(BaseModel):
    """Interactive Brokers configuration."""

    host: str = "127.0.0.1"
    port: int = Field(
        default=7497,
        description="7497=TWS Paper, 7496=TWS Live, 4002=Gateway Paper, 4001=Gateway Live",
    )
    client_id: int = Field(default=1, ge=0, le=32)
    account: Optional[str] = None
    readonly: bool = False
    timeout: int = Field(default=60, ge=1, le=300)
    max_rate_per_second: float = Field(default=45.0, description="IB API pacing limit")

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        valid_ports = [7497, 7496, 4002, 4001]
        if v not in valid_ports:
            raise ValueError(f"Port must be one of {valid_ports}")
        return v


class PolygonConfig(BaseModel):
    """Polygon.io configuration."""

    api_key: SecretStr = SecretStr("")
    base_url: str = "https://api.polygon.io"
    ws_url: str = "wss://socket.polygon.io"
    max_requests_per_minute: int = Field(default=5, ge=1, le=1000)
    timeout: int = Field(default=30, ge=1, le=120)
    retry_attempts: int = Field(default=3, ge=0, le=10)


class BinanceConfig(BaseModel):
    """Binance exchange configuration."""

    api_key: SecretStr = SecretStr("")
    api_secret: SecretStr = SecretStr("")
    testnet: bool = True
    base_url: str = "https://api.binance.com"
    testnet_url: str = "https://testnet.binance.vision"
    ws_url: str = "wss://stream.binance.com:9443/ws"
    testnet_ws_url: str = "wss://testnet.binance.vision/ws"
    recv_window: int = Field(default=5000, ge=1000, le=60000)

    @property
    def active_base_url(self) -> str:
        return self.testnet_url if self.testnet else self.base_url

    @property
    def active_ws_url(self) -> str:
        return self.testnet_ws_url if self.testnet else self.ws_url


class CoinbaseConfig(BaseModel):
    """Coinbase exchange configuration."""

    api_key: SecretStr = SecretStr("")
    api_secret: SecretStr = SecretStr("")
    passphrase: SecretStr = SecretStr("")
    sandbox: bool = True
    base_url: str = "https://api.exchange.coinbase.com"
    sandbox_url: str = "https://api-public.sandbox.exchange.coinbase.com"
    ws_url: str = "wss://ws-feed.exchange.coinbase.com"
    sandbox_ws_url: str = "wss://ws-feed-public.sandbox.exchange.coinbase.com"

    @property
    def active_base_url(self) -> str:
        return self.sandbox_url if self.sandbox else self.base_url

    @property
    def active_ws_url(self) -> str:
        return self.sandbox_ws_url if self.sandbox else self.ws_url


class AlpacaConfig(BaseModel):
    """Alpaca Markets configuration."""

    api_key: SecretStr = SecretStr("")
    api_secret: SecretStr = SecretStr("")
    paper: bool = True
    base_url: str = "https://api.alpaca.markets"
    paper_url: str = "https://paper-api.alpaca.markets"
    data_url: str = "https://data.alpaca.markets"

    @property
    def active_base_url(self) -> str:
        return self.paper_url if self.paper else self.base_url


class BrokerConfig(BaseModel):
    """Unified broker configuration."""

    interactive_brokers: InteractiveBrokersConfig = Field(default_factory=InteractiveBrokersConfig)
    polygon: PolygonConfig = Field(default_factory=PolygonConfig)
    binance: BinanceConfig = Field(default_factory=BinanceConfig)
    coinbase: CoinbaseConfig = Field(default_factory=CoinbaseConfig)
    alpaca: AlpacaConfig = Field(default_factory=AlpacaConfig)


# =============================================================================
# Risk Management Configuration
# =============================================================================


class RiskConfig(BaseModel):
    """Risk management configuration."""

    # Position limits
    max_position_size_pct: float = Field(
        default=0.02, ge=0.001, le=0.1, description="Max 2% per position"
    )
    max_strategy_allocation_pct: float = Field(
        default=0.15, ge=0.01, le=0.5, description="Max 15% per strategy"
    )
    max_sector_allocation_pct: float = Field(
        default=0.25, ge=0.05, le=0.5, description="Max 25% per sector"
    )

    # Drawdown limits
    max_portfolio_drawdown_pct: float = Field(
        default=0.20, ge=0.05, le=0.5, description="Max 20% drawdown"
    )
    max_strategy_drawdown_pct: float = Field(
        default=0.15, ge=0.05, le=0.3, description="Max 15% strategy drawdown"
    )

    # VaR/Risk metrics
    var_confidence_level: float = Field(default=0.95, ge=0.9, le=0.99)
    daily_var_limit_pct: float = Field(
        default=0.025, ge=0.01, le=0.1, description="2.5% daily VaR limit"
    )

    # Correlation limits
    max_strategy_correlation: float = Field(default=0.7, ge=0.3, le=0.9)

    # Kelly criterion
    kelly_fraction: float = Field(default=0.25, ge=0.1, le=0.5, description="Use 1/4 Kelly")

    # Kill switches
    enable_kill_switch: bool = True
    kill_switch_loss_threshold_pct: float = Field(default=0.10, ge=0.01, le=0.3)


# =============================================================================
# Execution Configuration
# =============================================================================


class ExecutionConfig(BaseModel):
    """Execution and order management configuration."""

    # Slippage and costs
    default_slippage_bps: float = Field(default=5.0, ge=0, le=100)
    commission_per_share: float = Field(default=0.005, ge=0, le=0.1)
    min_commission: float = Field(default=1.0, ge=0, le=10)

    # Execution algorithms
    default_algo: str = Field(default="VWAP", pattern="^(VWAP|TWAP|POV|IS|MARKET|LIMIT)$")
    algo_participation_rate: float = Field(default=0.1, ge=0.01, le=0.5)

    # Order management
    max_order_retry_attempts: int = Field(default=3, ge=0, le=10)
    order_timeout_seconds: int = Field(default=60, ge=5, le=300)

    # Latency requirements
    max_execution_latency_ms: int = Field(default=100, ge=10, le=1000)


# =============================================================================
# Monitoring Configuration
# =============================================================================


class MonitoringConfig(BaseModel):
    """Monitoring and alerting configuration."""

    # Prometheus
    prometheus_enabled: bool = True
    prometheus_port: int = Field(default=9090, ge=1024, le=65535)

    # Grafana
    grafana_enabled: bool = True
    grafana_port: int = Field(default=3000, ge=1024, le=65535)
    grafana_user: str = "admin"
    grafana_password: SecretStr = SecretStr("admin123")

    # Alerting
    alert_email: Optional[str] = None
    slack_webhook_url: Optional[SecretStr] = None
    pagerduty_key: Optional[SecretStr] = None

    # Metrics
    metrics_retention_days: int = Field(default=30, ge=1, le=365)
    health_check_interval_seconds: int = Field(default=30, ge=5, le=300)


# =============================================================================
# Main Settings Class
# =============================================================================


class Settings(BaseSettings):
    """
    Main application settings with environment variable support.

    Environment variables are prefixed with QUANT_ and use double underscores
    for nested settings (e.g., QUANT_DATABASE__CLICKHOUSE__HOST).
    """

    model_config = SettingsConfigDict(
        env_prefix="QUANT_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Core settings
    environment: Environment = Environment.DEVELOPMENT
    app_name: str = "quant-trading-system"
    version: str = "1.0.0"
    debug: bool = False
    log_level: LogLevel = LogLevel.INFO

    # Paths
    base_dir: Path = Field(default_factory=lambda: Path(__file__).parent.parent.parent)
    data_dir: Path = Field(default_factory=lambda: Path("data"))
    logs_dir: Path = Field(default_factory=lambda: Path("logs"))
    cache_dir: Path = Field(default_factory=lambda: Path("cache"))

    # API settings
    api_host: str = "0.0.0.0"
    api_port: int = Field(default=8000, ge=1024, le=65535)
    api_workers: int = Field(default=4, ge=1, le=32)

    # Component configurations
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    brokers: BrokerConfig = Field(default_factory=BrokerConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)

    # Feature flags
    enable_paper_trading: bool = True
    enable_live_trading: bool = False
    enable_backtesting: bool = True
    enable_ml_models: bool = True

    @model_validator(mode="after")
    def validate_trading_modes(self) -> "Settings":
        """Ensure live trading is only enabled in production with explicit flag."""
        if self.enable_live_trading and self.environment != Environment.PRODUCTION:
            raise ValueError("Live trading can only be enabled in production environment")
        if self.enable_live_trading and self.enable_paper_trading:
            raise ValueError("Cannot enable both live and paper trading simultaneously")
        return self

    @field_validator("data_dir", "logs_dir", "cache_dir", mode="after")
    @classmethod
    def ensure_dir_exists(cls, v: Path) -> Path:
        """Ensure directories exist."""
        v.mkdir(parents=True, exist_ok=True)
        return v

    def get_log_config(self) -> Dict[str, Any]:
        """Generate logging configuration dictionary."""
        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
                "json": {
                    "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
                    "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": self.log_level.value,
                    "formatter": "standard",
                    "stream": "ext://sys.stdout",
                },
                "file": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "level": self.log_level.value,
                    "formatter": "json",
                    "filename": str(self.logs_dir / "quant_system.log"),
                    "maxBytes": 100 * 1024 * 1024,  # 100MB
                    "backupCount": 10,
                },
            },
            "loggers": {
                "": {
                    "level": self.log_level.value,
                    "handlers": ["console", "file"],
                    "propagate": False,
                },
                "uvicorn": {
                    "level": "INFO",
                    "handlers": ["console"],
                    "propagate": False,
                },
            },
        }


# =============================================================================
# Settings Factory
# =============================================================================


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Uses LRU cache to ensure singleton pattern.
    Call get_settings.cache_clear() to reload settings.
    """
    return Settings()


def get_settings_for_env(env: Environment) -> Settings:
    """
    Get settings for a specific environment.

    Useful for testing or explicit environment switching.
    """
    return Settings(environment=env)


# =============================================================================
# Environment-specific presets
# =============================================================================


class DevelopmentSettings(Settings):
    """Development environment preset."""

    environment: Environment = Environment.DEVELOPMENT
    debug: bool = True
    log_level: LogLevel = LogLevel.DEBUG
    enable_paper_trading: bool = True
    enable_live_trading: bool = False


class StagingSettings(Settings):
    """Staging environment preset."""

    environment: Environment = Environment.STAGING
    debug: bool = False
    log_level: LogLevel = LogLevel.INFO
    enable_paper_trading: bool = True
    enable_live_trading: bool = False


class ProductionSettings(Settings):
    """Production environment preset."""

    environment: Environment = Environment.PRODUCTION
    debug: bool = False
    log_level: LogLevel = LogLevel.WARNING
    enable_paper_trading: bool = False
    enable_live_trading: bool = True


# Export main components
__all__ = [
    "Settings",
    "get_settings",
    "get_settings_for_env",
    "Environment",
    "LogLevel",
    "DatabaseConfig",
    "BrokerConfig",
    "RiskConfig",
    "ExecutionConfig",
    "MonitoringConfig",
    "ClickHouseConfig",
    "TimescaleDBConfig",
    "RedisConfig",
    "InteractiveBrokersConfig",
    "PolygonConfig",
    "BinanceConfig",
    "CoinbaseConfig",
    "AlpacaConfig",
    "DevelopmentSettings",
    "StagingSettings",
    "ProductionSettings",
]
