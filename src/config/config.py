"""
Configuration management using Pydantic for type safety and validation.
Supports multiple environments: development, staging, production.
"""

from typing import Optional, Dict, Any, List
from pathlib import Path
from functools import lru_cache
from datetime import timedelta

from pydantic import BaseSettings, Field, validator, SecretStr, AnyHttpUrl
from pydantic.networks import PostgresDsn, RedisDsn, HttpUrl
import yaml


class DatabaseConfig(BaseSettings):
    """Database configuration."""

    # PostgreSQL/TimescaleDB
    postgres_host: str = Field("localhost", env="POSTGRES_HOST")
    postgres_port: int = Field(5432, env="POSTGRES_PORT")
    postgres_user: str = Field("quant", env="POSTGRES_USER")
    postgres_password: SecretStr = Field("quant123", env="POSTGRES_PASSWORD")
    postgres_db: str = Field("quant_trading", env="POSTGRES_DB")
    postgres_pool_size: int = Field(20, env="POSTGRES_POOL_SIZE")
    postgres_max_overflow: int = Field(40, env="POSTGRES_MAX_OVERFLOW")
    postgres_echo: bool = Field(False, env="POSTGRES_ECHO")

    # ClickHouse
    clickhouse_host: str = Field("localhost", env="CLICKHOUSE_HOST")
    clickhouse_port: int = Field(9000, env="CLICKHOUSE_PORT")
    clickhouse_user: str = Field("quant", env="CLICKHOUSE_USER")
    clickhouse_password: SecretStr = Field("quant123", env="CLICKHOUSE_PASSWORD")
    clickhouse_database: str = Field("quant_analytics", env="CLICKHOUSE_DATABASE")

    # Redis
    redis_host: str = Field("localhost", env="REDIS_HOST")
    redis_port: int = Field(6379, env="REDIS_PORT")
    redis_password: Optional[SecretStr] = Field(None, env="REDIS_PASSWORD")
    redis_db: int = Field(0, env="REDIS_DB")
    redis_pool_size: int = Field(50, env="REDIS_POOL_SIZE")

    @property
    def postgres_url(self) -> str:
        """Generate PostgreSQL connection URL."""
        password = self.postgres_password.get_secret_value()
        return f"postgresql://{self.postgres_user}:{password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def redis_url(self) -> str:
        """Generate Redis connection URL."""
        if self.redis_password:
            password = self.redis_password.get_secret_value()
            return f"redis://:{password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    class Config:
        env_prefix = "DB_"
        case_sensitive = False


class BrokerConfig(BaseSettings):
    """Broker configuration for different trading venues."""

    # Interactive Brokers
    ib_host: str = Field("127.0.0.1", env="IB_HOST")
    ib_port: int = Field(7497, env="IB_PORT")  # 7497 for paper, 7496 for live
    ib_client_id: int = Field(1, env="IB_CLIENT_ID")
    ib_account: Optional[str] = Field(None, env="IB_ACCOUNT")

    # Alpaca
    alpaca_api_key: Optional[SecretStr] = Field(None, env="ALPACA_API_KEY")
    alpaca_secret_key: Optional[SecretStr] = Field(None, env="ALPACA_SECRET_KEY")
    alpaca_base_url: HttpUrl = Field("https://paper-api.alpaca.markets", env="ALPACA_BASE_URL")

    # Binance
    binance_api_key: Optional[SecretStr] = Field(None, env="BINANCE_API_KEY")
    binance_secret_key: Optional[SecretStr] = Field(None, env="BINANCE_SECRET_KEY")
    binance_testnet: bool = Field(True, env="BINANCE_TESTNET")

    # Polygon.io
    polygon_api_key: Optional[SecretStr] = Field(None, env="POLYGON_API_KEY")

    class Config:
        env_prefix = "BROKER_"
        case_sensitive = False


class TradingConfig(BaseSettings):
    """Trading system configuration."""

    # Risk Management
    max_position_size: float = Field(0.02, env="MAX_POSITION_SIZE")  # 2% per position
    max_portfolio_risk: float = Field(0.06, env="MAX_PORTFOLIO_RISK")  # 6% total risk
    max_correlation: float = Field(0.7, env="MAX_CORRELATION")
    max_drawdown: float = Field(0.20, env="MAX_DRAWDOWN")  # 20% max drawdown
    daily_var_limit: float = Field(0.025, env="DAILY_VAR_LIMIT")  # 2.5% daily VaR

    # Position Sizing
    kelly_fraction: float = Field(0.25, env="KELLY_FRACTION")  # 1/4 Kelly
    min_position_size: float = Field(1000.0, env="MIN_POSITION_SIZE")  # $1000 minimum
    max_position_value: float = Field(100000.0, env="MAX_POSITION_VALUE")  # $100k maximum

    # Execution
    slippage_model: str = Field("fixed", env="SLIPPAGE_MODEL")  # fixed, linear, square_root
    default_slippage: float = Field(0.001, env="DEFAULT_SLIPPAGE")  # 10 bps
    market_impact_const: float = Field(0.1, env="MARKET_IMPACT_CONST")

    # Strategy Limits
    max_strategies: int = Field(12, env="MAX_STRATEGIES")
    strategy_timeout: int = Field(300, env="STRATEGY_TIMEOUT")  # 5 minutes

    class Config:
        env_prefix = "TRADING_"
        case_sensitive = False


class BacktestConfig(BaseSettings):
    """Backtesting configuration."""

    initial_capital: float = Field(100000.0, env="BACKTEST_INITIAL_CAPITAL")
    commission_rate: float = Field(0.001, env="BACKTEST_COMMISSION_RATE")
    slippage_rate: float = Field(0.001, env="BACKTEST_SLIPPAGE_RATE")

    # Validation
    walk_forward_splits: int = Field(5, env="WALK_FORWARD_SPLITS")
    train_test_ratio: float = Field(0.7, env="TRAIN_TEST_RATIO")
    min_samples: int = Field(252, env="MIN_SAMPLES")  # 1 year of daily data

    # CPCV Parameters
    cpcv_n_splits: int = Field(10, env="CPCV_N_SPLITS")
    cpcv_embargo_td: int = Field(5, env="CPCV_EMBARGO_TD")

    class Config:
        env_prefix = "BACKTEST_"
        case_sensitive = False


class MLConfig(BaseSettings):
    """Machine Learning configuration."""

    # Model Parameters
    random_seed: int = Field(42, env="ML_RANDOM_SEED")
    test_size: float = Field(0.2, env="ML_TEST_SIZE")
    n_jobs: int = Field(-1, env="ML_N_JOBS")

    # XGBoost
    xgb_n_estimators: int = Field(100, env="XGB_N_ESTIMATORS")
    xgb_max_depth: int = Field(6, env="XGB_MAX_DEPTH")
    xgb_learning_rate: float = Field(0.1, env="XGB_LEARNING_RATE")
    xgb_subsample: float = Field(0.8, env="XGB_SUBSAMPLE")

    # LightGBM
    lgbm_n_estimators: int = Field(100, env="LGBM_N_ESTIMATORS")
    lgbm_max_depth: int = Field(6, env="LGBM_MAX_DEPTH")
    lgbm_learning_rate: float = Field(0.1, env="LGBM_LEARNING_RATE")
    lgbm_feature_fraction: float = Field(0.8, env="LGBM_FEATURE_FRACTION")

    # Feature Engineering
    feature_lookback_days: int = Field(252, env="FEATURE_LOOKBACK_DAYS")
    max_features: int = Field(50, env="MAX_FEATURES")
    feature_selection_method: str = Field("mutual_info", env="FEATURE_SELECTION_METHOD")

    # Model Storage
    model_storage_path: Path = Field(Path("models/"), env="MODEL_STORAGE_PATH")

    class Config:
        env_prefix = "ML_"
        case_sensitive = False


class APIConfig(BaseSettings):
    """API configuration."""

    host: str = Field("0.0.0.0", env="API_HOST")
    port: int = Field(8000, env="API_PORT")
    workers: int = Field(4, env="API_WORKERS")

    # Security
    secret_key: SecretStr = Field("your-secret-key-change-this", env="API_SECRET_KEY")
    algorithm: str = Field("HS256", env="API_ALGORITHM")
    access_token_expire_minutes: int = Field(30, env="API_ACCESS_TOKEN_EXPIRE")

    # CORS
    cors_origins: List[str] = Field(["*"], env="API_CORS_ORIGINS")
    cors_credentials: bool = Field(True, env="API_CORS_CREDENTIALS")
    cors_methods: List[str] = Field(["*"], env="API_CORS_METHODS")
    cors_headers: List[str] = Field(["*"], env="API_CORS_HEADERS")

    # Rate Limiting
    rate_limit_enabled: bool = Field(True, env="API_RATE_LIMIT_ENABLED")
    rate_limit_requests: int = Field(100, env="API_RATE_LIMIT_REQUESTS")
    rate_limit_period: int = Field(60, env="API_RATE_LIMIT_PERIOD")  # seconds

    class Config:
        env_prefix = "API_"
        case_sensitive = False


class MonitoringConfig(BaseSettings):
    """Monitoring and logging configuration."""

    # Logging
    log_level: str = Field("INFO", env="LOG_LEVEL")
    log_format: str = Field("json", env="LOG_FORMAT")  # json or text
    log_file: Optional[Path] = Field(None, env="LOG_FILE")
    log_rotation: str = Field("1 day", env="LOG_ROTATION")
    log_retention: str = Field("30 days", env="LOG_RETENTION")

    # Metrics
    metrics_enabled: bool = Field(True, env="METRICS_ENABLED")
    metrics_port: int = Field(9091, env="METRICS_PORT")

    # Alerting
    alert_enabled: bool = Field(False, env="ALERT_ENABLED")
    alert_webhook_url: Optional[HttpUrl] = Field(None, env="ALERT_WEBHOOK_URL")
    alert_email: Optional[str] = Field(None, env="ALERT_EMAIL")

    # Sentry
    sentry_dsn: Optional[SecretStr] = Field(None, env="SENTRY_DSN")
    sentry_environment: str = Field("development", env="SENTRY_ENVIRONMENT")
    sentry_traces_sample_rate: float = Field(0.1, env="SENTRY_TRACES_SAMPLE_RATE")

    class Config:
        env_prefix = "MONITORING_"
        case_sensitive = False


class Settings(BaseSettings):
    """Main settings aggregating all configurations."""

    # Environment
    environment: str = Field("development", env="ENVIRONMENT")
    debug: bool = Field(False, env="DEBUG")
    testing: bool = Field(False, env="TESTING")

    # Application
    app_name: str = Field("Quantitative Trading System", env="APP_NAME")
    app_version: str = Field("1.0.0", env="APP_VERSION")

    # Sub-configurations
    database: DatabaseConfig = DatabaseConfig()
    broker: BrokerConfig = BrokerConfig()
    trading: TradingConfig = TradingConfig()
    backtest: BacktestConfig = BacktestConfig()
    ml: MLConfig = MLConfig()
    api: APIConfig = APIConfig()
    monitoring: MonitoringConfig = MonitoringConfig()

    @validator("environment")
    def validate_environment(cls, v):
        """Validate environment value."""
        allowed = ["development", "staging", "production"]
        if v not in allowed:
            raise ValueError(f"Environment must be one of: {allowed}")
        return v

    @property
    def is_production(self) -> bool:
        """Check if running in production."""
        return self.environment == "production"

    @property
    def is_development(self) -> bool:
        """Check if running in development."""
        return self.environment == "development"

    def load_yaml_config(self, config_path: Path) -> Dict[str, Any]:
        """Load additional configuration from YAML file."""
        if config_path.exists():
            with open(config_path, "r") as f:
                return yaml.safe_load(f)
        return {}

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

        # Allow loading from environment variables
        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (
                init_settings,
                env_settings,
                file_secret_settings,
            )


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Returns:
        Settings: Application settings
    """
    return Settings()


# Create settings instance
settings = get_settings()

# Export commonly used configurations
db_config = settings.database
broker_config = settings.broker
trading_config = settings.trading
ml_config = settings.ml
api_config = settings.api
monitoring_config = settings.monitoring
