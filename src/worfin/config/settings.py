"""
config/settings.py
Central configuration — all values read from environment / .env file.
Never hardcode anything that appears here.
"""
from __future__ import annotations

from enum import Enum
from functools import lru_cache

from pydantic import PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(str, Enum):
    DEVELOPMENT = "development"
    PAPER = "paper"
    LIVE = "live"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Environment ──────────────────────────────────────────────────────────
    environment: Environment = Environment.DEVELOPMENT
    log_level: str = "INFO"
    trading_capital_gbp: float = 10_000.0

    # ── Database ─────────────────────────────────────────────────────────────
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "metals_trading"
    db_user: str = "metals_user"
    db_password: str = ""
    database_url: str = ""

    @field_validator("database_url", mode="before")
    @classmethod
    def build_database_url(cls, v: str, info: any) -> str:  # type: ignore[override]
        if v:
            return v
        values = info.data
        return (
            f"postgresql://{values.get('db_user')}:{values.get('db_password')}"
            f"@{values.get('db_host')}:{values.get('db_port')}/{values.get('db_name')}"
        )

    # ── Interactive Brokers ───────────────────────────────────────────────────
    ibkr_host: str = "127.0.0.1"
    ibkr_port_live: int = 4001
    ibkr_port_paper: int = 4002
    ibkr_client_id: int = 1
    ibkr_account_id: str = ""

    @property
    def ibkr_port(self) -> int:
        """Return correct port based on environment."""
        return self.ibkr_port_live if self.environment == Environment.LIVE else self.ibkr_port_paper

    # ── Data Sources ─────────────────────────────────────────────────────────
    nasdaq_data_link_api_key: str = ""
    fred_api_key: str = ""

    # ── Monitoring ────────────────────────────────────────────────────────────
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    alert_email_from: str = ""
    alert_email_to: str = ""
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_password: str = ""

    # ── Safety guards ─────────────────────────────────────────────────────────
    @property
    def is_live(self) -> bool:
        return self.environment == Environment.LIVE

    @property
    def is_paper(self) -> bool:
        return self.environment == Environment.PAPER

    @property
    def is_dev(self) -> bool:
        return self.environment == Environment.DEVELOPMENT


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Singleton settings instance. Use this everywhere."""
    return Settings()