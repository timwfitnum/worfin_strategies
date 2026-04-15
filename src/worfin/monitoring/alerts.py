"""
monitoring/alerts.py
Alert system: Telegram bot + structured logging.

All risk breaches, data issues, and system events flow through here.
Telegram provides mobile-accessible real-time alerts.
Structured JSON logs provide the audit trail.

Never let alert failures mask the underlying issue —
alerts are fire-and-forget. Always log first, then alert.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from enum import Enum

logger = logging.getLogger(__name__)


class AlertLevel(str, Enum):
    INFO = "INFO"  # Informational — daily reports, signals
    WARNING = "WARNING"  # Needs attention — data quality, vol alerts
    CRITICAL = "CRITICAL"  # Immediate action — risk limits, system failures
    KILL = "KILL"  # Kill switch conditions — flatten/liquidate


# Alert icons for Telegram readability
_ICONS = {
    AlertLevel.INFO: "ℹ️",
    AlertLevel.WARNING: "⚠️",
    AlertLevel.CRITICAL: "🚨",
    AlertLevel.KILL: "🔴",
}


class AlertManager:
    """
    Central alert dispatcher.
    Logs all alerts with structured JSON, sends Telegram for WARNING+.
    """

    def __init__(self) -> None:
        self._telegram_client: object | None = None
        self._telegram_chat_id: str | None = None
        self._initialized = False

    def configure(self, telegram_token: str, telegram_chat_id: str) -> None:
        """Configure Telegram. Call once at startup."""
        try:
            import telegram  # python-telegram-bot

            self._telegram_client = telegram.Bot(token=telegram_token)
            self._telegram_chat_id = telegram_chat_id
            self._initialized = True
            logger.info("Telegram alerts configured for chat %s", telegram_chat_id)
        except ImportError:
            logger.warning("python-telegram-bot not installed — Telegram alerts disabled.")
        except Exception as e:
            logger.error("Failed to configure Telegram: %s", e)

    def send(
        self,
        level: AlertLevel,
        message: str,
        context: dict | None = None,
        strategy_id: str | None = None,
        ticker: str | None = None,
    ) -> None:
        """
        Send an alert.

        Always logs to structured JSON logger.
        Sends Telegram for WARNING and above.
        """
        now = datetime.now(UTC)
        icon = _ICONS[level]

        # ── Structured log (always) ───────────────────────────────────────────
        log_record = {
            "timestamp": now.isoformat(),
            "level": level.value,
            "message": message,
            "strategy_id": strategy_id,
            "ticker": ticker,
            "context": context or {},
        }

        log_fn = {
            AlertLevel.INFO: logger.info,
            AlertLevel.WARNING: logger.warning,
            AlertLevel.CRITICAL: logger.error,
            AlertLevel.KILL: logger.critical,
        }[level]

        log_fn("ALERT [%s]: %s | %s", level.value, message, json.dumps(context or {}))

        # ── Telegram (WARNING and above) ──────────────────────────────────────
        if level in (AlertLevel.WARNING, AlertLevel.CRITICAL, AlertLevel.KILL):
            self._send_telegram(icon, level, message, strategy_id, ticker, context, now)

    def _send_telegram(
        self,
        icon: str,
        level: AlertLevel,
        message: str,
        strategy_id: str | None,
        ticker: str | None,
        context: dict | None,
        timestamp: datetime,
    ) -> None:
        """Send Telegram message. Non-blocking — never raises."""
        if not self._initialized:
            return

        parts = [f"{icon} *METALS TRADING — {level.value}*"]
        if strategy_id:
            parts.append(f"Strategy: `{strategy_id}`")
        if ticker:
            parts.append(f"Metal: `{ticker}`")
        parts.append(f"\n{message}")
        if context:
            ctx_str = "\n".join(f"• {k}: `{v}`" for k, v in context.items())
            parts.append(f"\n{ctx_str}")
        parts.append(f"\n_{timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}_")

        text = "\n".join(parts)

        try:
            import asyncio

            asyncio.run(
                self._telegram_client.send_message(  # type: ignore[union-attr]
                    chat_id=self._telegram_chat_id,
                    text=text,
                    parse_mode="Markdown",
                )
            )
        except Exception as e:
            # NEVER let alert failure propagate — log and continue
            logger.error("Telegram alert failed (non-critical): %s", e)

    # ── Convenience methods for common alert patterns ─────────────────────────

    def data_stale(self, ticker: str, days_stale: int) -> None:
        self.send(
            AlertLevel.WARNING,
            f"{ticker} price data has not updated for {days_stale} trading day(s). "
            f"Signals for this metal are blocked until data refreshes.",
            context={"days_stale": days_stale},
            ticker=ticker,
        )

    def outlier_detected(self, ticker: str, daily_return: float, z_score: float) -> None:
        self.send(
            AlertLevel.WARNING,
            f"{ticker}: Outlier daily return detected — {daily_return:.1%} ({z_score:.1f}σ). "
            f"Investigate before using in signals. Data NOT auto-discarded.",
            context={
                "daily_return_pct": round(daily_return * 100, 2),
                "z_score": round(z_score, 1),
            },
            ticker=ticker,
        )

    def risk_limit_approaching(
        self, limit_name: str, current: float, limit: float, ticker: str | None = None
    ) -> None:
        self.send(
            AlertLevel.WARNING,
            f"Risk limit approaching: {limit_name} at {current:.1%} of {limit:.1%} limit.",
            context={"current": round(current, 4), "limit": round(limit, 4)},
            ticker=ticker,
        )

    def circuit_breaker_triggered(
        self, action: str, reason: str, current_value: float, threshold: float
    ) -> None:
        level = (
            AlertLevel.KILL
            if "FLATTEN" in action or "STOP" in action or "SUSPEND" in action
            else AlertLevel.CRITICAL
        )
        self.send(
            level,
            f"CIRCUIT BREAKER: {action} triggered. {reason}",
            context={
                "current_value_pct": round(current_value * 100, 2),
                "threshold_pct": round(threshold * 100, 2),
            },
        )

    def order_rejected(self, ticker: str, strategy_id: str, reason: str) -> None:
        self.send(
            AlertLevel.CRITICAL,
            f"Order REJECTED: {strategy_id} {ticker}. Reason: {reason}. Human review required.",
            strategy_id=strategy_id,
            ticker=ticker,
        )

    def reconciliation_mismatch(self, ticker: str, system_qty: float, broker_qty: float) -> None:
        self.send(
            AlertLevel.CRITICAL,
            f"RECONCILIATION MISMATCH: {ticker}. System: {system_qty} lots, Broker: {broker_qty} lots. "
            f"New orders BLOCKED until resolved.",
            context={"system_qty": system_qty, "broker_qty": broker_qty},
            ticker=ticker,
        )

    def kill_switch_activated(self, triggered_by: str, reason: str) -> None:
        self.send(
            AlertLevel.KILL,
            f"KILL SWITCH ACTIVATED by {triggered_by}. All positions being flattened. Reason: {reason}",
            context={"triggered_by": triggered_by, "reason": reason},
        )

    def system_startup(self, environment: str) -> None:
        self.send(
            AlertLevel.INFO,
            f"System starting up in {environment} mode.",
            context={"environment": environment},
        )

    def daily_report(self, nav: float, daily_pnl: float, mtd_pnl: float) -> None:
        self.send(
            AlertLevel.INFO,
            f"Daily close report: NAV £{nav:,.0f} | "
            f"Day P&L: £{daily_pnl:+,.0f} ({daily_pnl/nav:.1%}) | "
            f"MTD P&L: £{mtd_pnl:+,.0f} ({mtd_pnl/nav:.1%})",
        )


# ── Singleton instance ────────────────────────────────────────────────────────
_alert_manager: AlertManager | None = None


def get_alert_manager() -> AlertManager:
    """Get or create the singleton AlertManager."""
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
        from worfin.config.settings import get_settings

        settings = get_settings()
        if settings.telegram_bot_token and settings.telegram_chat_id:
            _alert_manager.configure(settings.telegram_bot_token, settings.telegram_chat_id)
    return _alert_manager
