"""
execution/engine.py
The daily execution cycle.

─── SCOPE (Piece 3) ────────────────────────────────────────────────────────
The `ExecutionEngine` orchestrates the 8-step daily cycle specified in
execution/CLAUDE.md. It wires together:
  • broker/ibkr.py       — IBKR connectivity, positions, orders, market data
  • orders.py            — Order lifecycle state machine + audit writes
  • pretrade_checks.py   — the 8 mandatory pre-trade validations
  • strategies/          — signal generation (via BaseStrategy.run)
  • risk/sizing.py       — compute_lots() for vol-targeted sizing
  • monitoring/alerts.py — Telegram alerts on rejections and warnings

Eight-step cycle (run once per day, 14:00–16:00 London):
  1. Compute target positions per strategy
         ├─ Load recent prices from clean_data.continuous_prices + term_structure
         ├─ Call strategy.run(data, as_of) → SignalResult
         └─ compute_lots() for each ticker in the strategy's universe
  2. Query current positions from IBKR
  3. Compute deltas (target - current per ticker per strategy)
  4. Pre-trade checks — ALL 8 checks must pass or the order is rejected
  5. Generate orders per non-zero delta
  6. Submit with escalation (passive → aggressive → market), 60s per step
  7. Post-fill reconciliation — broker positions vs cycle targets
  8. Write cycle completion event to audit.system_events

─── KEY DESIGN DECISIONS ──────────────────────────────────────────────────
  • Single-strategy only for Piece 3. Multi-strategy raises NotImplementedError
    until per-strategy position attribution is wired to positions.current_positions.
  • Compute targets in-memory — no write-through to positions.target_positions.
    (Audit trail lives in audit.system_events with full context_json.)
  • Rebalance cadence enforced per strategy via the last "strategy_rebalance"
    event in audit.system_events. No new table — uses existing audit schema.
  • Sizing price source: yesterday's close from clean_data.continuous_prices.
    Live snapshot from IBKR is used ONLY for order pricing (passive at mid,
    aggressive at bid/offer).
  • Full 3-step escalation with delayed market data on paper. Market-order
    fallback fires a Telegram WARNING alert (per execution/CLAUDE.md).
  • Correlation ID on every cycle — propagated through audit events so a
    cycle's end-to-end story can be reconstructed from the logs.

─── WHAT'S NOT HERE ───────────────────────────────────────────────────────
  • Signal-generation loop / cron scheduling → scripts/run_paper_trading.py (Piece 4)
  • Deep reconciliation with full P&L attribution → Component 5 (not in Comp 3)
  • Roll execution as calendar spreads → separate concern, not daily-cycle
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from sqlalchemy import text

from worfin.backtest.pretrade_integration import build_portfolio_state, compute_adv
from worfin.config.metals import get_metal
from worfin.config.settings import get_settings
from worfin.execution.broker import (
    BrokerConnectionError,
    BrokerOrderError,
    BrokerPermissionError,
    IBKRBroker,
    OrderStatusValue,
    OrderType,
    Quote,
    Side,
    get_broker,
)
from worfin.execution.orders import (
    AuditSeverity,
    Order,
    OrderManager,
)
from worfin.execution.pretrade_checks import PreTradeChecker
from worfin.monitoring.alerts import AlertLevel, get_alert_manager
from worfin.risk.sizing import compute_lots

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from worfin.strategies.base import BaseStrategy, SignalResult

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# Escalation protocol
ESCALATION_WAIT_SECONDS: int = 60  # Time to wait at each escalation step
CANCEL_CONFIRM_WAIT_SECONDS: float = 2.0  # Time for broker to process cancel
ORDER_POLL_INTERVAL_SECONDS: float = 1.0  # Status polling cadence
PARTIAL_FILL_THRESHOLD: float = 0.30  # >=30% unfilled → escalate; <30% → leave

# Reconciliation tolerance
RECONCILIATION_TOLERANCE_LOTS: int = 1  # Per execution/CLAUDE.md — ±1 lot

# Data loading
PRICE_HISTORY_DAYS: int = 120  # Enough for 60d momentum + 60d vol + buffer


# ─────────────────────────────────────────────────────────────────────────────
# RESULT DATA CLASSES
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class StrategyRebalanceResult:
    """Outcome of one strategy's rebalance step within a cycle."""

    strategy_id: str
    skipped: bool
    skip_reason: str | None = None
    targets: dict[str, int] = field(default_factory=dict)  # {ticker: lots}
    deltas: dict[str, int] = field(default_factory=dict)  # {ticker: lots}
    orders: list[Order] = field(default_factory=list)

    @classmethod
    def skipped_result(cls, strategy_id: str, reason: str) -> StrategyRebalanceResult:
        return cls(strategy_id=strategy_id, skipped=True, skip_reason=reason)

    @property
    def n_orders(self) -> int:
        return len(self.orders)

    @property
    def n_filled(self) -> int:
        return sum(1 for o in self.orders if o.status == OrderStatusValue.FILLED)

    @property
    def n_rejected(self) -> int:
        return sum(1 for o in self.orders if o.status == OrderStatusValue.REJECTED)


@dataclass
class ReconciliationResult:
    """Comparison of post-cycle broker positions vs cycle targets."""

    targets: dict[str, int]  # aggregated across all strategies
    actual: dict[str, int]  # from broker
    mismatches: dict[str, tuple[int, int, int]]  # {ticker: (target, actual, diff)}

    @property
    def is_clean(self) -> bool:
        return not self.mismatches


@dataclass
class CycleResult:
    """Complete record of one execution cycle."""

    correlation_id: str
    cycle_start: datetime
    cycle_end: datetime | None = None
    environment: str = "paper"
    account_nav_usd: float | None = None
    usd_gbp_rate: float | None = None
    strategy_results: list[StrategyRebalanceResult] = field(default_factory=list)
    reconciliation: ReconciliationResult | None = None
    safe_state: bool = False  # True if cycle entered safe state and didn't trade
    safe_state_reason: str | None = None

    @property
    def duration_seconds(self) -> float | None:
        if self.cycle_end is None:
            return None
        return (self.cycle_end - self.cycle_start).total_seconds()


# ─────────────────────────────────────────────────────────────────────────────
# EXECUTION ENGINE
# ─────────────────────────────────────────────────────────────────────────────


class ExecutionEngine:
    """
    Orchestrates one daily execution cycle.

    Construct once per process; call `run_cycle()` once per trading day.
    Engine is NOT re-entrant — do not run two cycles concurrently on the
    same instance.

    Args:
        strategies: list of strategy instances (BaseStrategy subclasses).
                    Piece 3 supports a single-strategy list; multi-strategy
                    raises NotImplementedError at cycle start.
        db_engine:  SQLAlchemy engine for the execution DB (paper or live).
                    Required — engine cannot run without DB access for data
                    loading and audit writes.
        broker:     Override for testing. Normally None → uses singleton.
        trading_capital_gbp:  NAV used for position sizing (NOT the broker's
                    paper NAV — which is fake on paper accounts). Defaults
                    to settings.trading_capital_gbp.
    """

    def __init__(
        self,
        strategies: list[BaseStrategy],
        db_engine: Engine,
        broker: IBKRBroker | None = None,
        trading_capital_gbp: float | None = None,
    ) -> None:
        if not strategies:
            raise ValueError("ExecutionEngine requires at least one strategy")
        self._strategies = strategies
        self._db_engine = db_engine
        self._broker = broker if broker is not None else get_broker()
        self._order_mgr = OrderManager(engine=db_engine)
        self._pretrade = PreTradeChecker()
        self._settings = get_settings()
        self._nav_gbp = (
            trading_capital_gbp
            if trading_capital_gbp is not None
            else self._settings.trading_capital_gbp
        )

    # ── Public API ─────────────────────────────────────────────────────────

    async def run_cycle(self) -> CycleResult:
        """
        Execute one daily rebalance cycle. Returns a CycleResult for
        inspection / logging / alerting.

        Never raises from within the 8-step flow — all errors are caught,
        logged, and returned in the CycleResult. The only raise is if the
        engine's preconditions (e.g. multi-strategy) aren't met.
        """
        if len(self._strategies) > 1:
            raise NotImplementedError(
                "Multi-strategy execution requires per-strategy position "
                "attribution in positions.current_positions. Piece 3 supports "
                "single-strategy only. Pass [s4_strategy] for the first paper run."
            )

        correlation_id = str(uuid.uuid4())[:8]
        cycle_start = datetime.now(UTC)
        logger.info(
            "═══ Cycle start  corr=%s  env=%s  strategies=%s",
            correlation_id,
            self._settings.environment.value,
            [s.strategy_id for s in self._strategies],
        )

        result = CycleResult(
            correlation_id=correlation_id,
            cycle_start=cycle_start,
            environment=self._settings.environment.value,
        )

        try:
            # ── Preflight: connect, load shared context ────────────────────
            if not await self._preflight(result):
                result.cycle_end = datetime.now(UTC)
                self._audit_cycle_complete(result)
                return result

            # ── STEP 2 (upfront): current positions from IBKR ──────────────
            try:
                broker_positions = await self._broker.get_positions()
                broker_positions_int = {t: int(round(v)) for t, v in broker_positions.items()}
            except BrokerConnectionError as e:
                result.safe_state = True
                result.safe_state_reason = f"broker_query_failed: {e}"
                logger.error("Safe state: %s", result.safe_state_reason)
                result.cycle_end = datetime.now(UTC)
                self._audit_cycle_complete(result)
                return result

            # ── Run each strategy's rebalance ──────────────────────────────
            for strategy in self._strategies:
                try:
                    srr = await self._run_strategy(
                        strategy=strategy,
                        broker_positions=broker_positions_int,
                        usd_gbp=result.usd_gbp_rate or 0.0,
                        correlation_id=correlation_id,
                    )
                    result.strategy_results.append(srr)
                except Exception as e:
                    logger.exception("Strategy %s blew up: %s", strategy.strategy_id, e)
                    result.strategy_results.append(
                        StrategyRebalanceResult.skipped_result(
                            strategy.strategy_id, f"exception: {type(e).__name__}: {e}"
                        )
                    )

            # ── STEP 7: Reconciliation ─────────────────────────────────────
            result.reconciliation = await self._reconcile(result.strategy_results, correlation_id)

        finally:
            # ── STEP 8: Cycle completion audit event ───────────────────────
            result.cycle_end = datetime.now(UTC)
            self._audit_cycle_complete(result)
            self._daily_report(result)

        logger.info(
            "═══ Cycle done  corr=%s  duration=%.1fs  n_orders=%d",
            correlation_id,
            result.duration_seconds or 0,
            sum(sr.n_orders for sr in result.strategy_results),
        )
        return result

    # ── Preflight ──────────────────────────────────────────────────────────

    async def _preflight(self, result: CycleResult) -> bool:
        """
        Connect broker, load FX rate, load account summary.
        Sets result.usd_gbp_rate and result.account_nav_usd.
        Returns False if the cycle cannot proceed (sets safe_state).
        """
        # Broker connection
        if not self._broker.is_connected():
            try:
                await self._broker.connect()
            except BrokerConnectionError as e:
                result.safe_state = True
                result.safe_state_reason = f"broker_connect_failed: {e}"
                logger.error("Safe state: %s", result.safe_state_reason)
                return False

        # FX rate — required for sizing
        try:
            from worfin.data.ingestion.fx_rates import get_usd_gbp

            result.usd_gbp_rate = float(get_usd_gbp(for_date=date.today(), engine=self._db_engine))
        except Exception as e:
            result.safe_state = True
            result.safe_state_reason = f"fx_rate_unavailable: {e}"
            logger.error("Safe state: %s", result.safe_state_reason)
            return False

        # Account summary (context only — sizing uses trading_capital_gbp)
        try:
            acct = await self._broker.get_account_summary()
            result.account_nav_usd = acct.get("nav_usd")
        except Exception as e:
            logger.warning("Account summary fetch failed (non-fatal): %s", e)

        return True

    # ── Per-strategy rebalance ─────────────────────────────────────────────

    async def _run_strategy(
        self,
        strategy: BaseStrategy,
        broker_positions: dict[str, int],
        usd_gbp: float,
        correlation_id: str,
    ) -> StrategyRebalanceResult:
        """
        Steps 1 and 3–6 for a single strategy.
        """
        strategy_id = strategy.strategy_id

        # Rebalance cadence check
        if not self._should_rebalance(strategy):
            logger.info("%s: not due for rebalance — skipping", strategy_id)
            return StrategyRebalanceResult.skipped_result(strategy_id, "not_due_for_rebalance")

        # ── STEP 1: Load data, compute signals, size positions ─────────────
        try:
            price_data = self._load_strategy_data(strategy)
        except Exception as e:
            logger.exception("%s: data load failed", strategy_id)
            return StrategyRebalanceResult.skipped_result(strategy_id, f"data_load_failed: {e}")

        as_of = datetime.now(UTC)
        signal_result: SignalResult = strategy.run(price_data, as_of=as_of)

        if not signal_result.is_actionable:
            reason = f"signal_not_actionable invalid_tickers={signal_result.invalid_tickers}"
            logger.warning("%s: %s", strategy_id, reason)
            self._write_strategy_rebalance_event(
                strategy_id, correlation_id, skipped=True, reason=reason
            )
            return StrategyRebalanceResult.skipped_result(strategy_id, reason)

        # Target lots per ticker
        targets = self._compute_targets(
            strategy=strategy,
            signal_result=signal_result,
            price_data=price_data,
            usd_gbp=usd_gbp,
        )
        logger.info("%s targets: %s", strategy_id, targets)

        # ── STEP 3: Deltas vs current positions ────────────────────────────
        # Single-strategy: broker totals ARE this strategy's holdings.
        # Multi-strategy is gated at the top of run_cycle.
        current = self._get_strategy_positions(strategy_id, broker_positions)
        deltas: dict[str, int] = {}
        for ticker in strategy.universe:
            tgt = targets.get(ticker, 0)
            cur = current.get(ticker, 0)
            d = tgt - cur
            if d != 0:
                deltas[ticker] = d
        logger.info("%s deltas: %s", strategy_id, deltas)

        result = StrategyRebalanceResult(
            strategy_id=strategy_id,
            skipped=False,
            targets=targets,
            deltas=deltas,
        )

        if not deltas:
            logger.info("%s: no deltas to trade", strategy_id)
            self._write_strategy_rebalance_event(
                strategy_id, correlation_id, skipped=False, reason="no_deltas"
            )
            return result

        # ── Build portfolio state for pre-trade checks (STEP 4 context) ───
        # Use current prices from the just-loaded price data
        current_prices = self._latest_close(price_data)
        adv = compute_adv(price_data, pd.Timestamp(as_of))

        # Count orders today (for daily-order-count check) — from audit
        orders_today = self._count_orders_today()

        portfolio = build_portfolio_state(
            nav_gbp=self._nav_gbp,
            current_lots=current,
            current_prices_usd=current_prices,
            usd_gbp_rate=usd_gbp,
            orders_today=orders_today,
            adv_by_ticker=adv,
        )

        # ── STEPS 5 + 6: orders with escalation ────────────────────────────
        for ticker, delta_lots in deltas.items():
            side = Side.BUY if delta_lots > 0 else Side.SELL
            qty = abs(delta_lots)
            signal_direction = 1 if signal_result.signals.get(ticker, 0) >= 0 else -1
            # Orders built by escalation helper flow back into result.orders
            orders = await self._submit_with_escalation(
                strategy_id=strategy_id,
                ticker=ticker,
                side=side,
                lots=qty,
                signal_direction=signal_direction,
                signal_timestamp=signal_result.computed_at,
                portfolio=portfolio,
                usd_gbp=usd_gbp,
            )
            result.orders.extend(orders)

        # Log rebalance event for cadence tracking
        self._write_strategy_rebalance_event(
            strategy_id,
            correlation_id,
            skipped=False,
            reason="completed",
            extra={
                "n_orders": result.n_orders,
                "n_filled": result.n_filled,
                "n_rejected": result.n_rejected,
            },
        )
        return result

    # ── Data loading ───────────────────────────────────────────────────────

    def _load_strategy_data(self, strategy: BaseStrategy) -> dict[str, pd.DataFrame]:
        """
        Load PRICE_HISTORY_DAYS of continuous_prices + term_structure for
        each ticker in the strategy's universe. Merges by price_timestamp
        into the single DataFrame shape S1/S4 expect:
          columns = [close, open, high, low, volume,
                     cash_price, f3m_price, dte_cash_3m]
          index   = price_timestamp (UTC-aware)
        """
        # 2× buffer accounts for weekends / holidays
        cutoff = datetime.now(UTC) - timedelta(days=PRICE_HISTORY_DAYS * 2)
        sql_cont = text(
            """
            SELECT price_timestamp, ticker, close, open, high, low, volume
            FROM clean_data.continuous_prices
            WHERE ticker = ANY(:tickers)
              AND price_timestamp >= :cutoff
            ORDER BY ticker, price_timestamp
            """
        )
        sql_term = text(
            """
            SELECT price_timestamp, ticker, cash_price, f3m_price, dte_cash_3m
            FROM clean_data.term_structure
            WHERE ticker = ANY(:tickers)
              AND price_timestamp >= :cutoff
            ORDER BY ticker, price_timestamp
            """
        )
        with self._db_engine.begin() as conn:
            cont_df = pd.read_sql_query(
                sql_cont,
                conn,
                params={
                    "tickers": strategy.universe,
                    "cutoff": cutoff,
                },
                parse_dates=["price_timestamp"],
            )
            term_df = pd.read_sql_query(
                sql_term,
                conn,
                params={
                    "tickers": strategy.universe,
                    "cutoff": cutoff,
                },
                parse_dates=["price_timestamp"],
            )

        result: dict[str, pd.DataFrame] = {}
        for ticker in strategy.universe:
            c = cont_df[cont_df["ticker"] == ticker].drop(columns=["ticker"])
            t = term_df[term_df["ticker"] == ticker].drop(columns=["ticker"])
            if c.empty:
                logger.warning(
                    "No continuous_prices data for %s — strategy will skip",
                    ticker,
                )
                # Include an empty DF so validate_inputs fails cleanly
                result[ticker] = pd.DataFrame(
                    columns=[
                        "close",
                        "open",
                        "high",
                        "low",
                        "volume",
                        "cash_price",
                        "f3m_price",
                        "dte_cash_3m",
                    ]
                )
                continue
            # Outer-join continuous + term_structure on timestamp
            merged = c.merge(t, on="price_timestamp", how="left").set_index("price_timestamp")
            # Ensure index is tz-aware UTC
            if merged.index.tz is None:
                merged.index = merged.index.tz_localize(UTC)
            else:
                merged.index = merged.index.tz_convert(UTC)
            # Convert Numeric columns from decimal.Decimal to float
            for col in merged.columns:
                if col == "dte_cash_3m":
                    merged[col] = pd.to_numeric(merged[col], errors="coerce").astype("Int64")
                else:
                    merged[col] = pd.to_numeric(merged[col], errors="coerce")
            result[ticker] = merged.sort_index()
        return result

    @staticmethod
    def _latest_close(
        price_data: dict[str, pd.DataFrame],
    ) -> dict[str, float]:
        """Most-recent close price per ticker."""
        result: dict[str, float] = {}
        for ticker, df in price_data.items():
            if df.empty or "close" not in df.columns:
                continue
            latest = df["close"].dropna()
            if latest.empty:
                continue
            result[ticker] = float(latest.iloc[-1])
        return result

    # ── Target computation ────────────────────────────────────────────────

    def _compute_targets(
        self,
        strategy: BaseStrategy,
        signal_result: SignalResult,
        price_data: dict[str, pd.DataFrame],
        usd_gbp: float,
    ) -> dict[str, int]:
        """
        Produce integer lot counts per ticker via compute_lots().
        Always rounds toward zero (conservative).
        """
        result: dict[str, int] = {}
        vols = self._compute_vols(price_data, signal_result.computed_at)
        prices = self._latest_close(price_data)

        for ticker in strategy.universe:
            signal = signal_result.signals.get(ticker, 0.0)
            if ticker not in vols or ticker not in prices:
                result[ticker] = 0
                continue
            vol_20d, vol_60d = vols[ticker]
            lots_float = compute_lots(
                strategy_id=strategy.strategy_id,
                ticker=ticker,
                total_capital_gbp=self._nav_gbp,
                realised_vol_20d=vol_20d,
                realised_vol_60d=vol_60d,
                signal=signal,
                current_price_usd=prices[ticker],
                usd_gbp_rate=usd_gbp,
            )
            # Live trading: whole lots only. Conservative rounding.
            result[ticker] = int(np.fix(lots_float))  # trunc toward zero
        return result

    @staticmethod
    def _compute_vols(
        price_data: dict[str, pd.DataFrame],
        as_of: datetime,
    ) -> dict[str, tuple[float, float]]:
        """
        Rolling 20d and 60d annualised realised volatility from log returns.
        Only uses data up to `as_of` — strict no-look-ahead.
        """
        as_of_ts = pd.Timestamp(as_of)
        result: dict[str, tuple[float, float]] = {}
        for ticker, df in price_data.items():
            if df.empty or "close" not in df.columns:
                continue
            closes = df[df.index <= as_of_ts]["close"].dropna()
            if len(closes) < 61:
                continue
            log_rets = np.log(closes / closes.shift(1)).dropna()
            vol_20 = float(log_rets.tail(20).std() * np.sqrt(252))
            vol_60 = float(log_rets.tail(60).std() * np.sqrt(252))
            # NaN guards: compute_lots has its own floor but surface cleanly
            if np.isnan(vol_20) or np.isnan(vol_60):
                continue
            result[ticker] = (vol_20, vol_60)
        return result

    # ── Positions attribution (single-strategy simplification) ─────────────

    def _get_strategy_positions(
        self, strategy_id: str, broker_positions: dict[str, int]
    ) -> dict[str, int]:
        """
        Current positions attributable to this strategy.

        Piece 3: single-strategy case only. Broker totals = strategy totals.
        Multi-strategy → requires reading positions.current_positions table
        per (strategy_id, ticker). That's a future-piece concern.
        """
        if len(self._strategies) > 1:  # defensive; run_cycle gates this too
            raise NotImplementedError("Per-strategy position attribution not implemented")
        return dict(broker_positions)

    # ── Escalation protocol (3-step) ──────────────────────────────────────

    async def _submit_with_escalation(
        self,
        strategy_id: str,
        ticker: str,
        side: Side,
        lots: int,
        signal_direction: int,
        signal_timestamp: datetime,
        portfolio,  # PortfolioState — circular import avoided via duck-typing
        usd_gbp: float,
    ) -> list[Order]:
        """
        Execute a single delta using the 3-step escalation protocol.

        Between steps:
          • If remaining < PARTIAL_FILL_THRESHOLD of original → leave unfilled
          • Otherwise escalate to next step

        Step 3 (market) fires a Telegram WARNING alert per execution/CLAUDE.md.
        """
        orders: list[Order] = []
        total_filled = 0
        parent_id: str | None = None

        steps = [
            OrderType.LIMIT_PASSIVE,
            OrderType.LIMIT_AGGRESSIVE,
            OrderType.MARKET,
        ]

        for step_idx, step in enumerate(steps):
            remaining = lots - total_filled
            if remaining <= 0:
                break

            # Decide whether to escalate to this step (skip the check on step 0)
            if step_idx > 0:
                unfilled_pct = remaining / lots
                if unfilled_pct < PARTIAL_FILL_THRESHOLD:
                    logger.info(
                        "%s: %d/%d filled (%.0f%% unfilled) — below %.0f%% "
                        "threshold, leaving remainder",
                        ticker,
                        total_filled,
                        lots,
                        unfilled_pct * 100,
                        PARTIAL_FILL_THRESHOLD * 100,
                    )
                    break

            # Refresh quote at each step
            try:
                quote = await self._broker.get_quote(ticker)
            except (BrokerOrderError, BrokerPermissionError) as e:
                logger.warning("%s: quote fetch failed on %s: %s", ticker, step.value, e)
                quote = Quote(ticker=ticker, timestamp=datetime.now(UTC))

            # Determine order price for this step
            limit_price = self._price_for_step(step, side, quote)
            if step != OrderType.MARKET and (limit_price is None or limit_price <= 0):
                logger.warning(
                    "%s: no limit price available for %s — trying next step",
                    ticker,
                    step.value,
                )
                continue

            # Build order
            order = self._order_mgr.create(
                strategy_id=strategy_id,
                ticker=ticker,
                side=side,
                lots=remaining,
                order_type=step,
                limit_price=limit_price,
                arrival_mid_price=quote.mid,
                parent_internal_id=parent_id,
            )

            # Alert on market fallback (step 3)
            if step == OrderType.MARKET:
                self._alert_market_fallback(order)

            # Try the step: pre-trade check → submit → wait → cancel remainder
            await self._try_order_step(
                order=order,
                signal_direction=signal_direction,
                signal_timestamp=signal_timestamp,
                portfolio=portfolio,
                usd_gbp=usd_gbp,
                current_mid=quote.mid,
            )

            orders.append(order)
            total_filled += order.lots_filled
            parent_id = order.internal_id

            # If rejected, stop the escalation chain
            if order.status == OrderStatusValue.REJECTED:
                logger.warning(
                    "%s: order rejected on %s — stopping escalation",
                    ticker,
                    step.value,
                )
                break

        return orders

    @staticmethod
    def _price_for_step(step: OrderType, side: Side, quote: Quote) -> float | None:
        """The limit price corresponding to this escalation step."""
        if step == OrderType.LIMIT_PASSIVE:
            return quote.mid
        if step == OrderType.LIMIT_AGGRESSIVE:
            # Cross the spread: buy at offer, sell at bid
            return quote.offer if side == Side.BUY else quote.bid
        # MARKET
        return None

    async def _try_order_step(
        self,
        order: Order,
        signal_direction: int,
        signal_timestamp: datetime,
        portfolio,
        usd_gbp: float,
        current_mid: float | None,
    ) -> None:
        """
        Run one escalation step end-to-end for `order`:
          1. Pre-trade check
          2. Submit to broker
          3. Wait up to ESCALATION_WAIT_SECONDS for fills
          4. Cancel remainder if still working

        Updates `order` in place via OrderManager; never raises.
        """
        # ── 1. Pre-trade check ─────────────────────────────────────────────
        metal = get_metal(order.ticker)
        # Signed lots for the checker: direction comes from side
        signed_lots = order.lots_requested if order.side == Side.BUY else -order.lots_requested
        # Proposed notional in USD: lots × lot_size × price
        reference_price = order.limit_price if order.limit_price is not None else current_mid
        if reference_price is None or reference_price <= 0:
            self._order_mgr.mark_rejected(order, "No reference price available for pre-trade check")
            return

        proposed_notional_usd = signed_lots * metal.lot_size * reference_price

        check = self._pretrade.check_order(
            ticker=order.ticker,
            strategy_id=order.strategy_id,
            proposed_lots=signed_lots,
            proposed_notional_usd=proposed_notional_usd,
            current_mid_price=current_mid if current_mid else reference_price,
            order_price=reference_price,
            signal_timestamp=signal_timestamp,
            signal_direction=signal_direction,
            portfolio=portfolio,
            usd_gbp_rate=usd_gbp,
        )

        if not check.all_passed:
            failed = ", ".join(f"{c.check_name}({c.message})" for c in check.failed_checks)
            self._order_mgr.mark_rejected(order, f"Pre-trade check failed: {failed}")
            return

        # ── 2. Submit to broker ───────────────────────────────────────────
        try:
            ibkr_id = await self._broker.submit_order(order.to_broker_request())
        except BrokerPermissionError as e:
            self._order_mgr.mark_rejected(order, f"broker permission: {e}")
            return
        except BrokerConnectionError as e:
            self._order_mgr.mark_rejected(order, f"broker disconnected: {e}")
            return
        except BrokerOrderError as e:
            self._order_mgr.mark_rejected(order, f"broker rejected: {e}")
            return
        except Exception as e:
            logger.exception("Unexpected broker error on %s", order.ticker)
            self._order_mgr.mark_rejected(order, f"unexpected broker error: {e}")
            return

        self._order_mgr.mark_submitted(order, ibkr_id)

        # ── 3. Wait for fills ─────────────────────────────────────────────
        await self._wait_for_fills(order, ESCALATION_WAIT_SECONDS)

        # ── 4. Cancel remainder if still working ──────────────────────────
        if not order.is_terminal:
            try:
                await self._broker.cancel_order(order.ibkr_order_id or "")
                await asyncio.sleep(CANCEL_CONFIRM_WAIT_SECONDS)
                await self._sync_order_status(order)
            except Exception as e:
                logger.warning("Cancel failed for %s: %s", order.ticker, e)
            if not order.is_terminal:
                # Force to cancelled in our state machine — the broker may
                # still be processing, but from the engine's perspective
                # this attempt is done.
                self._order_mgr.mark_cancelled(order)

    async def _wait_for_fills(self, order: Order, timeout_seconds: int) -> None:
        """
        Poll order status until terminal or timeout.

        New fills observed on the broker side are recorded via
        OrderManager.record_fill so slippage and audit trail stay consistent.
        """
        deadline = datetime.now(UTC) + timedelta(seconds=timeout_seconds)
        while datetime.now(UTC) < deadline:
            await self._sync_order_status(order)
            if order.is_terminal:
                return
            await asyncio.sleep(ORDER_POLL_INTERVAL_SECONDS)

    async def _sync_order_status(self, order: Order) -> None:
        """
        Pull the broker's view and reconcile with our Order:
          • Any newly-filled lots since last poll → record_fill
          • If broker says CANCELLED/REJECTED and we don't → transition
        """
        if order.ibkr_order_id is None:
            return
        try:
            broker_status = await self._broker.get_order_status(order.ibkr_order_id)
        except BrokerOrderError:
            return  # Order not visible; try again next poll

        # Any new fills?
        if broker_status.filled_lots > order.lots_filled:
            new_lots = broker_status.filled_lots - order.lots_filled
            # The broker aggregates; we synthesise a single Fill for the delta
            from worfin.execution.orders import Fill

            fill = Fill(
                fill_timestamp=broker_status.last_updated,
                lots_filled=int(new_lots),
                fill_price=broker_status.avg_fill_price or 0.0,
                commission_usd=broker_status.commission_usd,
                ibkr_exec_id=None,
            )
            try:
                self._order_mgr.record_fill(order, fill)
            except Exception as e:
                logger.exception(
                    "record_fill failed for order %s: %s",
                    order.internal_id,
                    e,
                )

        # Broker side terminal states we haven't reflected yet
        if (
            broker_status.status == OrderStatusValue.REJECTED
            and order.status != OrderStatusValue.REJECTED
        ):
            self._order_mgr.mark_rejected(order, broker_status.error_message or "broker rejected")
        elif broker_status.status == OrderStatusValue.CANCELLED and not order.is_terminal:
            self._order_mgr.mark_cancelled(order)

    # ── Alerts ────────────────────────────────────────────────────────────

    @staticmethod
    def _alert_market_fallback(order: Order) -> None:
        """Telegram WARNING when market-order fallback fires (step 3)."""
        try:
            get_alert_manager().send(
                AlertLevel.WARNING,
                (
                    f"Market order fallback: {order.ticker} "
                    f"{order.side.value} {order.lots_requested} lots. "
                    f"Both limit steps failed to fill ≥70%."
                ),
                strategy_id=order.strategy_id,
                ticker=order.ticker,
                context={"internal_id": order.internal_id},
            )
        except Exception as e:
            logger.error("Market-fallback alert failed: %s", e)

    # ── Reconciliation ────────────────────────────────────────────────────

    async def _reconcile(
        self,
        strategy_results: list[StrategyRebalanceResult],
        correlation_id: str,
    ) -> ReconciliationResult:
        """
        Compare post-cycle broker positions to cycle-aggregated targets.
        Writes one row to audit.reconciliation_log regardless of outcome.
        """
        # Aggregate targets across strategies (sum per ticker)
        cycle_targets: dict[str, int] = {}
        for srr in strategy_results:
            if srr.skipped:
                continue
            for ticker, lots in srr.targets.items():
                cycle_targets[ticker] = cycle_targets.get(ticker, 0) + lots

        try:
            broker_positions = await self._broker.get_positions()
            actual = {t: int(round(v)) for t, v in broker_positions.items()}
        except BrokerConnectionError as e:
            logger.error("Reconciliation: broker query failed: %s", e)
            actual = {}

        all_tickers = set(cycle_targets.keys()) | set(actual.keys())
        mismatches: dict[str, tuple[int, int, int]] = {}
        for ticker in all_tickers:
            tgt = cycle_targets.get(ticker, 0)
            act = actual.get(ticker, 0)
            diff = act - tgt
            if abs(diff) > RECONCILIATION_TOLERANCE_LOTS:
                mismatches[ticker] = (tgt, act, diff)

        recon = ReconciliationResult(
            targets=cycle_targets,
            actual=actual,
            mismatches=mismatches,
        )

        # Write to audit.reconciliation_log
        self._write_reconciliation_log(recon, correlation_id)

        # Alert on mismatch
        if mismatches:
            logger.warning(
                "Reconciliation: %d mismatches. %s",
                len(mismatches),
                mismatches,
            )
            try:
                mgr = get_alert_manager()
                for ticker, (tgt, act, _diff) in mismatches.items():
                    mgr.reconciliation_mismatch(ticker=ticker, system_qty=tgt, broker_qty=act)
            except Exception as e:
                logger.error("Reconciliation alert failed: %s", e)
        else:
            logger.info("Reconciliation clean: %d positions match", len(actual))

        return recon

    def _write_reconciliation_log(self, recon: ReconciliationResult, correlation_id: str) -> None:
        """INSERT one row into audit.reconciliation_log."""
        status = "clean" if recon.is_clean else "mismatch"
        details = {
            "correlation_id": correlation_id,
            "targets": recon.targets,
            "actual": recon.actual,
            "mismatches": {
                k: {"target": v[0], "actual": v[1], "diff": v[2]}
                for k, v in recon.mismatches.items()
            },
        }
        sql = text(
            """
            INSERT INTO audit.reconciliation_log
                (reconciled_at, status, discrepancies, total_value_diff_gbp, details_json)
            VALUES
                (:reconciled_at, :status, :discrepancies, :total_value_diff_gbp, :details_json)
            """
        )
        params = {
            "reconciled_at": datetime.now(UTC),
            "status": status,
            "discrepancies": len(recon.mismatches),
            "total_value_diff_gbp": None,  # Filled by Component 5 (deep recon)
            "details_json": json.dumps(details, default=str),
        }
        try:
            with self._db_engine.begin() as conn:
                conn.execute(sql, params)
        except Exception:
            logger.exception(
                "Failed to write audit.reconciliation_log (correlation=%s)",
                correlation_id,
            )

    # ── Rebalance cadence ─────────────────────────────────────────────────

    def _should_rebalance(self, strategy: BaseStrategy) -> bool:
        """
        True iff today is (rebalance_every_n_days) or more days since the
        strategy's last completed rebalance, per audit.system_events.
        """
        n_days = getattr(strategy.config, "rebalance_every_n_days", 1)
        if n_days <= 1:
            return True  # Daily strategy → rebalance every cycle

        last = self._last_rebalance(strategy.strategy_id)
        if last is None:
            logger.info(
                "%s: no prior rebalance — rebalancing now",
                strategy.strategy_id,
            )
            return True
        age_days = (datetime.now(UTC) - last).days
        return age_days >= n_days

    def _last_rebalance(self, strategy_id: str) -> datetime | None:
        """Most recent strategy_rebalance event for this strategy."""
        sql = text(
            """
            SELECT MAX(event_timestamp) AS last_ts
            FROM audit.system_events
            WHERE event_type = 'strategy_rebalance'
              AND strategy_id = :strategy_id
            """
        )
        try:
            with self._db_engine.begin() as conn:
                row = conn.execute(sql, {"strategy_id": strategy_id}).fetchone()
            return row[0] if row and row[0] else None
        except Exception:
            logger.exception("Failed to read last rebalance for %s", strategy_id)
            return None

    def _write_strategy_rebalance_event(
        self,
        strategy_id: str,
        correlation_id: str,
        skipped: bool,
        reason: str,
        extra: dict | None = None,
    ) -> None:
        """Append a strategy_rebalance event. Used for cadence tracking."""
        context = {
            "correlation_id": correlation_id,
            "skipped": skipped,
            "reason": reason,
        }
        if extra:
            context.update(extra)

        sql = text(
            """
            INSERT INTO audit.system_events
                (event_timestamp, event_type, severity, strategy_id,
                 ticker, message, context_json)
            VALUES
                (:event_timestamp, :event_type, :severity, :strategy_id,
                 :ticker, :message, :context_json)
            """
        )
        params = {
            "event_timestamp": datetime.now(UTC),
            "event_type": "strategy_rebalance",
            "severity": AuditSeverity.INFO.value,
            "strategy_id": strategy_id,
            "ticker": None,
            "message": (
                f"Strategy {strategy_id} rebalance "
                f"{'skipped' if skipped else 'completed'}: {reason}"
            ),
            "context_json": json.dumps(context, default=str),
        }
        try:
            with self._db_engine.begin() as conn:
                conn.execute(sql, params)
        except Exception:
            logger.exception(
                "Failed to write strategy_rebalance event for %s",
                strategy_id,
            )

    # ── Utility: orders-today count for pre-trade check ────────────────────

    def _count_orders_today(self) -> int:
        """Count orders.order_log rows inserted since midnight UTC today."""
        midnight = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        sql = text(
            """
            SELECT COUNT(*)
            FROM orders.order_log
            WHERE order_timestamp >= :midnight
            """
        )
        try:
            with self._db_engine.begin() as conn:
                row = conn.execute(sql, {"midnight": midnight}).fetchone()
            return int(row[0]) if row else 0
        except Exception:
            logger.exception("Failed to count orders today")
            return 0

    # ── Cycle completion audit + daily report ──────────────────────────────

    def _audit_cycle_complete(self, result: CycleResult) -> None:
        """One audit.system_events row summarising the whole cycle."""
        severity = AuditSeverity.WARNING if result.safe_state else AuditSeverity.INFO
        message = (
            f"Cycle {result.correlation_id} "
            f"{'ENTERED SAFE STATE' if result.safe_state else 'completed'}: "
            f"{len(result.strategy_results)} strategies, "
            f"{sum(sr.n_orders for sr in result.strategy_results)} orders, "
            f"{sum(sr.n_filled for sr in result.strategy_results)} filled, "
            f"{sum(sr.n_rejected for sr in result.strategy_results)} rejected"
        )
        context = {
            "correlation_id": result.correlation_id,
            "environment": result.environment,
            "duration_seconds": result.duration_seconds,
            "safe_state": result.safe_state,
            "safe_state_reason": result.safe_state_reason,
            "account_nav_usd": result.account_nav_usd,
            "usd_gbp_rate": result.usd_gbp_rate,
            "strategies": [
                {
                    "strategy_id": sr.strategy_id,
                    "skipped": sr.skipped,
                    "skip_reason": sr.skip_reason,
                    "n_orders": sr.n_orders,
                    "n_filled": sr.n_filled,
                    "n_rejected": sr.n_rejected,
                }
                for sr in result.strategy_results
            ],
            "reconciliation_clean": (
                result.reconciliation.is_clean if result.reconciliation else None
            ),
        }
        sql = text(
            """
            INSERT INTO audit.system_events
                (event_timestamp, event_type, severity, strategy_id,
                 ticker, message, context_json)
            VALUES
                (:event_timestamp, :event_type, :severity, :strategy_id,
                 :ticker, :message, :context_json)
            """
        )
        params = {
            "event_timestamp": datetime.now(UTC),
            "event_type": "cycle_complete",
            "severity": severity.value,
            "strategy_id": None,
            "ticker": None,
            "message": message,
            "context_json": json.dumps(context, default=str),
        }
        try:
            with self._db_engine.begin() as conn:
                conn.execute(sql, params)
        except Exception:
            logger.exception("Failed to write cycle_complete audit event")

    @staticmethod
    def _daily_report(result: CycleResult) -> None:
        """Telegram INFO daily report. Non-fatal on failure."""
        try:
            mgr = get_alert_manager()
            if result.safe_state:
                mgr.send(
                    AlertLevel.WARNING,
                    f"Cycle {result.correlation_id} entered safe state: {result.safe_state_reason}",
                    context={"duration_s": result.duration_seconds},
                )
                return
            n_orders = sum(sr.n_orders for sr in result.strategy_results)
            n_filled = sum(sr.n_filled for sr in result.strategy_results)
            n_rejected = sum(sr.n_rejected for sr in result.strategy_results)
            recon_status = (
                "clean"
                if result.reconciliation and result.reconciliation.is_clean
                else f"MISMATCHES {len(result.reconciliation.mismatches) if result.reconciliation else '?'}"
            )
            mgr.send(
                AlertLevel.INFO,
                (
                    f"Cycle {result.correlation_id} done. "
                    f"orders={n_orders} filled={n_filled} "
                    f"rejected={n_rejected} recon={recon_status}"
                ),
                context={
                    "duration_s": round(result.duration_seconds or 0, 1),
                    "nav_usd": result.account_nav_usd,
                },
            )
        except Exception as e:
            logger.error("Daily report alert failed: %s", e)
