"""
execution/broker/ibkr.py
Interactive Brokers connector via ib_insync (IB Gateway, NOT TWS).

─── CRITICAL WARNING ────────────────────────────────────────────────────────
  This module submits and cancels REAL orders with REAL money (on port 4001).
  On port 4002 they're paper — but the code path is identical. No shortcuts.
─────────────────────────────────────────────────────────────────────────────

Ports:
  4002 — Paper trading (current default for Tier 0 → Tier 1)
  4001 — Live trading (NEVER touched until 8-12 weeks of paper + Tim sign-off)

Design decisions (see execution/CLAUDE.md for full spec):
  1. ONE IB connection per process — singleton via get_broker()
  2. ib_insync async throughout — engine.py is also async
  3. Contract specs come from config/metals.py — no hardcoded symbols
  4. Live-port guard: refuses to open 4001 unless ENVIRONMENT=live
  5. kill_switch() composes cancel_all + flatten_all primitives — <60s target
  6. Contract cache: per-ticker, invalidate on roll (TODO — orders.py territory)

Permissions (IBKR UK):
  - Metals (LME OTC)  → Cu/Al/Zn/Ni/Pb/Sn via exchange=ICEEU
  - Futures (CME/NYMEX) → Gold/Silver/Platinum/Palladium via exchange=NYMEX
  Paper accounts inherit permissions from the linked live account.
  Apply via Client Portal → Settings → Trading Permissions.

Status mapping (IBKR orderStatus.status → OrderStatusValue):
  PendingSubmit / ApiPending           → PENDING
  PreSubmitted / Submitted             → SUBMITTED
  Submitted + filled > 0               → PARTIAL
  Filled                               → FILLED
  Cancelled / ApiCancelled             → CANCELLED
  Inactive                             → REJECTED
  https://interactivebrokers.github.io/tws-api/order_submission.html
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

from worfin.config.metals import ALL_METALS
from worfin.config.settings import Environment, get_settings

if TYPE_CHECKING:
    from ib_insync import IB, Contract, Trade

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

KILL_SWITCH_TIMEOUT_SECONDS: int = 60
CONNECT_TIMEOUT_SECONDS: int = 20
ORDER_ACK_TIMEOUT_SECONDS: float = 2.0  # Wait for orderId after placeOrder


# ─────────────────────────────────────────────────────────────────────────────
# EXCEPTIONS
# ─────────────────────────────────────────────────────────────────────────────


class BrokerConnectionError(RuntimeError):
    """Broker connection is unavailable or unhealthy."""


class BrokerOrderError(RuntimeError):
    """Order submission, query, or cancellation failed."""


class BrokerPermissionError(RuntimeError):
    """Account lacks trading permissions for a contract.

    Common cause: LME OTC metals or COMEX futures permissions not granted.
    Apply via IBKR Client Portal → Settings → Trading Permissions.
    """


# ─────────────────────────────────────────────────────────────────────────────
# DATA CLASSES / ENUMS
# ─────────────────────────────────────────────────────────────────────────────


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    LIMIT_PASSIVE = "limit_passive"  # Limit at mid
    LIMIT_AGGRESSIVE = "limit_aggressive"  # Limit at best bid/offer (crosses spread)
    MARKET = "market"


class OrderStatusValue(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


@dataclass(frozen=True)
class BrokerOrderRequest:
    """
    Minimal DTO the broker needs to submit an order.
    orders.py builds this from its internal Order when ready to submit.
    """

    ticker: str  # Internal ticker (CA, AH, GC, ...)
    side: Side
    lots: int  # Always positive (direction is in `side`)
    order_type: OrderType
    limit_price: float | None  # Required for LIMIT_*, ignored for MARKET
    client_tag: str  # Internal order id for reconciliation (written to ibkr_order.orderRef)


@dataclass
class BrokerOrderStatus:
    """Current state of an order as reported by IBKR."""

    ibkr_order_id: str
    status: OrderStatusValue
    filled_lots: int = 0
    remaining_lots: int = 0
    avg_fill_price: float | None = None
    commission_usd: float | None = None
    last_updated: datetime = field(default_factory=lambda: datetime.now(UTC))
    error_message: str | None = None  # Populated for REJECTED


@dataclass(frozen=True)
class Quote:
    """
    A snapshot market quote for a contract. All prices in USD per unit.

    Paper accounts typically receive delayed data (~15min); live accounts
    with a NYMEX/metals subscription receive real-time. For the slow daily
    rebalance this distinction doesn't matter for paper testing.
    """

    ticker: str
    timestamp: datetime
    bid: float | None = None
    offer: float | None = None
    last: float | None = None

    @property
    def mid(self) -> float | None:
        """Mid price if both sides are present, else None."""
        if self.bid is None or self.offer is None:
            return None
        if self.bid <= 0 or self.offer <= 0:
            return None
        return (self.bid + self.offer) / 2.0

    @property
    def is_live(self) -> bool:
        """True if we have both bid and offer (≠ NaN, > 0)."""
        return self.bid is not None and self.offer is not None and self.bid > 0 and self.offer > 0


# ─────────────────────────────────────────────────────────────────────────────
# STATUS MAPPING
# ─────────────────────────────────────────────────────────────────────────────

_IBKR_STATUS_MAP: dict[str, OrderStatusValue] = {
    "PendingSubmit": OrderStatusValue.PENDING,
    "ApiPending": OrderStatusValue.PENDING,
    "PreSubmitted": OrderStatusValue.SUBMITTED,
    "Submitted": OrderStatusValue.SUBMITTED,
    "PendingCancel": OrderStatusValue.SUBMITTED,
    "Filled": OrderStatusValue.FILLED,
    "Cancelled": OrderStatusValue.CANCELLED,
    "ApiCancelled": OrderStatusValue.CANCELLED,
    "Inactive": OrderStatusValue.REJECTED,
}

# IBKR statuses that mean "done, don't touch"
_TERMINAL_IBKR_STATUSES: set[str] = {"Filled", "Cancelled", "ApiCancelled", "Inactive"}


# ─────────────────────────────────────────────────────────────────────────────
# BROKER CLASS
# ─────────────────────────────────────────────────────────────────────────────


class IBKRBroker:
    """
    IB Gateway connector. One instance per process.

    Use get_broker() to obtain the singleton. Do not instantiate directly in
    production code — tests may construct mocked instances.
    """

    def __init__(self) -> None:
        self._ib: IB | None = None
        self._connected: bool = False
        self._settings = get_settings()
        # Internal ticker → ib_insync Contract (cached after first qualify)
        self._contract_cache: dict[str, Contract] = {}
        # ibkr_order_id (str) → Trade object, for status queries
        self._active_trades: dict[str, Trade] = {}

    # ── Connection management ───────────────────────────────────────────────

    async def connect(self, port: int | None = None) -> None:
        """
        Connect to IB Gateway. Idempotent — no-op if already connected.

        If port is not provided, reads settings.ibkr_port (paper/live by env).
        NEVER hardcodes. Refuses live-port connect unless ENVIRONMENT=live.
        """
        if self._connected and self._ib is not None and self._ib.isConnected():
            logger.debug("Broker already connected — skipping connect()")
            return

        # Lazy import — ib_insync is an optional dependency (pyproject [broker])
        try:
            from ib_insync import IB
        except ImportError as e:
            raise BrokerConnectionError(
                "ib_insync not installed. Run: pip install -e '.[broker]'"
            ) from e

        host = self._settings.ibkr_host
        resolved_port = port if port is not None else self._settings.ibkr_port
        client_id = self._settings.ibkr_client_id

        # ─── SAFETY GUARD: live port requires explicit ENVIRONMENT=live ────
        if (
            resolved_port == self._settings.ibkr_port_live
            and self._settings.environment != Environment.LIVE
        ):
            raise BrokerConnectionError(
                f"Refusing to connect to live port {resolved_port} while "
                f"ENVIRONMENT={self._settings.environment.value!r}. "
                f"Set ENVIRONMENT=live in .env to use port {self._settings.ibkr_port_live}."
            )

        logger.info(
            "Connecting to IB Gateway at %s:%d (clientId=%d, env=%s)",
            host,
            resolved_port,
            client_id,
            self._settings.environment.value,
        )

        self._ib = IB()
        try:
            await self._ib.connectAsync(
                host=host,
                port=resolved_port,
                clientId=client_id,
                timeout=CONNECT_TIMEOUT_SECONDS,
            )
        except Exception as e:
            self._ib = None
            self._connected = False
            raise BrokerConnectionError(
                f"Failed to connect to IB Gateway at {host}:{resolved_port}: {e}"
            ) from e

        self._connected = True
        accounts = self._ib.managedAccounts()
        logger.info(
            "Connected to IB Gateway. Managed accounts: %s (env=%s, port=%d)",
            accounts,
            self._settings.environment.value,
            resolved_port,
        )

        # Sanity check: expected account ID appears in managed accounts
        expected = self._settings.ibkr_account_id
        if expected and expected not in accounts:
            logger.warning(
                "Expected IBKR_ACCOUNT_ID=%s not in managed accounts %s. Double-check .env.",
                expected,
                accounts,
            )

    async def disconnect(self) -> None:
        """Disconnect from IB Gateway. Safe to call multiple times."""
        if self._ib is None or not self._connected:
            return
        logger.info("Disconnecting from IB Gateway")
        try:
            self._ib.disconnect()
        except Exception as e:
            logger.warning("Error during disconnect (non-fatal): %s", e)
        self._connected = False
        self._active_trades.clear()

    def is_connected(self) -> bool:
        """True iff the underlying ib_insync connection is live."""
        return self._connected and self._ib is not None and self._ib.isConnected()

    def _require_connection(self) -> IB:
        """Return the live IB handle, or raise BrokerConnectionError."""
        if not self.is_connected():
            raise BrokerConnectionError("Not connected to IB Gateway. Call connect() first.")
        assert self._ib is not None  # for type checker
        return self._ib

    # ── Contract resolution ─────────────────────────────────────────────────

    async def _resolve_contract(self, ticker: str) -> Contract:
        """
        Build and qualify an ib_insync Contract from a MetalSpec.

        Results cached per-ticker. First call per session hits
        reqContractDetails() — which catches symbology mistakes and
        permission errors before an order is ever sent.

        TODO(orders.py): cache invalidation on futures roll.
        """
        if ticker in self._contract_cache:
            return self._contract_cache[ticker]

        from ib_insync import Future

        spec = ALL_METALS.get(ticker)
        if spec is None:
            raise KeyError(f"Unknown metal ticker: {ticker!r}. Valid: {sorted(ALL_METALS.keys())}")

        ib = self._require_connection()

        contract = Future(
            symbol=spec.ibkr_symbol,
            exchange=spec.ibkr_exchange,
            currency=spec.ibkr_currency,
        )

        try:
            qualified = await ib.qualifyContractsAsync(contract)
        except Exception as e:
            raise BrokerOrderError(
                f"qualifyContractsAsync failed for {ticker} "
                f"({spec.ibkr_symbol}@{spec.ibkr_exchange}): {e}"
            ) from e

        if not qualified or qualified[0].conId == 0:
            # Most common cause: permissions.
            permission_needed = "Metals" if spec.ibkr_exchange == "ICEEU" else "Futures"
            raise BrokerPermissionError(
                f"Contract for {ticker} ({spec.ibkr_symbol}@{spec.ibkr_exchange}) "
                f"could not be resolved. Likely cause: account lacks trading "
                f"permissions. Apply via IBKR Client Portal → Settings → "
                f"Trading Permissions → {permission_needed}."
            )

        resolved = qualified[0]
        self._contract_cache[ticker] = resolved
        logger.info(
            "Resolved %s → %s@%s conId=%s expiry=%s",
            ticker,
            resolved.symbol,
            resolved.exchange,
            resolved.conId,
            resolved.lastTradeDateOrContractMonth,
        )
        return resolved

    # ── Account & positions ─────────────────────────────────────────────────

    async def get_positions(self) -> dict[str, float]:
        """
        Current positions keyed by internal ticker.

        Returns {ticker: lots_signed}: positive = long, negative = short.
        Positions in contracts not present in config/metals.py are logged and
        ignored (do not surface to strategy code).
        """
        ib = self._require_connection()
        positions = await ib.reqPositionsAsync()

        # Reverse lookup: IBKR symbol → internal ticker
        sym_to_ticker: dict[str, str] = {}
        for internal, spec in ALL_METALS.items():
            # Key on (symbol, exchange) to disambiguate HG@NYMEX vs a future HG on
            # a different venue, and to tolerate Cu/Al routing changes cleanly.
            sym_to_ticker[f"{spec.ibkr_symbol}@{spec.ibkr_exchange}"] = internal

        result: dict[str, float] = {}
        for pos in positions:
            if pos.position == 0:
                continue
            key = f"{pos.contract.symbol}@{pos.contract.exchange}"
            internal = sym_to_ticker.get(key)
            if internal is None:
                logger.warning(
                    "Position in %s (%s lots) ignored — not in metals universe",
                    key,
                    pos.position,
                )
                continue
            # Accumulate in case multiple expiries exist for the same metal
            result[internal] = result.get(internal, 0.0) + float(pos.position)

        logger.info("Current positions: %s", result)
        return result

    async def get_account_summary(self) -> dict[str, float]:
        """
        NAV / margin / buying power from IBKR, in account base currency.

        Returns dict with keys:
          nav_usd, cash_usd, margin_used_usd, available_funds_usd, buying_power_usd

        Note: despite the *_usd suffix, values are in the account's base
        currency (typically USD for an IBKR US paper account).
        """
        ib = self._require_connection()
        account = self._settings.ibkr_account_id or ""

        values = await ib.accountSummaryAsync(account=account)

        key_map = {
            "NetLiquidation": "nav_usd",
            "TotalCashValue": "cash_usd",
            "MaintMarginReq": "margin_used_usd",
            "AvailableFunds": "available_funds_usd",
            "BuyingPower": "buying_power_usd",
        }

        summary: dict[str, float] = {}
        for v in values:
            if v.tag in key_map and v.currency in ("USD", "BASE", ""):
                try:
                    summary[key_map[v.tag]] = float(v.value)
                except (ValueError, TypeError):
                    logger.warning(
                        "Could not parse account summary %s=%r as float",
                        v.tag,
                        v.value,
                    )

        logger.info(
            "Account summary: NAV=%s avail=%s margin=%s",
            summary.get("nav_usd"),
            summary.get("available_funds_usd"),
            summary.get("margin_used_usd"),
        )
        return summary

    async def get_quote(self, ticker: str) -> Quote:
        """
        Fetch a snapshot quote for the ticker.

        Uses whatever market data type IBKR is configured for — paper
        accounts default to delayed data (~15min lag, free). For real-time
        quotes, the engine should call reqMarketDataType(1) on the underlying
        IB client before cycle start AND the account must have the relevant
        market data subscription.

        Returns a Quote with bid/offer/last populated where available.
        Quote.is_live is True iff both bid and offer are valid positive
        numbers — the engine checks this before using mid for a passive
        limit or bid/offer for an aggressive limit.
        """
        from math import isnan

        ib = self._require_connection()
        contract = await self._resolve_contract(ticker)

        try:
            tickers = await ib.reqTickersAsync(contract)
        except Exception as e:
            raise BrokerOrderError(f"reqTickersAsync failed for {ticker}: {e}") from e

        if not tickers:
            logger.warning("No ticker data returned for %s", ticker)
            return Quote(ticker=ticker, timestamp=datetime.now(UTC))

        t = tickers[0]

        def _safe_float(x: object) -> float | None:
            """Return None for NaN, None, or non-positive values."""
            if x is None:
                return None
            try:
                v = float(x)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                return None
            if isnan(v) or v <= 0:
                return None
            return v

        quote = Quote(
            ticker=ticker,
            timestamp=datetime.now(UTC),
            bid=_safe_float(t.bid),
            offer=_safe_float(t.ask),
            last=_safe_float(t.last),
        )
        logger.info(
            "Quote %s: bid=%s offer=%s last=%s mid=%s",
            ticker,
            quote.bid,
            quote.offer,
            quote.last,
            quote.mid,
        )
        return quote

    # ── Order submission ────────────────────────────────────────────────────

    async def submit_order(self, order: BrokerOrderRequest) -> str:
        """
        Submit a single order. Returns IBKR order id as a string.

        Does NOT wait for fill — caller polls get_order_status() or attaches
        to event streams. Use the engine-layer submit_with_escalation() for
        the full 3-step passive→aggressive→market protocol.
        """
        from ib_insync import LimitOrder, MarketOrder

        if order.lots <= 0:
            raise BrokerOrderError(
                f"Order lots must be positive (direction is in `side`); got {order.lots}"
            )

        ib = self._require_connection()
        contract = await self._resolve_contract(order.ticker)

        action = "BUY" if order.side == Side.BUY else "SELL"

        ibkr_order: Any
        if order.order_type == OrderType.MARKET:
            ibkr_order = MarketOrder(action=action, totalQuantity=order.lots)
        elif order.order_type in (
            OrderType.LIMIT_PASSIVE,
            OrderType.LIMIT_AGGRESSIVE,
        ):
            if order.limit_price is None:
                raise BrokerOrderError(f"{order.order_type.value} requires limit_price")
            ibkr_order = LimitOrder(
                action=action,
                totalQuantity=order.lots,
                lmtPrice=order.limit_price,
            )
        else:  # pragma: no cover — enum exhausted
            raise BrokerOrderError(f"Unknown order_type: {order.order_type}")

        # Tag with our internal id so post-trade reconciliation is unambiguous
        ibkr_order.orderRef = order.client_tag

        logger.info(
            "Submitting %s: %s %d %s @ %s (tag=%s)",
            order.order_type.value,
            action,
            order.lots,
            order.ticker,
            order.limit_price if order.limit_price is not None else "MKT",
            order.client_tag,
        )

        try:
            trade = ib.placeOrder(contract, ibkr_order)
        except Exception as e:
            raise BrokerOrderError(f"placeOrder failed for {order.ticker}: {e}") from e

        # Wait briefly for IBKR to acknowledge with an orderId
        deadline_iters = int(ORDER_ACK_TIMEOUT_SECONDS / 0.1)
        for _ in range(deadline_iters):
            if trade.order.orderId > 0:
                break
            await asyncio.sleep(0.1)

        if trade.order.orderId <= 0:
            raise BrokerOrderError(
                f"Order submitted but no orderId returned within "
                f"{ORDER_ACK_TIMEOUT_SECONDS}s for {order.ticker}"
            )

        order_id = str(trade.order.orderId)
        self._active_trades[order_id] = trade
        logger.info(
            "Order submitted: id=%s ticker=%s ibkr_status=%s",
            order_id,
            order.ticker,
            trade.orderStatus.status,
        )
        return order_id

    async def get_order_status(self, order_id: str) -> BrokerOrderStatus:
        """Current status of an order by IBKR order id."""
        ib = self._require_connection()

        trade = self._active_trades.get(order_id)
        if trade is None:
            # Fallback: scan all trades in the session
            for t in ib.trades():
                if str(t.order.orderId) == order_id:
                    trade = t
                    self._active_trades[order_id] = t
                    break

        if trade is None:
            raise BrokerOrderError(f"No trade found for order_id={order_id}")

        return self._trade_to_status(trade)

    async def cancel_order(self, order_id: str) -> bool:
        """
        Cancel a working order. Returns True if a cancel request was sent;
        False if the order was not found or was already terminal.
        """
        ib = self._require_connection()
        trade = self._active_trades.get(order_id)
        if trade is None:
            for t in ib.trades():
                if str(t.order.orderId) == order_id:
                    trade = t
                    break

        if trade is None:
            logger.warning("cancel_order: no trade found for id=%s", order_id)
            return False

        if trade.orderStatus.status in _TERMINAL_IBKR_STATUSES:
            logger.info(
                "cancel_order: order %s already in terminal state %s",
                order_id,
                trade.orderStatus.status,
            )
            return False

        try:
            ib.cancelOrder(trade.order)
            logger.info("Cancel request sent for order %s", order_id)
            return True
        except Exception as e:
            logger.error("Cancel failed for %s: %s", order_id, e)
            return False

    def _trade_to_status(self, trade: Trade) -> BrokerOrderStatus:
        """Translate an ib_insync Trade to our BrokerOrderStatus."""
        ibkr_status = trade.orderStatus.status
        status = _IBKR_STATUS_MAP.get(ibkr_status, OrderStatusValue.PENDING)

        filled = int(trade.orderStatus.filled or 0)
        remaining = int(trade.orderStatus.remaining or 0)

        # Partial-fill detection: Submitted + some fills
        if status == OrderStatusValue.SUBMITTED and filled > 0:
            status = OrderStatusValue.PARTIAL

        avg_price = (
            float(trade.orderStatus.avgFillPrice) if trade.orderStatus.avgFillPrice else None
        )

        # Sum commissions from fills (if any)
        commission: float | None = None
        if trade.fills:
            total = 0.0
            any_report = False
            for f in trade.fills:
                if f.commissionReport and f.commissionReport.commission:
                    total += float(f.commissionReport.commission)
                    any_report = True
            commission = total if any_report else None

        # Rejection detail from trade log
        error_msg: str | None = None
        if status == OrderStatusValue.REJECTED and trade.log:
            error_msg = trade.log[-1].message

        return BrokerOrderStatus(
            ibkr_order_id=str(trade.order.orderId),
            status=status,
            filled_lots=filled,
            remaining_lots=remaining,
            avg_fill_price=avg_price,
            commission_usd=commission,
            error_message=error_msg,
        )

    # ── Kill switch ────────────────────────────────────────────────────────

    async def kill_switch(self, triggered_by: str, reason: str) -> None:
        """
        Emergency flatten. MUST complete in <60 seconds.

        Sequence:
          1. Telegram alert (fire-and-forget, BEFORE work starts)
          2. Cancel ALL open orders (fast — milliseconds per order)
          3. Flatten ALL positions via MARKET orders (few seconds per order)
          4. Audit write to audit.system_events (best-effort; orders.py will
             add this when Piece 2 lands — for now, logger.critical suffices)

        NEVER raises. Every error is logged and the sequence continues.
        On timeout, logs CRITICAL and returns — manual intervention required.
        """
        t_start = datetime.now(UTC)
        logger.critical("KILL SWITCH ACTIVATED by %s — reason: %s", triggered_by, reason)

        # Alert FIRST, in case the broker work hangs
        try:
            from worfin.monitoring.alerts import get_alert_manager

            get_alert_manager().kill_switch_activated(triggered_by, reason)
        except Exception as e:
            logger.error("Kill-switch alert failed (non-fatal): %s", e)

        cancelled = 0
        flattened = 0
        try:
            async with asyncio.timeout(KILL_SWITCH_TIMEOUT_SECONDS):
                cancelled = await self._cancel_all_orders()
                logger.critical("Kill switch: cancel requests sent for %d order(s)", cancelled)
                flattened = await self._flatten_all_positions()
                logger.critical(
                    "Kill switch: submitted %d flattening MARKET order(s)",
                    flattened,
                )
        except TimeoutError:
            logger.critical(
                "KILL SWITCH EXCEEDED %ds — cancelled=%d flattened=%d. "
                "POSITIONS MAY STILL BE OPEN. MANUAL INTERVENTION REQUIRED.",
                KILL_SWITCH_TIMEOUT_SECONDS,
                cancelled,
                flattened,
            )
        except Exception as e:
            logger.critical(
                "KILL SWITCH hit unexpected error: %s — cancelled=%d flattened=%d. "
                "MANUAL INTERVENTION REQUIRED.",
                e,
                cancelled,
                flattened,
            )

        elapsed = (datetime.now(UTC) - t_start).total_seconds()
        logger.critical(
            "Kill switch completed in %.2fs (cancelled=%d flattened=%d)",
            elapsed,
            cancelled,
            flattened,
        )

    async def _cancel_all_orders(self) -> int:
        """Cancel every open order. Returns count of cancel requests sent."""
        ib = self._require_connection()
        count = 0
        for trade in list(ib.trades()):
            if trade.orderStatus.status in _TERMINAL_IBKR_STATUSES:
                continue
            try:
                ib.cancelOrder(trade.order)
                count += 1
            except Exception as e:
                logger.error(
                    "Kill switch: failed to cancel order %s: %s",
                    trade.order.orderId,
                    e,
                )
        return count

    async def _flatten_all_positions(self) -> int:
        """Submit MARKET orders to close every open position. Returns count."""
        from ib_insync import MarketOrder

        ib = self._require_connection()
        positions = await ib.reqPositionsAsync()
        count = 0

        for pos in positions:
            if pos.position == 0:
                continue
            action = "SELL" if pos.position > 0 else "BUY"
            qty = int(abs(pos.position))
            order = MarketOrder(action=action, totalQuantity=qty)
            order.orderRef = f"KILL_SWITCH_{pos.contract.symbol}"
            try:
                ib.placeOrder(pos.contract, order)
                count += 1
                logger.critical(
                    "KILL SWITCH: flattening %s %s %d lots @ MARKET",
                    action,
                    pos.contract.symbol,
                    qty,
                )
            except Exception as e:
                logger.critical(
                    "KILL SWITCH: FAILED to flatten %s: %s",
                    pos.contract.symbol,
                    e,
                )
        return count


# ─────────────────────────────────────────────────────────────────────────────
# SINGLETON ACCESS
# ─────────────────────────────────────────────────────────────────────────────

_broker: IBKRBroker | None = None


def get_broker() -> IBKRBroker:
    """
    Singleton IBKRBroker. One TCP connection per process.

    Do NOT construct IBKRBroker() directly outside of tests.
    """
    global _broker
    if _broker is None:
        _broker = IBKRBroker()
    return _broker
