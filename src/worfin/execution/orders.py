"""
execution/orders.py
Order lifecycle management and state machine.

─── SCOPE (Piece 2) ────────────────────────────────────────────────────────
This module owns the Order lifecycle. It does NOT submit to the broker
(broker/ibkr.py) and does NOT decide escalation strategy (engine.py).

Responsibilities:
  1. Order + Fill dataclasses — internal representation, richer than
     BrokerOrderRequest
  2. OrderManager — state machine with DB persistence
       • orders.order_log   — one row per order (INSERT on create, UPDATE on state change)
       • orders.fill_log    — one row per fill (INSERT only)
       • audit.system_events — one row per lifecycle event
  3. Automatic slippage computation when arrival_mid_price is known

Not in this piece (see engine.py — Piece 3):
  • Escalation protocol (passive → aggressive → market)
  • Market data fetching
  • Partial-fill decision logic (< 30% unfilled = leave; ≥ 30% = escalate)
  • Order batching / portfolio-level sequencing

─── STATE MACHINE ──────────────────────────────────────────────────────────

      ┌─────────────────────────────────────────────────────────┐
      │                                                         ▼
  PENDING ──► SUBMITTED ──► PARTIAL ──► FILLED     (terminal)
      │           │             │     ↘ CANCELLED  (terminal)
      │           │             └─────► REJECTED   (terminal)
      │           └─► CANCELLED / REJECTED
      └────► REJECTED / CANCELLED

Terminal states: FILLED, CANCELLED, REJECTED.  No transitions out.
The full legal-transitions table is in _LEGAL_TRANSITIONS below.

─── ID MODEL ──────────────────────────────────────────────────────────────
  • Order.internal_id       — UUID4, our primary key, written to IBKR orderRef
  • Order.ibkr_order_id     — assigned by IBKR on submit acknowledgement
  • Order.db_id             — BigInt from orders.order_log.id (RETURNING)
  • Fill.ibkr_exec_id       — IBKR execId for per-fill reconciliation
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING

from sqlalchemy import text

from worfin.execution.broker import (
    BrokerOrderRequest,
    OrderStatusValue,
    OrderType,
    Side,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# EXCEPTIONS
# ─────────────────────────────────────────────────────────────────────────────


class InvalidStateTransition(RuntimeError):
    """Attempted a state transition not allowed by the state machine."""


class OrderValidationError(ValueError):
    """Order created with invalid parameters, or fill violates invariants."""


# ─────────────────────────────────────────────────────────────────────────────
# AUDIT SEVERITY (matches audit.system_events.severity comment)
# ─────────────────────────────────────────────────────────────────────────────


class AuditSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    KILL = "kill"


# ─────────────────────────────────────────────────────────────────────────────
# STATE MACHINE
# ─────────────────────────────────────────────────────────────────────────────


_TERMINAL_STATES: frozenset[OrderStatusValue] = frozenset(
    {
        OrderStatusValue.FILLED,
        OrderStatusValue.CANCELLED,
        OrderStatusValue.REJECTED,
    }
)


# Legal forward transitions: from → allowed to-states
_LEGAL_TRANSITIONS: dict[OrderStatusValue, frozenset[OrderStatusValue]] = {
    OrderStatusValue.PENDING: frozenset(
        {
            OrderStatusValue.SUBMITTED,
            OrderStatusValue.REJECTED,
            # CANCELLED allowed: e.g. cancelled before submit (shouldn't happen
            # in the normal flow but good safety net)
            OrderStatusValue.CANCELLED,
        }
    ),
    OrderStatusValue.SUBMITTED: frozenset(
        {
            OrderStatusValue.PARTIAL,
            OrderStatusValue.FILLED,
            OrderStatusValue.CANCELLED,
            OrderStatusValue.REJECTED,
        }
    ),
    OrderStatusValue.PARTIAL: frozenset(
        {
            OrderStatusValue.FILLED,
            OrderStatusValue.CANCELLED,
            OrderStatusValue.REJECTED,
        }
    ),
    OrderStatusValue.FILLED: frozenset(),  # terminal
    OrderStatusValue.CANCELLED: frozenset(),  # terminal
    OrderStatusValue.REJECTED: frozenset(),  # terminal
}


def _check_transition(current: OrderStatusValue, target: OrderStatusValue) -> None:
    """Raise InvalidStateTransition if target isn't reachable from current."""
    if target not in _LEGAL_TRANSITIONS[current]:
        legal = sorted(s.value for s in _LEGAL_TRANSITIONS[current])
        raise InvalidStateTransition(
            f"Cannot transition {current.value} → {target.value}. "
            f"Legal from {current.value}: {legal}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# DATA CLASSES
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Fill:
    """
    A single fill event from the broker. Immutable.

    slippage_bps is (fill_price - arrival_mid) / arrival_mid × 10000 × side_sign,
    where side_sign is +1 for BUY (higher fill is worse) and -1 for SELL.
    Positive = adverse, negative = favourable.
    """

    fill_timestamp: datetime
    lots_filled: int  # Always positive
    fill_price: float
    commission_usd: float | None = None
    ibkr_exec_id: str | None = None
    slippage_bps: float | None = None  # vs arrival_mid; None if not computable


@dataclass
class Order:
    """
    Internal representation of an order.

    Richer than BrokerOrderRequest — carries state, fills, IDs, timestamps.
    Fields grouped by mutability; state fields should only change via
    OrderManager methods to keep the state machine + audit log consistent.
    """

    # ── Identity (set at construction, never modified)
    internal_id: str  # UUID4 — written to IBKR orderRef for reconciliation
    strategy_id: str  # e.g., "S4"
    ticker: str
    side: Side
    lots_requested: int  # Always positive (direction is in `side`)
    order_type: OrderType
    limit_price: float | None
    created_at: datetime

    # ── State (mutate only via OrderManager)
    status: OrderStatusValue = OrderStatusValue.PENDING
    ibkr_order_id: str | None = None
    fills: list[Fill] = field(default_factory=list)
    rejection_reason: str | None = None
    cancelled_at: datetime | None = None
    last_updated: datetime = field(default_factory=lambda: datetime.now(UTC))

    # ── Context (optional, for analytics / escalation chains)
    arrival_mid_price: float | None = None  # Used for slippage calc
    parent_internal_id: str | None = None  # For escalation: step-N points to step-(N-1)
    db_id: int | None = None  # orders.order_log.id — set on INSERT

    # ── Derived properties ──────────────────────────────────────────────────

    @property
    def lots_filled(self) -> int:
        return sum(f.lots_filled for f in self.fills)

    @property
    def lots_remaining(self) -> int:
        return max(0, self.lots_requested - self.lots_filled)

    @property
    def avg_fill_price(self) -> float | None:
        if not self.fills:
            return None
        total_lots = self.lots_filled
        if total_lots <= 0:
            return None
        weighted = sum(f.lots_filled * f.fill_price for f in self.fills)
        return weighted / total_lots

    @property
    def total_commission_usd(self) -> float:
        return sum((f.commission_usd or 0.0) for f in self.fills)

    @property
    def is_terminal(self) -> bool:
        return self.status in _TERMINAL_STATES

    # ── Conversion ──────────────────────────────────────────────────────────

    def to_broker_request(self) -> BrokerOrderRequest:
        """Build the DTO the broker needs to submit this order."""
        return BrokerOrderRequest(
            ticker=self.ticker,
            side=self.side,
            lots=self.lots_requested,
            order_type=self.order_type,
            limit_price=self.limit_price,
            client_tag=self.internal_id,
        )


# ─────────────────────────────────────────────────────────────────────────────
# ORDER MANAGER
# ─────────────────────────────────────────────────────────────────────────────


class OrderManager:
    """
    Manages the Order lifecycle.

    - Creates Orders (INSERT into orders.order_log)
    - Transitions through states (UPDATE + audit)
    - Records fills (INSERT into orders.fill_log + state transition)

    DB persistence via SQLAlchemy. Engine=None enables in-memory operation
    for tests and for first-phase paper trading before the paper DB is wired.

    All DB-write failures are caught and logged — in-memory Order state is
    authoritative during a cycle; DB sync is best-effort. Reconciliation
    (Piece 5) will resolve any drift.
    """

    def __init__(self, engine: Engine | None = None) -> None:
        self._engine = engine

    # ── Creation ────────────────────────────────────────────────────────────

    def create(
        self,
        strategy_id: str,
        ticker: str,
        side: Side,
        lots: int,
        order_type: OrderType,
        limit_price: float | None,
        arrival_mid_price: float | None = None,
        parent_internal_id: str | None = None,
    ) -> Order:
        """Create a PENDING Order. Writes to orders.order_log + audit."""
        self._validate_params(strategy_id, ticker, lots, order_type, limit_price)

        order = Order(
            internal_id=str(uuid.uuid4()),
            strategy_id=strategy_id,
            ticker=ticker,
            side=side,
            lots_requested=lots,
            order_type=order_type,
            limit_price=limit_price,
            created_at=datetime.now(UTC),
            arrival_mid_price=arrival_mid_price,
            parent_internal_id=parent_internal_id,
        )

        self._insert_order_log(order)
        self._audit_event(
            order,
            event_type="order_created",
            severity=AuditSeverity.INFO,
            message=(
                f"Order {order.internal_id} created: "
                f"{side.value} {lots} {ticker} "
                f"({order_type.value}, strategy={strategy_id})"
            ),
            context={
                "internal_id": order.internal_id,
                "order_type": order_type.value,
                "limit_price": limit_price,
                "arrival_mid_price": arrival_mid_price,
                "parent_internal_id": parent_internal_id,
            },
        )
        logger.info(
            "Order created: %s %s %d %s @ %s (strategy=%s, id=%s)",
            order_type.value,
            side.value,
            lots,
            ticker,
            limit_price if limit_price is not None else "MKT",
            strategy_id,
            order.internal_id,
        )
        return order

    @staticmethod
    def _validate_params(
        strategy_id: str,
        ticker: str,
        lots: int,
        order_type: OrderType,
        limit_price: float | None,
    ) -> None:
        if lots <= 0:
            raise OrderValidationError(
                f"lots must be positive (direction is in `side`); got {lots}"
            )
        if order_type in (OrderType.LIMIT_PASSIVE, OrderType.LIMIT_AGGRESSIVE):
            if limit_price is None:
                raise OrderValidationError(f"{order_type.value} requires limit_price")
            if limit_price <= 0:
                raise OrderValidationError(f"limit_price must be positive; got {limit_price}")
        # Schema: strategy_id varchar(4), ticker varchar(4)
        if len(strategy_id) > 4:
            raise OrderValidationError(f"strategy_id must be ≤4 chars; got {strategy_id!r}")
        if len(ticker) > 4:
            raise OrderValidationError(f"ticker must be ≤4 chars; got {ticker!r}")

    # ── State transitions ───────────────────────────────────────────────────

    def mark_submitted(self, order: Order, ibkr_order_id: str) -> None:
        """PENDING → SUBMITTED. Called after the broker returns an order id."""
        if not ibkr_order_id:
            raise OrderValidationError("ibkr_order_id required on mark_submitted")
        order.ibkr_order_id = ibkr_order_id
        self._transition(
            order,
            OrderStatusValue.SUBMITTED,
            severity=AuditSeverity.INFO,
            extra={"ibkr_order_id": ibkr_order_id},
        )
        self._update_order_log(order)

    def mark_rejected(self, order: Order, reason: str) -> None:
        """
        * → REJECTED. Called when the broker rejects the order.

        Per execution/CLAUDE.md: rejections are CRITICAL. Fires a Telegram
        alert via the AlertManager singleton. Does not retry.
        """
        order.rejection_reason = reason
        self._transition(
            order,
            OrderStatusValue.REJECTED,
            severity=AuditSeverity.CRITICAL,
            extra={"reason": reason},
        )
        self._update_order_log(order)
        self._fire_rejection_alert(order, reason)

    def mark_cancelled(self, order: Order) -> None:
        """*(non-terminal)* → CANCELLED. Common in escalation flow."""
        order.cancelled_at = datetime.now(UTC)
        self._transition(
            order,
            OrderStatusValue.CANCELLED,
            severity=AuditSeverity.INFO,
            extra={"cancelled_at": order.cancelled_at.isoformat()},
        )
        self._update_order_log(order)

    def record_fill(self, order: Order, fill: Fill) -> None:
        """
        Record a fill. Handles:
          SUBMITTED + partial  → PARTIAL
          SUBMITTED + full     → FILLED
          PARTIAL + more       → PARTIAL (stays, fill_log INSERT + audit)
          PARTIAL + completes  → FILLED

        Slippage is computed automatically from arrival_mid_price if the
        fill doesn't already carry slippage_bps.
        """
        if fill.lots_filled <= 0:
            raise OrderValidationError(f"fill.lots_filled must be positive; got {fill.lots_filled}")
        if order.is_terminal:
            raise InvalidStateTransition(
                f"Cannot record fill on terminal order "
                f"(status={order.status.value}, id={order.internal_id})"
            )
        if order.status == OrderStatusValue.PENDING:
            raise InvalidStateTransition(
                f"Cannot fill order {order.internal_id} before submission (status=pending)"
            )

        prospective_total = order.lots_filled + fill.lots_filled
        if prospective_total > order.lots_requested:
            raise OrderValidationError(
                f"Fill would overfill: filled={order.lots_filled} + "
                f"new={fill.lots_filled} > requested={order.lots_requested} "
                f"(order {order.internal_id})"
            )

        # Auto-populate slippage if not already set and we have arrival_mid
        fill = self._populate_slippage(order, fill)

        # Append + persist fill
        order.fills.append(fill)
        self._insert_fill_log(order, fill)

        # Decide target state
        target = (
            OrderStatusValue.FILLED
            if prospective_total == order.lots_requested
            else OrderStatusValue.PARTIAL
        )

        if target != order.status:
            # State change — transition with audit
            self._transition(
                order,
                target,
                severity=AuditSeverity.INFO,
                extra={
                    "lots_filled_cumulative": prospective_total,
                    "lots_remaining": order.lots_requested - prospective_total,
                    "fill_price": fill.fill_price,
                    "slippage_bps": fill.slippage_bps,
                },
            )
        else:
            # Stayed in PARTIAL — no state change, but audit the fill itself
            self._audit_event(
                order,
                event_type="fill_received",
                severity=AuditSeverity.INFO,
                message=(
                    f"Fill on order {order.internal_id}: "
                    f"{fill.lots_filled} @ {fill.fill_price:.4f} "
                    f"(cumulative {prospective_total}/{order.lots_requested})"
                ),
                context={
                    "fill_price": fill.fill_price,
                    "lots_filled": fill.lots_filled,
                    "cumulative": prospective_total,
                    "slippage_bps": fill.slippage_bps,
                    "ibkr_exec_id": fill.ibkr_exec_id,
                },
            )

        self._update_order_log(order)

    @staticmethod
    def _populate_slippage(order: Order, fill: Fill) -> Fill:
        """
        Return a new Fill with slippage_bps populated, or original if
        slippage can't be computed (missing arrival_mid, or already set).
        """
        if fill.slippage_bps is not None:
            return fill
        if order.arrival_mid_price is None or order.arrival_mid_price <= 0:
            return fill
        side_sign = 1 if order.side == Side.BUY else -1
        slippage = (
            (fill.fill_price - order.arrival_mid_price)
            / order.arrival_mid_price
            * 10_000
            * side_sign
        )
        return Fill(
            fill_timestamp=fill.fill_timestamp,
            lots_filled=fill.lots_filled,
            fill_price=fill.fill_price,
            commission_usd=fill.commission_usd,
            ibkr_exec_id=fill.ibkr_exec_id,
            slippage_bps=slippage,
        )

    # ── Internals: state transition core ────────────────────────────────────

    def _transition(
        self,
        order: Order,
        target: OrderStatusValue,
        severity: AuditSeverity,
        extra: dict,
    ) -> None:
        """
        Apply a state transition. Validates legality, updates the Order's
        in-memory state, and writes an audit event. Does NOT persist the
        order_log UPDATE — caller does that after any other field writes.
        """
        from_status = order.status
        _check_transition(from_status, target)

        order.status = target
        order.last_updated = datetime.now(UTC)

        context = {
            "from_status": from_status.value,
            "to_status": target.value,
            "internal_id": order.internal_id,
            "ibkr_order_id": order.ibkr_order_id,
            "lots_requested": order.lots_requested,
            "lots_filled": order.lots_filled,
            **extra,
        }
        self._audit_event(
            order,
            event_type="order_state_change",
            severity=severity,
            message=(
                f"Order {order.internal_id} {from_status.value} → "
                f"{target.value} ({order.side.value} {order.lots_requested} "
                f"{order.ticker})"
            ),
            context=context,
        )
        logger.info(
            "Order state: %s %s → %s (strategy=%s, ticker=%s)",
            order.internal_id,
            from_status.value,
            target.value,
            order.strategy_id,
            order.ticker,
        )

    # ── Internals: alerts ───────────────────────────────────────────────────

    @staticmethod
    def _fire_rejection_alert(order: Order, reason: str) -> None:
        """Telegram CRITICAL alert on rejection. Never raises."""
        try:
            from worfin.monitoring.alerts import get_alert_manager

            get_alert_manager().order_rejected(
                ticker=order.ticker,
                strategy_id=order.strategy_id,
                reason=reason,
            )
        except Exception as e:
            # Never let alert failure mask the rejection itself
            logger.error(
                "Failed to send rejection alert for order %s: %s",
                order.internal_id,
                e,
            )

    # ── Internals: DB writes ────────────────────────────────────────────────

    def _insert_order_log(self, order: Order) -> None:
        """INSERT into orders.order_log and capture db_id via RETURNING."""
        if self._engine is None:
            return
        sql = text(
            """
            INSERT INTO orders.order_log
                (order_timestamp, strategy_id, ticker, order_type, side,
                 lots, limit_price, ibkr_order_id, status)
            VALUES
                (:order_timestamp, :strategy_id, :ticker, :order_type,
                 :side, :lots, :limit_price, :ibkr_order_id, :status)
            RETURNING id
            """
        )
        params = {
            "order_timestamp": order.created_at,
            "strategy_id": order.strategy_id,
            "ticker": order.ticker,
            "order_type": order.order_type.value,
            "side": order.side.value,
            "lots": order.lots_requested,
            "limit_price": order.limit_price,
            "ibkr_order_id": order.ibkr_order_id,
            "status": order.status.value,
        }
        try:
            with self._engine.begin() as conn:
                result = conn.execute(sql, params)
                order.db_id = int(result.scalar_one())
        except Exception:
            logger.exception(
                "Failed to INSERT order %s into orders.order_log. "
                "In-memory state preserved; DB out of sync until recovery.",
                order.internal_id,
            )

    def _update_order_log(self, order: Order) -> None:
        """UPDATE orders.order_log (status + ibkr_order_id)."""
        if self._engine is None or order.db_id is None:
            return
        sql = text(
            """
            UPDATE orders.order_log
            SET status        = :status,
                ibkr_order_id = :ibkr_order_id
            WHERE id = :id
            """
        )
        params = {
            "id": order.db_id,
            "status": order.status.value,
            "ibkr_order_id": order.ibkr_order_id,
        }
        try:
            with self._engine.begin() as conn:
                conn.execute(sql, params)
        except Exception:
            logger.exception(
                "Failed to UPDATE order %s (db_id=%s) in orders.order_log",
                order.internal_id,
                order.db_id,
            )

    def _insert_fill_log(self, order: Order, fill: Fill) -> None:
        """INSERT into orders.fill_log. order_id FKs to orders.order_log.id."""
        if self._engine is None or order.db_id is None:
            return
        sql = text(
            """
            INSERT INTO orders.fill_log
                (fill_timestamp, order_id, ticker, strategy_id,
                 fill_price, lots_filled, commission_usd, slippage_bps)
            VALUES
                (:fill_timestamp, :order_id, :ticker, :strategy_id,
                 :fill_price, :lots_filled, :commission_usd, :slippage_bps)
            """
        )
        params = {
            "fill_timestamp": fill.fill_timestamp,
            "order_id": order.db_id,
            "ticker": order.ticker,
            "strategy_id": order.strategy_id,
            "fill_price": fill.fill_price,
            "lots_filled": fill.lots_filled,
            "commission_usd": fill.commission_usd,
            "slippage_bps": fill.slippage_bps,
        }
        try:
            with self._engine.begin() as conn:
                conn.execute(sql, params)
        except Exception:
            logger.exception(
                "Failed to INSERT fill for order %s (db_id=%s) into orders.fill_log",
                order.internal_id,
                order.db_id,
            )

    def _audit_event(
        self,
        order: Order,
        event_type: str,
        severity: AuditSeverity,
        message: str,
        context: dict,
    ) -> None:
        """INSERT into audit.system_events. Swallow errors — log only."""
        if self._engine is None:
            return
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
            "event_type": event_type,
            "severity": severity.value,
            "strategy_id": order.strategy_id,
            "ticker": order.ticker,
            "message": message,
            "context_json": json.dumps(context, default=str),
        }
        try:
            with self._engine.begin() as conn:
                conn.execute(sql, params)
        except Exception:
            logger.exception(
                "Failed to write audit event (type=%s) for order %s — logging only",
                event_type,
                order.internal_id,
            )
