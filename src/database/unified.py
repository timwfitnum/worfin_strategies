"""
Unified Data Models for Quantitative Trading System
Provides normalized representations across all data providers
"""

from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict


# =============================================================================
# Enums
# =============================================================================


class Side(str, Enum):
    """Trade/Order side"""

    BUY = "buy"
    SELL = "sell"
    UNKNOWN = "unknown"


class OrderType(str, Enum):
    """Order types"""

    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"


class OrderStatus(str, Enum):
    """Order status"""

    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class TimeInForce(str, Enum):
    """Time in force"""

    DAY = "DAY"
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
    GTD = "GTD"
    OPG = "OPG"
    CLS = "CLS"


class AssetClass(str, Enum):
    """Asset classes"""

    EQUITY = "equity"
    OPTION = "option"
    FUTURE = "future"
    CRYPTO = "crypto"
    FOREX = "forex"
    BOND = "bond"
    INDEX = "index"


class DataSource(str, Enum):
    """Data provider sources"""

    INTERACTIVE_BROKERS = "interactive_brokers"
    POLYGON = "polygon"
    BINANCE = "binance"
    COINBASE = "coinbase"
    ALPACA = "alpaca"
    YAHOO = "yahoo"
    FRED = "fred"
    INTERNAL = "internal"


class SignalType(str, Enum):
    """Signal types"""

    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


class PositionSide(str, Enum):
    """Position side"""

    LONG = "long"
    SHORT = "short"


# =============================================================================
# Base Models
# =============================================================================


class BaseMarketData(BaseModel):
    """Base class for all market data"""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        str_strip_whitespace=True,
    )

    symbol: str = Field(..., description="Normalized internal symbol")
    exchange: str = Field(default="", description="Exchange identifier")
    timestamp: datetime = Field(..., description="Event timestamp (UTC)")
    received_at: datetime = Field(
        default_factory=lambda: datetime.utcnow(), description="Time received by system"
    )
    data_source: DataSource = Field(..., description="Data provider source")
    sequence_number: int = Field(default=0, description="Sequence for ordering")


# =============================================================================
# Tick Data Models
# =============================================================================


class Trade(BaseMarketData):
    """Normalized trade tick"""

    trade_id: str = Field(..., description="Unique trade identifier")
    price: float = Field(..., gt=0, description="Trade price")
    volume: float = Field(..., gt=0, description="Trade volume")
    side: Side = Field(default=Side.UNKNOWN, description="Trade side")

    exchange_timestamp: Optional[datetime] = None
    conditions: List[str] = Field(default_factory=list)
    is_odd_lot: bool = False

    @property
    def notional_value(self) -> float:
        return self.price * self.volume


class Quote(BaseMarketData):
    """Normalized quote tick"""

    bid_price: float = Field(..., ge=0)
    bid_size: float = Field(..., ge=0)
    ask_price: float = Field(..., ge=0)
    ask_size: float = Field(..., ge=0)

    bid_exchange: str = ""
    ask_exchange: str = ""

    @property
    def mid_price(self) -> float:
        return (self.bid_price + self.ask_price) / 2

    @property
    def spread(self) -> float:
        return self.ask_price - self.bid_price

    @property
    def spread_bps(self) -> float:
        if self.mid_price == 0:
            return 0
        return (self.spread / self.mid_price) * 10000

    @property
    def imbalance(self) -> float:
        total = self.bid_size + self.ask_size
        if total == 0:
            return 0
        return (self.bid_size - self.ask_size) / total


# =============================================================================
# OHLCV Models
# =============================================================================


class Bar(BaseModel):
    """Normalized OHLCV bar"""

    model_config = ConfigDict(frozen=True, extra="forbid")

    symbol: str
    exchange: str = ""
    timestamp: datetime
    timeframe: str

    open: float = Field(..., gt=0)
    high: float = Field(..., gt=0)
    low: float = Field(..., gt=0)
    close: float = Field(..., gt=0)
    volume: float = Field(..., ge=0)

    trades: int = Field(default=0, ge=0)
    vwap: float = Field(default=0, ge=0)
    buy_volume: float = Field(default=0, ge=0)
    sell_volume: float = Field(default=0, ge=0)

    data_source: DataSource = DataSource.INTERNAL
    is_complete: bool = True

    @property
    def typical_price(self) -> float:
        return (self.high + self.low + self.close) / 3

    @property
    def range(self) -> float:
        return self.high - self.low

    @property
    def is_bullish(self) -> bool:
        return self.close > self.open


# =============================================================================
# Order Book Models
# =============================================================================


class OrderBookLevel(BaseModel):
    """Single order book level"""

    model_config = ConfigDict(frozen=True)

    price: float = Field(..., gt=0)
    size: float = Field(..., ge=0)
    order_count: int = Field(default=0, ge=0)


class OrderBook(BaseMarketData):
    """Normalized order book snapshot"""

    bids: List[OrderBookLevel] = Field(default_factory=list)
    asks: List[OrderBookLevel] = Field(default_factory=list)

    @property
    def best_bid(self) -> Optional[OrderBookLevel]:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> Optional[OrderBookLevel]:
        return self.asks[0] if self.asks else None

    @property
    def mid_price(self) -> Optional[float]:
        if self.best_bid and self.best_ask:
            return (self.best_bid.price + self.best_ask.price) / 2
        return None

    @property
    def spread(self) -> Optional[float]:
        if self.best_bid and self.best_ask:
            return self.best_ask.price - self.best_bid.price
        return None

    @property
    def total_bid_depth(self) -> float:
        return sum(level.size for level in self.bids)

    @property
    def total_ask_depth(self) -> float:
        return sum(level.size for level in self.asks)


# =============================================================================
# Asset and Reference Data Models
# =============================================================================


class Asset(BaseModel):
    """Normalized asset/instrument definition"""

    model_config = ConfigDict(extra="ignore")

    symbol: str
    name: Optional[str] = None
    asset_class: AssetClass
    asset_subclass: Optional[str] = None

    exchange: Optional[str] = None
    primary_exchange: Optional[str] = None

    currency: str = "USD"
    multiplier: float = 1.0
    tick_size: Optional[float] = None
    lot_size: float = 1.0

    min_order_size: float = 1.0
    max_order_size: Optional[float] = None
    margin_requirement: Optional[float] = None

    underlying_symbol: Optional[str] = None
    expiration_date: Optional[date] = None
    strike_price: Optional[float] = None
    option_type: Optional[str] = None

    # Provider mappings
    ib_con_id: Optional[int] = None
    ib_symbol: Optional[str] = None
    polygon_ticker: Optional[str] = None
    binance_symbol: Optional[str] = None
    coinbase_product_id: Optional[str] = None
    yahoo_symbol: Optional[str] = None

    sector: Optional[str] = None
    industry: Optional[str] = None

    is_active: bool = True
    is_tradeable: bool = True


class CorporateAction(BaseModel):
    """Corporate action event"""

    symbol: str
    action_type: str
    ex_date: date

    split_from: Optional[float] = None
    split_to: Optional[float] = None

    dividend_amount: Optional[float] = None
    dividend_type: Optional[str] = None

    new_symbol: Optional[str] = None
    conversion_ratio: Optional[float] = None
    cash_amount: Optional[float] = None

    announcement_date: Optional[date] = None
    payment_date: Optional[date] = None
    description: Optional[str] = None
    data_source: DataSource = DataSource.INTERNAL


# =============================================================================
# Trading Models
# =============================================================================


class Signal(BaseModel):
    """Trading signal from a strategy"""

    model_config = ConfigDict(extra="forbid")

    signal_id: UUID = Field(default_factory=uuid4)
    timestamp: datetime

    strategy_id: int
    strategy_name: str
    symbol: str

    signal_type: SignalType
    strength: float = Field(..., ge=-1, le=1)
    confidence: float = Field(default=1.0, ge=0, le=1)

    target_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    horizon_minutes: Optional[int] = None

    features: Dict[str, float] = Field(default_factory=dict)
    reasoning: Optional[str] = None
    model_version: Optional[str] = None


class Order(BaseModel):
    """Trading order"""

    model_config = ConfigDict(extra="ignore")

    order_id: UUID = Field(default_factory=uuid4)
    client_order_id: Optional[str] = None
    broker_order_id: Optional[str] = None

    created_at: datetime = Field(default_factory=datetime.utcnow)
    submitted_at: Optional[datetime] = None
    filled_at: Optional[datetime] = None

    strategy_id: Optional[int] = None
    signal_id: Optional[UUID] = None

    symbol: str
    side: Side
    order_type: OrderType
    time_in_force: TimeInForce = TimeInForce.DAY

    quantity: float = Field(..., gt=0)
    filled_quantity: float = Field(default=0, ge=0)

    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    avg_fill_price: Optional[float] = None

    broker: Optional[str] = None
    exchange: Optional[str] = None
    algo_type: Optional[str] = None

    status: OrderStatus = OrderStatus.PENDING
    reject_reason: Optional[str] = None

    commission: float = Field(default=0, ge=0)
    fees: float = Field(default=0, ge=0)

    decision_price: Optional[float] = None
    arrival_price: Optional[float] = None

    @property
    def remaining_quantity(self) -> float:
        return self.quantity - self.filled_quantity

    @property
    def fill_pct(self) -> float:
        return (self.filled_quantity / self.quantity) * 100 if self.quantity > 0 else 0

    @property
    def is_complete(self) -> bool:
        return self.status in {
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }

    @property
    def total_cost(self) -> float:
        return self.commission + self.fees


class Position(BaseModel):
    """Trading position"""

    model_config = ConfigDict(extra="ignore")

    position_id: UUID = Field(default_factory=uuid4)

    strategy_id: Optional[int] = None
    symbol: str

    side: PositionSide
    quantity: float = Field(..., gt=0)

    avg_entry_price: float = Field(..., gt=0)
    total_cost_basis: float

    current_price: Optional[float] = None
    market_value: Optional[float] = None

    unrealized_pnl: float = Field(default=0)
    unrealized_pnl_pct: float = Field(default=0)
    realized_pnl: float = Field(default=0)

    delta: Optional[float] = None
    gamma: Optional[float] = None
    vega: Optional[float] = None
    theta: Optional[float] = None

    target_price: Optional[float] = None
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None

    opened_at: datetime = Field(default_factory=datetime.utcnow)
    closed_at: Optional[datetime] = None

    is_open: bool = Field(default=True)

    def update_pnl(self, current_price: float) -> None:
        self.current_price = current_price
        self.market_value = self.quantity * current_price

        if self.side == PositionSide.LONG:
            self.unrealized_pnl = self.market_value - self.total_cost_basis
        else:
            self.unrealized_pnl = self.total_cost_basis - self.market_value

        if self.total_cost_basis > 0:
            self.unrealized_pnl_pct = self.unrealized_pnl / self.total_cost_basis


class Fill(BaseModel):
    """Trade execution fill"""

    fill_id: UUID = Field(default_factory=uuid4)
    order_id: UUID

    symbol: str
    side: Side
    quantity: float = Field(..., gt=0)
    price: float = Field(..., gt=0)

    executed_at: datetime
    broker: Optional[str] = None
    exchange: Optional[str] = None
    broker_trade_id: Optional[str] = None

    commission: float = Field(default=0, ge=0)
    fees: float = Field(default=0, ge=0)

    @property
    def notional_value(self) -> float:
        return self.quantity * self.price


# =============================================================================
# Crypto-Specific Models
# =============================================================================


class FundingRate(BaseModel):
    """Perpetual futures funding rate"""

    symbol: str
    exchange: str
    timestamp: datetime

    funding_rate: float
    funding_interval_hours: int = 8
    next_funding_time: Optional[datetime] = None

    mark_price: Optional[float] = None
    index_price: Optional[float] = None

    data_source: DataSource


class LiquidationEvent(BaseModel):
    """Liquidation event on exchange"""

    symbol: str
    exchange: str
    timestamp: datetime

    side: Side
    quantity: float
    price: float

    notional_value: float = 0

    data_source: DataSource


# =============================================================================
# Aggregated Models
# =============================================================================


class MarketSnapshot(BaseModel):
    """Complete market snapshot for a symbol"""

    symbol: str
    timestamp: datetime

    last_price: Optional[float] = None
    last_size: Optional[float] = None
    last_trade_time: Optional[datetime] = None

    bid_price: Optional[float] = None
    bid_size: Optional[float] = None
    ask_price: Optional[float] = None
    ask_size: Optional[float] = None

    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    vwap: Optional[float] = None

    change: Optional[float] = None
    change_pct: Optional[float] = None

    @property
    def mid_price(self) -> Optional[float]:
        if self.bid_price and self.ask_price:
            return (self.bid_price + self.ask_price) / 2
        return self.last_price


class MarketDataEvent(BaseModel):
    """Generic market data event for streaming"""

    event_type: str
    symbol: str
    timestamp: datetime
    data: Union[Trade, Quote, Bar, OrderBook]
    data_source: DataSource


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "Side",
    "OrderType",
    "OrderStatus",
    "TimeInForce",
    "AssetClass",
    "DataSource",
    "SignalType",
    "PositionSide",
    "Trade",
    "Quote",
    "Bar",
    "OrderBook",
    "OrderBookLevel",
    "Asset",
    "CorporateAction",
    "Signal",
    "Order",
    "Position",
    "Fill",
    "FundingRate",
    "LiquidationEvent",
    "MarketSnapshot",
    "MarketDataEvent",
]
