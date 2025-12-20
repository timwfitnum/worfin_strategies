"""
Base Data Feed Module for Quantitative Trading System.
Provides abstract interfaces and common functionality for all data providers.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Union

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# =============================================================================
# Enumerations
# =============================================================================


class DataSource(str, Enum):
    """Data source enumeration."""

    INTERACTIVE_BROKERS = "interactive_brokers"
    POLYGON = "polygon"
    BINANCE = "binance"
    COINBASE = "coinbase"
    ALPACA = "alpaca"
    YAHOO = "yahoo"
    FRED = "fred"


class AssetClass(str, Enum):
    """Asset class enumeration."""

    EQUITY = "equity"
    OPTION = "option"
    FUTURE = "future"
    FOREX = "forex"
    CRYPTO = "crypto"
    BOND = "bond"
    ETF = "etf"
    INDEX = "index"


class DataType(str, Enum):
    """Data type enumeration."""

    TRADE = "trade"
    QUOTE = "quote"
    BAR = "bar"
    ORDERBOOK = "orderbook"
    FUNDAMENTAL = "fundamental"
    NEWS = "news"


class ConnectionState(str, Enum):
    """Connection state enumeration."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"


# =============================================================================
# Normalized Data Models
# =============================================================================


class NormalizedTick(BaseModel):
    """Normalized trade tick data model."""

    timestamp: datetime = Field(..., description="Trade timestamp in UTC")
    source: DataSource
    symbol: str = Field(..., description="Normalized symbol")
    raw_symbol: str = Field(..., description="Original symbol from source")
    exchange: str

    price: Decimal = Field(..., ge=0)
    volume: Decimal = Field(..., ge=0)
    side: str = Field(default="unknown", pattern="^(buy|sell|unknown)$")

    trade_id: Optional[str] = None
    conditions: List[str] = Field(default_factory=list)

    # Metadata
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    latency_ms: Optional[float] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            Decimal: lambda v: str(v),
        }


class NormalizedQuote(BaseModel):
    """Normalized quote (bid/ask) data model."""

    timestamp: datetime
    source: DataSource
    symbol: str
    raw_symbol: str
    exchange: str

    bid_price: Decimal = Field(..., ge=0)
    bid_size: Decimal = Field(..., ge=0)
    ask_price: Decimal = Field(..., ge=0)
    ask_size: Decimal = Field(..., ge=0)

    # Derived fields
    spread: Decimal = Field(default=Decimal("0"))
    mid_price: Decimal = Field(default=Decimal("0"))

    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def __init__(self, **data):
        super().__init__(**data)
        if self.spread == 0:
            self.spread = self.ask_price - self.bid_price
        if self.mid_price == 0:
            self.mid_price = (self.bid_price + self.ask_price) / 2


class NormalizedBar(BaseModel):
    """Normalized OHLCV bar data model."""

    timestamp: datetime
    source: DataSource
    symbol: str
    raw_symbol: str
    exchange: str
    timeframe: str = Field(..., pattern="^(1m|5m|15m|30m|1h|4h|1d|1w|1M)$")

    open: Decimal = Field(..., ge=0)
    high: Decimal = Field(..., ge=0)
    low: Decimal = Field(..., ge=0)
    close: Decimal = Field(..., ge=0)
    volume: Decimal = Field(..., ge=0)

    vwap: Optional[Decimal] = None
    trades: Optional[int] = None

    # For adjusted prices
    adj_close: Optional[Decimal] = None


class NormalizedOrderBook(BaseModel):
    """Normalized order book snapshot."""

    timestamp: datetime
    source: DataSource
    symbol: str
    exchange: str

    bids: List[tuple[Decimal, Decimal]] = Field(default_factory=list)  # [(price, size), ...]
    asks: List[tuple[Decimal, Decimal]] = Field(default_factory=list)

    sequence_num: Optional[int] = None

    @property
    def best_bid(self) -> Optional[Decimal]:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> Optional[Decimal]:
        return self.asks[0][0] if self.asks else None

    @property
    def spread(self) -> Optional[Decimal]:
        if self.best_bid and self.best_ask:
            return self.best_ask - self.best_bid
        return None


# =============================================================================
# Event System
# =============================================================================


@dataclass
class DataEvent:
    """Generic data event for streaming."""

    event_type: DataType
    data: Union[NormalizedTick, NormalizedQuote, NormalizedBar, NormalizedOrderBook]
    source: DataSource
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# Type alias for event callbacks
EventCallback = Callable[[DataEvent], None]
AsyncEventCallback = Callable[[DataEvent], asyncio.coroutine]


# =============================================================================
# Connection Management
# =============================================================================


class ConnectionManager:
    """Manages connection state with automatic reconnection."""

    def __init__(
        self,
        name: str,
        max_retries: int = 5,
        retry_delay: float = 5.0,
        backoff_multiplier: float = 2.0,
        max_delay: float = 300.0,
    ):
        self.name = name
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.backoff_multiplier = backoff_multiplier
        self.max_delay = max_delay

        self.state = ConnectionState.DISCONNECTED
        self.retry_count = 0
        self.last_error: Optional[Exception] = None
        self.connected_at: Optional[datetime] = None
        self.disconnected_at: Optional[datetime] = None

        self._state_callbacks: List[Callable[[ConnectionState], None]] = []

    def on_state_change(self, callback: Callable[[ConnectionState], None]):
        """Register state change callback."""
        self._state_callbacks.append(callback)

    def set_state(self, state: ConnectionState):
        """Update connection state and notify callbacks."""
        old_state = self.state
        self.state = state

        if state == ConnectionState.CONNECTED:
            self.connected_at = datetime.now(timezone.utc)
            self.retry_count = 0
        elif state == ConnectionState.DISCONNECTED:
            self.disconnected_at = datetime.now(timezone.utc)

        if old_state != state:
            logger.info(f"{self.name}: State changed {old_state.value} -> {state.value}")
            for callback in self._state_callbacks:
                try:
                    callback(state)
                except Exception as e:
                    logger.error(f"State callback error: {e}")

    def get_retry_delay(self) -> float:
        """Calculate exponential backoff delay."""
        delay = self.retry_delay * (self.backoff_multiplier**self.retry_count)
        return min(delay, self.max_delay)

    def should_retry(self) -> bool:
        """Check if should attempt reconnection."""
        return self.retry_count < self.max_retries

    def increment_retry(self):
        """Increment retry counter."""
        self.retry_count += 1


# =============================================================================
# Rate Limiting
# =============================================================================


class RateLimiter:
    """Token bucket rate limiter for API calls."""

    def __init__(
        self,
        rate: float,  # Requests per second
        burst: int = 1,  # Maximum burst size
    ):
        self.rate = rate
        self.burst = burst
        self.tokens = float(burst)
        self.last_update = (
            asyncio.get_event_loop().time() if asyncio.get_event_loop().is_running() else 0
        )
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> float:
        """
        Acquire tokens, waiting if necessary.
        Returns the time waited.
        """
        async with self._lock:
            now = asyncio.get_event_loop().time()

            # Add tokens based on time elapsed
            elapsed = now - self.last_update
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            self.last_update = now

            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0

            # Calculate wait time
            wait_time = (tokens - self.tokens) / self.rate
            await asyncio.sleep(wait_time)

            self.tokens = 0
            self.last_update = asyncio.get_event_loop().time()
            return wait_time


# =============================================================================
# Abstract Base Classes
# =============================================================================


class BaseDataFeed(ABC):
    """Abstract base class for all data feeds."""

    def __init__(self, name: str, source: DataSource):
        self.name = name
        self.source = source
        self.connection = ConnectionManager(name)
        self._event_callbacks: List[EventCallback] = []
        self._async_event_callbacks: List[AsyncEventCallback] = []
        self._subscriptions: set[str] = set()
        self._running = False

    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection to data source."""
        pass

    @abstractmethod
    async def disconnect(self):
        """Disconnect from data source."""
        pass

    @abstractmethod
    async def subscribe(self, symbols: List[str], data_types: List[DataType]):
        """Subscribe to market data for symbols."""
        pass

    @abstractmethod
    async def unsubscribe(self, symbols: List[str]):
        """Unsubscribe from market data."""
        pass

    def on_event(self, callback: EventCallback):
        """Register synchronous event callback."""
        self._event_callbacks.append(callback)

    def on_event_async(self, callback: AsyncEventCallback):
        """Register asynchronous event callback."""
        self._async_event_callbacks.append(callback)

    async def _emit_event(self, event: DataEvent):
        """Emit event to all registered callbacks."""
        # Sync callbacks
        for callback in self._event_callbacks:
            try:
                callback(event)
            except Exception as e:
                logger.error(f"Event callback error: {e}")

        # Async callbacks
        for callback in self._async_event_callbacks:
            try:
                await callback(event)
            except Exception as e:
                logger.error(f"Async event callback error: {e}")

    @property
    def is_connected(self) -> bool:
        return self.connection.state == ConnectionState.CONNECTED

    async def health_check(self) -> Dict[str, Any]:
        """Return health status of the feed."""
        return {
            "name": self.name,
            "source": self.source.value,
            "state": self.connection.state.value,
            "connected_at": self.connection.connected_at,
            "subscriptions": list(self._subscriptions),
            "retry_count": self.connection.retry_count,
        }


class BaseHistoricalDataFeed(BaseDataFeed):
    """Base class for feeds supporting historical data queries."""

    @abstractmethod
    async def get_historical_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "1d",
    ) -> List[NormalizedBar]:
        """Fetch historical OHLCV bars."""
        pass

    @abstractmethod
    async def get_historical_trades(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None,
    ) -> List[NormalizedTick]:
        """Fetch historical trades."""
        pass


class BaseRealtimeDataFeed(BaseDataFeed):
    """Base class for real-time streaming feeds."""

    @abstractmethod
    async def start_streaming(self):
        """Start the real-time data stream."""
        pass

    @abstractmethod
    async def stop_streaming(self):
        """Stop the real-time data stream."""
        pass


# =============================================================================
# Symbol Mapping
# =============================================================================


class SymbolMapper:
    """Maps symbols between different data sources."""

    def __init__(self):
        # Internal format: "ASSET.EXCHANGE" (e.g., "AAPL.NYSE", "BTC.BINANCE")
        self._mappings: Dict[DataSource, Dict[str, str]] = {source: {} for source in DataSource}
        self._reverse_mappings: Dict[DataSource, Dict[str, str]] = {
            source: {} for source in DataSource
        }

    def register_mapping(
        self,
        internal_symbol: str,
        source: DataSource,
        external_symbol: str,
    ):
        """Register a symbol mapping."""
        self._mappings[source][internal_symbol] = external_symbol
        self._reverse_mappings[source][external_symbol] = internal_symbol

    def to_external(self, internal_symbol: str, source: DataSource) -> str:
        """Convert internal symbol to source-specific format."""
        return self._mappings[source].get(internal_symbol, internal_symbol)

    def to_internal(self, external_symbol: str, source: DataSource) -> str:
        """Convert source-specific symbol to internal format."""
        return self._reverse_mappings[source].get(external_symbol, external_symbol)

    def load_crypto_mappings(self):
        """Load common crypto symbol mappings."""
        crypto_mappings = {
            "BTC.USD": {
                DataSource.BINANCE: "BTCUSDT",
                DataSource.COINBASE: "BTC-USD",
            },
            "ETH.USD": {
                DataSource.BINANCE: "ETHUSDT",
                DataSource.COINBASE: "ETH-USD",
            },
            "SOL.USD": {
                DataSource.BINANCE: "SOLUSDT",
                DataSource.COINBASE: "SOL-USD",
            },
        }

        for internal, sources in crypto_mappings.items():
            for source, external in sources.items():
                self.register_mapping(internal, source, external)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Enums
    "DataSource",
    "AssetClass",
    "DataType",
    "ConnectionState",
    # Models
    "NormalizedTick",
    "NormalizedQuote",
    "NormalizedBar",
    "NormalizedOrderBook",
    "DataEvent",
    # Classes
    "ConnectionManager",
    "RateLimiter",
    "BaseDataFeed",
    "BaseHistoricalDataFeed",
    "BaseRealtimeDataFeed",
    "SymbolMapper",
    # Type aliases
    "EventCallback",
    "AsyncEventCallback",
]
