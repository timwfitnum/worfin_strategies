"""
Base Data Provider Interface
Abstract base classes for all data feed implementations
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from pydantic import BaseModel

from src.data.models.unified import (
    Asset,
    Bar,
    CorporateAction,
    DataSource,
    MarketDataEvent,
    OrderBook,
    Quote,
    Trade,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Connection States
# =============================================================================


class ConnectionState(str, Enum):
    """Connection state enumeration"""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"


class SubscriptionType(str, Enum):
    """Types of market data subscriptions"""

    TRADES = "trades"
    QUOTES = "quotes"
    BARS = "bars"
    ORDER_BOOK = "order_book"
    ALL = "all"


# =============================================================================
# Data Provider Configuration
# =============================================================================


class ProviderConfig(BaseModel):
    """Base configuration for data providers"""

    name: str
    enabled: bool = True

    # Connection settings
    connect_timeout: int = 30
    read_timeout: int = 60
    max_retries: int = 5
    retry_delay: float = 1.0
    retry_backoff: float = 2.0
    max_retry_delay: float = 60.0

    # Rate limiting
    requests_per_second: float = 10.0
    requests_per_minute: int = 300

    # Reconnection
    auto_reconnect: bool = True
    reconnect_delay: float = 5.0
    max_reconnect_attempts: int = 10

    # Health check
    health_check_interval: int = 30
    stale_data_threshold: int = 60


# =============================================================================
# Callback Types
# =============================================================================

TradeCallback = Callable[[Trade], None]
QuoteCallback = Callable[[Quote], None]
BarCallback = Callable[[Bar], None]
OrderBookCallback = Callable[[OrderBook], None]
EventCallback = Callable[[MarketDataEvent], None]
ErrorCallback = Callable[[Exception], None]
ConnectionCallback = Callable[[ConnectionState], None]


# =============================================================================
# Base Provider Interface
# =============================================================================


class BaseDataProvider(ABC):
    """
    Abstract base class for all data providers.
    Defines the interface that all implementations must follow.
    """

    def __init__(self, config: ProviderConfig):
        self.config = config
        self.source = DataSource.INTERNAL  # Override in subclasses

        # State
        self._state = ConnectionState.DISCONNECTED
        self._connected = False
        self._subscriptions: Dict[str, Set[SubscriptionType]] = {}

        # Callbacks
        self._trade_callbacks: List[TradeCallback] = []
        self._quote_callbacks: List[QuoteCallback] = []
        self._bar_callbacks: List[BarCallback] = []
        self._orderbook_callbacks: List[OrderBookCallback] = []
        self._error_callbacks: List[ErrorCallback] = []
        self._connection_callbacks: List[ConnectionCallback] = []

        # Metrics
        self._last_data_time: Dict[str, datetime] = {}
        self._message_count = 0
        self._error_count = 0
        self._reconnect_count = 0

        # Tasks
        self._tasks: List[asyncio.Task] = []

        logger.info(f"Initialized {self.__class__.__name__} provider")

    # -------------------------------------------------------------------------
    # Abstract Methods - Must be implemented
    # -------------------------------------------------------------------------

    @abstractmethod
    async def connect(self) -> bool:
        """
        Establish connection to the data provider.
        Returns True if successful.
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the data provider."""
        pass

    @abstractmethod
    async def subscribe(
        self,
        symbols: List[str],
        subscription_types: List[SubscriptionType],
    ) -> bool:
        """
        Subscribe to market data for symbols.
        Returns True if successful.
        """
        pass

    @abstractmethod
    async def unsubscribe(
        self,
        symbols: List[str],
        subscription_types: Optional[List[SubscriptionType]] = None,
    ) -> bool:
        """
        Unsubscribe from market data.
        If subscription_types is None, unsubscribe from all types.
        """
        pass

    @abstractmethod
    async def get_historical_bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None,
    ) -> List[Bar]:
        """Fetch historical OHLCV bars."""
        pass

    @abstractmethod
    async def get_historical_trades(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None,
    ) -> List[Trade]:
        """Fetch historical trades."""
        pass

    @abstractmethod
    def normalize_symbol(self, provider_symbol: str) -> str:
        """Convert provider symbol to internal format."""
        pass

    @abstractmethod
    def denormalize_symbol(self, internal_symbol: str) -> str:
        """Convert internal symbol to provider format."""
        pass

    # -------------------------------------------------------------------------
    # Optional Methods - Override as needed
    # -------------------------------------------------------------------------

    async def get_historical_quotes(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None,
    ) -> List[Quote]:
        """Fetch historical quotes (if supported)."""
        raise NotImplementedError(f"{self.__class__.__name__} does not support historical quotes")

    async def get_order_book(
        self,
        symbol: str,
        depth: int = 10,
    ) -> Optional[OrderBook]:
        """Get current order book snapshot (if supported)."""
        raise NotImplementedError(f"{self.__class__.__name__} does not support order book")

    async def get_asset_info(self, symbol: str) -> Optional[Asset]:
        """Get asset/instrument information."""
        raise NotImplementedError(f"{self.__class__.__name__} does not support asset info")

    async def get_corporate_actions(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> List[CorporateAction]:
        """Get corporate actions for a symbol."""
        raise NotImplementedError(f"{self.__class__.__name__} does not support corporate actions")

    async def search_symbols(
        self,
        query: str,
        asset_class: Optional[str] = None,
    ) -> List[Asset]:
        """Search for symbols."""
        raise NotImplementedError(f"{self.__class__.__name__} does not support symbol search")

    # -------------------------------------------------------------------------
    # State Management
    # -------------------------------------------------------------------------

    @property
    def state(self) -> ConnectionState:
        """Get current connection state."""
        return self._state

    @property
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._state == ConnectionState.CONNECTED

    @property
    def subscribed_symbols(self) -> Set[str]:
        """Get set of subscribed symbols."""
        return set(self._subscriptions.keys())

    def _set_state(self, state: ConnectionState) -> None:
        """Update connection state and notify callbacks."""
        if self._state != state:
            logger.info(f"{self.__class__.__name__} state: {self._state.value} -> {state.value}")
            self._state = state
            for callback in self._connection_callbacks:
                try:
                    callback(state)
                except Exception as e:
                    logger.error(f"Error in connection callback: {e}")

    # -------------------------------------------------------------------------
    # Callback Registration
    # -------------------------------------------------------------------------

    def on_trade(self, callback: TradeCallback) -> None:
        """Register trade callback."""
        self._trade_callbacks.append(callback)

    def on_quote(self, callback: QuoteCallback) -> None:
        """Register quote callback."""
        self._quote_callbacks.append(callback)

    def on_bar(self, callback: BarCallback) -> None:
        """Register bar callback."""
        self._bar_callbacks.append(callback)

    def on_orderbook(self, callback: OrderBookCallback) -> None:
        """Register order book callback."""
        self._orderbook_callbacks.append(callback)

    def on_error(self, callback: ErrorCallback) -> None:
        """Register error callback."""
        self._error_callbacks.append(callback)

    def on_connection_change(self, callback: ConnectionCallback) -> None:
        """Register connection state callback."""
        self._connection_callbacks.append(callback)

    # -------------------------------------------------------------------------
    # Event Emission
    # -------------------------------------------------------------------------

    def _emit_trade(self, trade: Trade) -> None:
        """Emit trade to all registered callbacks."""
        self._message_count += 1
        self._last_data_time[trade.symbol] = datetime.utcnow()

        for callback in self._trade_callbacks:
            try:
                callback(trade)
            except Exception as e:
                logger.error(f"Error in trade callback: {e}")

    def _emit_quote(self, quote: Quote) -> None:
        """Emit quote to all registered callbacks."""
        self._message_count += 1
        self._last_data_time[quote.symbol] = datetime.utcnow()

        for callback in self._quote_callbacks:
            try:
                callback(quote)
            except Exception as e:
                logger.error(f"Error in quote callback: {e}")

    def _emit_bar(self, bar: Bar) -> None:
        """Emit bar to all registered callbacks."""
        self._message_count += 1
        self._last_data_time[bar.symbol] = datetime.utcnow()

        for callback in self._bar_callbacks:
            try:
                callback(bar)
            except Exception as e:
                logger.error(f"Error in bar callback: {e}")

    def _emit_orderbook(self, orderbook: OrderBook) -> None:
        """Emit order book to all registered callbacks."""
        self._message_count += 1
        self._last_data_time[orderbook.symbol] = datetime.utcnow()

        for callback in self._orderbook_callbacks:
            try:
                callback(orderbook)
            except Exception as e:
                logger.error(f"Error in orderbook callback: {e}")

    def _emit_error(self, error: Exception) -> None:
        """Emit error to all registered callbacks."""
        self._error_count += 1

        for callback in self._error_callbacks:
            try:
                callback(error)
            except Exception as e:
                logger.error(f"Error in error callback: {e}")

    # -------------------------------------------------------------------------
    # Reconnection Logic
    # -------------------------------------------------------------------------

    async def _reconnect_loop(self) -> None:
        """Handle automatic reconnection."""
        if not self.config.auto_reconnect:
            return

        attempt = 0
        delay = self.config.reconnect_delay

        while attempt < self.config.max_reconnect_attempts:
            attempt += 1
            self._reconnect_count += 1
            self._set_state(ConnectionState.RECONNECTING)

            logger.info(
                f"{self.__class__.__name__} reconnect attempt {attempt}/"
                f"{self.config.max_reconnect_attempts}"
            )

            try:
                if await self.connect():
                    # Resubscribe to all symbols
                    if self._subscriptions:
                        for symbol, types in self._subscriptions.items():
                            await self.subscribe([symbol], list(types))
                    return
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")

            await asyncio.sleep(delay)
            delay = min(delay * self.config.retry_backoff, self.config.max_retry_delay)

        logger.error(f"{self.__class__.__name__} max reconnection attempts reached")
        self._set_state(ConnectionState.ERROR)

    # -------------------------------------------------------------------------
    # Health Monitoring
    # -------------------------------------------------------------------------

    async def _health_check_loop(self) -> None:
        """Monitor connection health."""
        while self.is_connected:
            await asyncio.sleep(self.config.health_check_interval)

            if not self.is_connected:
                break

            # Check for stale data
            now = datetime.utcnow()
            threshold = timedelta(seconds=self.config.stale_data_threshold)

            for symbol, last_time in self._last_data_time.items():
                if now - last_time > threshold:
                    logger.warning(f"Stale data detected for {symbol}")

    def get_metrics(self) -> Dict[str, Any]:
        """Get provider metrics."""
        return {
            "provider": self.__class__.__name__,
            "state": self._state.value,
            "subscribed_symbols": len(self._subscriptions),
            "message_count": self._message_count,
            "error_count": self._error_count,
            "reconnect_count": self._reconnect_count,
            "last_data_times": {k: v.isoformat() for k, v in self._last_data_time.items()},
        }

    # -------------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------------

    async def shutdown(self) -> None:
        """Clean shutdown of the provider."""
        logger.info(f"Shutting down {self.__class__.__name__}")

        # Cancel all tasks
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Disconnect
        await self.disconnect()

        # Clear state
        self._subscriptions.clear()
        self._trade_callbacks.clear()
        self._quote_callbacks.clear()
        self._bar_callbacks.clear()
        self._orderbook_callbacks.clear()
        self._error_callbacks.clear()
        self._connection_callbacks.clear()


# =============================================================================
# Streaming Provider Base
# =============================================================================


class BaseStreamingProvider(BaseDataProvider):
    """
    Base class for streaming (WebSocket) data providers.
    Adds streaming-specific functionality.
    """

    def __init__(self, config: ProviderConfig):
        super().__init__(config)
        self._ws = None
        self._receive_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

    @abstractmethod
    async def _connect_websocket(self) -> Any:
        """Establish WebSocket connection."""
        pass

    @abstractmethod
    async def _handle_message(self, message: Any) -> None:
        """Process incoming WebSocket message."""
        pass

    async def _receive_loop(self) -> None:
        """Main receive loop for WebSocket messages."""
        while self.is_connected and self._ws:
            try:
                message = await self._ws.recv()
                await self._handle_message(message)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"WebSocket receive error: {e}")
                self._emit_error(e)
                if self.config.auto_reconnect:
                    asyncio.create_task(self._reconnect_loop())
                break

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats."""
        # Override in subclasses if needed
        pass


# =============================================================================
# REST Provider Base
# =============================================================================


class BaseRESTProvider(BaseDataProvider):
    """
    Base class for REST API data providers.
    Adds rate limiting and request management.
    """

    def __init__(self, config: ProviderConfig):
        super().__init__(config)
        self._session = None
        self._rate_limiter: Optional[asyncio.Semaphore] = None
        self._request_times: List[datetime] = []

    async def _ensure_rate_limit(self) -> None:
        """Ensure we don't exceed rate limits."""
        now = datetime.utcnow()

        # Clean old request times
        cutoff = now - timedelta(seconds=60)
        self._request_times = [t for t in self._request_times if t > cutoff]

        # Check per-minute limit
        if len(self._request_times) >= self.config.requests_per_minute:
            wait_time = (self._request_times[0] - cutoff).total_seconds()
            if wait_time > 0:
                logger.debug(f"Rate limit reached, waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)

        # Check per-second limit
        recent_cutoff = now - timedelta(seconds=1)
        recent_requests = sum(1 for t in self._request_times if t > recent_cutoff)

        if recent_requests >= self.config.requests_per_second:
            await asyncio.sleep(1.0 / self.config.requests_per_second)

        self._request_times.append(now)

    async def _make_request(
        self,
        method: str,
        url: str,
        **kwargs,
    ) -> Any:
        """Make rate-limited HTTP request."""
        await self._ensure_rate_limit()

        for attempt in range(self.config.max_retries):
            try:
                async with self._session.request(method, url, **kwargs) as response:
                    response.raise_for_status()
                    return await response.json()
            except Exception as e:
                if attempt == self.config.max_retries - 1:
                    raise
                delay = self.config.retry_delay * (self.config.retry_backoff**attempt)
                logger.warning(f"Request failed, retrying in {delay}s: {e}")
                await asyncio.sleep(delay)


# =============================================================================
# Provider Registry
# =============================================================================


class ProviderRegistry:
    """Registry for managing multiple data providers."""

    _providers: Dict[DataSource, BaseDataProvider] = {}

    @classmethod
    def register(cls, source: DataSource, provider: BaseDataProvider) -> None:
        """Register a data provider."""
        cls._providers[source] = provider
        logger.info(f"Registered provider: {source.value}")

    @classmethod
    def get(cls, source: DataSource) -> Optional[BaseDataProvider]:
        """Get a registered provider."""
        return cls._providers.get(source)

    @classmethod
    def get_all(cls) -> Dict[DataSource, BaseDataProvider]:
        """Get all registered providers."""
        return cls._providers.copy()

    @classmethod
    async def connect_all(cls) -> Dict[DataSource, bool]:
        """Connect all registered providers."""
        results = {}
        for source, provider in cls._providers.items():
            try:
                results[source] = await provider.connect()
            except Exception as e:
                logger.error(f"Failed to connect {source.value}: {e}")
                results[source] = False
        return results

    @classmethod
    async def disconnect_all(cls) -> None:
        """Disconnect all registered providers."""
        for provider in cls._providers.values():
            try:
                await provider.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting provider: {e}")

    @classmethod
    async def shutdown_all(cls) -> None:
        """Shutdown all registered providers."""
        for provider in cls._providers.values():
            try:
                await provider.shutdown()
            except Exception as e:
                logger.error(f"Error shutting down provider: {e}")
        cls._providers.clear()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "ConnectionState",
    "SubscriptionType",
    "ProviderConfig",
    "BaseDataProvider",
    "BaseStreamingProvider",
    "BaseRESTProvider",
    "ProviderRegistry",
    "TradeCallback",
    "QuoteCallback",
    "BarCallback",
    "OrderBookCallback",
    "EventCallback",
    "ErrorCallback",
    "ConnectionCallback",
]
