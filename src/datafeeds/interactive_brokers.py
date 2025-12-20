"""
Interactive Brokers Data Feed Integration.
Handles real-time and historical data with proper API pacing and reconnection.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set

from ib_insync import (
    IB,
    Contract,
    Stock,
    Future,
    Option,
    Forex,
    Index,
    BarData,
    Trade as IBTrade,
    Ticker,
    util,
)

from .base import (
    BaseHistoricalDataFeed,
    BaseRealtimeDataFeed,
    ConnectionManager,
    ConnectionState,
    DataEvent,
    DataSource,
    DataType,
    NormalizedBar,
    NormalizedQuote,
    NormalizedTick,
    RateLimiter,
)

logger = logging.getLogger(__name__)


class IBDataFeed(BaseHistoricalDataFeed, BaseRealtimeDataFeed):
    """
    Interactive Brokers data feed with proper pacing and reconnection.

    Features:
    - Automatic reconnection with exponential backoff
    - API pacing to avoid rate limits (50 messages/second)
    - Historical data downloads with pacing limits
    - Real-time market data streaming
    - Contract details and corporate actions
    """

    # IB API limits
    MAX_MESSAGES_PER_SECOND = 45  # Conservative limit (official is 50)
    MAX_HISTORICAL_REQUESTS_PER_10_MIN = 60
    MAX_CONCURRENT_SUBSCRIPTIONS = 100

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7497,  # 7497=TWS Paper, 7496=TWS Live, 4002=Gateway Paper
        client_id: int = 1,
        readonly: bool = False,
        timeout: int = 60,
    ):
        super().__init__(name="InteractiveBrokers", source=DataSource.INTERACTIVE_BROKERS)

        self.host = host
        self.port = port
        self.client_id = client_id
        self.readonly = readonly
        self.timeout = timeout

        # IB connection
        self.ib = IB()

        # Rate limiters
        self._message_limiter = RateLimiter(
            rate=self.MAX_MESSAGES_PER_SECOND,
            burst=10,
        )
        self._historical_limiter = RateLimiter(
            rate=self.MAX_HISTORICAL_REQUESTS_PER_10_MIN / 600,  # Per second
            burst=5,
        )

        # Tracking
        self._contracts: Dict[str, Contract] = {}
        self._tickers: Dict[str, Ticker] = {}
        self._subscription_lock = asyncio.Lock()

        # Set up IB event handlers
        self._setup_event_handlers()

    def _setup_event_handlers(self):
        """Configure IB event callbacks."""
        self.ib.connectedEvent += self._on_connected
        self.ib.disconnectedEvent += self._on_disconnected
        self.ib.errorEvent += self._on_error
        self.ib.pendingTickersEvent += self._on_pending_tickers

    def _on_connected(self):
        """Handle successful connection."""
        logger.info(f"Connected to IB Gateway at {self.host}:{self.port}")
        self.connection.set_state(ConnectionState.CONNECTED)

    def _on_disconnected(self):
        """Handle disconnection."""
        logger.warning("Disconnected from IB Gateway")
        self.connection.set_state(ConnectionState.DISCONNECTED)

        # Schedule reconnection
        if self._running:
            asyncio.create_task(self._reconnect())

    def _on_error(self, reqId: int, errorCode: int, errorString: str, contract: Contract):
        """Handle IB errors."""
        # Error codes reference: https://interactivebrokers.github.io/tws-api/message_codes.html

        # Informational messages (not errors)
        if errorCode in [2104, 2106, 2158]:  # Market data farm messages
            logger.debug(f"IB Info [{errorCode}]: {errorString}")
            return

        # Warnings
        if errorCode in [2103, 2105, 2107]:  # Market data connection issues
            logger.warning(f"IB Warning [{errorCode}]: {errorString}")
            return

        # Pacing violations
        if errorCode == 162:  # Historical data pacing violation
            logger.warning(f"Historical data pacing violation: {errorString}")
            return

        # Connection errors that may require reconnection
        if errorCode in [1100, 1101, 1102]:  # Connectivity issues
            logger.error(f"IB Connection Error [{errorCode}]: {errorString}")
            return

        # Contract-related errors
        if errorCode in [200, 354]:  # No security definition, invalid request
            logger.warning(f"Contract error [{errorCode}] for {contract}: {errorString}")
            return

        # Log other errors
        logger.error(f"IB Error [{errorCode}] reqId={reqId}: {errorString}")

    def _on_pending_tickers(self, tickers: Set[Ticker]):
        """Process pending ticker updates."""
        for ticker in tickers:
            asyncio.create_task(self._process_ticker_update(ticker))

    async def _process_ticker_update(self, ticker: Ticker):
        """Process a ticker update into normalized events."""
        symbol = self._get_symbol_from_contract(ticker.contract)

        # Emit trade tick if last trade updated
        if ticker.last is not None and ticker.lastSize is not None:
            tick = NormalizedTick(
                timestamp=datetime.now(timezone.utc),
                source=DataSource.INTERACTIVE_BROKERS,
                symbol=symbol,
                raw_symbol=ticker.contract.symbol,
                exchange=ticker.contract.exchange or "SMART",
                price=Decimal(str(ticker.last)),
                volume=Decimal(str(ticker.lastSize)),
                side="unknown",  # IB doesn't provide trade direction
            )
            await self._emit_event(
                DataEvent(
                    event_type=DataType.TRADE,
                    data=tick,
                    source=self.source,
                )
            )

        # Emit quote if bid/ask updated
        if ticker.bid is not None and ticker.ask is not None:
            quote = NormalizedQuote(
                timestamp=datetime.now(timezone.utc),
                source=DataSource.INTERACTIVE_BROKERS,
                symbol=symbol,
                raw_symbol=ticker.contract.symbol,
                exchange=ticker.contract.exchange or "SMART",
                bid_price=Decimal(str(ticker.bid)) if ticker.bid > 0 else Decimal("0"),
                bid_size=Decimal(str(ticker.bidSize)) if ticker.bidSize else Decimal("0"),
                ask_price=Decimal(str(ticker.ask)) if ticker.ask > 0 else Decimal("0"),
                ask_size=Decimal(str(ticker.askSize)) if ticker.askSize else Decimal("0"),
            )
            await self._emit_event(
                DataEvent(
                    event_type=DataType.QUOTE,
                    data=quote,
                    source=self.source,
                )
            )

    def _get_symbol_from_contract(self, contract: Contract) -> str:
        """Convert IB contract to internal symbol format."""
        if contract.secType == "STK":
            return f"{contract.symbol}.{contract.primaryExchange or contract.exchange or 'SMART'}"
        elif contract.secType == "FUT":
            return f"{contract.symbol}{contract.lastTradeDateOrContractMonth}.{contract.exchange}"
        elif contract.secType == "OPT":
            return f"{contract.symbol}_{contract.lastTradeDateOrContractMonth}_{contract.strike}_{contract.right}"
        elif contract.secType == "CASH":
            return f"{contract.symbol}.IDEALPRO"
        else:
            return contract.symbol

    def _create_contract(
        self,
        symbol: str,
        sec_type: str = "STK",
        exchange: str = "SMART",
        currency: str = "USD",
        **kwargs,
    ) -> Contract:
        """Create IB contract from symbol."""
        if sec_type == "STK":
            return Stock(symbol, exchange, currency)
        elif sec_type == "FUT":
            return Future(symbol, kwargs.get("expiry", ""), exchange, currency)
        elif sec_type == "OPT":
            return Option(
                symbol,
                kwargs.get("expiry", ""),
                kwargs.get("strike", 0),
                kwargs.get("right", "C"),
                exchange,
                currency=currency,
            )
        elif sec_type == "CASH":
            return Forex(symbol[:3] + symbol[3:])  # e.g., "EURUSD" -> EUR/USD
        elif sec_type == "IND":
            return Index(symbol, exchange, currency)
        else:
            contract = Contract()
            contract.symbol = symbol
            contract.secType = sec_type
            contract.exchange = exchange
            contract.currency = currency
            return contract

    async def connect(self) -> bool:
        """Connect to IB Gateway/TWS."""
        self.connection.set_state(ConnectionState.CONNECTING)

        try:
            await self.ib.connectAsync(
                host=self.host,
                port=self.port,
                clientId=self.client_id,
                readonly=self.readonly,
                timeout=self.timeout,
            )

            self._running = True
            self.connection.set_state(ConnectionState.CONNECTED)

            # Log account info
            accounts = self.ib.managedAccounts()
            logger.info(f"Connected. Accounts: {accounts}")

            return True

        except Exception as e:
            logger.error(f"Failed to connect to IB: {e}")
            self.connection.set_state(ConnectionState.ERROR)
            self.connection.last_error = e
            return False

    async def disconnect(self):
        """Disconnect from IB."""
        self._running = False

        # Cancel all subscriptions
        for ticker in list(self._tickers.values()):
            self.ib.cancelMktData(ticker.contract)

        self._tickers.clear()
        self._subscriptions.clear()

        if self.ib.isConnected():
            self.ib.disconnect()

        self.connection.set_state(ConnectionState.DISCONNECTED)

    async def _reconnect(self):
        """Attempt to reconnect with exponential backoff."""
        if not self._running:
            return

        self.connection.set_state(ConnectionState.RECONNECTING)

        while self.connection.should_retry() and self._running:
            delay = self.connection.get_retry_delay()
            logger.info(f"Reconnecting in {delay:.1f}s (attempt {self.connection.retry_count + 1})")

            await asyncio.sleep(delay)

            if await self.connect():
                # Resubscribe to previous symbols
                if self._subscriptions:
                    await self.subscribe(
                        list(self._subscriptions),
                        [DataType.TRADE, DataType.QUOTE],
                    )
                return

            self.connection.increment_retry()

        logger.error("Max reconnection attempts reached")
        self.connection.set_state(ConnectionState.ERROR)

    async def subscribe(self, symbols: List[str], data_types: List[DataType]):
        """Subscribe to real-time market data."""
        async with self._subscription_lock:
            for symbol in symbols:
                if len(self._subscriptions) >= self.MAX_CONCURRENT_SUBSCRIPTIONS:
                    logger.warning(
                        f"Max subscriptions ({self.MAX_CONCURRENT_SUBSCRIPTIONS}) reached"
                    )
                    break

                if symbol in self._subscriptions:
                    continue

                # Create and qualify contract
                contract = self._create_contract(symbol)

                try:
                    await self._message_limiter.acquire()

                    # Qualify contract to get full details
                    qualified = await self.ib.qualifyContractsAsync(contract)
                    if not qualified:
                        logger.warning(f"Could not qualify contract for {symbol}")
                        continue

                    contract = qualified[0]
                    self._contracts[symbol] = contract

                    # Request market data
                    ticker = self.ib.reqMktData(
                        contract,
                        genericTickList="",  # Empty for default tick types
                        snapshot=False,
                        regulatorySnapshot=False,
                    )

                    self._tickers[symbol] = ticker
                    self._subscriptions.add(symbol)

                    logger.info(f"Subscribed to {symbol}")

                except Exception as e:
                    logger.error(f"Failed to subscribe to {symbol}: {e}")

    async def unsubscribe(self, symbols: List[str]):
        """Unsubscribe from market data."""
        async with self._subscription_lock:
            for symbol in symbols:
                if symbol not in self._subscriptions:
                    continue

                if symbol in self._tickers:
                    ticker = self._tickers[symbol]
                    self.ib.cancelMktData(ticker.contract)
                    del self._tickers[symbol]

                if symbol in self._contracts:
                    del self._contracts[symbol]

                self._subscriptions.discard(symbol)
                logger.info(f"Unsubscribed from {symbol}")

    async def start_streaming(self):
        """Start the event loop for streaming data."""
        logger.info("IB data streaming started")

        while self._running:
            if self.ib.isConnected():
                # Let ib_insync process events
                await asyncio.sleep(0.01)
            else:
                await asyncio.sleep(1.0)

    async def stop_streaming(self):
        """Stop streaming."""
        self._running = False

    async def get_historical_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "1d",
    ) -> List[NormalizedBar]:
        """
        Fetch historical OHLCV data.

        Note: IB has strict pacing limits for historical data:
        - Max 60 requests per 10 minutes
        - Max 1 year of daily data per request
        - Max 1 week of 1-min data per request
        """
        # Map timeframe to IB bar size
        bar_size_map = {
            "1m": "1 min",
            "5m": "5 mins",
            "15m": "15 mins",
            "30m": "30 mins",
            "1h": "1 hour",
            "4h": "4 hours",
            "1d": "1 day",
            "1w": "1 week",
            "1M": "1 month",
        }

        bar_size = bar_size_map.get(timeframe, "1 day")

        # Calculate duration string
        days = (end - start).days
        if days <= 1:
            duration = "1 D"
        elif days <= 7:
            duration = f"{days} D"
        elif days <= 365:
            weeks = days // 7 + 1
            duration = f"{weeks} W"
        else:
            years = days // 365 + 1
            duration = f"{years} Y"

        # Get or create contract
        if symbol not in self._contracts:
            contract = self._create_contract(symbol)
            qualified = await self.ib.qualifyContractsAsync(contract)
            if qualified:
                contract = qualified[0]
                self._contracts[symbol] = contract
            else:
                logger.error(f"Could not qualify contract for {symbol}")
                return []
        else:
            contract = self._contracts[symbol]

        # Apply rate limiting
        await self._historical_limiter.acquire()

        try:
            bars = await self.ib.reqHistoricalDataAsync(
                contract,
                endDateTime=end.strftime("%Y%m%d %H:%M:%S"),
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=True,  # Regular trading hours only
                formatDate=1,
            )

            normalized_bars = []
            for bar in bars:
                normalized_bars.append(
                    NormalizedBar(
                        timestamp=bar.date
                        if isinstance(bar.date, datetime)
                        else datetime.strptime(str(bar.date), "%Y%m%d"),
                        source=DataSource.INTERACTIVE_BROKERS,
                        symbol=symbol,
                        raw_symbol=contract.symbol,
                        exchange=contract.exchange,
                        timeframe=timeframe,
                        open=Decimal(str(bar.open)),
                        high=Decimal(str(bar.high)),
                        low=Decimal(str(bar.low)),
                        close=Decimal(str(bar.close)),
                        volume=Decimal(str(bar.volume)),
                        vwap=Decimal(str(bar.average)) if hasattr(bar, "average") else None,
                        trades=bar.barCount if hasattr(bar, "barCount") else None,
                    )
                )

            logger.info(f"Retrieved {len(normalized_bars)} bars for {symbol}")
            return normalized_bars

        except Exception as e:
            logger.error(f"Failed to get historical data for {symbol}: {e}")
            return []

    async def get_historical_trades(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None,
    ) -> List[NormalizedTick]:
        """
        Fetch historical trades (tick data).
        Note: IB historical tick data is limited and expensive.
        """
        # Get or create contract
        if symbol not in self._contracts:
            contract = self._create_contract(symbol)
            qualified = await self.ib.qualifyContractsAsync(contract)
            if qualified:
                contract = qualified[0]
                self._contracts[symbol] = contract
            else:
                return []
        else:
            contract = self._contracts[symbol]

        await self._historical_limiter.acquire()

        try:
            # IB reqHistoricalTicks returns last/bid/ask ticks
            ticks = await self.ib.reqHistoricalTicksAsync(
                contract,
                startDateTime=start,
                endDateTime=end,
                numberOfTicks=limit or 1000,
                whatToShow="TRADES",
                useRth=True,
            )

            normalized_ticks = []
            for tick in ticks:
                normalized_ticks.append(
                    NormalizedTick(
                        timestamp=tick.time,
                        source=DataSource.INTERACTIVE_BROKERS,
                        symbol=symbol,
                        raw_symbol=contract.symbol,
                        exchange=tick.exchange or contract.exchange,
                        price=Decimal(str(tick.price)),
                        volume=Decimal(str(tick.size)),
                        side="unknown",
                    )
                )

            return normalized_ticks

        except Exception as e:
            logger.error(f"Failed to get historical ticks for {symbol}: {e}")
            return []

    async def get_contract_details(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get detailed contract information."""
        contract = self._create_contract(symbol)

        try:
            await self._message_limiter.acquire()
            details_list = await self.ib.reqContractDetailsAsync(contract)

            if details_list:
                details = details_list[0]
                return {
                    "symbol": details.contract.symbol,
                    "secType": details.contract.secType,
                    "exchange": details.contract.exchange,
                    "currency": details.contract.currency,
                    "longName": details.longName,
                    "industry": details.industry,
                    "category": details.category,
                    "subcategory": details.subcategory,
                    "minTick": details.minTick,
                    "priceMagnifier": details.priceMagnifier,
                    "tradingHours": details.tradingHours,
                    "liquidHours": details.liquidHours,
                }
            return None

        except Exception as e:
            logger.error(f"Failed to get contract details for {symbol}: {e}")
            return None

    async def health_check(self) -> Dict[str, Any]:
        """Extended health check for IB connection."""
        base_health = await super().health_check()

        base_health.update(
            {
                "host": self.host,
                "port": self.port,
                "client_id": self.client_id,
                "is_connected": self.ib.isConnected() if self.ib else False,
                "active_subscriptions": len(self._tickers),
                "pending_tickers": len(self.ib.pendingTickers()) if self.ib else 0,
            }
        )

        return base_health


# Export
__all__ = ["IBDataFeed"]
