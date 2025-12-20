"""
Interactive Brokers Data Provider
Handles real-time and historical data from IB TWS/Gateway
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from ib_insync import IB, Contract, Stock, Future, Option, Forex, Crypto
from ib_insync import util as ib_util
from ib_insync.ticker import Ticker
from ib_insync.objects import BarData, HistoricalTickBidAsk, HistoricalTickLast

from src.data.models.unified import (
    Asset,
    AssetClass,
    Bar,
    CorporateAction,
    DataSource,
    OrderBook,
    OrderBookLevel,
    Quote,
    Side,
    Trade,
)
from src.data.providers.base import (
    BaseDataProvider,
    ConnectionState,
    ProviderConfig,
    SubscriptionType,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================


class IBConfig(ProviderConfig):
    """Interactive Brokers specific configuration"""

    name: str = "interactive_brokers"

    # Connection
    host: str = "127.0.0.1"
    port: int = 4002  # 4001=live, 4002=paper
    client_id: int = 1
    timeout: int = 60
    readonly: bool = False
    account: Optional[str] = None

    # API limits (IB specific)
    max_requests_per_second: float = 45.0
    max_messages_per_second: int = 50
    pacing_violation_pause: int = 30

    # Market data
    market_data_type: int = 1  # 1=Live, 2=Frozen, 3=Delayed, 4=Delayed-Frozen

    # Historical data
    historical_data_timeout: int = 120
    max_bars_per_request: int = 1000


# =============================================================================
# Symbol Mapping
# =============================================================================


class IBSymbolMapper:
    """Maps between internal symbols and IB contracts"""

    # Common equity mappings (internal -> IB)
    EQUITY_EXCHANGE_MAP = {
        "": "SMART",
        "NYSE": "NYSE",
        "NASDAQ": "NASDAQ",
        "ARCA": "ARCA",
        "BATS": "BATS",
    }

    # Crypto mappings
    CRYPTO_MAP = {
        "BTC-USD": ("BTC", "USD"),
        "ETH-USD": ("ETH", "USD"),
        "LTC-USD": ("LTC", "USD"),
    }

    @classmethod
    def to_contract(
        cls,
        symbol: str,
        asset_class: AssetClass = AssetClass.EQUITY,
        exchange: str = "",
        **kwargs,
    ) -> Contract:
        """Convert internal symbol to IB Contract"""

        if asset_class == AssetClass.EQUITY:
            return Stock(
                symbol=symbol,
                exchange=cls.EQUITY_EXCHANGE_MAP.get(exchange, "SMART"),
                currency=kwargs.get("currency", "USD"),
            )

        elif asset_class == AssetClass.FUTURE:
            return Future(
                symbol=symbol,
                exchange=kwargs.get("exchange", ""),
                lastTradeDateOrContractMonth=kwargs.get("expiry", ""),
                currency=kwargs.get("currency", "USD"),
            )

        elif asset_class == AssetClass.OPTION:
            return Option(
                symbol=symbol,
                lastTradeDateOrContractMonth=kwargs.get("expiry", ""),
                strike=kwargs.get("strike", 0),
                right=kwargs.get("right", "C"),
                exchange=kwargs.get("exchange", "SMART"),
                currency=kwargs.get("currency", "USD"),
            )

        elif asset_class == AssetClass.FOREX:
            parts = symbol.split("/")
            if len(parts) == 2:
                return Forex(pair=f"{parts[0]}{parts[1]}")
            return Forex(symbol=symbol)

        elif asset_class == AssetClass.CRYPTO:
            if symbol in cls.CRYPTO_MAP:
                base, quote = cls.CRYPTO_MAP[symbol]
                return Crypto(symbol=base, exchange="PAXOS", currency=quote)
            return Crypto(symbol=symbol.split("-")[0], exchange="PAXOS", currency="USD")

        else:
            raise ValueError(f"Unsupported asset class: {asset_class}")

    @classmethod
    def from_contract(cls, contract: Contract) -> str:
        """Convert IB Contract to internal symbol"""

        if isinstance(contract, Stock):
            return contract.symbol

        elif isinstance(contract, Future):
            return f"{contract.symbol}{contract.lastTradeDateOrContractMonth}"

        elif isinstance(contract, Option):
            right = "C" if contract.right == "C" else "P"
            return f"{contract.symbol}{contract.lastTradeDateOrContractMonth}{right}{int(contract.strike)}"

        elif isinstance(contract, Forex):
            return f"{contract.symbol[:3]}/{contract.symbol[3:]}"

        elif isinstance(contract, Crypto):
            return f"{contract.symbol}-{contract.currency}"

        return contract.symbol


# =============================================================================
# Interactive Brokers Provider
# =============================================================================


class InteractiveBrokersProvider(BaseDataProvider):
    """
    Interactive Brokers data provider using ib_insync.

    Features:
    - Real-time market data streaming
    - Historical data with pacing management
    - Contract details and corporate actions
    - Automatic reconnection
    """

    def __init__(self, config: IBConfig):
        super().__init__(config)
        self.config: IBConfig = config
        self.source = DataSource.INTERACTIVE_BROKERS

        # IB connection
        self._ib = IB()

        # Contract cache
        self._contracts: Dict[str, Contract] = {}
        self._qualified_contracts: Dict[str, Contract] = {}

        # Ticker subscriptions
        self._tickers: Dict[str, Ticker] = {}

        # Pacing management
        self._historical_request_times: List[datetime] = []
        self._last_pacing_violation: Optional[datetime] = None

        # Register IB callbacks
        self._setup_callbacks()

    def _setup_callbacks(self) -> None:
        """Setup ib_insync event callbacks"""

        self._ib.connectedEvent += self._on_connected
        self._ib.disconnectedEvent += self._on_disconnected
        self._ib.errorEvent += self._on_error
        self._ib.pendingTickersEvent += self._on_pending_tickers

    # -------------------------------------------------------------------------
    # Connection Management
    # -------------------------------------------------------------------------

    async def connect(self) -> bool:
        """Connect to IB Gateway/TWS"""
        try:
            self._set_state(ConnectionState.CONNECTING)

            await self._ib.connectAsync(
                host=self.config.host,
                port=self.config.port,
                clientId=self.config.client_id,
                timeout=self.config.timeout,
                readonly=self.config.readonly,
            )

            # Set market data type
            self._ib.reqMarketDataType(self.config.market_data_type)

            self._set_state(ConnectionState.CONNECTED)
            logger.info(f"Connected to IB Gateway at {self.config.host}:{self.config.port}")

            # Start health check
            self._tasks.append(asyncio.create_task(self._health_check_loop()))

            return True

        except Exception as e:
            logger.error(f"Failed to connect to IB: {e}")
            self._set_state(ConnectionState.ERROR)
            self._emit_error(e)
            return False

    async def disconnect(self) -> None:
        """Disconnect from IB"""
        try:
            if self._ib.isConnected():
                # Cancel all market data subscriptions
                for ticker in self._tickers.values():
                    self._ib.cancelMktData(ticker.contract)

                self._ib.disconnect()

            self._set_state(ConnectionState.DISCONNECTED)
            logger.info("Disconnected from IB Gateway")

        except Exception as e:
            logger.error(f"Error disconnecting from IB: {e}")

    def _on_connected(self) -> None:
        """Handle connection event"""
        self._set_state(ConnectionState.CONNECTED)

    def _on_disconnected(self) -> None:
        """Handle disconnection event"""
        self._set_state(ConnectionState.DISCONNECTED)

        if self.config.auto_reconnect:
            asyncio.create_task(self._reconnect_loop())

    def _on_error(
        self,
        reqId: int,
        errorCode: int,
        errorString: str,
        contract: Contract,
    ) -> None:
        """Handle IB error events"""

        # Pacing violation
        if errorCode == 162:
            logger.warning(f"IB pacing violation: {errorString}")
            self._last_pacing_violation = datetime.utcnow()

        # Data farm connection lost
        elif errorCode in (1100, 1101, 1102):
            logger.warning(f"IB connectivity issue: {errorString}")

        # Market data farm disconnected
        elif errorCode == 2104:
            logger.info(f"IB market data farm connected: {errorString}")

        else:
            logger.error(f"IB error {errorCode}: {errorString}")
            self._error_count += 1

    # -------------------------------------------------------------------------
    # Subscription Management
    # -------------------------------------------------------------------------

    async def subscribe(
        self,
        symbols: List[str],
        subscription_types: List[SubscriptionType],
    ) -> bool:
        """Subscribe to market data for symbols"""
        try:
            for symbol in symbols:
                # Get or qualify contract
                contract = await self._get_qualified_contract(symbol)
                if not contract:
                    logger.error(f"Could not qualify contract for {symbol}")
                    continue

                # Request market data
                ticker = self._ib.reqMktData(
                    contract,
                    genericTickList="",
                    snapshot=False,
                    regulatorySnapshot=False,
                )

                self._tickers[symbol] = ticker

                # Track subscription
                if symbol not in self._subscriptions:
                    self._subscriptions[symbol] = set()
                self._subscriptions[symbol].update(subscription_types)

                logger.info(f"Subscribed to {symbol} market data")

            return True

        except Exception as e:
            logger.error(f"Error subscribing to market data: {e}")
            return False

    async def unsubscribe(
        self,
        symbols: List[str],
        subscription_types: Optional[List[SubscriptionType]] = None,
    ) -> bool:
        """Unsubscribe from market data"""
        try:
            for symbol in symbols:
                if symbol in self._tickers:
                    ticker = self._tickers[symbol]
                    self._ib.cancelMktData(ticker.contract)
                    del self._tickers[symbol]

                if symbol in self._subscriptions:
                    del self._subscriptions[symbol]

                logger.info(f"Unsubscribed from {symbol}")

            return True

        except Exception as e:
            logger.error(f"Error unsubscribing: {e}")
            return False

    def _on_pending_tickers(self, tickers: List[Ticker]) -> None:
        """Handle incoming ticker updates"""
        for ticker in tickers:
            symbol = self.normalize_symbol(ticker.contract.symbol)

            # Emit trade if we have last price
            if ticker.last and ticker.lastSize:
                trade = Trade(
                    symbol=symbol,
                    exchange=ticker.contract.exchange,
                    timestamp=datetime.utcnow(),
                    data_source=self.source,
                    trade_id=f"ib_{symbol}_{datetime.utcnow().timestamp()}",
                    price=ticker.last,
                    volume=ticker.lastSize,
                    side=Side.UNKNOWN,
                )
                self._emit_trade(trade)

            # Emit quote if we have bid/ask
            if ticker.bid and ticker.ask:
                quote = Quote(
                    symbol=symbol,
                    exchange=ticker.contract.exchange,
                    timestamp=datetime.utcnow(),
                    data_source=self.source,
                    bid_price=ticker.bid,
                    bid_size=ticker.bidSize or 0,
                    ask_price=ticker.ask,
                    ask_size=ticker.askSize or 0,
                )
                self._emit_quote(quote)

    # -------------------------------------------------------------------------
    # Historical Data
    # -------------------------------------------------------------------------

    async def get_historical_bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None,
    ) -> List[Bar]:
        """Fetch historical OHLCV bars from IB"""

        # Pacing check
        await self._check_historical_pacing()

        contract = await self._get_qualified_contract(symbol)
        if not contract:
            return []

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
        }

        bar_size = bar_size_map.get(timeframe, "1 min")

        # Calculate duration
        duration = self._calculate_duration(start, end, timeframe)

        try:
            bars = await asyncio.wait_for(
                self._ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime=end,
                    durationStr=duration,
                    barSizeSetting=bar_size,
                    whatToShow="TRADES",
                    useRTH=False,
                    formatDate=2,
                ),
                timeout=self.config.historical_data_timeout,
            )

            self._historical_request_times.append(datetime.utcnow())

            # Convert to our Bar format
            result = []
            for bar in bars:
                result.append(
                    Bar(
                        symbol=symbol,
                        exchange=contract.exchange,
                        timestamp=bar.date,
                        timeframe=timeframe,
                        open=bar.open,
                        high=bar.high,
                        low=bar.low,
                        close=bar.close,
                        volume=bar.volume,
                        trades=bar.barCount or 0,
                        vwap=bar.average or 0,
                        data_source=self.source,
                    )
                )

            logger.info(f"Retrieved {len(result)} bars for {symbol}")
            return result

        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching historical data for {symbol}")
            return []
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            return []

    async def get_historical_trades(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None,
    ) -> List[Trade]:
        """Fetch historical trades (tick data)"""

        await self._check_historical_pacing()

        contract = await self._get_qualified_contract(symbol)
        if not contract:
            return []

        try:
            ticks = await asyncio.wait_for(
                self._ib.reqHistoricalTicksAsync(
                    contract,
                    startDateTime=start,
                    endDateTime=end,
                    numberOfTicks=limit or 1000,
                    whatToShow="TRADES",
                    useRth=False,
                ),
                timeout=self.config.historical_data_timeout,
            )

            self._historical_request_times.append(datetime.utcnow())

            result = []
            for tick in ticks:
                result.append(
                    Trade(
                        symbol=symbol,
                        exchange=contract.exchange,
                        timestamp=tick.time,
                        data_source=self.source,
                        trade_id=f"ib_hist_{symbol}_{tick.time.timestamp()}",
                        price=tick.price,
                        volume=tick.size,
                        side=Side.UNKNOWN,
                    )
                )

            logger.info(f"Retrieved {len(result)} historical trades for {symbol}")
            return result

        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching historical trades for {symbol}")
            return []
        except Exception as e:
            logger.error(f"Error fetching historical trades: {e}")
            return []

    async def _check_historical_pacing(self) -> None:
        """Check and enforce IB historical data pacing limits"""

        # If we had a pacing violation recently, wait
        if self._last_pacing_violation:
            elapsed = (datetime.utcnow() - self._last_pacing_violation).total_seconds()
            if elapsed < self.config.pacing_violation_pause:
                wait_time = self.config.pacing_violation_pause - elapsed
                logger.info(f"Pacing violation cooldown, waiting {wait_time:.0f}s")
                await asyncio.sleep(wait_time)
                self._last_pacing_violation = None

        # IB limit: 60 requests per 10 minutes
        now = datetime.utcnow()
        cutoff = now - timedelta(minutes=10)
        self._historical_request_times = [t for t in self._historical_request_times if t > cutoff]

        if len(self._historical_request_times) >= 55:  # Leave some buffer
            wait_time = (self._historical_request_times[0] - cutoff).total_seconds() + 5
            logger.info(f"Approaching pacing limit, waiting {wait_time:.0f}s")
            await asyncio.sleep(wait_time)

    def _calculate_duration(
        self,
        start: datetime,
        end: datetime,
        timeframe: str,
    ) -> str:
        """Calculate IB duration string"""
        delta = end - start

        if delta.days > 365:
            years = delta.days // 365
            return f"{min(years, 20)} Y"
        elif delta.days > 30:
            months = delta.days // 30
            return f"{min(months, 12)} M"
        elif delta.days > 0:
            return f"{min(delta.days, 365)} D"
        else:
            hours = delta.seconds // 3600
            return f"{max(hours, 1)} H"

    # -------------------------------------------------------------------------
    # Contract Management
    # -------------------------------------------------------------------------

    async def _get_qualified_contract(
        self,
        symbol: str,
        asset_class: AssetClass = AssetClass.EQUITY,
    ) -> Optional[Contract]:
        """Get a qualified IB contract"""

        cache_key = f"{symbol}_{asset_class.value}"

        if cache_key in self._qualified_contracts:
            return self._qualified_contracts[cache_key]

        try:
            contract = IBSymbolMapper.to_contract(symbol, asset_class)
            qualified = await self._ib.qualifyContractsAsync(contract)

            if qualified:
                self._qualified_contracts[cache_key] = qualified[0]
                return qualified[0]

            logger.warning(f"Could not qualify contract for {symbol}")
            return None

        except Exception as e:
            logger.error(f"Error qualifying contract {symbol}: {e}")
            return None

    async def get_asset_info(self, symbol: str) -> Optional[Asset]:
        """Get contract details from IB"""

        contract = await self._get_qualified_contract(symbol)
        if not contract:
            return None

        try:
            details = await self._ib.reqContractDetailsAsync(contract)
            if not details:
                return None

            detail = details[0]

            return Asset(
                symbol=symbol,
                name=detail.longName,
                asset_class=AssetClass.EQUITY,  # Simplified
                exchange=contract.exchange,
                primary_exchange=contract.primaryExchange,
                currency=contract.currency,
                multiplier=float(contract.multiplier) if contract.multiplier else 1.0,
                tick_size=detail.minTick,
                ib_con_id=contract.conId,
                ib_symbol=contract.symbol,
                sector=detail.category,
                industry=detail.industry,
                is_active=True,
                is_tradeable=detail.marketRuleIds is not None,
            )

        except Exception as e:
            logger.error(f"Error getting asset info for {symbol}: {e}")
            return None

    # -------------------------------------------------------------------------
    # Symbol Normalization
    # -------------------------------------------------------------------------

    def normalize_symbol(self, provider_symbol: str) -> str:
        """Convert IB symbol to internal format"""
        return provider_symbol.upper()

    def denormalize_symbol(self, internal_symbol: str) -> str:
        """Convert internal symbol to IB format"""
        return internal_symbol.upper()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "IBConfig",
    "IBSymbolMapper",
    "InteractiveBrokersProvider",
]
