"""
Binance Exchange Data Feed Integration.
WebSocket streams for trades, quotes, and order book with REST API fallback.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed

from ..base import (
    BaseHistoricalDataFeed,
    BaseRealtimeDataFeed,
    ConnectionState,
    DataEvent,
    DataSource,
    DataType,
    NormalizedBar,
    NormalizedOrderBook,
    NormalizedQuote,
    NormalizedTick,
    RateLimiter,
)

logger = logging.getLogger(__name__)


class BinanceDataFeed(BaseHistoricalDataFeed, BaseRealtimeDataFeed):
    """
    Binance exchange data feed.

    Features:
    - Real-time trades, quotes, and order book via WebSocket
    - Historical klines/OHLCV via REST API
    - Funding rate data for perpetual futures
    - Automatic reconnection and stream management
    """

    # Endpoints
    SPOT_REST_URL = "https://api.binance.com"
    SPOT_WS_URL = "wss://stream.binance.com:9443/ws"
    FUTURES_REST_URL = "https://fapi.binance.com"
    FUTURES_WS_URL = "wss://fstream.binance.com/ws"

    # Testnet endpoints
    TESTNET_REST_URL = "https://testnet.binance.vision"
    TESTNET_WS_URL = "wss://testnet.binance.vision/ws"

    # Rate limits (conservative)
    MAX_REQUESTS_PER_MINUTE = 1200
    MAX_WS_STREAMS = 1024

    def __init__(
        self,
        api_key: str = "",
        api_secret: str = "",
        testnet: bool = True,
        use_futures: bool = False,
    ):
        super().__init__(name="Binance", source=DataSource.BINANCE)

        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.use_futures = use_futures

        # Set URLs based on configuration
        if testnet:
            self.rest_url = self.TESTNET_REST_URL
            self.ws_url = self.TESTNET_WS_URL
        elif use_futures:
            self.rest_url = self.FUTURES_REST_URL
            self.ws_url = self.FUTURES_WS_URL
        else:
            self.rest_url = self.SPOT_REST_URL
            self.ws_url = self.SPOT_WS_URL

        # Rate limiter
        self._rate_limiter = RateLimiter(
            rate=self.MAX_REQUESTS_PER_MINUTE / 60,
            burst=20,
        )

        # WebSocket connections
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_streams: Dict[str, str] = {}  # symbol -> stream_name

        # HTTP session
        self._session: Optional[aiohttp.ClientSession] = None

        # Order book cache for incremental updates
        self._orderbooks: Dict[str, NormalizedOrderBook] = {}

    def _sign_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Sign request with HMAC SHA256."""
        if not self.api_secret:
            return params

        params["timestamp"] = int(time.time() * 1000)
        query_string = urlencode(params)
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        params["signature"] = signature
        return params

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            headers = {}
            if self.api_key:
                headers["X-MBX-APIKEY"] = self.api_key
            self._session = aiohttp.ClientSession(
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
            )
        return self._session

    async def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Make rate-limited REST API request."""
        await self._rate_limiter.acquire()

        session = await self._get_session()
        url = f"{self.rest_url}{endpoint}"

        params = params or {}
        if signed:
            params = self._sign_request(params)

        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:
                    logger.warning("Rate limit exceeded")
                    retry_after = int(response.headers.get("Retry-After", 60))
                    await asyncio.sleep(retry_after)
                    return await self._make_request(endpoint, params, signed)
                else:
                    error_text = await response.text()
                    logger.error(f"API error {response.status}: {error_text}")
                    return None
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return None

    async def connect(self) -> bool:
        """Connect to Binance WebSocket."""
        self.connection.set_state(ConnectionState.CONNECTING)

        try:
            self._ws = await websockets.connect(
                self.ws_url,
                ping_interval=20,
                ping_timeout=10,
            )

            self._running = True
            self.connection.set_state(ConnectionState.CONNECTED)
            logger.info(f"Connected to Binance WebSocket ({self.ws_url})")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to Binance: {e}")
            self.connection.set_state(ConnectionState.ERROR)
            self.connection.last_error = e
            return False

    async def disconnect(self):
        """Disconnect from Binance."""
        self._running = False

        if self._ws:
            await self._ws.close()
            self._ws = None

        if self._session:
            await self._session.close()
            self._session = None

        self._subscriptions.clear()
        self._ws_streams.clear()
        self._orderbooks.clear()
        self.connection.set_state(ConnectionState.DISCONNECTED)

    async def subscribe(self, symbols: List[str], data_types: List[DataType]):
        """Subscribe to real-time streams."""
        if not self._ws:
            logger.error("WebSocket not connected")
            return

        streams = []
        for symbol in symbols:
            symbol_lower = symbol.lower()

            if DataType.TRADE in data_types:
                stream = f"{symbol_lower}@trade"
                streams.append(stream)
                self._ws_streams[symbol] = stream

            if DataType.QUOTE in data_types:
                stream = f"{symbol_lower}@bookTicker"
                streams.append(stream)

            if DataType.ORDERBOOK in data_types:
                stream = f"{symbol_lower}@depth20@100ms"
                streams.append(stream)

        if streams:
            # Binance uses SUBSCRIBE method
            sub_msg = {
                "method": "SUBSCRIBE",
                "params": streams,
                "id": int(time.time() * 1000),
            }
            await self._ws.send(json.dumps(sub_msg))
            self._subscriptions.update(symbols)
            logger.info(f"Subscribed to {len(streams)} Binance streams")

    async def unsubscribe(self, symbols: List[str]):
        """Unsubscribe from streams."""
        if not self._ws:
            return

        streams = []
        for symbol in symbols:
            symbol_lower = symbol.lower()
            streams.extend(
                [
                    f"{symbol_lower}@trade",
                    f"{symbol_lower}@bookTicker",
                    f"{symbol_lower}@depth20@100ms",
                ]
            )

        if streams:
            unsub_msg = {
                "method": "UNSUBSCRIBE",
                "params": streams,
                "id": int(time.time() * 1000),
            }
            await self._ws.send(json.dumps(unsub_msg))
            self._subscriptions.difference_update(symbols)

    async def start_streaming(self):
        """Start WebSocket message processing."""
        logger.info("Binance streaming started")

        while self._running and self._ws:
            try:
                message = await asyncio.wait_for(self._ws.recv(), timeout=30)
                await self._process_message(message)

            except asyncio.TimeoutError:
                continue

            except ConnectionClosed:
                logger.warning("Binance WebSocket closed")
                if self._running:
                    await self._reconnect()
                break

            except Exception as e:
                logger.error(f"Streaming error: {e}")
                await asyncio.sleep(1)

    async def stop_streaming(self):
        """Stop streaming."""
        self._running = False

    async def _process_message(self, message: str):
        """Process incoming WebSocket message."""
        try:
            data = json.loads(message)

            # Handle subscription responses
            if "result" in data:
                return

            event_type = data.get("e")

            if event_type == "trade":
                tick = self._parse_trade(data)
                await self._emit_event(
                    DataEvent(
                        event_type=DataType.TRADE,
                        data=tick,
                        source=self.source,
                    )
                )

            elif event_type == "bookTicker":
                quote = self._parse_book_ticker(data)
                await self._emit_event(
                    DataEvent(
                        event_type=DataType.QUOTE,
                        data=quote,
                        source=self.source,
                    )
                )

            elif event_type == "depthUpdate":
                orderbook = self._parse_depth_update(data)
                await self._emit_event(
                    DataEvent(
                        event_type=DataType.ORDERBOOK,
                        data=orderbook,
                        source=self.source,
                    )
                )

        except Exception as e:
            logger.error(f"Failed to process message: {e}")

    def _parse_trade(self, data: Dict) -> NormalizedTick:
        """Parse trade message."""
        return NormalizedTick(
            timestamp=datetime.fromtimestamp(data["T"] / 1000, tz=timezone.utc),
            source=DataSource.BINANCE,
            symbol=data["s"],
            raw_symbol=data["s"],
            exchange="BINANCE",
            price=Decimal(data["p"]),
            volume=Decimal(data["q"]),
            side="sell" if data["m"] else "buy",  # m=True means buyer is market maker (sell)
            trade_id=str(data["t"]),
        )

    def _parse_book_ticker(self, data: Dict) -> NormalizedQuote:
        """Parse book ticker (best bid/ask)."""
        return NormalizedQuote(
            timestamp=datetime.now(timezone.utc),
            source=DataSource.BINANCE,
            symbol=data["s"],
            raw_symbol=data["s"],
            exchange="BINANCE",
            bid_price=Decimal(data["b"]),
            bid_size=Decimal(data["B"]),
            ask_price=Decimal(data["a"]),
            ask_size=Decimal(data["A"]),
        )

    def _parse_depth_update(self, data: Dict) -> NormalizedOrderBook:
        """Parse depth snapshot."""
        symbol = data["s"]

        bids = [(Decimal(price), Decimal(qty)) for price, qty in data.get("b", [])]
        asks = [(Decimal(price), Decimal(qty)) for price, qty in data.get("a", [])]

        return NormalizedOrderBook(
            timestamp=datetime.fromtimestamp(data["E"] / 1000, tz=timezone.utc),
            source=DataSource.BINANCE,
            symbol=symbol,
            exchange="BINANCE",
            bids=sorted(bids, key=lambda x: x[0], reverse=True),
            asks=sorted(asks, key=lambda x: x[0]),
            sequence_num=data.get("u"),
        )

    async def _reconnect(self):
        """Reconnect to WebSocket."""
        if not self._running:
            return

        self.connection.set_state(ConnectionState.RECONNECTING)

        while self.connection.should_retry() and self._running:
            delay = self.connection.get_retry_delay()
            logger.info(f"Reconnecting in {delay:.1f}s")

            await asyncio.sleep(delay)

            if await self.connect():
                if self._subscriptions:
                    await self.subscribe(
                        list(self._subscriptions),
                        [DataType.TRADE, DataType.QUOTE],
                    )
                return

            self.connection.increment_retry()

        self.connection.set_state(ConnectionState.ERROR)

    async def get_historical_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "1d",
    ) -> List[NormalizedBar]:
        """Fetch historical klines."""
        # Map timeframe to Binance interval
        interval_map = {
            "1m": "1m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1h": "1h",
            "4h": "4h",
            "1d": "1d",
            "1w": "1w",
            "1M": "1M",
        }

        interval = interval_map.get(timeframe, "1d")

        endpoint = "/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": int(start.timestamp() * 1000),
            "endTime": int(end.timestamp() * 1000),
            "limit": 1000,
        }

        result = await self._make_request(endpoint, params)

        if not result:
            return []

        bars = []
        for kline in result:
            bars.append(
                NormalizedBar(
                    timestamp=datetime.fromtimestamp(kline[0] / 1000, tz=timezone.utc),
                    source=DataSource.BINANCE,
                    symbol=symbol,
                    raw_symbol=symbol,
                    exchange="BINANCE",
                    timeframe=timeframe,
                    open=Decimal(kline[1]),
                    high=Decimal(kline[2]),
                    low=Decimal(kline[3]),
                    close=Decimal(kline[4]),
                    volume=Decimal(kline[5]),
                    trades=int(kline[8]),
                )
            )

        logger.info(f"Retrieved {len(bars)} bars for {symbol}")
        return bars

    async def get_historical_trades(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None,
    ) -> List[NormalizedTick]:
        """Fetch historical trades."""
        endpoint = "/api/v3/trades"
        params = {
            "symbol": symbol,
            "limit": min(limit or 1000, 1000),
        }

        result = await self._make_request(endpoint, params)

        if not result:
            return []

        ticks = []
        for trade in result:
            ticks.append(
                NormalizedTick(
                    timestamp=datetime.fromtimestamp(trade["time"] / 1000, tz=timezone.utc),
                    source=DataSource.BINANCE,
                    symbol=symbol,
                    raw_symbol=symbol,
                    exchange="BINANCE",
                    price=Decimal(trade["price"]),
                    volume=Decimal(trade["qty"]),
                    side="sell" if trade["isBuyerMaker"] else "buy",
                    trade_id=str(trade["id"]),
                )
            )

        return ticks

    async def get_funding_rate(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get current funding rate for perpetual futures."""
        if not self.use_futures:
            self.rest_url = self.FUTURES_REST_URL

        endpoint = "/fapi/v1/fundingRate"
        params = {"symbol": symbol, "limit": 1}

        result = await self._make_request(endpoint, params)

        if result and len(result) > 0:
            return {
                "symbol": result[0]["symbol"],
                "funding_rate": Decimal(result[0]["fundingRate"]),
                "funding_time": datetime.fromtimestamp(
                    result[0]["fundingTime"] / 1000, tz=timezone.utc
                ),
            }
        return None

    async def get_exchange_info(self) -> Optional[Dict[str, Any]]:
        """Get exchange trading rules and symbol info."""
        endpoint = "/api/v3/exchangeInfo"
        return await self._make_request(endpoint)

    async def get_ticker_24h(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get 24-hour price change statistics."""
        endpoint = "/api/v3/ticker/24hr"
        params = {"symbol": symbol}
        return await self._make_request(endpoint, params)


# Export
__all__ = ["BinanceDataFeed"]
