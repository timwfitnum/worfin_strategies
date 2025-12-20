"""
Coinbase Exchange Data Feed Integration.
WebSocket streams for trades, quotes, and order book with REST API for historical data.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

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


class CoinbaseDataFeed(BaseHistoricalDataFeed, BaseRealtimeDataFeed):
    """
    Coinbase Exchange data feed.

    Features:
    - Real-time trades, ticker, and order book via WebSocket
    - Historical candles via REST API
    - Authentication for private channels
    - Automatic reconnection and heartbeat handling
    """

    # Endpoints
    REST_URL = "https://api.exchange.coinbase.com"
    WS_URL = "wss://ws-feed.exchange.coinbase.com"

    # Sandbox endpoints for testing
    SANDBOX_REST_URL = "https://api-public.sandbox.exchange.coinbase.com"
    SANDBOX_WS_URL = "wss://ws-feed-public.sandbox.exchange.coinbase.com"

    # Rate limits
    MAX_REQUESTS_PER_SECOND = 10

    def __init__(
        self,
        api_key: str = "",
        api_secret: str = "",
        passphrase: str = "",
        sandbox: bool = True,
    ):
        super().__init__(name="Coinbase", source=DataSource.COINBASE)

        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.sandbox = sandbox

        # Set URLs
        if sandbox:
            self.rest_url = self.SANDBOX_REST_URL
            self.ws_url = self.SANDBOX_WS_URL
        else:
            self.rest_url = self.REST_URL
            self.ws_url = self.WS_URL

        # Rate limiter
        self._rate_limiter = RateLimiter(
            rate=self.MAX_REQUESTS_PER_SECOND,
            burst=5,
        )

        # WebSocket
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

        # HTTP session
        self._session: Optional[aiohttp.ClientSession] = None

        # Order book state
        self._orderbooks: Dict[str, Dict] = {}

        # Channel subscriptions
        self._channels: Dict[str, List[str]] = {}

    def _sign_request(
        self,
        timestamp: str,
        method: str,
        path: str,
        body: str = "",
    ) -> Dict[str, str]:
        """Generate authentication headers."""
        if not self.api_secret:
            return {}

        message = timestamp + method.upper() + path + body
        hmac_key = base64.b64decode(self.api_secret)
        signature = hmac.new(
            hmac_key,
            message.encode("utf-8"),
            hashlib.sha256,
        )
        signature_b64 = base64.b64encode(signature.digest()).decode("utf-8")

        return {
            "CB-ACCESS-KEY": self.api_key,
            "CB-ACCESS-SIGN": signature_b64,
            "CB-ACCESS-TIMESTAMP": timestamp,
            "CB-ACCESS-PASSPHRASE": self.passphrase,
        }

    def _get_ws_auth(self) -> Dict[str, Any]:
        """Generate WebSocket authentication message."""
        if not self.api_secret:
            return {}

        timestamp = str(time.time())
        message = timestamp + "GET" + "/users/self/verify"
        hmac_key = base64.b64decode(self.api_secret)
        signature = hmac.new(
            hmac_key,
            message.encode("utf-8"),
            hashlib.sha256,
        )
        signature_b64 = base64.b64encode(signature.digest()).decode("utf-8")

        return {
            "signature": signature_b64,
            "key": self.api_key,
            "passphrase": self.passphrase,
            "timestamp": timestamp,
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
            )
        return self._session

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Optional[Any]:
        """Make rate-limited REST API request."""
        await self._rate_limiter.acquire()

        session = await self._get_session()
        url = f"{self.rest_url}{endpoint}"

        timestamp = str(time.time())
        body_str = json.dumps(body) if body else ""

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        headers.update(self._sign_request(timestamp, method, endpoint, body_str))

        try:
            if method.upper() == "GET":
                async with session.get(url, params=params, headers=headers) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:
                        logger.warning("Rate limit exceeded")
                        await asyncio.sleep(1)
                        return await self._make_request(method, endpoint, params, body)
                    else:
                        logger.error(f"API error {response.status}: {await response.text()}")
                        return None
            elif method.upper() == "POST":
                async with session.post(url, json=body, headers=headers) as response:
                    if response.status in [200, 201]:
                        return await response.json()
                    else:
                        logger.error(f"API error {response.status}: {await response.text()}")
                        return None
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return None

    async def connect(self) -> bool:
        """Connect to Coinbase WebSocket."""
        self.connection.set_state(ConnectionState.CONNECTING)

        try:
            self._ws = await websockets.connect(
                self.ws_url,
                ping_interval=30,
                ping_timeout=10,
            )

            self._running = True
            self.connection.set_state(ConnectionState.CONNECTED)
            logger.info(f"Connected to Coinbase WebSocket ({self.ws_url})")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to Coinbase: {e}")
            self.connection.set_state(ConnectionState.ERROR)
            self.connection.last_error = e
            return False

    async def disconnect(self):
        """Disconnect from Coinbase."""
        self._running = False

        if self._ws:
            await self._ws.close()
            self._ws = None

        if self._session:
            await self._session.close()
            self._session = None

        self._subscriptions.clear()
        self._channels.clear()
        self._orderbooks.clear()
        self.connection.set_state(ConnectionState.DISCONNECTED)

    async def subscribe(self, symbols: List[str], data_types: List[DataType]):
        """Subscribe to real-time channels."""
        if not self._ws:
            logger.error("WebSocket not connected")
            return

        # Map symbols to Coinbase product_ids
        product_ids = [self._to_product_id(s) for s in symbols]

        channels = []
        if DataType.TRADE in data_types:
            channels.append("matches")
        if DataType.QUOTE in data_types:
            channels.append("ticker")
        if DataType.ORDERBOOK in data_types:
            channels.append("level2")

        # Build subscribe message
        sub_msg: Dict[str, Any] = {
            "type": "subscribe",
            "product_ids": product_ids,
            "channels": channels,
        }

        # Add authentication if available
        auth = self._get_ws_auth()
        if auth:
            sub_msg.update(auth)

        await self._ws.send(json.dumps(sub_msg))

        self._subscriptions.update(symbols)
        for symbol in symbols:
            self._channels[symbol] = channels

        logger.info(f"Subscribed to {len(product_ids)} Coinbase products")

    async def unsubscribe(self, symbols: List[str]):
        """Unsubscribe from channels."""
        if not self._ws:
            return

        product_ids = [self._to_product_id(s) for s in symbols]

        for symbol in symbols:
            channels = self._channels.get(symbol, [])
            if channels:
                unsub_msg = {
                    "type": "unsubscribe",
                    "product_ids": product_ids,
                    "channels": channels,
                }
                await self._ws.send(json.dumps(unsub_msg))

                self._subscriptions.discard(symbol)
                self._channels.pop(symbol, None)

    async def start_streaming(self):
        """Start WebSocket message processing."""
        logger.info("Coinbase streaming started")

        while self._running and self._ws:
            try:
                message = await asyncio.wait_for(self._ws.recv(), timeout=60)
                await self._process_message(message)

            except asyncio.TimeoutError:
                # Send heartbeat subscription to keep connection alive
                continue

            except ConnectionClosed:
                logger.warning("Coinbase WebSocket closed")
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
            msg_type = data.get("type")

            if msg_type == "subscriptions":
                logger.debug(f"Subscription confirmed: {data}")

            elif msg_type == "ticker":
                quote = self._parse_ticker(data)
                await self._emit_event(
                    DataEvent(
                        event_type=DataType.QUOTE,
                        data=quote,
                        source=self.source,
                    )
                )

            elif msg_type in ["match", "last_match"]:
                tick = self._parse_match(data)
                await self._emit_event(
                    DataEvent(
                        event_type=DataType.TRADE,
                        data=tick,
                        source=self.source,
                    )
                )

            elif msg_type == "snapshot":
                self._handle_l2_snapshot(data)

            elif msg_type == "l2update":
                orderbook = self._handle_l2_update(data)
                if orderbook:
                    await self._emit_event(
                        DataEvent(
                            event_type=DataType.ORDERBOOK,
                            data=orderbook,
                            source=self.source,
                        )
                    )

            elif msg_type == "heartbeat":
                pass  # Ignore heartbeats

            elif msg_type == "error":
                logger.error(f"Coinbase error: {data.get('message')}")

        except Exception as e:
            logger.error(f"Failed to process message: {e}")

    def _to_product_id(self, symbol: str) -> str:
        """Convert internal symbol to Coinbase product_id."""
        # Handle different formats: BTCUSD -> BTC-USD
        if "-" not in symbol and len(symbol) >= 6:
            return f"{symbol[:3]}-{symbol[3:]}"
        return symbol

    def _from_product_id(self, product_id: str) -> str:
        """Convert Coinbase product_id to internal symbol."""
        return product_id.replace("-", "")

    def _parse_ticker(self, data: Dict) -> NormalizedQuote:
        """Parse ticker message."""
        return NormalizedQuote(
            timestamp=datetime.fromisoformat(data["time"].replace("Z", "+00:00")),
            source=DataSource.COINBASE,
            symbol=self._from_product_id(data["product_id"]),
            raw_symbol=data["product_id"],
            exchange="COINBASE",
            bid_price=Decimal(data["best_bid"]) if data.get("best_bid") else Decimal("0"),
            bid_size=Decimal(data.get("best_bid_size", "0")),
            ask_price=Decimal(data["best_ask"]) if data.get("best_ask") else Decimal("0"),
            ask_size=Decimal(data.get("best_ask_size", "0")),
        )

    def _parse_match(self, data: Dict) -> NormalizedTick:
        """Parse match (trade) message."""
        return NormalizedTick(
            timestamp=datetime.fromisoformat(data["time"].replace("Z", "+00:00")),
            source=DataSource.COINBASE,
            symbol=self._from_product_id(data["product_id"]),
            raw_symbol=data["product_id"],
            exchange="COINBASE",
            price=Decimal(data["price"]),
            volume=Decimal(data["size"]),
            side=data["side"],
            trade_id=str(data["trade_id"]),
        )

    def _handle_l2_snapshot(self, data: Dict):
        """Handle L2 order book snapshot."""
        product_id = data["product_id"]
        symbol = self._from_product_id(product_id)

        self._orderbooks[symbol] = {
            "bids": {Decimal(price): Decimal(size) for price, size in data.get("bids", [])},
            "asks": {Decimal(price): Decimal(size) for price, size in data.get("asks", [])},
        }

        logger.debug(f"L2 snapshot received for {symbol}")

    def _handle_l2_update(self, data: Dict) -> Optional[NormalizedOrderBook]:
        """Handle L2 order book update."""
        product_id = data["product_id"]
        symbol = self._from_product_id(product_id)

        if symbol not in self._orderbooks:
            return None

        book = self._orderbooks[symbol]

        for change in data.get("changes", []):
            side, price, size = change
            price = Decimal(price)
            size = Decimal(size)

            book_side = "bids" if side == "buy" else "asks"

            if size == 0:
                book[book_side].pop(price, None)
            else:
                book[book_side][price] = size

        # Convert to normalized format
        bids = sorted(book["bids"].items(), key=lambda x: x[0], reverse=True)[:20]
        asks = sorted(book["asks"].items(), key=lambda x: x[0])[:20]

        return NormalizedOrderBook(
            timestamp=datetime.fromisoformat(data["time"].replace("Z", "+00:00")),
            source=DataSource.COINBASE,
            symbol=symbol,
            exchange="COINBASE",
            bids=[(p, s) for p, s in bids],
            asks=[(p, s) for p, s in asks],
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
        """Fetch historical candles."""
        product_id = self._to_product_id(symbol)

        # Map timeframe to granularity (seconds)
        granularity_map = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "1h": 3600,
            "6h": 21600,
            "1d": 86400,
        }

        granularity = granularity_map.get(timeframe, 86400)

        endpoint = f"/products/{product_id}/candles"
        params = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "granularity": granularity,
        }

        result = await self._make_request("GET", endpoint, params)

        if not result:
            return []

        bars = []
        # Coinbase returns: [timestamp, low, high, open, close, volume]
        for candle in result:
            bars.append(
                NormalizedBar(
                    timestamp=datetime.fromtimestamp(candle[0], tz=timezone.utc),
                    source=DataSource.COINBASE,
                    symbol=symbol,
                    raw_symbol=product_id,
                    exchange="COINBASE",
                    timeframe=timeframe,
                    open=Decimal(str(candle[3])),
                    high=Decimal(str(candle[2])),
                    low=Decimal(str(candle[1])),
                    close=Decimal(str(candle[4])),
                    volume=Decimal(str(candle[5])),
                )
            )

        # Coinbase returns in descending order, reverse for chronological
        bars.reverse()

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
        product_id = self._to_product_id(symbol)

        endpoint = f"/products/{product_id}/trades"
        params = {"limit": min(limit or 1000, 1000)}

        result = await self._make_request("GET", endpoint, params)

        if not result:
            return []

        ticks = []
        for trade in result:
            trade_time = datetime.fromisoformat(trade["time"].replace("Z", "+00:00"))

            # Filter by time range
            if trade_time < start or trade_time > end:
                continue

            ticks.append(
                NormalizedTick(
                    timestamp=trade_time,
                    source=DataSource.COINBASE,
                    symbol=symbol,
                    raw_symbol=product_id,
                    exchange="COINBASE",
                    price=Decimal(trade["price"]),
                    volume=Decimal(trade["size"]),
                    side=trade["side"],
                    trade_id=str(trade["trade_id"]),
                )
            )

        return ticks

    async def get_products(self) -> Optional[List[Dict[str, Any]]]:
        """Get list of available products."""
        endpoint = "/products"
        return await self._make_request("GET", endpoint)

    async def get_product_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get current ticker for a product."""
        product_id = self._to_product_id(symbol)
        endpoint = f"/products/{product_id}/ticker"
        return await self._make_request("GET", endpoint)

    async def get_product_book(
        self,
        symbol: str,
        level: int = 2,
    ) -> Optional[Dict[str, Any]]:
        """Get order book for a product."""
        product_id = self._to_product_id(symbol)
        endpoint = f"/products/{product_id}/book"
        params = {"level": level}
        return await self._make_request("GET", endpoint, params)


# Export
__all__ = ["CoinbaseDataFeed"]
