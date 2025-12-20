"""
Polygon.io Data Feed Integration.
WebSocket client for real-time trades/quotes and REST API for historical data.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed

from .base import (
    BaseHistoricalDataFeed,
    BaseRealtimeDataFeed,
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


class PolygonDataFeed(BaseHistoricalDataFeed, BaseRealtimeDataFeed):
    """
    Polygon.io data feed with WebSocket streaming and REST API.

    Features:
    - Real-time trades and quotes via WebSocket
    - Historical OHLCV data via REST API
    - Rate limiting for API compliance
    - Automatic reconnection
    - Reference data synchronization
    """

    REST_BASE_URL = "https://api.polygon.io"
    WS_URL = "wss://socket.polygon.io/stocks"
    WS_CRYPTO_URL = "wss://socket.polygon.io/crypto"

    def __init__(
        self,
        api_key: str,
        max_requests_per_minute: int = 5,  # Free tier limit
        timeout: int = 30,
    ):
        super().__init__(name="Polygon", source=DataSource.POLYGON)

        self.api_key = api_key
        self.timeout = timeout

        # Rate limiter for REST API
        self._rate_limiter = RateLimiter(
            rate=max_requests_per_minute / 60,  # Per second
            burst=5,
        )

        # WebSocket connection
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_task: Optional[asyncio.Task] = None

        # HTTP session for REST API
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout))
        return self._session

    async def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Make rate-limited REST API request."""
        await self._rate_limiter.acquire()

        session = await self._get_session()
        url = f"{self.REST_BASE_URL}{endpoint}"

        params = params or {}
        params["apiKey"] = self.api_key

        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:
                    logger.warning("Rate limit exceeded, waiting...")
                    await asyncio.sleep(60)
                    return await self._make_request(endpoint, params)
                else:
                    logger.error(f"API error {response.status}: {await response.text()}")
                    return None
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return None

    async def connect(self) -> bool:
        """Connect to Polygon WebSocket."""
        self.connection.set_state(ConnectionState.CONNECTING)

        try:
            self._ws = await websockets.connect(
                self.WS_URL,
                extra_headers={"Authorization": f"Bearer {self.api_key}"},
            )

            # Authenticate
            auth_msg = json.dumps({"action": "auth", "params": self.api_key})
            await self._ws.send(auth_msg)

            # Wait for auth response
            response = await asyncio.wait_for(self._ws.recv(), timeout=10)
            data = json.loads(response)

            if data[0].get("status") == "auth_success":
                logger.info("Polygon WebSocket authenticated successfully")
                self.connection.set_state(ConnectionState.CONNECTED)
                self._running = True
                return True
            else:
                logger.error(f"Polygon auth failed: {data}")
                self.connection.set_state(ConnectionState.ERROR)
                return False

        except Exception as e:
            logger.error(f"Failed to connect to Polygon: {e}")
            self.connection.set_state(ConnectionState.ERROR)
            self.connection.last_error = e
            return False

    async def disconnect(self):
        """Disconnect from Polygon."""
        self._running = False

        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass

        if self._ws:
            await self._ws.close()
            self._ws = None

        if self._session:
            await self._session.close()
            self._session = None

        self._subscriptions.clear()
        self.connection.set_state(ConnectionState.DISCONNECTED)

    async def subscribe(self, symbols: List[str], data_types: List[DataType]):
        """Subscribe to real-time data."""
        if not self._ws or self._ws.closed:
            logger.error("WebSocket not connected")
            return

        channels = []
        for symbol in symbols:
            if DataType.TRADE in data_types:
                channels.append(f"T.{symbol}")
            if DataType.QUOTE in data_types:
                channels.append(f"Q.{symbol}")

        if channels:
            sub_msg = json.dumps({"action": "subscribe", "params": ",".join(channels)})
            await self._ws.send(sub_msg)

            self._subscriptions.update(symbols)
            logger.info(f"Subscribed to {len(channels)} channels")

    async def unsubscribe(self, symbols: List[str]):
        """Unsubscribe from real-time data."""
        if not self._ws or self._ws.closed:
            return

        channels = []
        for symbol in symbols:
            channels.extend([f"T.{symbol}", f"Q.{symbol}"])

        if channels:
            unsub_msg = json.dumps({"action": "unsubscribe", "params": ",".join(channels)})
            await self._ws.send(unsub_msg)

            self._subscriptions.difference_update(symbols)
            logger.info(f"Unsubscribed from {len(symbols)} symbols")

    async def start_streaming(self):
        """Start WebSocket message processing."""
        logger.info("Polygon streaming started")

        while self._running and self._ws:
            try:
                message = await asyncio.wait_for(self._ws.recv(), timeout=30)
                await self._process_message(message)

            except asyncio.TimeoutError:
                # Send heartbeat
                try:
                    await self._ws.ping()
                except:
                    break

            except ConnectionClosed:
                logger.warning("WebSocket connection closed")
                if self._running:
                    await self._reconnect()
                break

            except Exception as e:
                logger.error(f"Streaming error: {e}")
                await asyncio.sleep(1)

        logger.info("Polygon streaming stopped")

    async def stop_streaming(self):
        """Stop streaming."""
        self._running = False

    async def _process_message(self, message: str):
        """Process incoming WebSocket message."""
        try:
            data_list = json.loads(message)

            for data in data_list:
                event_type = data.get("ev")

                if event_type == "T":  # Trade
                    tick = self._parse_trade(data)
                    await self._emit_event(
                        DataEvent(
                            event_type=DataType.TRADE,
                            data=tick,
                            source=self.source,
                        )
                    )

                elif event_type == "Q":  # Quote
                    quote = self._parse_quote(data)
                    await self._emit_event(
                        DataEvent(
                            event_type=DataType.QUOTE,
                            data=quote,
                            source=self.source,
                        )
                    )

                elif event_type == "status":
                    logger.debug(f"Status message: {data.get('message')}")

        except Exception as e:
            logger.error(f"Failed to process message: {e}")

    def _parse_trade(self, data: Dict) -> NormalizedTick:
        """Parse trade message to normalized tick."""
        return NormalizedTick(
            timestamp=datetime.fromtimestamp(data["t"] / 1000, tz=timezone.utc),
            source=DataSource.POLYGON,
            symbol=data["sym"],
            raw_symbol=data["sym"],
            exchange=data.get("x", ""),
            price=Decimal(str(data["p"])),
            volume=Decimal(str(data["s"])),
            side="unknown",
            trade_id=data.get("i"),
            conditions=data.get("c", []),
        )

    def _parse_quote(self, data: Dict) -> NormalizedQuote:
        """Parse quote message to normalized quote."""
        return NormalizedQuote(
            timestamp=datetime.fromtimestamp(data["t"] / 1000, tz=timezone.utc),
            source=DataSource.POLYGON,
            symbol=data["sym"],
            raw_symbol=data["sym"],
            exchange=data.get("bx", ""),
            bid_price=Decimal(str(data["bp"])),
            bid_size=Decimal(str(data["bs"])),
            ask_price=Decimal(str(data["ap"])),
            ask_size=Decimal(str(data["as"])),
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
        """Fetch historical OHLCV data from REST API."""
        # Map timeframe to Polygon format
        timeframe_map = {
            "1m": ("minute", 1),
            "5m": ("minute", 5),
            "15m": ("minute", 15),
            "30m": ("minute", 30),
            "1h": ("hour", 1),
            "4h": ("hour", 4),
            "1d": ("day", 1),
            "1w": ("week", 1),
            "1M": ("month", 1),
        }

        multiplier, timespan = timeframe_map.get(timeframe, ("day", 1))
        if isinstance(multiplier, tuple):
            timespan, multiplier = multiplier

        endpoint = f"/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{start.strftime('%Y-%m-%d')}/{end.strftime('%Y-%m-%d')}"

        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,
        }

        result = await self._make_request(endpoint, params)

        if not result or "results" not in result:
            return []

        bars = []
        for bar_data in result["results"]:
            bars.append(
                NormalizedBar(
                    timestamp=datetime.fromtimestamp(bar_data["t"] / 1000, tz=timezone.utc),
                    source=DataSource.POLYGON,
                    symbol=symbol,
                    raw_symbol=symbol,
                    exchange="",
                    timeframe=timeframe,
                    open=Decimal(str(bar_data["o"])),
                    high=Decimal(str(bar_data["h"])),
                    low=Decimal(str(bar_data["l"])),
                    close=Decimal(str(bar_data["c"])),
                    volume=Decimal(str(bar_data["v"])),
                    vwap=Decimal(str(bar_data.get("vw", 0))) if bar_data.get("vw") else None,
                    trades=bar_data.get("n"),
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
        """Fetch historical trades from REST API."""
        endpoint = f"/v3/trades/{symbol}"

        params = {
            "timestamp.gte": int(start.timestamp() * 1000000000),  # Nanoseconds
            "timestamp.lte": int(end.timestamp() * 1000000000),
            "limit": min(limit or 50000, 50000),
            "sort": "timestamp",
        }

        result = await self._make_request(endpoint, params)

        if not result or "results" not in result:
            return []

        ticks = []
        for trade in result["results"]:
            ticks.append(
                NormalizedTick(
                    timestamp=datetime.fromtimestamp(
                        trade["participant_timestamp"] / 1000000000, tz=timezone.utc
                    ),
                    source=DataSource.POLYGON,
                    symbol=symbol,
                    raw_symbol=symbol,
                    exchange=trade.get("exchange", ""),
                    price=Decimal(str(trade["price"])),
                    volume=Decimal(str(trade["size"])),
                    side="unknown",
                    trade_id=trade.get("id"),
                    conditions=[str(c) for c in trade.get("conditions", [])],
                )
            )

        return ticks

    async def get_ticker_details(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get ticker reference data."""
        endpoint = f"/v3/reference/tickers/{symbol}"
        result = await self._make_request(endpoint)

        if result and "results" in result:
            return result["results"]
        return None

    async def get_market_status(self) -> Optional[Dict[str, Any]]:
        """Get current market status."""
        endpoint = "/v1/marketstatus/now"
        return await self._make_request(endpoint)

    async def get_previous_close(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get previous day's close data."""
        endpoint = f"/v2/aggs/ticker/{symbol}/prev"
        result = await self._make_request(endpoint)

        if result and "results" in result and result["results"]:
            return result["results"][0]
        return None

    async def get_snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get current snapshot for a ticker."""
        endpoint = f"/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}"
        result = await self._make_request(endpoint)

        if result and "ticker" in result:
            return result["ticker"]
        return None

    async def health_check(self) -> Dict[str, Any]:
        """Extended health check."""
        base_health = await super().health_check()

        base_health.update(
            {
                "ws_connected": self._ws is not None and not self._ws.closed,
                "session_active": self._session is not None and not self._session.closed,
            }
        )

        # Check market status
        market_status = await self.get_market_status()
        if market_status:
            base_health["market_status"] = market_status.get("market", "unknown")

        return base_health


# Export
__all__ = ["PolygonDataFeed"]
