"""
Polygon.io Data Provider
Handles real-time and historical data from Polygon.io
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiohttp
import websockets
from websockets.client import WebSocketClientProtocol

from src.data.models.unified import (
    Asset,
    AssetClass,
    Bar,
    CorporateAction,
    DataSource,
    Quote,
    Side,
    Trade,
)
from src.data.providers.base import (
    BaseStreamingProvider,
    ConnectionState,
    ProviderConfig,
    SubscriptionType,
)

logger = logging.getLogger(__name__)


class PolygonConfig(ProviderConfig):
    """Polygon.io configuration"""

    name: str = "polygon"
    api_key: str = ""
    base_url: str = "https://api.polygon.io"
    ws_url: str = "wss://socket.polygon.io"
    requests_per_minute: int = 5
    adjusted: bool = True
    limit: int = 50000


class PolygonProvider(BaseStreamingProvider):
    """Polygon.io data provider with WebSocket streaming and REST API"""

    def __init__(self, config: PolygonConfig):
        super().__init__(config)
        self.config: PolygonConfig = config
        self.source = DataSource.POLYGON
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[WebSocketClientProtocol] = None
        self._authenticated = False
        self._request_times: List[datetime] = []

    async def connect(self) -> bool:
        """Connect to Polygon API and WebSocket"""
        try:
            self._set_state(ConnectionState.CONNECTING)
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.config.read_timeout)
            )

            # Verify API key
            async with self._session.get(
                f"{self.config.base_url}/v2/aggs/ticker/AAPL/prev",
                params={"apiKey": self.config.api_key},
            ) as response:
                if response.status == 401:
                    raise ValueError("Invalid Polygon API key")
                response.raise_for_status()

            # Connect WebSocket
            await self._connect_websocket()

            self._set_state(ConnectionState.CONNECTED)
            logger.info("Connected to Polygon.io")

            self._receive_task = asyncio.create_task(self._receive_loop())
            self._tasks.append(self._receive_task)
            self._tasks.append(asyncio.create_task(self._health_check_loop()))

            return True
        except Exception as e:
            logger.error(f"Failed to connect to Polygon: {e}")
            self._set_state(ConnectionState.ERROR)
            self._emit_error(e)
            return False

    async def _connect_websocket(self) -> Any:
        """Establish WebSocket connection"""
        ws_url = f"{self.config.ws_url}/stocks"
        self._ws = await websockets.connect(ws_url, ping_interval=30, ping_timeout=10)

        auth_message = {"action": "auth", "params": self.config.api_key}
        await self._ws.send(json.dumps(auth_message))

        response = await self._ws.recv()
        data = json.loads(response)

        if isinstance(data, list) and data and data[0].get("status") == "auth_success":
            self._authenticated = True
            logger.info("Polygon WebSocket authenticated")
        else:
            raise ValueError(f"Polygon auth failed: {data}")

        return self._ws

    async def disconnect(self) -> None:
        """Disconnect from Polygon"""
        try:
            if self._ws:
                await self._ws.close()
                self._ws = None
            if self._session:
                await self._session.close()
                self._session = None
            self._authenticated = False
            self._set_state(ConnectionState.DISCONNECTED)
            logger.info("Disconnected from Polygon")
        except Exception as e:
            logger.error(f"Error disconnecting from Polygon: {e}")

    async def subscribe(
        self,
        symbols: List[str],
        subscription_types: List[SubscriptionType],
    ) -> bool:
        """Subscribe to market data"""
        try:
            if not self._ws or not self._authenticated:
                return False

            channels = []
            for symbol in symbols:
                polygon_symbol = self.denormalize_symbol(symbol)
                for sub_type in subscription_types:
                    if sub_type in (SubscriptionType.TRADES, SubscriptionType.ALL):
                        channels.append(f"T.{polygon_symbol}")
                    if sub_type in (SubscriptionType.QUOTES, SubscriptionType.ALL):
                        channels.append(f"Q.{polygon_symbol}")
                    if sub_type in (SubscriptionType.BARS, SubscriptionType.ALL):
                        channels.append(f"AM.{polygon_symbol}")

                if symbol not in self._subscriptions:
                    self._subscriptions[symbol] = set()
                self._subscriptions[symbol].update(subscription_types)

            if channels:
                message = {"action": "subscribe", "params": ",".join(channels)}
                await self._ws.send(json.dumps(message))
                logger.info(f"Subscribed to Polygon channels: {channels}")
            return True
        except Exception as e:
            logger.error(f"Error subscribing to Polygon: {e}")
            return False

    async def unsubscribe(
        self,
        symbols: List[str],
        subscription_types: Optional[List[SubscriptionType]] = None,
    ) -> bool:
        """Unsubscribe from market data"""
        try:
            if not self._ws:
                return False

            channels = []
            for symbol in symbols:
                polygon_symbol = self.denormalize_symbol(symbol)
                types = subscription_types or [SubscriptionType.ALL]
                for sub_type in types:
                    if sub_type in (SubscriptionType.TRADES, SubscriptionType.ALL):
                        channels.append(f"T.{polygon_symbol}")
                    if sub_type in (SubscriptionType.QUOTES, SubscriptionType.ALL):
                        channels.append(f"Q.{polygon_symbol}")
                    if sub_type in (SubscriptionType.BARS, SubscriptionType.ALL):
                        channels.append(f"AM.{polygon_symbol}")

                if symbol in self._subscriptions:
                    del self._subscriptions[symbol]

            if channels:
                message = {"action": "unsubscribe", "params": ",".join(channels)}
                await self._ws.send(json.dumps(message))
            return True
        except Exception as e:
            logger.error(f"Error unsubscribing from Polygon: {e}")
            return False

    async def _handle_message(self, message: Any) -> None:
        """Process incoming WebSocket message"""
        try:
            data = json.loads(message)
            if not isinstance(data, list):
                return

            for event in data:
                event_type = event.get("ev")
                if event_type == "T":
                    self._handle_trade(event)
                elif event_type == "Q":
                    self._handle_quote(event)
                elif event_type == "AM":
                    self._handle_bar(event)
        except Exception as e:
            logger.error(f"Error handling Polygon message: {e}")

    def _handle_trade(self, event: Dict[str, Any]) -> None:
        """Handle trade event"""
        symbol = self.normalize_symbol(event.get("sym", ""))
        ts_ns = event.get("t", 0)
        timestamp = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)

        trade = Trade(
            symbol=symbol,
            exchange=event.get("x", ""),
            timestamp=timestamp,
            data_source=self.source,
            trade_id=event.get("i", f"poly_{ts_ns}"),
            price=event.get("p", 0),
            volume=event.get("s", 0),
            side=Side.UNKNOWN,
            conditions=event.get("c", []),
        )
        self._emit_trade(trade)

    def _handle_quote(self, event: Dict[str, Any]) -> None:
        """Handle quote event"""
        symbol = self.normalize_symbol(event.get("sym", ""))
        ts_ns = event.get("t", 0)
        timestamp = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)

        quote = Quote(
            symbol=symbol,
            exchange="",
            timestamp=timestamp,
            data_source=self.source,
            bid_price=event.get("bp", 0),
            bid_size=event.get("bs", 0),
            ask_price=event.get("ap", 0),
            ask_size=event.get("as", 0),
        )
        self._emit_quote(quote)

    def _handle_bar(self, event: Dict[str, Any]) -> None:
        """Handle bar event"""
        symbol = self.normalize_symbol(event.get("sym", ""))
        ts_ms = event.get("s", 0)
        timestamp = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

        bar = Bar(
            symbol=symbol,
            timestamp=timestamp,
            timeframe="1m",
            open=event.get("o", 0),
            high=event.get("h", 0),
            low=event.get("l", 0),
            close=event.get("c", 0),
            volume=event.get("v", 0),
            trades=event.get("n", 0),
            vwap=event.get("vw", 0),
            data_source=self.source,
        )
        self._emit_bar(bar)

    async def _ensure_rate_limit(self) -> None:
        """Ensure rate limits"""
        now = datetime.utcnow()
        cutoff = now - timedelta(seconds=60)
        self._request_times = [t for t in self._request_times if t > cutoff]

        if len(self._request_times) >= self.config.requests_per_minute:
            wait_time = (self._request_times[0] - cutoff).total_seconds() + 1
            await asyncio.sleep(wait_time)
        self._request_times.append(now)

    async def get_historical_bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None,
    ) -> List[Bar]:
        """Fetch historical bars"""
        await self._ensure_rate_limit()

        timeframe_map = {
            "1m": ("minute", 1),
            "5m": ("minute", 5),
            "15m": ("minute", 15),
            "1h": ("hour", 1),
            "1d": ("day", 1),
        }

        if timeframe not in timeframe_map:
            return []

        multiplier_type, multiplier = timeframe_map[timeframe]
        polygon_symbol = self.denormalize_symbol(symbol)

        url = (
            f"{self.config.base_url}/v2/aggs/ticker/{polygon_symbol}/"
            f"range/{multiplier}/{multiplier_type}/{start.strftime('%Y-%m-%d')}/{end.strftime('%Y-%m-%d')}"
        )

        params = {
            "apiKey": self.config.api_key,
            "adjusted": str(self.config.adjusted).lower(),
            "sort": "asc",
            "limit": limit or self.config.limit,
        }

        try:
            async with self._session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

            bars = []
            for r in data.get("results", []):
                timestamp = datetime.fromtimestamp(r.get("t", 0) / 1000, tz=timezone.utc)
                bars.append(
                    Bar(
                        symbol=symbol,
                        timestamp=timestamp,
                        timeframe=timeframe,
                        open=r.get("o", 0),
                        high=r.get("h", 0),
                        low=r.get("l", 0),
                        close=r.get("c", 0),
                        volume=r.get("v", 0),
                        trades=r.get("n", 0),
                        vwap=r.get("vw", 0),
                        data_source=self.source,
                    )
                )

            logger.info(f"Retrieved {len(bars)} bars for {symbol}")
            return bars
        except Exception as e:
            logger.error(f"Error fetching Polygon bars: {e}")
            return []

    async def get_historical_trades(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None,
    ) -> List[Trade]:
        """Fetch historical trades"""
        await self._ensure_rate_limit()

        polygon_symbol = self.denormalize_symbol(symbol)
        url = f"{self.config.base_url}/v3/trades/{polygon_symbol}"

        params = {
            "apiKey": self.config.api_key,
            "timestamp.gte": int(start.timestamp() * 1e9),
            "timestamp.lte": int(end.timestamp() * 1e9),
            "limit": limit or 50000,
            "sort": "timestamp",
            "order": "asc",
        }

        try:
            async with self._session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

            trades = []
            for r in data.get("results", []):
                ts_ns = r.get("sip_timestamp", r.get("participant_timestamp", 0))
                timestamp = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
                trades.append(
                    Trade(
                        symbol=symbol,
                        exchange=str(r.get("exchange", "")),
                        timestamp=timestamp,
                        data_source=self.source,
                        trade_id=r.get("id", f"poly_{ts_ns}"),
                        price=r.get("price", 0),
                        volume=r.get("size", 0),
                        side=Side.UNKNOWN,
                    )
                )

            logger.info(f"Retrieved {len(trades)} trades for {symbol}")
            return trades
        except Exception as e:
            logger.error(f"Error fetching Polygon trades: {e}")
            return []

    async def get_asset_info(self, symbol: str) -> Optional[Asset]:
        """Get ticker details"""
        await self._ensure_rate_limit()

        polygon_symbol = self.denormalize_symbol(symbol)
        url = f"{self.config.base_url}/v3/reference/tickers/{polygon_symbol}"

        try:
            async with self._session.get(url, params={"apiKey": self.config.api_key}) as response:
                if response.status == 404:
                    return None
                response.raise_for_status()
                data = await response.json()

            result = data.get("results", {})
            market = result.get("market", "").lower()
            asset_class_map = {
                "stocks": AssetClass.EQUITY,
                "crypto": AssetClass.CRYPTO,
                "fx": AssetClass.FOREX,
            }

            return Asset(
                symbol=symbol,
                name=result.get("name"),
                asset_class=asset_class_map.get(market, AssetClass.EQUITY),
                exchange=result.get("primary_exchange"),
                currency=result.get("currency_name", "USD"),
                polygon_ticker=result.get("ticker"),
                is_active=result.get("active", True),
            )
        except Exception as e:
            logger.error(f"Error fetching Polygon asset info: {e}")
            return None

    def normalize_symbol(self, provider_symbol: str) -> str:
        return provider_symbol.upper()

    def denormalize_symbol(self, internal_symbol: str) -> str:
        return internal_symbol.upper()


__all__ = ["PolygonConfig", "PolygonProvider"]
