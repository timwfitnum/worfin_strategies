"""
Real-time Data Pipeline with Redis Streams
Handles message queuing, deduplication, and distribution
"""

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set

import redis.asyncio as aioredis
from redis.asyncio.client import Redis

from src.data.models.unified import (
    Bar,
    DataSource,
    MarketDataEvent,
    OrderBook,
    Quote,
    Trade,
)
from src.core.config import RedisSettings

logger = logging.getLogger(__name__)


class DataPipeline:
    """
    Real-time data pipeline using Redis Streams.

    Features:
    - Multi-provider data aggregation
    - Deduplication with configurable windows
    - Consumer groups for scaling
    - Latency monitoring
    - Automatic stream trimming
    """

    # Stream names
    STREAM_TRADES = "stream:trades"
    STREAM_QUOTES = "stream:quotes"
    STREAM_BARS = "stream:bars"
    STREAM_ORDERBOOK = "stream:orderbook"

    def __init__(
        self,
        redis_config: RedisSettings,
        consumer_group: str = "quant_consumers",
        consumer_name: Optional[str] = None,
        dedup_window_seconds: int = 60,
        max_stream_length: int = 100_000,
    ):
        self.redis_config = redis_config
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name or f"consumer_{id(self)}"
        self.dedup_window_seconds = dedup_window_seconds
        self.max_stream_length = max_stream_length

        # Redis connection
        self._redis: Optional[Redis] = None

        # Deduplication cache (in-memory bloom filter alternative)
        self._seen_hashes: Dict[str, datetime] = {}
        self._dedup_cleanup_task: Optional[asyncio.Task] = None

        # Callbacks
        self._trade_handlers: List[Callable[[Trade], None]] = []
        self._quote_handlers: List[Callable[[Quote], None]] = []
        self._bar_handlers: List[Callable[[Bar], None]] = []
        self._orderbook_handlers: List[Callable[[OrderBook], None]] = []

        # Metrics
        self._messages_processed = 0
        self._duplicates_filtered = 0
        self._latency_samples: List[float] = []

        # Consumer tasks
        self._consumer_tasks: List[asyncio.Task] = []
        self._running = False

    async def connect(self) -> bool:
        """Connect to Redis and initialize streams"""
        try:
            self._redis = await aioredis.from_url(
                self.redis_config.url,
                decode_responses=True,
                max_connections=self.redis_config.pool_size,
            )

            # Test connection
            await self._redis.ping()

            # Create consumer groups (if they don't exist)
            for stream in [
                self.STREAM_TRADES,
                self.STREAM_QUOTES,
                self.STREAM_BARS,
                self.STREAM_ORDERBOOK,
            ]:
                try:
                    await self._redis.xgroup_create(
                        stream,
                        self.consumer_group,
                        mkstream=True,
                        id="0",
                    )
                except Exception as e:
                    if "BUSYGROUP" not in str(e):
                        raise

            # Start dedup cleanup
            self._dedup_cleanup_task = asyncio.create_task(self._cleanup_dedup_cache())

            logger.info(f"Data pipeline connected to Redis")
            return True

        except Exception as e:
            logger.error(f"Failed to connect data pipeline: {e}")
            return False

    async def disconnect(self) -> None:
        """Disconnect from Redis"""
        self._running = False

        # Cancel consumer tasks
        for task in self._consumer_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Cancel cleanup task
        if self._dedup_cleanup_task:
            self._dedup_cleanup_task.cancel()

        # Close Redis
        if self._redis:
            await self._redis.close()

        logger.info("Data pipeline disconnected")

    # -------------------------------------------------------------------------
    # Publishing
    # -------------------------------------------------------------------------

    async def publish_trade(self, trade: Trade) -> bool:
        """Publish trade to stream"""
        if self._is_duplicate(trade):
            self._duplicates_filtered += 1
            return False

        data = {
            "symbol": trade.symbol,
            "exchange": trade.exchange,
            "timestamp": trade.timestamp.isoformat(),
            "trade_id": trade.trade_id,
            "price": str(trade.price),
            "volume": str(trade.volume),
            "side": trade.side.value,
            "data_source": trade.data_source.value,
            "received_at": datetime.utcnow().isoformat(),
        }

        try:
            await self._redis.xadd(
                self.STREAM_TRADES,
                data,
                maxlen=self.max_stream_length,
                approximate=True,
            )
            self._messages_processed += 1
            return True
        except Exception as e:
            logger.error(f"Error publishing trade: {e}")
            return False

    async def publish_quote(self, quote: Quote) -> bool:
        """Publish quote to stream"""
        if self._is_duplicate(quote):
            self._duplicates_filtered += 1
            return False

        data = {
            "symbol": quote.symbol,
            "exchange": quote.exchange,
            "timestamp": quote.timestamp.isoformat(),
            "bid_price": str(quote.bid_price),
            "bid_size": str(quote.bid_size),
            "ask_price": str(quote.ask_price),
            "ask_size": str(quote.ask_size),
            "data_source": quote.data_source.value,
            "received_at": datetime.utcnow().isoformat(),
        }

        try:
            await self._redis.xadd(
                self.STREAM_QUOTES,
                data,
                maxlen=self.max_stream_length,
                approximate=True,
            )
            self._messages_processed += 1
            return True
        except Exception as e:
            logger.error(f"Error publishing quote: {e}")
            return False

    async def publish_bar(self, bar: Bar) -> bool:
        """Publish bar to stream"""
        data = {
            "symbol": bar.symbol,
            "exchange": bar.exchange,
            "timestamp": bar.timestamp.isoformat(),
            "timeframe": bar.timeframe,
            "open": str(bar.open),
            "high": str(bar.high),
            "low": str(bar.low),
            "close": str(bar.close),
            "volume": str(bar.volume),
            "trades": str(bar.trades),
            "vwap": str(bar.vwap),
            "data_source": bar.data_source.value,
        }

        try:
            await self._redis.xadd(
                self.STREAM_BARS,
                data,
                maxlen=self.max_stream_length,
                approximate=True,
            )
            self._messages_processed += 1
            return True
        except Exception as e:
            logger.error(f"Error publishing bar: {e}")
            return False

    # -------------------------------------------------------------------------
    # Consuming
    # -------------------------------------------------------------------------

    async def start_consuming(self) -> None:
        """Start consuming from all streams"""
        self._running = True

        # Start consumer tasks
        self._consumer_tasks = [
            asyncio.create_task(
                self._consume_stream(self.STREAM_TRADES, self._handle_trade_message)
            ),
            asyncio.create_task(
                self._consume_stream(self.STREAM_QUOTES, self._handle_quote_message)
            ),
            asyncio.create_task(self._consume_stream(self.STREAM_BARS, self._handle_bar_message)),
        ]

        logger.info("Started consuming from data streams")

    async def _consume_stream(
        self,
        stream: str,
        handler: Callable[[Dict[str, str]], None],
    ) -> None:
        """Consume messages from a stream"""
        while self._running:
            try:
                messages = await self._redis.xreadgroup(
                    self.consumer_group,
                    self.consumer_name,
                    {stream: ">"},
                    count=100,
                    block=1000,
                )

                if messages:
                    for stream_name, stream_messages in messages:
                        for message_id, data in stream_messages:
                            try:
                                # Track latency
                                if "received_at" in data:
                                    received = datetime.fromisoformat(data["received_at"])
                                    latency = (datetime.utcnow() - received).total_seconds() * 1000
                                    self._latency_samples.append(latency)
                                    if len(self._latency_samples) > 1000:
                                        self._latency_samples = self._latency_samples[-1000:]

                                # Handle message
                                handler(data)

                                # Acknowledge
                                await self._redis.xack(stream, self.consumer_group, message_id)

                            except Exception as e:
                                logger.error(f"Error processing message: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in stream consumer: {e}")
                await asyncio.sleep(1)

    def _handle_trade_message(self, data: Dict[str, str]) -> None:
        """Handle incoming trade message"""
        trade = Trade(
            symbol=data["symbol"],
            exchange=data.get("exchange", ""),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            data_source=DataSource(data.get("data_source", "internal")),
            trade_id=data["trade_id"],
            price=float(data["price"]),
            volume=float(data["volume"]),
            side=data.get("side", "unknown"),
        )

        for handler in self._trade_handlers:
            try:
                handler(trade)
            except Exception as e:
                logger.error(f"Error in trade handler: {e}")

    def _handle_quote_message(self, data: Dict[str, str]) -> None:
        """Handle incoming quote message"""
        quote = Quote(
            symbol=data["symbol"],
            exchange=data.get("exchange", ""),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            data_source=DataSource(data.get("data_source", "internal")),
            bid_price=float(data["bid_price"]),
            bid_size=float(data["bid_size"]),
            ask_price=float(data["ask_price"]),
            ask_size=float(data["ask_size"]),
        )

        for handler in self._quote_handlers:
            try:
                handler(quote)
            except Exception as e:
                logger.error(f"Error in quote handler: {e}")

    def _handle_bar_message(self, data: Dict[str, str]) -> None:
        """Handle incoming bar message"""
        bar = Bar(
            symbol=data["symbol"],
            exchange=data.get("exchange", ""),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            timeframe=data["timeframe"],
            open=float(data["open"]),
            high=float(data["high"]),
            low=float(data["low"]),
            close=float(data["close"]),
            volume=float(data["volume"]),
            trades=int(data.get("trades", 0)),
            vwap=float(data.get("vwap", 0)),
            data_source=DataSource(data.get("data_source", "internal")),
        )

        for handler in self._bar_handlers:
            try:
                handler(bar)
            except Exception as e:
                logger.error(f"Error in bar handler: {e}")

    # -------------------------------------------------------------------------
    # Handler Registration
    # -------------------------------------------------------------------------

    def on_trade(self, handler: Callable[[Trade], None]) -> None:
        """Register trade handler"""
        self._trade_handlers.append(handler)

    def on_quote(self, handler: Callable[[Quote], None]) -> None:
        """Register quote handler"""
        self._quote_handlers.append(handler)

    def on_bar(self, handler: Callable[[Bar], None]) -> None:
        """Register bar handler"""
        self._bar_handlers.append(handler)

    # -------------------------------------------------------------------------
    # Deduplication
    # -------------------------------------------------------------------------

    def _is_duplicate(self, item: Any) -> bool:
        """Check if item is a duplicate"""
        # Create hash from key fields
        if isinstance(item, Trade):
            key = f"{item.symbol}:{item.trade_id}:{item.timestamp.timestamp()}"
        elif isinstance(item, Quote):
            key = f"{item.symbol}:{item.bid_price}:{item.ask_price}:{item.timestamp.timestamp()}"
        else:
            return False

        hash_key = hashlib.md5(key.encode()).hexdigest()

        if hash_key in self._seen_hashes:
            return True

        self._seen_hashes[hash_key] = datetime.utcnow()
        return False

    async def _cleanup_dedup_cache(self) -> None:
        """Periodically clean dedup cache"""
        while True:
            await asyncio.sleep(30)

            cutoff = datetime.utcnow() - timedelta(seconds=self.dedup_window_seconds)
            self._seen_hashes = {k: v for k, v in self._seen_hashes.items() if v > cutoff}

    # -------------------------------------------------------------------------
    # Metrics
    # -------------------------------------------------------------------------

    def get_metrics(self) -> Dict[str, Any]:
        """Get pipeline metrics"""
        avg_latency = (
            sum(self._latency_samples) / len(self._latency_samples) if self._latency_samples else 0
        )

        return {
            "messages_processed": self._messages_processed,
            "duplicates_filtered": self._duplicates_filtered,
            "avg_latency_ms": avg_latency,
            "dedup_cache_size": len(self._seen_hashes),
            "running": self._running,
        }


class DataAggregator:
    """
    Aggregates data from multiple providers with failover.

    Features:
    - Primary/backup provider selection
    - Quality-based routing
    - Gap detection and filling
    - Unified output stream
    """

    def __init__(self, pipeline: DataPipeline):
        self.pipeline = pipeline

        # Provider priority (higher = preferred)
        self._provider_priority: Dict[DataSource, int] = {
            DataSource.INTERACTIVE_BROKERS: 100,
            DataSource.POLYGON: 80,
            DataSource.BINANCE: 70,
            DataSource.COINBASE: 60,
            DataSource.ALPACA: 50,
        }

        # Latest data per symbol per provider
        self._latest_trades: Dict[str, Dict[DataSource, Trade]] = {}
        self._latest_quotes: Dict[str, Dict[DataSource, Quote]] = {}

        # Gap detection
        self._last_trade_time: Dict[str, datetime] = {}
        self._gap_threshold_seconds = 60

    async def process_trade(self, trade: Trade) -> None:
        """Process incoming trade with aggregation logic"""
        symbol = trade.symbol
        source = trade.data_source

        # Track latest per source
        if symbol not in self._latest_trades:
            self._latest_trades[symbol] = {}
        self._latest_trades[symbol][source] = trade

        # Check for gaps
        if symbol in self._last_trade_time:
            gap = (trade.timestamp - self._last_trade_time[symbol]).total_seconds()
            if gap > self._gap_threshold_seconds:
                logger.warning(f"Data gap detected for {symbol}: {gap:.1f}s")

        self._last_trade_time[symbol] = trade.timestamp

        # Determine if this is the best source
        best_source = self._get_best_source_for_symbol(symbol, self._latest_trades)

        if source == best_source:
            # Publish to unified stream
            await self.pipeline.publish_trade(trade)

    async def process_quote(self, quote: Quote) -> None:
        """Process incoming quote with aggregation logic"""
        symbol = quote.symbol
        source = quote.data_source

        if symbol not in self._latest_quotes:
            self._latest_quotes[symbol] = {}
        self._latest_quotes[symbol][source] = quote

        best_source = self._get_best_source_for_symbol(symbol, self._latest_quotes)

        if source == best_source:
            await self.pipeline.publish_quote(quote)

    def _get_best_source_for_symbol(
        self,
        symbol: str,
        data_dict: Dict[str, Dict[DataSource, Any]],
    ) -> Optional[DataSource]:
        """Get best data source for a symbol"""
        if symbol not in data_dict:
            return None

        available = data_dict[symbol]

        # Filter to sources with recent data (within 5 seconds)
        now = datetime.utcnow()
        recent_sources = []

        for source, data in available.items():
            if hasattr(data, "timestamp"):
                age = (now - data.timestamp).total_seconds()
                if age < 5:
                    recent_sources.append(source)

        if not recent_sources:
            recent_sources = list(available.keys())

        # Return highest priority
        return max(recent_sources, key=lambda s: self._provider_priority.get(s, 0))

    def set_provider_priority(self, source: DataSource, priority: int) -> None:
        """Set priority for a data source"""
        self._provider_priority[source] = priority


__all__ = ["DataPipeline", "DataAggregator"]
