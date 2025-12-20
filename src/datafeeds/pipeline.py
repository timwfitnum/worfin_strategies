"""
Real-time Data Pipeline with Redis Streams.

Provides:
- Asynchronous message processing with consumer groups
- Deduplication logic for multi-source data
- Latency monitoring
- Failover between data sources
- Data quality scoring
"""

import asyncio
import hashlib
import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import redis.asyncio as aioredis
from redis.asyncio import Redis

from .base import (
    DataEvent,
    DataSource,
    DataType,
    NormalizedBar,
    NormalizedQuote,
    NormalizedTick,
)

logger = logging.getLogger(__name__)


class StreamName(str, Enum):
    """Redis stream names."""

    TRADES = "stream:trades"
    QUOTES = "stream:quotes"
    BARS = "stream:bars"
    ORDERBOOKS = "stream:orderbooks"
    SIGNALS = "stream:signals"
    ALERTS = "stream:alerts"


@dataclass
class PipelineMetrics:
    """Pipeline performance metrics."""

    messages_received: int = 0
    messages_processed: int = 0
    messages_dropped: int = 0
    duplicates_filtered: int = 0
    errors: int = 0
    last_message_time: Optional[datetime] = None
    avg_latency_ms: float = 0.0
    max_latency_ms: float = 0.0
    latency_samples: List[float] = field(default_factory=list)

    def record_latency(self, latency_ms: float):
        """Record a latency sample."""
        self.latency_samples.append(latency_ms)
        if len(self.latency_samples) > 1000:
            self.latency_samples = self.latency_samples[-1000:]

        self.avg_latency_ms = sum(self.latency_samples) / len(self.latency_samples)
        self.max_latency_ms = max(self.max_latency_ms, latency_ms)


class DeduplicationCache:
    """
    LRU cache for deduplication using Redis.
    Uses trade_id/message hash to detect duplicates.
    """

    def __init__(
        self,
        redis: Redis,
        ttl_seconds: int = 300,
        prefix: str = "dedup:",
    ):
        self.redis = redis
        self.ttl = ttl_seconds
        self.prefix = prefix

    def _generate_key(self, event: DataEvent) -> str:
        """Generate unique key for deduplication."""
        data = event.data

        # Use trade_id if available
        if hasattr(data, "trade_id") and data.trade_id:
            return f"{self.prefix}{data.source}:{data.symbol}:{data.trade_id}"

        # Otherwise hash the content
        content = f"{data.timestamp}:{data.symbol}:{data.source}"
        if hasattr(data, "price"):
            content += f":{data.price}"
        if hasattr(data, "volume"):
            content += f":{data.volume}"

        hash_val = hashlib.md5(content.encode()).hexdigest()[:16]
        return f"{self.prefix}{hash_val}"

    async def is_duplicate(self, event: DataEvent) -> bool:
        """Check if event is a duplicate."""
        key = self._generate_key(event)

        # Try to set with NX (only if not exists)
        result = await self.redis.set(key, "1", nx=True, ex=self.ttl)

        # If result is None, key existed (duplicate)
        return result is None

    async def clear(self):
        """Clear all deduplication keys."""
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(cursor, match=f"{self.prefix}*", count=100)
            if keys:
                await self.redis.delete(*keys)
            if cursor == 0:
                break


class DataQualityScorer:
    """
    Scores data quality based on various metrics.
    """

    def __init__(self):
        self.symbol_stats: Dict[str, Dict] = {}

    def score_tick(self, tick: NormalizedTick) -> float:
        """Score a tick for data quality (0-1)."""
        score = 1.0
        symbol = tick.symbol

        # Initialize stats
        if symbol not in self.symbol_stats:
            self.symbol_stats[symbol] = {
                "last_price": None,
                "last_time": None,
                "price_history": [],
            }

        stats = self.symbol_stats[symbol]

        # Check timestamp freshness
        now = datetime.now(timezone.utc)
        age_seconds = (now - tick.timestamp).total_seconds()
        if age_seconds > 60:
            score -= 0.3  # Stale data
        elif age_seconds > 10:
            score -= 0.1

        # Check price reasonableness
        if stats["last_price"] is not None:
            price_change = abs(float(tick.price) - stats["last_price"]) / stats["last_price"]
            if price_change > 0.10:  # >10% move
                score -= 0.4  # Suspicious spike
            elif price_change > 0.05:
                score -= 0.2

        # Check timestamp ordering
        if stats["last_time"] is not None and tick.timestamp < stats["last_time"]:
            score -= 0.2  # Out of order

        # Update stats
        stats["last_price"] = float(tick.price)
        stats["last_time"] = tick.timestamp

        return max(0.0, score)

    def score_quote(self, quote: NormalizedQuote) -> float:
        """Score a quote for data quality."""
        score = 1.0

        # Check spread reasonableness
        if quote.ask_price > 0 and quote.bid_price > 0:
            spread = float(quote.ask_price - quote.bid_price) / float(quote.bid_price)
            if spread < 0:
                score -= 0.5  # Crossed market
            elif spread > 0.10:
                score -= 0.3  # Wide spread

        # Check sizes
        if quote.bid_size <= 0 or quote.ask_size <= 0:
            score -= 0.2

        return max(0.0, score)


class RedisStreamPublisher:
    """
    Publishes normalized data to Redis Streams.
    """

    def __init__(
        self,
        redis: Redis,
        max_stream_length: int = 100000,
    ):
        self.redis = redis
        self.max_length = max_stream_length
        self.metrics = PipelineMetrics()

    def _serialize_event(self, event: DataEvent) -> Dict[str, str]:
        """Serialize event to Redis stream format."""
        data = event.data

        result = {
            "type": event.event_type.value,
            "source": data.source.value,
            "symbol": data.symbol,
            "timestamp": data.timestamp.isoformat(),
            "received_at": datetime.now(timezone.utc).isoformat(),
        }

        if isinstance(data, NormalizedTick):
            result.update(
                {
                    "price": str(data.price),
                    "volume": str(data.volume),
                    "side": data.side,
                    "trade_id": data.trade_id or "",
                    "exchange": data.exchange,
                }
            )
        elif isinstance(data, NormalizedQuote):
            result.update(
                {
                    "bid_price": str(data.bid_price),
                    "bid_size": str(data.bid_size),
                    "ask_price": str(data.ask_price),
                    "ask_size": str(data.ask_size),
                    "exchange": data.exchange,
                }
            )
        elif isinstance(data, NormalizedBar):
            result.update(
                {
                    "open": str(data.open),
                    "high": str(data.high),
                    "low": str(data.low),
                    "close": str(data.close),
                    "volume": str(data.volume),
                    "timeframe": data.timeframe,
                    "exchange": data.exchange,
                }
            )

        return result

    async def publish(self, event: DataEvent) -> str:
        """Publish event to appropriate stream."""
        # Select stream based on event type
        stream_map = {
            DataType.TRADE: StreamName.TRADES.value,
            DataType.QUOTE: StreamName.QUOTES.value,
            DataType.BAR: StreamName.BARS.value,
            DataType.ORDERBOOK: StreamName.ORDERBOOKS.value,
        }

        stream = stream_map.get(event.event_type, StreamName.TRADES.value)

        # Serialize and publish
        data = self._serialize_event(event)

        message_id = await self.redis.xadd(
            stream,
            data,
            maxlen=self.max_length,
            approximate=True,
        )

        self.metrics.messages_processed += 1
        self.metrics.last_message_time = datetime.now(timezone.utc)

        # Calculate latency
        event_time = event.data.timestamp
        now = datetime.now(timezone.utc)
        latency_ms = (now - event_time).total_seconds() * 1000
        self.metrics.record_latency(latency_ms)

        return message_id


class RedisStreamConsumer:
    """
    Consumes data from Redis Streams using consumer groups.
    """

    def __init__(
        self,
        redis: Redis,
        group_name: str,
        consumer_name: str,
        streams: List[str],
        batch_size: int = 100,
        block_ms: int = 1000,
    ):
        self.redis = redis
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.streams = streams
        self.batch_size = batch_size
        self.block_ms = block_ms

        self._running = False
        self._handlers: Dict[str, List[Callable]] = {}

    async def setup(self):
        """Create consumer groups if they don't exist."""
        for stream in self.streams:
            try:
                await self.redis.xgroup_create(
                    stream,
                    self.group_name,
                    id="0",
                    mkstream=True,
                )
                logger.info(f"Created consumer group {self.group_name} for {stream}")
            except Exception as e:
                if "BUSYGROUP" not in str(e):
                    raise

    def add_handler(self, stream: str, handler: Callable):
        """Add message handler for a stream."""
        if stream not in self._handlers:
            self._handlers[stream] = []
        self._handlers[stream].append(handler)

    async def start(self):
        """Start consuming messages."""
        await self.setup()
        self._running = True

        logger.info(f"Starting consumer {self.consumer_name} in group {self.group_name}")

        while self._running:
            try:
                # Read from all streams
                streams_dict = {s: ">" for s in self.streams}

                messages = await self.redis.xreadgroup(
                    self.group_name,
                    self.consumer_name,
                    streams_dict,
                    count=self.batch_size,
                    block=self.block_ms,
                )

                if messages:
                    for stream_name, stream_messages in messages:
                        stream_name = (
                            stream_name.decode() if isinstance(stream_name, bytes) else stream_name
                        )

                        for message_id, data in stream_messages:
                            await self._process_message(stream_name, message_id, data)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Consumer error: {e}")
                await asyncio.sleep(1)

    async def stop(self):
        """Stop consuming."""
        self._running = False

    async def _process_message(
        self,
        stream: str,
        message_id: bytes,
        data: Dict[bytes, bytes],
    ):
        """Process a single message."""
        try:
            # Decode data
            decoded = {
                k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
                for k, v in data.items()
            }

            # Call handlers
            handlers = self._handlers.get(stream, [])
            for handler in handlers:
                await handler(decoded)

            # Acknowledge message
            await self.redis.xack(stream, self.group_name, message_id)

        except Exception as e:
            logger.error(f"Failed to process message {message_id}: {e}")


class DataPipeline:
    """
    Main data pipeline orchestrating feeds, processing, and storage.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        enable_dedup: bool = True,
        enable_quality_scoring: bool = True,
    ):
        self.redis_url = redis_url
        self.enable_dedup = enable_dedup
        self.enable_quality_scoring = enable_quality_scoring

        self.redis: Optional[Redis] = None
        self.publisher: Optional[RedisStreamPublisher] = None
        self.dedup_cache: Optional[DeduplicationCache] = None
        self.quality_scorer: Optional[DataQualityScorer] = None

        self._feeds: Dict[str, Any] = {}
        self._running = False
        self._tasks: List[asyncio.Task] = []

        # Metrics
        self.metrics = PipelineMetrics()

    async def initialize(self):
        """Initialize pipeline components."""
        self.redis = await aioredis.from_url(
            self.redis_url,
            decode_responses=False,
        )

        self.publisher = RedisStreamPublisher(self.redis)

        if self.enable_dedup:
            self.dedup_cache = DeduplicationCache(self.redis)

        if self.enable_quality_scoring:
            self.quality_scorer = DataQualityScorer()

        logger.info("Data pipeline initialized")

    def add_feed(self, name: str, feed: Any):
        """Add a data feed to the pipeline."""
        self._feeds[name] = feed

        # Register event handler
        async def handle_event(event: DataEvent):
            await self.process_event(event)

        feed.add_event_callback(handle_event)
        logger.info(f"Added feed: {name}")

    async def process_event(self, event: DataEvent):
        """Process incoming data event."""
        self.metrics.messages_received += 1

        try:
            # Deduplication
            if self.enable_dedup and self.dedup_cache:
                if await self.dedup_cache.is_duplicate(event):
                    self.metrics.duplicates_filtered += 1
                    return

            # Quality scoring
            if self.enable_quality_scoring and self.quality_scorer:
                if event.event_type == DataType.TRADE:
                    score = self.quality_scorer.score_tick(event.data)
                elif event.event_type == DataType.QUOTE:
                    score = self.quality_scorer.score_quote(event.data)
                else:
                    score = 1.0

                # Drop low quality data
                if score < 0.3:
                    self.metrics.messages_dropped += 1
                    logger.warning(f"Dropped low quality data: {event.data.symbol} score={score}")
                    return

            # Publish to stream
            await self.publisher.publish(event)

        except Exception as e:
            self.metrics.errors += 1
            logger.error(f"Failed to process event: {e}")

    async def start(self):
        """Start all feeds and the pipeline."""
        self._running = True

        # Connect and start all feeds
        for name, feed in self._feeds.items():
            logger.info(f"Starting feed: {name}")

            # Connect
            await feed.connect()

            # Start streaming task
            task = asyncio.create_task(feed.start_streaming())
            self._tasks.append(task)

        logger.info("Pipeline started with all feeds")

    async def stop(self):
        """Stop all feeds and the pipeline."""
        self._running = False

        # Stop all feeds
        for name, feed in self._feeds.items():
            logger.info(f"Stopping feed: {name}")
            await feed.stop_streaming()
            await feed.disconnect()

        # Cancel tasks
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self._tasks.clear()

        # Close Redis
        if self.redis:
            await self.redis.close()

        logger.info("Pipeline stopped")

    def get_metrics(self) -> Dict[str, Any]:
        """Get pipeline metrics."""
        return {
            "messages_received": self.metrics.messages_received,
            "messages_processed": self.metrics.messages_processed,
            "messages_dropped": self.metrics.messages_dropped,
            "duplicates_filtered": self.metrics.duplicates_filtered,
            "errors": self.metrics.errors,
            "avg_latency_ms": self.metrics.avg_latency_ms,
            "max_latency_ms": self.metrics.max_latency_ms,
            "last_message_time": self.metrics.last_message_time.isoformat()
            if self.metrics.last_message_time
            else None,
            "publisher_metrics": {
                "messages_processed": self.publisher.metrics.messages_processed
                if self.publisher
                else 0,
                "avg_latency_ms": self.publisher.metrics.avg_latency_ms if self.publisher else 0,
            },
            "feeds": {name: feed.connection.state.value for name, feed in self._feeds.items()},
        }


class SourceFailover:
    """
    Manages failover between multiple data sources.
    """

    def __init__(
        self,
        primary_feed: Any,
        secondary_feeds: List[Any],
        failover_threshold_seconds: float = 30.0,
        recovery_threshold_seconds: float = 60.0,
    ):
        self.primary = primary_feed
        self.secondaries = secondary_feeds
        self.failover_threshold = failover_threshold_seconds
        self.recovery_threshold = recovery_threshold_seconds

        self.active_feed = primary_feed
        self._last_data_time: Dict[str, datetime] = {}
        self._in_failover = False

    async def check_health(self) -> bool:
        """Check if active feed is healthy."""
        now = datetime.now(timezone.utc)

        # Check connection state
        if not self.active_feed.is_connected:
            return False

        # Check data freshness
        feed_name = self.active_feed.name
        last_time = self._last_data_time.get(feed_name)

        if last_time:
            age = (now - last_time).total_seconds()
            if age > self.failover_threshold:
                return False

        return True

    async def failover(self):
        """Switch to secondary feed."""
        if self._in_failover:
            return

        self._in_failover = True
        logger.warning(f"Initiating failover from {self.active_feed.name}")

        for secondary in self.secondaries:
            if secondary.is_connected or await secondary.connect():
                logger.info(f"Failing over to {secondary.name}")
                self.active_feed = secondary
                self._in_failover = False
                return

        logger.error("All secondary feeds unavailable")
        self._in_failover = False

    async def recover(self):
        """Attempt to recover to primary feed."""
        if self.active_feed == self.primary:
            return

        if await self.primary.connect():
            # Wait and verify primary is healthy
            await asyncio.sleep(self.recovery_threshold)

            if await self.check_health():
                logger.info("Recovering to primary feed")
                self.active_feed = self.primary

    def record_data(self, feed_name: str):
        """Record data reception time for a feed."""
        self._last_data_time[feed_name] = datetime.now(timezone.utc)


# Export
__all__ = [
    "StreamName",
    "PipelineMetrics",
    "DeduplicationCache",
    "DataQualityScorer",
    "RedisStreamPublisher",
    "RedisStreamConsumer",
    "DataPipeline",
    "SourceFailover",
]
