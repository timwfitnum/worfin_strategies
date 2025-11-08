"""
Database Abstraction Layer for Quantitative Trading System
Provides optimized interfaces for ClickHouse and TimescaleDB
"""

import asyncio
import logging
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

import asyncpg
import clickhouse_driver
import numpy as np
import pandas as pd
import redis
from clickhouse_driver import Client as ClickHouseClient
from redis.asyncio import Redis as AsyncRedis
from redis.connection import ConnectionPool
from sqlalchemy import create_engine, pool
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)


# =====================================================
# Configuration Classes
# =====================================================


@dataclass
class DatabaseConfig:
    """Database connection configuration"""

    # ClickHouse
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 9000
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "default"
    clickhouse_pool_size: int = 10

    # TimescaleDB
    timescale_host: str = "localhost"
    timescale_port: int = 5432
    timescale_user: str = "quant"
    timescale_password: str = "quant123"
    timescale_database: str = "quant_trading"
    timescale_pool_size: int = 20

    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: Optional[str] = None
    redis_db: int = 0
    redis_pool_size: int = 50


class QueryType(Enum):
    """Query type enumeration for routing"""

    TICK_DATA = "tick_data"
    OHLCV = "ohlcv"
    REFERENCE = "reference"
    POSITION = "position"
    SIGNAL = "signal"
    ANALYTICS = "analytics"


# =====================================================
# ClickHouse Manager
# =====================================================


class ClickHouseManager:
    """
    ClickHouse connection manager with pooling and batch operations
    Optimized for high-throughput tick data operations
    """

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.clients_pool: List[ClickHouseClient] = []
        self.current_client = 0
        self._init_pool()

    def _init_pool(self):
        """Initialize connection pool"""
        for _ in range(self.config.clickhouse_pool_size):
            client = ClickHouseClient(
                host=self.config.clickhouse_host,
                port=self.config.clickhouse_port,
                user=self.config.clickhouse_user,
                password=self.config.clickhouse_password,
                database=self.config.clickhouse_database,
                settings={
                    "max_threads": 8,
                    "max_memory_usage": 10_000_000_000,  # 10GB
                    "use_uncompressed_cache": True,
                    "query_profiler_real_time_period_ns": 1_000_000_000,
                },
            )
            self.clients_pool.append(client)

    def get_client(self) -> ClickHouseClient:
        """Get next client from pool (round-robin)"""
        client = self.clients_pool[self.current_client]
        self.current_client = (self.current_client + 1) % len(self.clients_pool)
        return client

    def insert_ticks(self, ticks: List[Dict[str, Any]], table: str = "ticks.trades"):
        """
        Batch insert tick data with optimal chunking

        Args:
            ticks: List of tick dictionaries
            table: Target table name
        """
        client = self.get_client()

        # Convert to DataFrame for efficient processing
        df = pd.DataFrame(ticks)

        # Ensure proper data types
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Insert in optimized chunks
        chunk_size = 100_000  # Optimal for ClickHouse
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            client.execute(
                f"INSERT INTO {table} VALUES", chunk.to_dict("records"), types_check=False
            )

        logger.info(f"Inserted {len(ticks)} ticks into {table}")

    def query_ohlcv(
        self, symbol: str, start_time: datetime, end_time: datetime, timeframe: str = "1m"
    ) -> pd.DataFrame:
        """
        Query OHLCV data with caching

        Args:
            symbol: Trading symbol
            start_time: Start timestamp
            end_time: End timestamp
            timeframe: Timeframe (1m, 5m, 1h, 1d)

        Returns:
            DataFrame with OHLCV data
        """
        client = self.get_client()

        table_map = {
            "1m": "ohlcv.bars_1m",
            "5m": "ohlcv.bars_5m",
            "1h": "ohlcv.bars_1h",
            "1d": "ohlcv.bars_daily",
        }

        table = table_map.get(timeframe, "ohlcv.bars_1m")

        query = f"""
        SELECT
            timestamp,
            open,
            high,
            low,
            close,
            volume,
            trades,
            vwap
        FROM {table}
        WHERE symbol = %(symbol)s
            AND timestamp >= %(start_time)s
            AND timestamp <= %(end_time)s
        ORDER BY timestamp
        """

        result = client.execute(
            query,
            {"symbol": symbol, "start_time": start_time, "end_time": end_time},
            with_column_types=True,
        )

        if result:
            columns = [col[0] for col in result[-1]]
            data = result[0]
            return pd.DataFrame(data, columns=columns)
        return pd.DataFrame()

    def get_latest_ticks(self, symbol: str, limit: int = 1000) -> pd.DataFrame:
        """Get latest ticks for a symbol"""
        client = self.get_client()

        query = """
        SELECT *
        FROM ticks.trades
        WHERE symbol = %(symbol)s
        ORDER BY timestamp DESC
        LIMIT %(limit)s
        """

        result = client.execute(query, {"symbol": symbol, "limit": limit}, with_column_types=True)

        if result:
            columns = [col[0] for col in result[-1]]
            data = result[0]
            df = pd.DataFrame(data, columns=columns)
            return df.sort_values("timestamp")
        return pd.DataFrame()

    def calculate_vwap(self, symbol: str, start_time: datetime, end_time: datetime) -> float:
        """Calculate VWAP for a period"""
        client = self.get_client()

        query = """
        SELECT sum(price * volume) / sum(volume) as vwap
        FROM ticks.trades
        WHERE symbol = %(symbol)s
            AND timestamp >= %(start_time)s
            AND timestamp <= %(end_time)s
        """

        result = client.execute(
            query, {"symbol": symbol, "start_time": start_time, "end_time": end_time}
        )

        return float(result[0][0]) if result and result[0][0] else 0.0

    def get_market_microstructure(self, symbol: str, date: datetime) -> Dict[str, Any]:
        """Get market microstructure metrics"""
        client = self.get_client()

        query = """
        WITH stats AS (
            SELECT
                count() as total_trades,
                sum(volume) as total_volume,
                avg(price) as avg_price,
                stddevPop(price) as price_std,
                max(price) as high,
                min(price) as low
            FROM ticks.trades
            WHERE symbol = %(symbol)s
                AND toDate(timestamp) = %(date)s
        ),
        spreads AS (
            SELECT
                avg(spread) as avg_spread,
                max(spread) as max_spread,
                min(spread) as min_spread,
                stddevPop(spread) as spread_std
            FROM ticks.quotes
            WHERE symbol = %(symbol)s
                AND toDate(timestamp) = %(date)s
        )
        SELECT
            stats.*,
            spreads.*,
            (stats.high - stats.low) / stats.avg_price as price_range_pct
        FROM stats, spreads
        """

        result = client.execute(
            query, {"symbol": symbol, "date": date.date()}, with_column_types=True
        )

        if result and result[0]:
            columns = [col[0] for col in result[-1]]
            data = dict(zip(columns, result[0][0]))
            return data
        return {}

    def close(self):
        """Close all connections in pool"""
        for client in self.clients_pool:
            client.disconnect()


# =====================================================
# TimescaleDB Manager
# =====================================================


class TimescaleDBManager:
    """
    TimescaleDB manager for reference data and positions
    Handles transactional operations with ACID compliance
    """

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine = self._create_engine()
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.async_pool = None

    def _create_engine(self):
        """Create SQLAlchemy engine with connection pooling"""
        connection_string = (
            f"postgresql://{self.config.timescale_user}:"
            f"{self.config.timescale_password}@"
            f"{self.config.timescale_host}:"
            f"{self.config.timescale_port}/"
            f"{self.config.timescale_database}"
        )

        return create_engine(
            connection_string,
            poolclass=pool.QueuePool,
            pool_size=self.config.timescale_pool_size,
            max_overflow=self.config.timescale_pool_size * 2,
            pool_recycle=3600,
            pool_pre_ping=True,
            echo=False,
        )

    async def init_async_pool(self):
        """Initialize async connection pool"""
        self.async_pool = await asyncpg.create_pool(
            host=self.config.timescale_host,
            port=self.config.timescale_port,
            user=self.config.timescale_user,
            password=self.config.timescale_password,
            database=self.config.timescale_database,
            min_size=5,
            max_size=self.config.timescale_pool_size,
            command_timeout=60,
        )

    @contextmanager
    def get_session(self):
        """Get database session with automatic cleanup"""
        session = self.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    @asynccontextmanager
    async def get_async_connection(self):
        """Get async database connection"""
        if not self.async_pool:
            await self.init_async_pool()

        async with self.async_pool.acquire() as connection:
            yield connection

    def get_assets(self, active_only: bool = True) -> pd.DataFrame:
        """Get asset universe"""
        with self.get_session() as session:
            query = """
            SELECT *
            FROM assets
            WHERE (:active_only = false OR active = true)
            ORDER BY symbol
            """

            df = pd.read_sql(query, session.bind, params={"active_only": active_only})
            return df

    def get_positions(
        self, strategy_id: Optional[int] = None, status: str = "open"
    ) -> pd.DataFrame:
        """Get current positions"""
        with self.get_session() as session:
            query = """
            SELECT p.*, a.name as asset_name, a.asset_class
            FROM positions p
            JOIN assets a ON p.symbol = a.symbol
            WHERE status = :status
            """

            params = {"status": status}

            if strategy_id:
                query += " AND strategy_id = :strategy_id"
                params["strategy_id"] = strategy_id

            df = pd.read_sql(query, session.bind, params=params)
            return df

    async def insert_signal(
        self,
        strategy_id: int,
        symbol: str,
        signal_type: str,
        strength: float,
        metadata: Optional[Dict] = None,
    ):
        """Insert trading signal asynchronously"""
        async with self.get_async_connection() as conn:
            await conn.execute(
                """
                INSERT INTO signals (
                    timestamp, strategy_id, symbol, signal_type,
                    strength, metadata, created_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                datetime.utcnow(),
                strategy_id,
                symbol,
                signal_type,
                strength,
                metadata or {},
                datetime.utcnow(),
            )

    async def update_position(
        self, position_id: int, current_price: float, quantity: Optional[float] = None
    ):
        """Update position with new price/quantity"""
        async with self.get_async_connection() as conn:
            if quantity is not None:
                await conn.execute(
                    """
                    UPDATE positions
                    SET current_price = $1,
                        quantity = $2,
                        updated_at = $3
                    WHERE id = $4
                    """,
                    current_price,
                    quantity,
                    datetime.utcnow(),
                    position_id,
                )
            else:
                await conn.execute(
                    """
                    UPDATE positions
                    SET current_price = $1,
                        updated_at = $2
                    WHERE id = $3
                    """,
                    current_price,
                    datetime.utcnow(),
                    position_id,
                )

    def get_performance_metrics(
        self, strategy_id: int, start_date: datetime, end_date: datetime
    ) -> pd.DataFrame:
        """Get strategy performance metrics"""
        with self.get_session() as session:
            query = """
            SELECT *
            FROM performance_metrics
            WHERE strategy_id = :strategy_id
                AND timestamp >= :start_date
                AND timestamp <= :end_date
            ORDER BY timestamp
            """

            df = pd.read_sql(
                query,
                session.bind,
                params={"strategy_id": strategy_id, "start_date": start_date, "end_date": end_date},
            )
            return df

    def get_risk_metrics(self, breach_only: bool = False) -> pd.DataFrame:
        """Get current risk metrics"""
        with self.get_session() as session:
            query = """
            SELECT *
            FROM risk_metrics
            WHERE timestamp >= NOW() - INTERVAL '1 hour'
            """

            if breach_only:
                query += " AND breach = true"

            query += " ORDER BY severity DESC, timestamp DESC"

            df = pd.read_sql(query, session.bind)
            return df

    async def close_async(self):
        """Close async connection pool"""
        if self.async_pool:
            await self.async_pool.close()


# =====================================================
# Redis Cache Manager
# =====================================================


class RedisCacheManager:
    """
    Redis manager for hot data caching and real-time state
    """

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.pool = self._create_pool()
        self.client = redis.Redis(connection_pool=self.pool)
        self.async_client = None

    def _create_pool(self):
        """Create Redis connection pool"""
        return ConnectionPool(
            host=self.config.redis_host,
            port=self.config.redis_port,
            password=self.config.redis_password,
            db=self.config.redis_db,
            max_connections=self.config.redis_pool_size,
            decode_responses=True,
        )

    async def init_async_client(self):
        """Initialize async Redis client"""
        self.async_client = await AsyncRedis(
            host=self.config.redis_host,
            port=self.config.redis_port,
            password=self.config.redis_password,
            db=self.config.redis_db,
            decode_responses=True,
        )

    def cache_tick(self, symbol: str, tick: Dict[str, Any], ttl: int = 7200):
        """Cache latest tick data"""
        key = f"tick:{symbol}:latest"
        self.client.hset(key, mapping=tick)
        self.client.expire(key, ttl)

    def get_cached_tick(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get cached tick data"""
        key = f"tick:{symbol}:latest"
        data = self.client.hgetall(key)
        return data if data else None

    def cache_ohlcv(self, symbol: str, timeframe: str, bars: pd.DataFrame, ttl: int = 300):
        """Cache OHLCV bars"""
        key = f"ohlcv:{symbol}:{timeframe}"
        # Convert DataFrame to JSON for caching
        json_data = bars.to_json(date_format="iso", orient="records")
        self.client.setex(key, ttl, json_data)

    def get_cached_ohlcv(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """Get cached OHLCV bars"""
        key = f"ohlcv:{symbol}:{timeframe}"
        data = self.client.get(key)
        if data:
            return pd.read_json(data, orient="records")
        return None

    def publish_signal(self, channel: str, signal: Dict[str, Any]):
        """Publish trading signal to channel"""
        import json

        self.client.publish(channel, json.dumps(signal))

    def subscribe_signals(self, channels: List[str]):
        """Subscribe to signal channels"""
        pubsub = self.client.pubsub()
        for channel in channels:
            pubsub.subscribe(channel)
        return pubsub

    async def async_set(self, key: str, value: Any, ttl: Optional[int] = None):
        """Async set with optional TTL"""
        if not self.async_client:
            await self.init_async_client()

        if ttl:
            await self.async_client.setex(key, ttl, value)
        else:
            await self.async_client.set(key, value)

    async def async_get(self, key: str) -> Optional[str]:
        """Async get value"""
        if not self.async_client:
            await self.init_async_client()

        return await self.async_client.get(key)


# =====================================================
# Unified Database Manager
# =====================================================


class DatabaseManager:
    """
    Unified database manager routing queries to appropriate backend
    """

    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig()
        self.clickhouse = ClickHouseManager(self.config)
        self.timescale = TimescaleDBManager(self.config)
        self.redis = RedisCacheManager(self.config)

        # Query routing configuration
        self.query_routes = {
            QueryType.TICK_DATA: self.clickhouse,
            QueryType.OHLCV: self.clickhouse,
            QueryType.REFERENCE: self.timescale,
            QueryType.POSITION: self.timescale,
            QueryType.SIGNAL: self.timescale,
            QueryType.ANALYTICS: self.clickhouse,
        }

    async def initialize(self):
        """Initialize async components"""
        await self.timescale.init_async_pool()
        await self.redis.init_async_client()

    def get_ohlcv_with_cache(
        self, symbol: str, start_time: datetime, end_time: datetime, timeframe: str = "1m"
    ) -> pd.DataFrame:
        """Get OHLCV data with Redis caching"""
        # Check cache first
        cached = self.redis.get_cached_ohlcv(symbol, timeframe)
        if cached is not None and len(cached) > 0:
            # Filter cached data by time range
            cached = cached[(cached["timestamp"] >= start_time) & (cached["timestamp"] <= end_time)]
            if len(cached) > 0:
                logger.info(f"Cache hit for {symbol} {timeframe}")
                return cached

        # Query from ClickHouse
        data = self.clickhouse.query_ohlcv(symbol, start_time, end_time, timeframe)

        # Cache the result
        if len(data) > 0:
            self.redis.cache_ohlcv(symbol, timeframe, data)

        return data

    async def process_tick_stream(self, ticks: List[Dict[str, Any]]):
        """Process incoming tick stream"""
        # Batch insert to ClickHouse
        self.clickhouse.insert_ticks(ticks)

        # Cache latest tick for each symbol
        for tick in ticks:
            symbol = tick.get("symbol")
            if symbol:
                self.redis.cache_tick(symbol, tick)

    def get_portfolio_snapshot(self, strategy_id: Optional[int] = None) -> Dict[str, Any]:
        """Get complete portfolio snapshot"""
        # Get positions from TimescaleDB
        positions = self.timescale.get_positions(strategy_id)

        # Get latest prices from Redis cache
        for idx, row in positions.iterrows():
            cached_tick = self.redis.get_cached_tick(row["symbol"])
            if cached_tick:
                positions.at[idx, "current_price"] = float(cached_tick.get("price", 0))

        # Calculate summary metrics
        total_value = positions["market_value"].sum()
        total_pnl = positions["unrealized_pnl"].sum()

        return {
            "positions": positions.to_dict("records"),
            "total_value": total_value,
            "total_pnl": total_pnl,
            "position_count": len(positions),
            "timestamp": datetime.utcnow(),
        }

    def close(self):
        """Close all database connections"""
        self.clickhouse.close()
        # TimescaleDB and Redis connections are handled by pools


# =====================================================
# Example Usage
# =====================================================

if __name__ == "__main__":
    # Example usage
    async def main():
        # Initialize database manager
        db = DatabaseManager()
        await db.initialize()

        # Get OHLCV data with caching
        ohlcv = db.get_ohlcv_with_cache(
            symbol="AAPL",
            start_time=datetime.now() - timedelta(days=7),
            end_time=datetime.now(),
            timeframe="1h",
        )
        print(f"Retrieved {len(ohlcv)} bars")

        # Get portfolio snapshot
        portfolio = db.get_portfolio_snapshot()
        print(f"Portfolio value: ${portfolio['total_value']:,.2f}")

        # Insert signal
        await db.timescale.insert_signal(
            strategy_id=1,
            symbol="AAPL",
            signal_type="buy",
            strength=0.75,
            metadata={"confidence": 0.85},
        )

        # Process tick stream
        sample_ticks = [
            {
                "timestamp": datetime.now(),
                "symbol": "AAPL",
                "price": 150.25,
                "volume": 1000,
                "side": "buy",
            }
        ]
        await db.process_tick_stream(sample_ticks)

        # Close connections
        db.close()

    # Run example
    asyncio.run(main())
