"""
Data Feed Infrastructure for Quantitative Trading System.

Provides unified data access across multiple providers:
- Interactive Brokers (equities, futures, options, forex)
- Polygon.io (US equities and options)
- Binance (cryptocurrency spot and futures)
- Coinbase (cryptocurrency)

All feeds normalize data to common formats defined in base.py.
"""

from .base import (
    # Enums
    AssetClass,
    ConnectionState,
    DataSource,
    DataType,
    # Models
    DataEvent,
    NormalizedBar,
    NormalizedOrderBook,
    NormalizedQuote,
    NormalizedTick,
    # Base classes
    BaseDataFeed,
    BaseHistoricalDataFeed,
    BaseRealtimeDataFeed,
    # Utilities
    ConnectionManager,
    RateLimiter,
    SymbolMapper,
)

from .interactive_brokers import IBDataFeed
from .polygon_client import PolygonDataFeed

# Crypto feeds
from .crypto import BinanceDataFeed, CoinbaseDataFeed

__all__ = [
    # Enums
    "AssetClass",
    "ConnectionState",
    "DataSource",
    "DataType",
    # Models
    "DataEvent",
    "NormalizedBar",
    "NormalizedOrderBook",
    "NormalizedQuote",
    "NormalizedTick",
    # Base classes
    "BaseDataFeed",
    "BaseHistoricalDataFeed",
    "BaseRealtimeDataFeed",
    # Utilities
    "ConnectionManager",
    "RateLimiter",
    "SymbolMapper",
    # Feed implementations
    "IBDataFeed",
    "PolygonDataFeed",
    "BinanceDataFeed",
    "CoinbaseDataFeed",
]
