"""
Cryptocurrency Exchange Data Feed Integrations.

Provides unified interfaces for major crypto exchanges:
- Binance (spot and futures)
- Coinbase (Exchange/Pro)
"""

from .binance import BinanceDataFeed
from .coinbase import CoinbaseDataFeed

__all__ = [
    "BinanceDataFeed",
    "CoinbaseDataFeed",
]
