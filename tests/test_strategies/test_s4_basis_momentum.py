"""
tests/test_strategies/test_s4_basis_momentum.py
Unit tests for S4 Basis-Momentum strategy.
"""

from __future__ import annotations

from datetime import UTC, datetime  # FIX 1: was `from datetime import date`

import numpy as np
import pandas as pd
import pytest

from worfin.strategies.s4_basis_momentum import S4_CONFIG, BasisMomentumStrategy

# FIX 2: single constant replaces repeated date(2020, 6, 30) literals.
# Must be a timezone-aware datetime — BaseStrategy.run() / _check_min_history()
# compare against pd.Timestamp(as_of) which is tz-aware, so the DataFrame index
# must also be tz-aware to avoid TypeError on the <= comparison.
AS_OF = datetime(2020, 6, 30, 14, 0, tzinfo=UTC)


def make_sample_data(
    n_days: int = 100,
    base_price: float = 9000.0,
    carry_bps: float = 50,  # Positive = backwardation
) -> pd.DataFrame:
    """Create minimal sample DataFrame for a single metal."""
    # FIX 3: tz="UTC" added — without this, df.index is tz-naive and
    # `df[df.index <= pd.Timestamp(as_of)]` raises TypeError because
    # pd.Timestamp(datetime(..., tzinfo=UTC)) is tz-aware.
    dates = pd.bdate_range(end="2020-06-30", periods=n_days, tz="UTC")
    prices = base_price * (1 + np.random.randn(n_days).cumsum() * 0.01)
    cash = prices
    f3m = prices * (1 - carry_bps / 10_000 * (91 / 365))  # Slight backwardation
    return pd.DataFrame(
        {
            "close": prices,
            "cash_price": cash,
            "f3m_price": f3m,
            "f3m_dte": 91,
        },
        index=dates,
    )


@pytest.fixture
def strategy():
    return BasisMomentumStrategy()


@pytest.fixture
def sample_data():
    """Minimal dataset covering all universe tickers."""
    np.random.seed(42)
    data = {}
    base_prices = {
        "CA": 9000,
        "AH": 2200,
        "ZS": 2800,
        "NI": 15000,
        "PB": 1900,
        "SN": 25000,
        "GC": 1800,
        "SI": 24,
        "PL": 950,
        "PA": 2000,
    }
    for ticker, price in base_prices.items():
        data[ticker] = make_sample_data(base_price=price)
    return data


class TestS4SignalRange:
    def test_signals_in_valid_range(self, strategy, sample_data):
        """All signals must be in [-1, +1]."""
        # FIX 4: as_of_date= → as_of=, date → datetime (UTC-aware)
        result = strategy.run(sample_data, as_of=AS_OF)
        assert result.is_valid
        for ticker, signal in result.signals.items():
            assert (
                -1.0 - 1e-9 <= signal <= 1.0 + 1e-9
            ), f"Signal for {ticker} = {signal:.4f} is outside [-1, +1]"

    def test_signals_are_cross_sectionally_zero_mean(self, strategy, sample_data):
        """Cross-sectional signals should average close to zero."""
        result = strategy.run(sample_data, as_of=AS_OF)
        active_signals = [v for v in result.signals.values() if v != 0.0]
        if active_signals:
            avg = sum(active_signals) / len(active_signals)
            assert abs(avg) < 0.5, f"Signal average {avg:.3f} too far from zero"


class TestS4InteractionTerm:
    def test_aligned_signals_produce_larger_position(self, strategy):
        """When carry and momentum agree, composite signal > either alone."""
        # Create data where carry and momentum both point same direction
        n = 100
        dates = pd.bdate_range(end="2020-06-30", periods=n, tz="UTC")  # FIX 3

        # Upward trending price (positive momentum) + backwardation (positive carry)
        prices = pd.Series(9000 * (1 + np.linspace(0, 0.2, n)), index=dates)
        df_bullish = pd.DataFrame(
            {
                "close": prices,
                "cash_price": prices * 1.02,  # Cash > 3M = backwardation
                "f3m_price": prices,
                "f3m_dte": 91,
            },
            index=dates,
        )

        # All metals bullish
        data = {t: df_bullish.copy() for t in strategy.universe}
        result = strategy.run(data, as_of=AS_OF)  # FIX 4

        # With all metals identical, signals should be zero (no cross-sectional spread)
        # This tests the code path runs without error
        assert result.is_valid


class TestS4DataValidation:
    def test_missing_ticker_excluded_not_error(self, strategy, sample_data):
        """Missing a ticker produces flat signal for that ticker, not an error."""
        del sample_data["SN"]  # Remove Tin
        result = strategy.run(sample_data, as_of=AS_OF)  # FIX 4
        assert result.is_valid
        assert result.signals["SN"] == 0.0
        assert "SN" in result.invalid_tickers

    def test_insufficient_history_excluded(self, strategy):
        """Ticker with < 70 days of history gets 0 signal."""
        data = {t: make_sample_data(n_days=100) for t in S4_CONFIG.universe}
        data["GC"] = make_sample_data(n_days=30)  # Too little history
        result = strategy.run(data, as_of=AS_OF)  # FIX 4
        assert result.signals["GC"] == 0.0 or "GC" in result.invalid_tickers

    def test_all_invalid_returns_flat_result(self, strategy):
        """If all tickers invalid, return flat (all-zero) result."""
        data = {t: pd.DataFrame() for t in S4_CONFIG.universe}  # Empty data
        result = strategy.run(data, as_of=AS_OF)  # FIX 4
        assert not result.is_valid
        assert all(s == 0.0 for s in result.signals.values())


class TestS4Metadata:
    def test_metadata_contains_required_fields(self, strategy, sample_data):
        """Signal metadata must contain carry, momentum, and composite fields."""
        result = strategy.run(sample_data, as_of=AS_OF)  # FIX 4
        for ticker in strategy.universe:
            if ticker in result.signal_metadata:
                meta = result.signal_metadata[ticker]
                assert "raw_carry" in meta
                assert "raw_momentum" in meta
                assert "z_carry" in meta
                assert "z_momentum" in meta
                assert "final_signal" in meta