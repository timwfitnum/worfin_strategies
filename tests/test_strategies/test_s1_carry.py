"""tests/test_strategies/test_s1_carry.py"""

from __future__ import annotations

from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest

from worfin.strategies.s1_carry import S1_CONFIG, CarryStrategy


def make_carry_data(n_days=60, base_price=9000.0, backwardation=True):
    """Sample data with known carry direction."""
    dates = pd.bdate_range(end="2020-06-30", periods=n_days, tz="UTC")
    prices = base_price * np.ones(n_days)
    cash = prices * (1.01 if backwardation else 0.99)
    f3m = prices
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
    return CarryStrategy()


AS_OF = datetime(2020, 6, 30, 14, 0, tzinfo=UTC)


class TestS1SignalRange:
    def test_signals_in_valid_range(self, strategy):
        np.random.seed(1)
        metals = {
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
        data = {}
        for t, p in metals.items():
            carry_bps = np.random.uniform(-0.02, 0.03)
            data[t] = make_carry_data(base_price=p, backwardation=(carry_bps > 0))

        result = strategy.run(data, as_of=AS_OF)
        assert result.is_valid
        for ticker, signal in result.signals.items():
            assert -1.0 - 1e-9 <= signal <= 1.0 + 1e-9

    def test_has_valid_window(self, strategy):
        np.random.seed(2)
        metals = {
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
        data = {t: make_carry_data(base_price=p) for t, p in metals.items()}
        result = strategy.run(data, as_of=AS_OF)
        assert result.valid_until > result.valid_from


class TestS1CarryDirection:
    def test_backwardation_positive_signal_relative(self, strategy):
        """Metal in backwardation should rank higher than metal in contango."""
        metals = {
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
        data = {}
        for t, p in metals.items():
            # All in slight contango except CA which is in backwardation
            data[t] = make_carry_data(base_price=p, backwardation=(t == "CA"))

        result = strategy.run(data, as_of=AS_OF)
        if result.is_valid:
            # CA (backwardation) should have positive signal vs most others
            assert result.signals.get("CA", 0) > 0

    def test_metadata_contains_carry_info(self, strategy):
        metals = {
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
        data = {t: make_carry_data(base_price=p) for t, p in metals.items()}
        result = strategy.run(data, as_of=AS_OF)
        for ticker in strategy.universe:
            if ticker in result.signal_metadata:
                meta = result.signal_metadata[ticker]
                assert "raw_carry" in meta
                assert "z_carry" in meta
                assert "in_backwardation" in meta


class TestS1Config:
    def test_frequency_is_daily(self):
        assert S1_CONFIG.frequency == "daily"

    def test_rebalance_is_weekly(self):
        assert S1_CONFIG.rebalance_freq == "weekly"

    def test_target_vol_lower_than_s4(self):
        from worfin.strategies.s4_basis_momentum import S4_CONFIG

        assert S1_CONFIG.target_vol < S4_CONFIG.target_vol
