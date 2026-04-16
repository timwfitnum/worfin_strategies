"""tests/test_data/test_carry_pipeline.py"""
from __future__ import annotations
import numpy as np
import pandas as pd
import pytest
from worfin.data.pipeline.carry import (
    compute_carry, compute_carry_series, cross_sectional_carry_zscore,
)


class TestComputeCarry:
    def test_backwardation_positive(self):
        carry = compute_carry(cash_price=9100.0, f3m_price=9000.0, dte=91)
        assert carry > 0

    def test_contango_negative(self):
        carry = compute_carry(cash_price=8900.0, f3m_price=9000.0, dte=91)
        assert carry < 0

    def test_zero_basis_zero_carry(self):
        carry = compute_carry(cash_price=9000.0, f3m_price=9000.0, dte=91)
        assert abs(carry) < 1e-10

    def test_invalid_dte_raises(self):
        with pytest.raises(ValueError):
            compute_carry(9000.0, 9000.0, dte=0)

    def test_invalid_price_raises(self):
        with pytest.raises(ValueError):
            compute_carry(0.0, 9000.0, dte=91)

    def test_annualisation(self):
        # At 91 days, carry should be approx 4× the 91-day rate
        carry_91 = compute_carry(9100.0, 9000.0, dte=91)
        # Approximate: (100/9100) * (365/91) ≈ 0.044
        assert abs(carry_91 - (100/9100) * (365/91)) < 0.001


class TestCarrySeries:
    def make_series(self, n=100, cash_mult=1.01):
        dates = pd.bdate_range(end="2020-06-30", periods=n, tz="UTC")
        prices = pd.Series(9000.0 * np.ones(n), index=dates)
        return prices * cash_mult, prices

    def test_produces_series_same_length(self):
        cash, f3m = self.make_series()
        result = compute_carry_series(cash, f3m, ticker="CA")
        assert len(result) == len(cash)

    def test_backwardation_series_positive(self):
        cash, f3m = self.make_series(cash_mult=1.01)
        result = compute_carry_series(cash, f3m, ticker="CA")
        assert (result.dropna() > 0).all()

    def test_nan_input_produces_nan(self):
        cash, f3m = self.make_series()
        cash.iloc[5] = np.nan
        result = compute_carry_series(cash, f3m, ticker="CA")
        assert pd.isna(result.iloc[5])


class TestCrossSectionalZscore:
    def test_output_range(self):
        dates = pd.bdate_range(end="2020-06-30", periods=50, tz="UTC")
        carry_dict = {
            t: pd.Series(np.random.randn(50) * 0.01, index=dates)
            for t in ["CA", "AH", "ZS", "GC", "SI", "NI", "PB", "SN", "PL", "PA"]
        }
        result = cross_sectional_carry_zscore(carry_dict)
        assert result.abs().max().max() <= 1.0 + 1e-9