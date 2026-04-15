"""
tests/test_risk/test_sizing.py
Unit tests for position sizing.

100% branch coverage required — this code directly controls trade size.
Every edge case and limit must be tested explicitly.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from worfin.risk.limits import (
    MAX_SINGLE_METAL_PCT,
    MIN_POSITION_NOTIONAL_GBP,
    STRATEGY_ALLOCATION,
    VOL_FLOOR,
)
from worfin.risk.sizing import compute_lots, compute_position_notional


# ─────────────────────────────────────────────────────────────────────────────
# VOL FLOOR TESTS (critical — this protected against March 2022 Nickel)
# ─────────────────────────────────────────────────────────────────────────────

class TestVolFloor:
    """The 10% vol floor is the most safety-critical parameter in the system."""

    def test_vol_floor_applied_when_actual_vol_below_floor(self):
        """If 20d vol is 5%, position should be same as if vol were 10%."""
        # With floor (5% → 10%): notional = target_vol/floor × allocation × capital
        notional_low_vol = compute_position_notional(
            strategy_id="S4",
            ticker="GC",
            total_capital_gbp=100_000,
            realised_vol_20d=0.05,   # Below floor
            realised_vol_60d=0.05,
            signal=1.0,
        )
        notional_at_floor = compute_position_notional(
            strategy_id="S4",
            ticker="GC",
            total_capital_gbp=100_000,
            realised_vol_20d=0.10,   # Exactly at floor
            realised_vol_60d=0.10,
            signal=1.0,
        )
        assert notional_low_vol == notional_at_floor, (
            "Position with vol=5% should equal position with vol=10% (floor applied)"
        )

    def test_vol_floor_does_not_apply_above_floor(self):
        """Normal vol (>10%) should not be affected by the floor."""
        notional_normal = compute_position_notional(
            strategy_id="S4",
            ticker="GC",
            total_capital_gbp=100_000,
            realised_vol_20d=0.20,
            realised_vol_60d=0.20,
            signal=1.0,
        )
        notional_at_floor = compute_position_notional(
            strategy_id="S4",
            ticker="GC",
            total_capital_gbp=100_000,
            realised_vol_20d=0.10,
            realised_vol_60d=0.10,
            signal=1.0,
        )
        assert notional_normal < notional_at_floor, (
            "Higher vol should produce smaller position than floor"
        )

    def test_vol_floor_constant_value(self):
        """The vol floor must be exactly 10% — never changed silently."""
        assert VOL_FLOOR == 0.10, (
            f"VOL_FLOOR must be 0.10 (10%), got {VOL_FLOOR}. "
            "Changing this requires documented justification + stress test re-run."
        )


# ─────────────────────────────────────────────────────────────────────────────
# ROBUSTNESS CAP TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestRobustnessCap:
    """60-day vol cap prevents oversizing when short-term vol is compressed."""

    def test_60d_cap_applied_when_60d_vol_lower(self):
        """When 60d vol is higher than 20d vol, the 60d cap should bind."""
        notional = compute_position_notional(
            strategy_id="S4",
            ticker="GC",
            total_capital_gbp=100_000,
            realised_vol_20d=0.12,   # Low 20d vol (compressed)
            realised_vol_60d=0.20,   # Higher 60d vol (captures longer-run risk)
            signal=1.0,
        )
        # Expected: notional capped to what 60d vol (0.20) would produce
        expected_cap = compute_position_notional(
            strategy_id="S4",
            ticker="GC",
            total_capital_gbp=100_000,
            realised_vol_20d=0.20,   # 60d vol
            realised_vol_60d=0.20,
            signal=1.0,
        )
        assert notional <= expected_cap + Decimal("0.01"), (
            "60d vol cap should prevent notional exceeding what 60d vol would produce"
        )

    def test_no_cap_when_20d_vol_higher(self):
        """When 20d vol is higher than 60d, no cap applies (20d is already conservative)."""
        notional_uncapped = compute_position_notional(
            strategy_id="S4",
            ticker="GC",
            total_capital_gbp=100_000,
            realised_vol_20d=0.25,
            realised_vol_60d=0.15,   # Lower 60d would produce LARGER notional
            signal=1.0,
        )
        # 60d cap would be LARGER here — so 20d result is smaller and cap doesn't bind
        notional_60d = compute_position_notional(
            strategy_id="S4",
            ticker="GC",
            total_capital_gbp=100_000,
            realised_vol_20d=0.15,
            realised_vol_60d=0.15,
            signal=1.0,
        )
        assert notional_uncapped <= notional_60d


# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL SCALING TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestSignalScaling:

    def test_zero_signal_returns_zero_notional(self):
        """No signal → no position. This is a safety property."""
        notional = compute_position_notional(
            strategy_id="S4", ticker="GC", total_capital_gbp=100_000,
            realised_vol_20d=0.15, realised_vol_60d=0.15, signal=0.0,
        )
        assert notional == Decimal("0")

    def test_half_signal_gives_half_notional(self):
        """Signal of 0.5 should give exactly half the notional of signal 1.0."""
        n1 = compute_position_notional(
            strategy_id="S4", ticker="GC", total_capital_gbp=100_000,
            realised_vol_20d=0.15, realised_vol_60d=0.15, signal=1.0,
        )
        n_half = compute_position_notional(
            strategy_id="S4", ticker="GC", total_capital_gbp=100_000,
            realised_vol_20d=0.15, realised_vol_60d=0.15, signal=0.5,
        )
        assert abs(float(n_half) - float(n1) * 0.5) < 1.0, (
            "Half-strength signal should produce half the notional"
        )

    def test_negative_signal_gives_negative_notional(self):
        """Negative signal → short position (negative notional)."""
        notional = compute_position_notional(
            strategy_id="S4", ticker="GC", total_capital_gbp=100_000,
            realised_vol_20d=0.15, realised_vol_60d=0.15, signal=-1.0,
        )
        assert notional < Decimal("0"), "Negative signal must produce short (negative notional)"

    def test_signal_outside_range_raises(self):
        """Signal outside [-1, +1] is a bug — must raise ValueError."""
        with pytest.raises(ValueError, match=r"\[-1, \+1\]"):
            compute_position_notional(
                strategy_id="S4", ticker="GC", total_capital_gbp=100_000,
                realised_vol_20d=0.15, realised_vol_60d=0.15, signal=1.5,
            )


# ─────────────────────────────────────────────────────────────────────────────
# POSITION LIMIT TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestPositionLimits:

    def test_notional_never_exceeds_20pct_nav(self):
        """Single-metal notional must never exceed 20% of NAV."""
        capital = 100_000
        max_allowed = capital * MAX_SINGLE_METAL_PCT

        # Use extreme parameters that would produce a huge position without the cap
        notional = compute_position_notional(
            strategy_id="S4", ticker="GC", total_capital_gbp=capital,
            realised_vol_20d=VOL_FLOOR,   # Smallest vol → biggest position
            realised_vol_60d=VOL_FLOOR,
            signal=1.0,
        )
        assert float(notional) <= max_allowed + 0.01, (
            f"Notional {notional} exceeds 20% NAV limit {max_allowed}"
        )

    def test_minimum_position_size_enforced(self):
        """Positions below £5,000 should return 0 (not worth the friction)."""
        # Very small capital → position will be below minimum
        notional = compute_position_notional(
            strategy_id="S4", ticker="GC", total_capital_gbp=5_000,
            realised_vol_20d=0.20, realised_vol_60d=0.20,
            signal=0.10,   # Small signal on small capital
        )
        assert notional == Decimal("0"), (
            f"Sub-minimum notional should return 0, got {notional}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# LIQUIDITY DISCOUNT TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestLiquidityDiscount:

    def test_tier3_position_smaller_than_tier1(self):
        """Tier 3 metals (Sn, Pd) should produce smaller positions than Tier 1."""
        gold_notional = compute_position_notional(
            strategy_id="S4", ticker="GC",   # Tier 1
            total_capital_gbp=100_000,
            realised_vol_20d=0.15, realised_vol_60d=0.15, signal=1.0,
        )
        palladium_notional = compute_position_notional(
            strategy_id="S4", ticker="PA",   # Tier 3
            total_capital_gbp=100_000,
            realised_vol_20d=0.15, realised_vol_60d=0.15, signal=1.0,
        )
        assert float(palladium_notional) < float(gold_notional), (
            "Tier 3 metal (PA) should have smaller notional than Tier 1 (GC)"
        )

    def test_tier3_discount_is_50pct(self):
        """Verify Tier 3 discount is exactly 50% vs Tier 1 (same vol, same signal)."""
        # Note: Gold (GC) = Tier 1, Palladium (PA) = Tier 3
        gold_n = compute_position_notional(
            strategy_id="S4", ticker="GC", total_capital_gbp=100_000,
            realised_vol_20d=0.15, realised_vol_60d=0.15, signal=1.0,
        )
        palladium_n = compute_position_notional(
            strategy_id="S4", ticker="PA", total_capital_gbp=100_000,
            realised_vol_20d=0.15, realised_vol_60d=0.15, signal=1.0,
        )
        # Palladium should be 50% of Gold (Tier 3 discount = 0.50)
        ratio = float(palladium_n) / float(gold_n)
        assert abs(ratio - 0.50) < 0.01, f"Expected Tier 3 discount of 50%, got {ratio:.1%}"


# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY ALLOCATION INTEGRITY
# ─────────────────────────────────────────────────────────────────────────────

class TestStrategyAllocations:

    def test_allocations_sum_to_one(self):
        """All strategy allocations must sum to exactly 1.0."""
        total = sum(STRATEGY_ALLOCATION.values())
        assert abs(total - 1.0) < 1e-10, (
            f"Strategy allocations sum to {total:.6f}, must be exactly 1.0"
        )

    def test_all_six_strategies_defined(self):
        """All six strategies must have allocations."""
        required = {"S1", "S2", "S3", "S4", "S5", "S6"}
        assert required.issubset(STRATEGY_ALLOCATION.keys())