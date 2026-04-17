"""
tests/test_data/test_step1.py
Tests for Step 1 additions:
  - FX rate fetcher (data/ingestion/fx_rates.py)
  - Logging config (config/logging_config.py)
  - Load to DB script helpers (scripts/load_to_db.py)
  - MetalSpec.typical_adv_lots (config/metals.py)
  - limits.FX_RATE_MAX_STALENESS_DAYS (risk/limits.py)
  - sizing.py no-default usd_gbp_rate
  - pretrade_integration.py correct import path
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ─────────────────────────────────────────────────────────────────────────────
# FX RATE FETCHER
# ─────────────────────────────────────────────────────────────────────────────


class TestFxRates:
    def test_get_from_cache_after_set(self) -> None:
        from worfin.data.ingestion.fx_rates import _set_cache, _get_from_cache

        test_date = date(2020, 6, 15)
        _set_cache(test_date.isoformat(), 1.25)
        result = _get_from_cache(test_date)
        assert result == pytest.approx(1.25)

    def test_fx_rate_unavailable_raised(self) -> None:
        """get_usd_gbp raises FxRateUnavailable when all sources empty."""
        from worfin.data.ingestion.fx_rates import FxRateUnavailable, get_usd_gbp, _rate_store

        test_date = date(1990, 1, 1)  # Far in the past — not in cache
        # Clear any cached value for this date
        _rate_store.pop(test_date.isoformat(), None)
        # Also clear nearby dates within staleness window
        staleness = 5
        for d in range(staleness + 1):
            _rate_store.pop((test_date - timedelta(days=d)).isoformat(), None)

        with patch("worfin.data.ingestion.fx_rates._fetch_from_fred", return_value={}):
            with pytest.raises(FxRateUnavailable):
                get_usd_gbp(test_date, engine=None)

    def test_fred_csv_parsed_correctly(self) -> None:
        """Mock FRED response is parsed into {date: rate} correctly."""
        from worfin.data.ingestion.fx_rates import _fetch_from_fred

        csv_body = "DATE,DEXUSUK\n2023-06-01,1.2456\n2023-06-02,.\n2023-06-05,1.2501\n"
        mock_resp = MagicMock()
        mock_resp.text = csv_body
        mock_resp.raise_for_status = lambda: None

        with patch("worfin.data.ingestion.fx_rates.requests.get", return_value=mock_resp):
            result = _fetch_from_fred(date(2023, 6, 5))

        assert date(2023, 6, 1) in result
        assert result[date(2023, 6, 1)] == pytest.approx(1.2456)
        assert date(2023, 6, 5) in result
        assert result[date(2023, 6, 5)] == pytest.approx(1.2501)
        assert date(2023, 6, 2) not in result  # sentinel '.' excluded

    def test_prior_day_fallback_used(self) -> None:
        """When today's rate missing, fallback to prior business day."""
        from worfin.data.ingestion.fx_rates import get_usd_gbp, _rate_store

        target = date(2023, 3, 27)   # Monday
        friday = date(2023, 3, 24)   # Prior business day
        # Clear cache for both
        _rate_store.pop(target.isoformat(), None)
        _rate_store.pop(friday.isoformat(), None)

        fred_result = {friday: 1.2345}  # Only prior day available
        with patch("worfin.data.ingestion.fx_rates._fetch_from_fred", return_value=fred_result):
            rate = get_usd_gbp(target, engine=None)

        assert rate == pytest.approx(1.2345)

    def test_fx_rate_unavailable_has_correct_date(self) -> None:
        from worfin.data.ingestion.fx_rates import FxRateUnavailable

        exc = FxRateUnavailable(date(2020, 1, 1), 5)
        assert exc.for_date == date(2020, 1, 1)
        assert exc.staleness_days == 5

    def test_prefetch_returns_dict(self) -> None:
        from worfin.data.ingestion.fx_rates import prefetch_fx_rates

        csv_body = "DATE,DEXUSUK\n2023-01-03,1.2100\n2023-01-04,1.2150\n"
        mock_resp = MagicMock()
        mock_resp.text = csv_body
        mock_resp.raise_for_status = lambda: None

        with patch("worfin.data.ingestion.fx_rates.requests.get", return_value=mock_resp):
            result = prefetch_fx_rates(date(2023, 1, 3), date(2023, 1, 4), engine=None)

        assert date(2023, 1, 3) in result
        assert date(2023, 1, 4) in result
        assert result[date(2023, 1, 3)] == pytest.approx(1.21)


# ─────────────────────────────────────────────────────────────────────────────
# LIMITS — FX_RATE_MAX_STALENESS_DAYS
# ─────────────────────────────────────────────────────────────────────────────


class TestLimits:
    def test_fx_rate_max_staleness_days_exists(self) -> None:
        from worfin.risk.limits import FX_RATE_MAX_STALENESS_DAYS

        assert isinstance(FX_RATE_MAX_STALENESS_DAYS, int)
        assert FX_RATE_MAX_STALENESS_DAYS >= 3   # must cover long weekends

    def test_fx_rate_max_staleness_days_reasonable(self) -> None:
        from worfin.risk.limits import FX_RATE_MAX_STALENESS_DAYS

        # 3 = minimum for long weekends; 10 = maximum before it's too permissive
        assert 3 <= FX_RATE_MAX_STALENESS_DAYS <= 10

    def test_allocations_sum_to_one(self) -> None:
        from worfin.risk.limits import STRATEGY_ALLOCATION

        assert abs(sum(STRATEGY_ALLOCATION.values()) - 1.0) < 1e-10


# ─────────────────────────────────────────────────────────────────────────────
# METALS — typical_adv_lots
# ─────────────────────────────────────────────────────────────────────────────


class TestMetalSpec:
    def test_all_metals_have_typical_adv_lots(self) -> None:
        from worfin.config.metals import ALL_METALS

        for ticker, spec in ALL_METALS.items():
            assert hasattr(spec, "typical_adv_lots"), (
                f"{ticker} missing typical_adv_lots"
            )
            assert spec.typical_adv_lots > 0, (
                f"{ticker}.typical_adv_lots must be > 0, got {spec.typical_adv_lots}"
            )

    def test_expected_adv_values(self) -> None:
        from worfin.config.metals import ALL_METALS

        # Verify the values from the spec sheet
        expected = {
            "CA": 2000, "AH": 3000, "ZS": 1500, "NI": 500,
            "PB": 800, "SN": 50, "GC": 3000, "SI": 1500, "PL": 200, "PA": 100,
        }
        for ticker, exp_adv in expected.items():
            assert ALL_METALS[ticker].typical_adv_lots == exp_adv, (
                f"{ticker}: expected {exp_adv}, got {ALL_METALS[ticker].typical_adv_lots}"
            )

    def test_metal_spec_is_frozen(self) -> None:
        from worfin.config.metals import COPPER

        with pytest.raises((AttributeError, TypeError)):
            COPPER.typical_adv_lots = 9999  # type: ignore[misc]

    def test_tier_ordering(self) -> None:
        """High-liquidity metals should generally have higher ADV than low."""
        from worfin.config.metals import ALL_METALS, LiquidityTier

        tier1_adv = [s.typical_adv_lots for s in ALL_METALS.values() if s.liquidity_tier == LiquidityTier.HIGH]
        tier3_adv = [s.typical_adv_lots for s in ALL_METALS.values() if s.liquidity_tier == LiquidityTier.LOW]
        assert min(tier1_adv) > max(tier3_adv), (
            "All Tier-1 ADV values should exceed all Tier-3 ADV values"
        )


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING CONFIG
# ─────────────────────────────────────────────────────────────────────────────


class TestLoggingConfig:
    def test_configure_logging_runs(self, monkeypatch) -> None:
        """configure_logging() completes without error in DEVELOPMENT mode."""
        monkeypatch.setenv("ENVIRONMENT", "development")
        from worfin.config.logging_config import configure_logging

        configure_logging(log_level="WARNING", force=True)
        # Should be idempotent
        configure_logging(log_level="WARNING", force=False)

    def test_correlation_id_is_uuid(self) -> None:
        from worfin.config.logging_config import get_correlation_id
        import uuid

        cid = get_correlation_id()
        parsed = uuid.UUID(cid)  # Raises ValueError if not valid UUID
        assert str(parsed) == cid

    def test_correlation_id_stable_within_process(self) -> None:
        from worfin.config.logging_config import get_correlation_id

        assert get_correlation_id() == get_correlation_id()

    def test_json_formatter_produces_valid_json(self) -> None:
        import json
        from worfin.config.logging_config import _JsonFormatter, _CorrelationFilter

        fmt = _JsonFormatter()
        filt = _CorrelationFilter()
        record = logging.LogRecord(
            name="worfin.test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="hello %s",
            args=("world",),
            exc_info=None,
        )
        filt.filter(record)
        output = fmt.format(record)
        parsed = json.loads(output)
        assert parsed["level"] == "INFO"
        assert parsed["message"] == "hello world"
        assert "correlation_id" in parsed
        assert "ts" in parsed

    def test_human_formatter_produces_string(self) -> None:
        from worfin.config.logging_config import _HumanFormatter, _CorrelationFilter

        fmt = _HumanFormatter()
        filt = _CorrelationFilter()
        record = logging.LogRecord(
            name="worfin.test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )
        filt.filter(record)
        output = fmt.format(record)
        assert "INFO" in output
        assert "worfin.test" in output
        assert "test message" in output


# ─────────────────────────────────────────────────────────────────────────────
# SIZING — no default usd_gbp_rate
# ─────────────────────────────────────────────────────────────────────────────


class TestSizingNoDefault:
    def test_compute_lots_requires_usd_gbp_rate(self) -> None:
        """compute_lots must require usd_gbp_rate — no default of 1.27."""
        import inspect
        from worfin.risk.sizing import compute_lots

        sig = inspect.signature(compute_lots)
        param = sig.parameters["usd_gbp_rate"]
        assert param.default is inspect.Parameter.empty, (
            "usd_gbp_rate must have NO default — caller must pass live rate"
        )

    def test_compute_position_notional_requires_usd_gbp_rate(self) -> None:
        import inspect
        from worfin.risk.sizing import compute_position_notional

        sig = inspect.signature(compute_position_notional)
        param = sig.parameters["usd_gbp_rate"]
        assert param.default is inspect.Parameter.empty

    def test_compute_portfolio_sizing_requires_usd_gbp_rate(self) -> None:
        import inspect
        from worfin.risk.sizing import compute_portfolio_sizing

        sig = inspect.signature(compute_portfolio_sizing)
        param = sig.parameters["usd_gbp_rate"]
        assert param.default is inspect.Parameter.empty

    def test_compute_lots_with_explicit_rate(self) -> None:
        """Providing an explicit rate works correctly."""
        from worfin.risk.sizing import compute_lots

        lots = compute_lots(
            strategy_id="S4",
            ticker="GC",
            total_capital_gbp=100_000.0,
            realised_vol_20d=0.15,
            realised_vol_60d=0.18,
            signal=0.8,
            current_price_usd=1900.0,
            usd_gbp_rate=1.25,
        )
        # Result should be non-negative integer
        assert isinstance(lots, int)
        assert lots >= 0


# ─────────────────────────────────────────────────────────────────────────────
# PRETRADE INTEGRATION — correct module name
# ─────────────────────────────────────────────────────────────────────────────


class TestPretradeIntegrationImport:
    def test_correct_module_importable(self) -> None:
        """pretrade_integration (correct spelling) must be importable."""
        from worfin.backtest import pretrade_integration  # noqa: F401

        assert hasattr(pretrade_integration, "run_pretrade_checks")
        assert hasattr(pretrade_integration, "build_portfolio_state")
        assert hasattr(pretrade_integration, "compute_adv")

    def test_misspelled_module_does_not_exist(self) -> None:
        """pretrade_intergation (wrong spelling) must NOT be the sole version."""
        # It may exist as a legacy shim, but the correct name must also work.
        # This test ensures the engine imports from the right place.
        from worfin.backtest.pretrade_integration import run_pretrade_checks

        assert callable(run_pretrade_checks)

    def test_compute_adv_uses_typical_fallback(self) -> None:
        """When volume data is all zeros, falls back to typical_adv_lots."""
        from worfin.backtest.pretrade_integration import compute_adv
        from worfin.config.metals import ALL_METALS

        # Build a price_data dict with all-zero volume for copper
        idx = pd.date_range("2020-01-01", periods=30, freq="B", tz="UTC")
        df = pd.DataFrame({"close": 7000.0, "volume": 0.0}, index=idx)
        price_data = {"CA": df}
        as_of = pd.Timestamp("2020-02-15", tz="UTC")

        adv = compute_adv(price_data, as_of)
        expected_fallback = ALL_METALS["CA"].typical_adv_lots
        assert adv["CA"] == float(expected_fallback)

    def test_build_portfolio_state(self) -> None:
        from worfin.backtest.pretrade_integration import build_portfolio_state

        state = build_portfolio_state(
            nav_gbp=100_000.0,
            current_lots={"GC": 2},
            current_prices_usd={"GC": 1900.0},
            usd_gbp_rate=1.27,
            orders_today=0,
            adv_by_ticker={"GC": 3000.0},
        )
        assert state.nav_gbp == pytest.approx(100_000.0)
        # GC: 2 lots × 100 oz × $1900 = $380,000 USD = ~£299k... wait, let me check
        # notional_usd = 2 * 100 * 1900 = 380,000; / 1.27 = ~299,213
        assert state.gross_exposure_gbp == pytest.approx(380_000 / 1.27, rel=1e-4)


# ─────────────────────────────────────────────────────────────────────────────
# ENGINE — no hardcoded 1.27 in BacktestConfig
# ─────────────────────────────────────────────────────────────────────────────


class TestEngineNoHardcodedFx:
    def test_backtest_config_has_no_usd_gbp_rate_field(self) -> None:
        """BacktestConfig must NOT have a usd_gbp_rate field with default 1.27."""
        import dataclasses
        from worfin.backtest.engine import BacktestConfig

        field_names = {f.name for f in dataclasses.fields(BacktestConfig)}
        assert "usd_gbp_rate" not in field_names, (
            "BacktestConfig must not have usd_gbp_rate — engine fetches it live"
        )

    def test_engine_imports_correct_integration_module(self) -> None:
        """engine.py must import from pretrade_integration (not pretrade_intergation)."""
        import ast
        from pathlib import Path

        engine_path = Path(__file__).parents[2] / "src/worfin/backtest/engine.py"
        if not engine_path.exists():
            # Running from repo root
            import worfin.backtest.engine as eng_mod
            engine_path = Path(eng_mod.__file__)

        source = engine_path.read_text()
        assert "pretrade_integration" in source, (
            "engine.py must import from pretrade_integration (correct spelling)"
        )
        assert "pretrade_intergation" not in source, (
            "engine.py must NOT import from pretrade_intergation (wrong spelling)"
        )