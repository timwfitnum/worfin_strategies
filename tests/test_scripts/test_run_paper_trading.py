"""
tests/test_scripts/test_run_paper_trading.py

Exercises the paper-trading cron entry point without touching any real
broker, DB, or Telegram.

The script is at /scripts/run_paper_trading.py (not on the normal import
path), so we load it via importlib at module scope and share the reference.
"""

from __future__ import annotations

import asyncio
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ─────────────────────────────────────────────────────────────────────────────
# Load scripts/run_paper_trading.py as a module
# ─────────────────────────────────────────────────────────────────────────────


def _load_runner():
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "run_paper_trading.py"
    assert script_path.is_file(), f"Script not found at {script_path}"
    spec = importlib.util.spec_from_file_location("run_paper_trading", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_paper_trading"] = module
    spec.loader.exec_module(module)
    return module


runner = _load_runner()


# ─────────────────────────────────────────────────────────────────────────────
# Settings reset — get_settings is lru_cached; any env change needs invalidation
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def reset_settings_cache():
    """Clear get_settings() cache before and after each test using it."""
    from worfin.config.settings import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ─────────────────────────────────────────────────────────────────────────────
# Shared doubles
# ─────────────────────────────────────────────────────────────────────────────


def _mock_db_engine() -> MagicMock:
    """
    SQLAlchemy engine with a begin() context manager whose .execute() returns
    rows shaped for both the DB reachability check (SELECT 1) and the
    alembic_version check (EXISTS → True).
    """
    engine = MagicMock(name="db_engine")
    conn = MagicMock(name="db_conn")
    row = MagicMock()
    row.__bool__ = lambda self: True
    row.__getitem__ = lambda self, _i: True  # EXISTS(...) = True
    conn.execute.return_value.fetchone.return_value = row
    # Context manager shape: engine.begin() → conn; __enter__/__exit__
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=conn)
    cm.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = cm
    return engine


def _mock_alert_manager() -> MagicMock:
    """AlertManager with every method present as a MagicMock."""
    mgr = MagicMock(name="alert_manager")
    mgr.startup_ping = MagicMock()
    mgr.send = MagicMock()
    return mgr


def _mock_strategy() -> MagicMock:
    s = MagicMock(name="strategy")
    s.strategy_id = "S4"
    s.universe = ["CA", "GC"]
    return s


class _FakeReconciliation(SimpleNamespace):
    @property
    def is_clean(self) -> bool:
        return not self.mismatches  # type: ignore[attr-defined]


class _FakeCycleResult(SimpleNamespace):
    """Mimics execution.engine.CycleResult for the bits main() inspects."""

    @property
    def duration_seconds(self) -> float:
        return getattr(self, "_duration", 1.23)


def _cycle_result(
    *,
    safe_state: bool = False,
    safe_state_reason: str | None = None,
    mismatches: dict | None = None,
) -> _FakeCycleResult:
    recon = _FakeReconciliation(mismatches=mismatches or {})
    return _FakeCycleResult(
        correlation_id="test1234",
        cycle_start=None,
        cycle_end=None,
        environment="paper",
        account_nav_usd=None,
        usd_gbp_rate=None,
        strategy_results=[],
        reconciliation=recon,
        safe_state=safe_state,
        safe_state_reason=safe_state_reason,
        _duration=1.23,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pure-helper tests
# ─────────────────────────────────────────────────────────────────────────────


class TestMaskAccountId:
    def test_standard_ibkr_id(self):
        assert runner.mask_account_id("U12345678") == "U1****5678"

    def test_empty_returns_not_set(self):
        assert runner.mask_account_id("") == "(not set)"

    def test_short_scrubbed(self):
        assert runner.mask_account_id("U123") == "****"

    def test_exact_minimum_length(self):
        # 6 chars is the boundary (first 2 + last 4)
        assert runner.mask_account_id("ABCDEF") == "AB****CDEF"


class TestBuildPaperDbUrl:
    def test_hardcoded_paper_db(self):
        url = runner.build_paper_db_url(db_user="u", db_password="p", db_host="h", db_port=5432)
        assert url == "postgresql://u:p@h:5432/worfin_paper"

    def test_never_reads_db_name(self):
        # Signature takes no db_name — the constant is the only source of truth
        import inspect

        sig = inspect.signature(runner.build_paper_db_url)
        assert "db_name" not in sig.parameters


class TestRedactPassword:
    def test_password_replaced(self):
        url = "postgresql://u:secret@h:5432/worfin_paper"
        assert runner._redact_password(url, "secret") == "postgresql://u:***@h:5432/worfin_paper"

    def test_empty_password_noop(self):
        url = "postgresql://u:@h:5432/worfin_paper"
        assert runner._redact_password(url, "") == url


# ─────────────────────────────────────────────────────────────────────────────
# Argparse tests
# ─────────────────────────────────────────────────────────────────────────────


class TestParseArgs:
    def test_defaults(self):
        args = runner.parse_args([])
        assert args.dry_run is False
        assert args.strategy == "S4"

    def test_dry_run_flag(self):
        args = runner.parse_args(["--dry-run"])
        assert args.dry_run is True

    def test_strategy_valid(self):
        args = runner.parse_args(["--strategy", "S4"])
        assert args.strategy == "S4"

    def test_strategy_invalid_rejected(self):
        with pytest.raises(SystemExit):
            runner.parse_args(["--strategy", "S99"])


# ─────────────────────────────────────────────────────────────────────────────
# DB preflight tests
# ─────────────────────────────────────────────────────────────────────────────


class TestCheckDbReachable:
    def test_ok(self):
        assert runner.check_db_reachable(_mock_db_engine()) is True

    def test_exception_returns_false(self):
        engine = MagicMock()
        engine.begin.side_effect = RuntimeError("nope")
        assert runner.check_db_reachable(engine) is False


class TestCheckMigrationsApplied:
    def test_alembic_version_exists(self):
        assert runner.check_migrations_applied(_mock_db_engine()) is True

    def test_alembic_version_missing(self):
        engine = MagicMock()
        conn = MagicMock()
        row = MagicMock()
        row.__bool__ = lambda self: True
        row.__getitem__ = lambda self, _i: False  # EXISTS → False
        conn.execute.return_value.fetchone.return_value = row
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=conn)
        cm.__exit__ = MagicMock(return_value=False)
        engine.begin.return_value = cm
        assert runner.check_migrations_applied(engine) is False

    def test_exception_returns_false(self):
        engine = MagicMock()
        engine.begin.side_effect = RuntimeError("db down")
        assert runner.check_migrations_applied(engine) is False


# ─────────────────────────────────────────────────────────────────────────────
# Strategy factory test
# ─────────────────────────────────────────────────────────────────────────────


class TestStrategyFactory:
    def test_s4_returns_basis_momentum(self):
        strat = runner.default_strategy_factory("S4")
        assert strat.strategy_id == "S4"

    def test_unknown_raises(self):
        with pytest.raises(ValueError):
            runner.default_strategy_factory("S99")


# ─────────────────────────────────────────────────────────────────────────────
# run_paper_trading async flow tests
# ─────────────────────────────────────────────────────────────────────────────


def _drive(
    *,
    environment: str = "paper",
    dry_run: bool = False,
    cycle_result=None,
    alert_manager=None,
    db_engine=None,
    cycle_raises: BaseException | None = None,
):
    """
    Drive run_paper_trading() with fully mocked deps. Returns the exit code.

    Patches get_settings, ExecutionEngine, and configure_logging.
    """
    from worfin.config.settings import Environment, Settings

    # Fresh Settings — do NOT call global get_settings (it's lru_cached)
    fake_settings = Settings(
        environment=Environment(environment),
        db_user="u",
        db_password="p",
        db_host="h",
        db_port=5432,
        db_name="metals_trading",  # deliberately the research DB — must be ignored
        trading_capital_gbp=10_000.0,
        ibkr_account_id="U12345678",
    )

    args = SimpleNamespace(dry_run=dry_run, strategy="S4")
    db_engine = db_engine if db_engine is not None else _mock_db_engine()
    alert_mgr = alert_manager if alert_manager is not None else _mock_alert_manager()

    # Build a fake ExecutionEngine whose run_cycle returns cycle_result
    fake_engine = MagicMock(name="ExecutionEngine_instance")
    if cycle_raises is not None:
        fake_engine.run_cycle = AsyncMock(side_effect=cycle_raises)
    else:
        fake_engine.run_cycle = AsyncMock(
            return_value=cycle_result if cycle_result is not None else _cycle_result()
        )

    engine_factory = MagicMock(return_value=fake_engine)

    with (
        # Both are imported inside run_paper_trading() — patch at the source
        patch("worfin.config.logging_config.configure_logging"),
        patch("worfin.config.settings.get_settings", return_value=fake_settings),
    ):
        exit_code = asyncio.run(
            runner.run_paper_trading(
                args,
                db_engine=db_engine,
                alert_manager=alert_mgr,
                strategy_factory=lambda _id: _mock_strategy(),
                engine_factory=engine_factory,
            )
        )

    return exit_code, fake_engine, alert_mgr, engine_factory


class TestRunPaperTrading:
    def test_live_env_refused(self):
        """ENVIRONMENT=live → exit 1, broker never constructed."""
        code, engine_mock, alert_mgr, engine_factory = _drive(environment="live")
        assert code == 1
        engine_factory.assert_not_called()
        alert_mgr.startup_ping.assert_not_called()
        engine_mock.run_cycle.assert_not_called()

    def test_dry_run_clean_exit_zero(self):
        """Dry-run: engine constructs, run_cycle NEVER called, exit 0."""
        code, engine_mock, alert_mgr, engine_factory = _drive(dry_run=True)
        assert code == 0
        engine_factory.assert_called_once()
        engine_mock.run_cycle.assert_not_called()
        # Startup ping fires even in dry-run (so ops can confirm wiring)
        alert_mgr.startup_ping.assert_called_once()

    def test_clean_cycle_exit_zero(self):
        code, engine_mock, alert_mgr, _ = _drive(cycle_result=_cycle_result())
        assert code == 0
        engine_mock.run_cycle.assert_awaited_once()
        alert_mgr.startup_ping.assert_called_once()

    def test_safe_state_cycle_exit_one(self):
        code, engine_mock, _, _ = _drive(
            cycle_result=_cycle_result(safe_state=True, safe_state_reason="broker_connect_failed")
        )
        assert code == 1
        engine_mock.run_cycle.assert_awaited_once()

    def test_reconciliation_mismatch_exit_zero(self):
        """Recon mismatches don't promote to non-zero — engine alerted already."""
        code, _, _, _ = _drive(cycle_result=_cycle_result(mismatches={"CA": (1, 2, 1)}))
        assert code == 0

    def test_db_unreachable_exit_one(self):
        broken = MagicMock()
        broken.begin.side_effect = RuntimeError("no route")
        code, _, _, engine_factory = _drive(db_engine=broken)
        assert code == 1
        engine_factory.assert_not_called()

    def test_missing_migrations_exit_one(self):
        engine = MagicMock()
        conn = MagicMock()
        row = MagicMock()
        row.__bool__ = lambda self: True
        row.__getitem__ = lambda self, _i: False  # alembic_version missing
        conn.execute.return_value.fetchone.return_value = row
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=conn)
        cm.__exit__ = MagicMock(return_value=False)
        engine.begin.return_value = cm
        code, _, _, engine_factory = _drive(db_engine=engine)
        assert code == 1
        engine_factory.assert_not_called()

    def test_startup_ping_failure_is_non_fatal(self):
        alert_mgr = _mock_alert_manager()
        alert_mgr.startup_ping.side_effect = RuntimeError("telegram down")
        code, engine_mock, _, _ = _drive(alert_manager=alert_mgr)
        assert code == 0  # Cycle still ran
        engine_mock.run_cycle.assert_awaited_once()


# ─────────────────────────────────────────────────────────────────────────────
# Top-level main() — exception handling
# ─────────────────────────────────────────────────────────────────────────────


class TestMain:
    def test_main_catches_uncaught_exception_returns_one(self):
        with patch.object(runner, "run_paper_trading", side_effect=RuntimeError("boom")):
            code = runner.main([])
        assert code == 1

    def test_main_keyboard_interrupt_returns_130(self):
        with patch.object(runner, "run_paper_trading", side_effect=KeyboardInterrupt):
            code = runner.main([])
        assert code == 130

    def test_main_propagates_system_exit(self):
        with patch.object(runner, "run_paper_trading", side_effect=SystemExit(2)):
            with pytest.raises(SystemExit) as exc_info:
                runner.main([])
            assert exc_info.value.code == 2
