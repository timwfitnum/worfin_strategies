#!/usr/bin/env python3
"""
scripts/smoke_test_ibkr.py
Manual smoke test for the IBKR broker connector.

Requires:
  - IB Gateway running and logged into a PAPER account (port 4002)
  - ib_insync installed (pip install -e '.[broker]')
  - .env populated with IBKR_HOST / IBKR_PORT_PAPER / IBKR_CLIENT_ID / IBKR_ACCOUNT_ID
  - ENVIRONMENT=paper (or =development) in .env — refuses to run on live

What it does (default):
  1. Connects to IB Gateway
  2. Fetches account summary (NAV, margin, etc.)
  3. Fetches current positions (should be empty for a fresh paper account)
  4. Disconnects cleanly

With --resolve, additionally:
  5. Resolves each metal contract via reqContractDetails() and verifies:
       • conId > 0
       • symbol/exchange/currency match config/metals.py
       • multiplier matches MetalSpec.lot_size  ← critical — mismatch = wrong sizing
     Per-ticker results are printed as ✅ / ⚠️ (mismatch) / ❌ (error).
     Partial permissions are expected — if Futures is approved but Metals isn't,
     the COMEX metals will resolve cleanly and the LME ones will fail. The
     script continues through all 10 tickers and reports at the end.

Usage:
    python scripts/smoke_test_ibkr.py              # default: connect + positions
    python scripts/smoke_test_ibkr.py --resolve    # + contract symbology check
    python scripts/smoke_test_ibkr.py --resolve --only CA,GC   # just these tickers

Exit codes:
    0 — all checks passed
    1 — connection or critical failure
    2 — refused (live environment)
    3 — resolution revealed problems (symbology mismatch or multi-ticker errors)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass
from enum import Enum

from worfin.config.metals import ALL_METALS, MetalSpec
from worfin.config.settings import Environment, get_settings
from worfin.execution.broker import (
    BrokerConnectionError,
    BrokerPermissionError,
    IBKRBroker,
    get_broker,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)-30s  %(message)s",
)
logger = logging.getLogger("smoke_test_ibkr")


# ─────────────────────────────────────────────────────────────────────────────
# RESOLUTION RESULT MODEL
# ─────────────────────────────────────────────────────────────────────────────


class ResolveStatus(str, Enum):
    OK = "ok"
    MISMATCH = "mismatch"  # Resolved, but attrs don't match MetalSpec
    PERMISSION = "permission"  # Permission error — expected until approval
    ERROR = "error"  # Other failure (network, unknown symbol, etc.)


@dataclass
class ResolveResult:
    ticker: str
    status: ResolveStatus
    message: str
    con_id: int | None = None
    resolved_symbol: str | None = None
    resolved_exchange: str | None = None
    resolved_currency: str | None = None
    resolved_multiplier: str | None = None
    expiry: str | None = None
    mismatches: list[str] | None = None


# ─────────────────────────────────────────────────────────────────────────────
# RESOLUTION + VERIFICATION
# ─────────────────────────────────────────────────────────────────────────────


async def resolve_one(broker: IBKRBroker, ticker: str) -> ResolveResult:
    """
    Resolve a single ticker via the broker's contract cache and verify the
    returned Contract's attributes against config/metals.py.

    Returns a ResolveResult — never raises. All errors become statuses.
    """
    spec: MetalSpec = ALL_METALS[ticker]
    try:
        contract = await broker._resolve_contract(ticker)  # noqa: SLF001 — intentional
    except BrokerPermissionError as e:
        return ResolveResult(
            ticker=ticker,
            status=ResolveStatus.PERMISSION,
            message=str(e),
        )
    except Exception as e:
        return ResolveResult(
            ticker=ticker,
            status=ResolveStatus.ERROR,
            message=f"{type(e).__name__}: {e}",
        )

    # Compare returned Contract against MetalSpec
    mismatches: list[str] = []

    if contract.symbol != spec.ibkr_symbol:
        mismatches.append(f"symbol: expected {spec.ibkr_symbol!r}, got {contract.symbol!r}")
    if contract.exchange != spec.ibkr_exchange:
        mismatches.append(f"exchange: expected {spec.ibkr_exchange!r}, got {contract.exchange!r}")
    if contract.currency != spec.ibkr_currency:
        mismatches.append(f"currency: expected {spec.ibkr_currency!r}, got {contract.currency!r}")

    # Multiplier: IBKR returns a string; MetalSpec.lot_size is a float.
    # Silent multiplier mismatch = silently wrong sizing. Flag loudly.
    try:
        ibkr_mult = float(contract.multiplier) if contract.multiplier else 0.0
        if ibkr_mult != spec.lot_size:
            mismatches.append(
                f"multiplier: expected {spec.lot_size} (MetalSpec.lot_size), "
                f"got {contract.multiplier!r} ({ibkr_mult}). "
                f"SILENT MISMATCH WOULD MIS-SIZE POSITIONS."
            )
    except (TypeError, ValueError):
        mismatches.append(f"multiplier: could not parse {contract.multiplier!r} as float")

    status = ResolveStatus.OK if not mismatches else ResolveStatus.MISMATCH
    message = "OK" if not mismatches else f"{len(mismatches)} mismatch(es)"

    return ResolveResult(
        ticker=ticker,
        status=status,
        message=message,
        con_id=contract.conId,
        resolved_symbol=contract.symbol,
        resolved_exchange=contract.exchange,
        resolved_currency=contract.currency,
        resolved_multiplier=str(contract.multiplier) if contract.multiplier else None,
        expiry=contract.lastTradeDateOrContractMonth or None,
        mismatches=mismatches or None,
    )


async def resolve_all(broker: IBKRBroker, tickers: list[str]) -> list[ResolveResult]:
    """
    Resolve all tickers concurrently. Each qualifyContractsAsync is ~1–3s
    round-trip; gather cuts total to the slowest single call.
    """
    coros = [resolve_one(broker, t) for t in tickers]
    return await asyncio.gather(*coros)


# ─────────────────────────────────────────────────────────────────────────────
# PRINT HELPERS
# ─────────────────────────────────────────────────────────────────────────────


_ICON = {
    ResolveStatus.OK: "✅",
    ResolveStatus.MISMATCH: "⚠️ ",
    ResolveStatus.PERMISSION: "🔒",
    ResolveStatus.ERROR: "❌",
}


def print_resolution_table(results: list[ResolveResult]) -> None:
    """Print a compact table of resolution outcomes."""
    logger.info("─" * 78)
    logger.info("SYMBOLOGY RESOLUTION RESULTS")
    logger.info("─" * 78)
    logger.info(
        "  %-7s %-5s %-8s %-9s %-10s %-4s %-10s %s",
        "STATUS",
        "TICK",
        "SYMBOL",
        "EXCHANGE",
        "EXPIRY",
        "CCY",
        "MULT",
        "DETAIL",
    )
    logger.info("  %s", "-" * 74)

    for r in results:
        icon = _ICON[r.status]
        logger.info(
            "  %s %-3s  %-5s %-8s %-9s %-10s %-4s %-10s %s",
            icon,
            r.status.value,
            r.ticker,
            r.resolved_symbol or "-",
            r.resolved_exchange or "-",
            r.expiry or "-",
            r.resolved_currency or "-",
            r.resolved_multiplier or "-",
            r.message,
        )

    # Detailed mismatch / error dump
    problems = [r for r in results if r.status in (ResolveStatus.MISMATCH, ResolveStatus.ERROR)]
    if problems:
        logger.info("")
        logger.info("  DETAILS:")
        for r in problems:
            logger.info("    %s %s — %s", _ICON[r.status], r.ticker, r.message)
            if r.mismatches:
                for m in r.mismatches:
                    logger.info("       • %s", m)


def summarise(results: list[ResolveResult]) -> tuple[int, int, int, int]:
    """Return (ok, mismatch, permission, error) counts."""
    ok = sum(1 for r in results if r.status == ResolveStatus.OK)
    mm = sum(1 for r in results if r.status == ResolveStatus.MISMATCH)
    perm = sum(1 for r in results if r.status == ResolveStatus.PERMISSION)
    err = sum(1 for r in results if r.status == ResolveStatus.ERROR)
    return ok, mm, perm, err


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="IBKR paper-Gateway smoke test (connect + positions, "
        "optionally resolve contract symbology)."
    )
    p.add_argument(
        "--resolve",
        action="store_true",
        help="Resolve each metal contract and verify against config/metals.py.",
    )
    p.add_argument(
        "--only",
        metavar="TICKERS",
        default=None,
        help="Comma-separated subset of tickers to resolve, e.g. CA,GC,SI. Default: all 10 metals.",
    )
    return p.parse_args()


async def main() -> int:
    args = parse_args()
    settings = get_settings()

    # Hard guard: refuse live. Smoke tests are paper-only.
    if settings.environment == Environment.LIVE:
        logger.error(
            "Refusing to smoke-test against LIVE environment. Set ENVIRONMENT=paper in .env."
        )
        return 2

    logger.info("═" * 70)
    logger.info("WorFIn IBKR smoke test")
    logger.info("═" * 70)
    logger.info("Environment:   %s", settings.environment.value)
    logger.info("IBKR host:     %s", settings.ibkr_host)
    logger.info("IBKR port:     %d (paper)", settings.ibkr_port)
    logger.info("Client ID:     %d", settings.ibkr_client_id)
    logger.info(
        "Account:       %s",
        settings.ibkr_account_id or "(not set in .env — will warn)",
    )
    logger.info("Resolve step:  %s", "enabled" if args.resolve else "skipped (use --resolve)")
    logger.info("─" * 70)

    broker = get_broker()
    exit_code = 0

    # ── 1. Connect ────────────────────────────────────────────────────────
    try:
        await broker.connect()
    except BrokerConnectionError as e:
        logger.error("❌ CONNECT FAILED: %s", e)
        logger.error(
            "Troubleshoot: Is IB Gateway running? Is port %d open? "
            "Is API enabled in Gateway → Configure → Settings → API? "
            "Is 127.0.0.1 in Trusted IPs?",
            settings.ibkr_port,
        )
        return 1

    logger.info("✅ Connected")

    try:
        # ── 2. Account summary ────────────────────────────────────────────
        try:
            summary = await broker.get_account_summary()
            logger.info("─" * 70)
            logger.info("ACCOUNT SUMMARY")
            for k, v in sorted(summary.items()):
                logger.info("  %-25s %s", k + ":", f"${v:,.2f}" if v else "n/a")
            if not summary:
                logger.warning(
                    "⚠️  Empty account summary — accountSummary request "
                    "returned no USD/BASE values. Check IBKR_ACCOUNT_ID in .env."
                )
            else:
                logger.info("✅ Account summary OK")
        except Exception as e:
            logger.error("❌ get_account_summary failed: %s", e)

        # ── 3. Positions ──────────────────────────────────────────────────
        try:
            positions = await broker.get_positions()
            logger.info("─" * 70)
            logger.info("POSITIONS")
            if not positions:
                logger.info("  (none — expected for a fresh paper account)")
            else:
                for ticker, lots in sorted(positions.items()):
                    logger.info("  %-5s %+d lots", ticker, lots)
            logger.info("✅ Positions OK")
        except Exception as e:
            logger.error("❌ get_positions failed: %s", e)

        # ── 4. (optional) Symbology resolution ────────────────────────────
        if args.resolve:
            logger.info("─" * 70)
            logger.info("RESOLVING CONTRACT SYMBOLOGY")

            if args.only:
                requested = [t.strip().upper() for t in args.only.split(",")]
                unknown = [t for t in requested if t not in ALL_METALS]
                if unknown:
                    logger.error(
                        "Unknown ticker(s) in --only: %s. Valid: %s",
                        unknown,
                        sorted(ALL_METALS.keys()),
                    )
                    return 1
                tickers = requested
            else:
                tickers = sorted(ALL_METALS.keys())

            logger.info("  Resolving %d ticker(s): %s", len(tickers), tickers)
            logger.info("  Each resolution is 1–3s; running concurrently via asyncio.gather.")

            results = await resolve_all(broker, tickers)
            print_resolution_table(results)

            ok, mm, perm, err = summarise(results)
            logger.info("─" * 70)
            logger.info(
                "SUMMARY: %d OK  |  %d mismatch  |  %d permission  |  %d error",
                ok,
                mm,
                perm,
                err,
            )

            if mm > 0:
                logger.error(
                    "⚠️  %d ticker(s) have symbology mismatches — "
                    "config/metals.py needs updating OR IBKR routing is wrong.",
                    mm,
                )
                exit_code = 3
            if err > 0:
                logger.error(
                    "❌ %d ticker(s) hit unexpected errors — investigate above.",
                    err,
                )
                exit_code = 3
            if perm > 0:
                logger.info(
                    "🔒 %d ticker(s) blocked by permissions — apply via IBKR "
                    "Client Portal → Settings → Trading Permissions.",
                    perm,
                )
                # Permission errors don't set exit_code — they're expected
                # while approvals are still pending.

        logger.info("─" * 70)
        if exit_code == 0:
            logger.info("✅ SMOKE TEST PASSED")
        else:
            logger.warning("⚠️  SMOKE TEST COMPLETED WITH ISSUES (exit=%d)", exit_code)
        logger.info("─" * 70)

        if not args.resolve:
            logger.info(
                "Next: run with --resolve to verify symbology for each metal "
                "in config/metals.py. Partial permissions are fine — expected "
                "while Metals/Futures approvals are pending."
            )

        return exit_code
    finally:
        await broker.disconnect()
        logger.info("Disconnected cleanly.")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
