# CLAUDE.md — /monitoring
# Monitoring, Alerting & Reconciliation | Part of: Metals Systematic Trading System

---

## SCOPE

Monitoring is non-negotiable at Tier 1+. Unattended systems MUST alert on failure.
This directory handles all operational visibility: alerts, reconciliation, data quality, reporting.

```
/monitoring/
├── alerts.py          ← Telegram bot + email alerting
├── reconciliation.py  ← Daily position reconciliation vs broker
├── data_quality.py    ← Staleness, outlier, cross-source validation
└── reporting.py       ← Daily P&L report generation
```

---

## ALERT LEVELS & CHANNELS

```python
# All alerts go to Telegram (mobile-accessible, instant)
# Critical alerts also go to email

class AlertLevel(Enum):
    INFO     = "ℹ️"   # Informational — daily reports, signals
    WARNING  = "⚠️"  # Needs attention soon — data quality, vol breaches
    CRITICAL = "🚨"  # Immediate action required — risk limits, system failures
    KILL     = "🔴"  # Kill switch conditions

ALERT_TRIGGERS = {
    # Data pipeline
    "data_not_updated":     (AlertLevel.WARNING,  "No new price data by 18:30 UTC"),
    "outlier_detected":     (AlertLevel.WARNING,  "Daily return >4σ — investigate before using"),
    "price_discrepancy":    (AlertLevel.WARNING,  ">0.5% price discrepancy between sources"),
    "signal_stale":         (AlertLevel.CRITICAL, "Signal >24h old — execution blocked"),

    # Risk limits
    "position_limit":       (AlertLevel.CRITICAL, "Single-metal notional approaching 20% NAV"),
    "daily_loss_warning":   (AlertLevel.CRITICAL, "Daily loss approaching 2% limit"),
    "daily_loss_breach":    (AlertLevel.KILL,     "Daily loss 2% NAV — FLATTENING ALL"),
    "drawdown_suspend":     (AlertLevel.KILL,     "Peak drawdown 10% — FULL SUSPENSION"),
    "hard_stop":            (AlertLevel.KILL,     "Drawdown 15% — SYSTEM SHUTDOWN"),

    # Strategy
    "vol_target_breach_1x": (AlertLevel.WARNING,  "Strategy vol > 1.5× target (Day X/5)"),
    "vol_target_breach_2x": (AlertLevel.CRITICAL, "Strategy vol > 2× target — HALVING ALLOCATION"),
    "correlation_spike":    (AlertLevel.WARNING,  "Pairwise correlation > 0.60 — reducing allocation"),
    "drawdown_budget":      (AlertLevel.CRITICAL, "Strategy drawdown budget breached — SUSPENDING"),

    # Execution
    "order_rejected":       (AlertLevel.CRITICAL, "Order rejected by broker — human review required"),
    "reconciliation_fail":  (AlertLevel.CRITICAL, "Position mismatch > £100 or 0.1% — investigating"),
    "high_market_order_rate": (AlertLevel.WARNING,"Market order rate >10% — review execution logic"),
    "high_slippage":        (AlertLevel.WARNING,  "Slippage >2× model — review execution"),

    # Infrastructure
    "ib_gateway_down":      (AlertLevel.CRITICAL, "IB Gateway disconnected"),
    "db_connection_failed": (AlertLevel.CRITICAL, "PostgreSQL connection failed"),
    "vps_unreachable":      (AlertLevel.CRITICAL, "VPS health check failed"),
}
```

---

## DAILY RECONCILIATION (NON-NEGOTIABLE)

Run every morning before market open:

```python
# reconciliation.py

async def daily_reconciliation():
    """
    Compare internal state vs broker reality.
    Block new orders until any discrepancy is resolved.

    Discrepancy threshold: > £100 OR > 0.1% of position notional
    """

    # 1. Query IBKR for all actual positions (reqPositions())
    broker_positions = await ibkr.get_positions()

    # 2. Compare with system's expected positions
    system_positions = await db.get_current_positions()

    # 3. Flag any discrepancies
    discrepancies = compare_positions(broker_positions, system_positions)

    for disc in discrepancies:
        if abs(disc.value_diff) > 100 or abs(disc.pct_diff) > 0.001:
            await alert.send(AlertLevel.CRITICAL,
                f"Position mismatch: {disc.metal} — "
                f"System: {disc.system_qty} lots, Broker: {disc.broker_qty} lots")
            await execution.block_new_orders(reason="reconciliation_failure")

    # 4. Compare P&L
    system_pnl = await db.get_computed_pnl()
    broker_pnl = await ibkr.get_reported_pnl()

    if abs(system_pnl - broker_pnl) > 100:
        await alert.send(AlertLevel.CRITICAL,
            f"P&L mismatch: System £{system_pnl:.2f} vs Broker £{broker_pnl:.2f}")

    # 5. Write reconciliation result to audit log
    await audit.log_reconciliation(discrepancies, timestamp=now())
```

---

## DATA QUALITY CHECKS

```python
# data_quality.py — run at Layer 2 (before signals are computed)

def check_staleness(last_update: datetime, metal: str, exchange: str) -> bool:
    """
    Returns True if data is fresh, False if stale.
    Suppresses false positives on exchange holidays.
    """
    if is_exchange_holiday(exchange, today()):
        return True  # Expected — not stale

    trading_days_since_update = count_trading_days(last_update, now())
    if trading_days_since_update > MAX_STALENESS_DAYS:
        alert.send(AlertLevel.WARNING,
            f"STALE DATA: {metal} on {exchange} not updated for {trading_days_since_update} days")
        return False
    return True


def check_outlier(daily_return: float, rolling_mean: float, rolling_std: float) -> bool:
    """
    Returns True if normal, False if suspicious outlier.
    Does NOT auto-discard — human investigation required.
    """
    z_score = abs((daily_return - rolling_mean) / rolling_std)
    if z_score > OUTLIER_THRESHOLD_SIGMA:  # 4σ
        alert.send(AlertLevel.WARNING,
            f"OUTLIER: {daily_return:.2%} daily return ({z_score:.1f}σ) — investigate before use")
        return False  # Flag but don't delete from raw_data
    return True


def cross_validate_prices(source_a: float, source_b: float, metal: str) -> bool:
    """Compare prices from two sources — alert if >0.5% discrepancy."""
    discrepancy = abs(source_a - source_b) / source_a
    if discrepancy > PRICE_DISCREPANCY_THRESHOLD:  # 0.5%
        alert.send(AlertLevel.WARNING,
            f"PRICE DISCREPANCY: {metal} — Source A: {source_a}, Source B: {source_b} "
            f"({discrepancy:.2%} difference)")
        return False
    return True
```

---

## DAILY P&L REPORT (sent 16:30 London)

```python
# reporting.py — generated every trading day

DAILY_REPORT_TEMPLATE = """
📊 METALS TRADING — DAILY REPORT
{date} | {time} London

💰 P&L SUMMARY
Portfolio NAV: £{nav:,.2f}
Daily P&L: £{daily_pnl:,.2f} ({daily_pct:+.2f}%)
MTD P&L:   £{mtd_pnl:,.2f} ({mtd_pct:+.2f}%)
YTD P&L:   £{ytd_pnl:,.2f} ({ytd_pct:+.2f}%)

📈 STRATEGY BREAKDOWN
{strategy_table}

🎯 POSITIONS
{position_table}

📡 SIGNALS (next rebalance)
{signal_table}

⚡ ALERTS TODAY
{alerts_today}

🔧 SYSTEM STATUS
Data pipeline: {data_status}
Last LME prices: {lme_timestamp}
Last COMEX prices: {comex_timestamp}
IB Gateway: {ibkr_status}
Reconciliation: {recon_status}
"""
```

---

## REVIEW CADENCE

| Review | Frequency | Scope |
|--------|-----------|-------|
| Daily P&L | Daily (automated) | P&L by strategy/metal, positions vs target, alerts |
| Weekly risk review | Weekly | Correlation matrix, factor exposures, strategy vol vs budget, data quality |
| Monthly performance | Monthly | Strategy attribution, rolling Sharpe, drawdown analysis, parameter stability |
| Quarterly stress test | Quarterly | Historical + hypothetical scenario replay, DR fire drill |
| Annual strategy review | Annually | Full re-backtest, OOS validation, parameter review, infrastructure audit |

**Every review produces a written record in the audit log.**
Template-based weekly and monthly reviews create institutional memory.

---

## INFRASTRUCTURE HEALTH CHECKS

```python
# Run every 5 minutes during trading hours, hourly overnight

HEALTH_CHECKS = {
    "postgresql": check_db_connection_and_query_time,
    "ib_gateway": check_ibkr_api_connection,
    "data_pipeline": check_last_successful_ingestion,
    "signal_engine": check_last_signal_generation,
    "disk_space": check_disk_usage,  # alert at >80%
    "memory": check_memory_usage,    # alert at >85%
    "backup": check_last_backup_success,
}
```

---

## TELEGRAM BOT COMMANDS

```
/status     — Current system health overview
/pnl        — Today's P&L summary
/positions  — Current open positions
/signals    — Latest strategy signals
/alerts     — Recent alerts (last 24h)
/kill_all   — 🔴 EMERGENCY: flatten all positions and cancel orders
/pause      — Pause signal generation (does not close positions)
/resume     — Resume signal generation after pause
/reconcile  — Force manual reconciliation run
```

---

## WHAT I SHOULD FLAG

When working in this directory:
- Alerts that have no defined escalation path
- Reconciliation that doesn't block new orders on discrepancy
- Daily reports that can fail silently (always wrap in try/except with fallback alert)
- Health checks running less than every 5 minutes during trading hours
- Missing alert for data pipeline staleness
- Kill switch command without confirmation step (but confirmation must be fast — <60 seconds)
- Any monitoring component that depends on the same process it's monitoring
