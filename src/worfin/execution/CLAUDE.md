# CLAUDE.md — /execution
# Execution Engine | Part of: Metals Systematic Trading System

---

## SCOPE & CRITICAL WARNING

This directory bridges signals → broker orders.
Every function here directly submits or cancels real orders with real money.
**Treat as safety-critical. No exceptions. No shortcuts.**

```
/execution/
├── engine.py           ← Core daily execution loop
├── orders.py           ← Order lifecycle management
├── pretrade_checks.py  ← All pre-trade validations (MANDATORY)
└── broker/
    └── ibkr.py         ← IB Gateway connector via ib_insync
```

---

## EXECUTION LOOP (DAILY)

```python
# engine.py — runs once per day after signal generation

async def run_execution_cycle():
    """
    Core execution loop. Runs 14:00–16:00 London time.
    All 8 steps must complete before the loop is considered done.
    """
    # Step 1: Read target positions from signals DB
    target_positions = await db.get_approved_target_positions()

    # Step 2: Query current positions from IBKR
    current_positions = await ibkr.get_positions()

    # Step 3: Compute deltas (what we need to trade)
    deltas = compute_deltas(target_positions, current_positions)

    # Step 4: PRE-TRADE RISK CHECKS (all must pass — block entire order if any fail)
    approved_orders = await pretrade_checks.validate_all(deltas)

    # Step 5: Generate orders
    orders = generate_orders(approved_orders)

    # Step 6: Submit to IBKR (with escalation protocol)
    fills = await submit_with_escalation(orders)

    # Step 7: Post-execution reconciliation
    await reconcile_positions(fills, target_positions)

    # Step 8: Write audit log
    await audit.log_execution_cycle(orders, fills, discrepancies)
```

---

## PRE-TRADE CHECKS (MANDATORY — block order if ANY fails)

```python
# pretrade_checks.py

class PreTradeChecker:
    """All checks run before ANY order is submitted."""

    def check_1_position_limits(self, order, portfolio) -> bool:
        """Order size within per-metal notional limit (20% of NAV)."""

    def check_2_gross_exposure(self, order, portfolio) -> bool:
        """Order won't breach portfolio gross exposure limit (250% NAV)."""

    def check_3_net_exposure(self, order, portfolio) -> bool:
        """Order won't breach portfolio net exposure limit (80% NAV)."""

    def check_4_liquidity_tier(self, order, metal_adv) -> bool:
        """Order within liquidity tier maximum (5%/3%/2% of ADV)."""

    def check_5_fat_finger(self, order, current_mid) -> bool:
        """Order price within 2% of current mid-price."""

    def check_6_daily_order_count(self, orders_today) -> bool:
        """Total daily orders below maximum (50 orders/day)."""

    def check_7_signal_direction(self, order, signal) -> bool:
        """Order direction matches the signal (prevents sign errors)."""

    def check_8_staleness(self, signal_timestamp) -> bool:
        """Signal is less than 24 hours old (prevents stale execution)."""
```

**Any check failure:**
1. Block the order — do not submit
2. Log to `audit.risk_breaches` with full context
3. Send Telegram alert immediately
4. Do NOT retry automatically — human review required

---

## ORDER ESCALATION PROTOCOL

```python
# orders.py — 3-step escalation for every order

async def submit_with_escalation(order: Order) -> Fill:
    """
    Step 1: Passive limit (60 seconds)
    Step 2: Aggressive limit (60 seconds)
    Step 3: Market order fallback (rare — flag for review)
    """

    # Step 1: Passive limit at mid-price
    # Expected fill rate: 60-80% Tier 1, 40-60% Tier 2/3
    limit_passive = LimitOrder(
        price=current_mid,
        tif=TimeInForce.SECONDS_60
    )
    fill = await ibkr.submit(limit_passive)
    if fill.is_complete:
        return fill

    # Step 2: Aggressive limit at best bid/offer
    # Crosses spread but still has price protection
    limit_aggressive = LimitOrder(
        price=best_offer if order.is_buy else best_bid,
        tif=TimeInForce.SECONDS_60
    )
    fill = await ibkr.submit(limit_aggressive)
    if fill.is_complete:
        return fill

    # Step 3: Market order fallback
    # Should be <10% of executions — alert if higher
    market = MarketOrder()
    alert_if_market_rate_high()  # alert if >10% of daily executions
    return await ibkr.submit(market)
```

**Why not just market orders?**
For Tier 1 metals, difference is 1–2 bps per trade.
Over 200+ trades/year across 6 strategies → 2–4% annualised return drag.
For Tier 3 metals (Sn, Pd): 5–15 bps/trade — enough to kill a marginal strategy.

---

## ORDER SIZING FOR MARKET IMPACT

```python
# Tier 1 (Cu, Al, Au, Ag): up to 50 lots in single clip
# Above 50: split into 10–20 lot child orders, 30-second intervals

# Tier 2 (Zn, Ni, Pb, Pt): up to 20 lots in single clip
# Above 20: split into 5–10 lot clips

# Tier 3 (Sn, Pd): up to 5 lots in single clip
# Above 5: split into 1–2 lot clips, 60-second intervals
# For Tin: CHECK ORDER BOOK DEPTH before executing (visible book may be 3-5 lots/side)

# At current scale (<£500k): unlikely to exceed these thresholds
# This logic becomes critical at Tier 3 institutional scale
```

---

## EXECUTION TIMING

| Metal Group | Window | Rationale | Avoid |
|-------------|--------|-----------|-------|
| LME Base Metals | 14:00–16:00 London | Deepest liquidity, tightest spreads, aligns with Ring | 01:00–07:00 Asian session |
| COMEX Gold | 14:00–16:00 London | London + NY both active | Overnight 23:00–07:00 |
| COMEX Silver | 14:00–16:00 London | Spreads 3-5× wider outside window | Same |
| COMEX Pt/Pd | 14:30–15:30 London | Short optimal window — thin liquidity | After 16:00 |
| S1/S4 signals | After 13:30 London | After official LME prices published | During Ring (13:00–13:30) |
| S5 Inventory | After 09:30 London | 30min after LME inventory report | Immediately on release |

---

## PARTIAL FILL HANDLING

```python
# After execution window closes, decide based on unfilled quantity:

if unfilled_pct < 0.30:
    # Less than 30% unfilled → leave it (cost of chasing > alpha)
    action = "LEAVE_UNFILLED"

elif unfilled_pct >= 0.30:
    # More than 30% unfilled → escalate to aggressive limit
    action = "ESCALATE_AGGRESSIVE"

# NEVER chase with a market order unless risk framework requires it
# (e.g., stop-loss exit that MUST execute)
```

---

## ERROR HANDLING

**Order rejection:**
- Log rejection reason to `audit.order_rejections`
- Alert via Telegram immediately
- Do NOT retry automatically
- Human review required before any retry

**API disconnection mid-execution:**
- Enter safe state: no new orders submitted
- Existing working orders remain active (broker manages server-side)
- On reconnect: reconcile positions BEFORE resuming
- Never assume positions are correct after a disconnection

**Stale signals (>24h old):**
- Refuse to execute
- Alert immediately
- Log to `audit.data_quality_flags`

**Broker outage:**
1. Retry connection for 15 minutes
2. If still down: log intended trades, wait for reconnection
3. Execute at next opportunity (verify price range still acceptable)
4. File incident report documenting P&L impact

---

## IBKR CONNECTOR SPECIFICS

```python
# broker/ibkr.py

# Use IB Gateway (NOT TWS) for unattended operation
# IB Gateway: headless, ~200MB RAM vs 1GB+ for TWS, more stable
# Port: 4001 (live), 4002 (paper trading)

# Key methods needed:
# ibkr.get_positions()           → current positions from broker
# ibkr.get_account_summary()     → NAV, margin, etc.
# ibkr.submit_order(order)       → submit order, return order_id
# ibkr.get_order_status(id)      → check fill status
# ibkr.cancel_order(id)          → cancel working order
# ibkr.cancel_all_orders()       → kill switch component
# ibkr.flatten_all_positions()   → kill switch component

# IBKR TWS API note:
# LME metals accessed via IBKR as exchange = "ICEEU"
# Always verify contract specs before first live order
```

---

## EXECUTION QUALITY TRACKING

Log these metrics for every trade:

| Metric | Target | Alert If |
|--------|--------|----------|
| Implementation Shortfall | <3 bps (T1), <8 bps (T2), <15 bps (T3) | >2× target |
| Spread Capture Rate | >40% for passive orders | <20% sustained |
| Passive Fill Rate | 60–80% for Tier 1 | <40% |
| Market Order Rate | <10% of executions | >20% |
| Slippage vs Model | Within ±50% of model | >2× model sustained |
| Roll Cost vs Model | Within 20% of fair value | >50% of fair value |

**Monthly report:** If implementation shortfall consistently >50% above model:
- Either the cost model is too optimistic (update and re-backtest)
- Or execution timing/tactics are wrong (review and adjust)
- Or market microstructure has changed (investigate)

---

## ROLL EXECUTION

**LME Rolls (S1 Carry positions):**
- Roll 2–3 business days before Cash prompt date
- ALWAYS execute as a SINGLE SPREAD ORDER (not two separate legs)
- LME spread trading is highly liquid — often more liquid than outright
- Eliminates leg risk (risk that one leg fills and the other doesn't)

**COMEX Rolls:**
- Execute as calendar spread on CME Globex (use IBKR combo/spread order type)
- Never leg into COMEX rolls separately

**Roll cost logging:**
```python
# Log every roll:
{
    "date": date,
    "metal": metal,
    "front_contract": front,
    "back_contract": back,
    "spread_price_paid": actual,
    "theoretical_fair_spread": model,
    "roll_slippage": actual - model
}
```

---

## KILL SWITCH IMPLEMENTATION

```python
async def kill_switch(triggered_by: str, reason: str):
    """
    Emergency flatten. Must complete within 60 seconds.
    Accessible via: Telegram /kill_all command, dedicated web endpoint, direct IBKR.
    """
    # Step 1: Cancel ALL working orders
    await ibkr.cancel_all_orders()

    # Step 2: Flatten ALL positions to market
    await ibkr.flatten_all_positions()

    # Step 3: Log to audit (immutable record)
    await audit.log_kill_switch(triggered_by, reason, timestamp=now())

    # Step 4: Alert all channels
    await telegram.send_urgent(f"KILL SWITCH ACTIVATED: {reason}")
    await email.send_urgent(f"KILL SWITCH ACTIVATED: {reason}")

    # Step 5: Disable signal engine
    await signal_engine.disable()
```

---

## WHAT I SHOULD FLAG

When working in this directory:
- Any execution path that bypasses pretrade_checks.py
- Market orders being used as default (should be rare fallback)
- Missing audit log entries for any order event
- Assuming positions are correct without querying broker API
- LME and COMEX rolls being executed as separate legs instead of spreads
- Any hardcoded credentials or connection strings
- Missing error handling for IBKR API disconnection scenarios
