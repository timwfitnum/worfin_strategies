"""
config/calendar.py
Exchange holiday calendars and trading day utilities.

LME holidays: https://www.lme.com/en/market-data/LME-trading-calendar
COMEX holidays: https://www.cmegroup.com/tools-information/holiday-calendar
"""
from __future__ import annotations

import datetime
from functools import lru_cache


# ─────────────────────────────────────────────────────────────────────────────
# HOLIDAY DEFINITIONS
# ─────────────────────────────────────────────────────────────────────────────
# Both LME and COMEX close on these UK/US public holidays.
# Update annually. Source from exchange websites each December.

_LME_HOLIDAYS_2025 = [
    datetime.date(2025, 1, 1),   # New Year's Day
    datetime.date(2025, 4, 18),  # Good Friday
    datetime.date(2025, 4, 21),  # Easter Monday
    datetime.date(2025, 5, 5),   # Early May Bank Holiday
    datetime.date(2025, 5, 26),  # Spring Bank Holiday
    datetime.date(2025, 8, 25),  # Summer Bank Holiday
    datetime.date(2025, 12, 25), # Christmas Day
    datetime.date(2025, 12, 26), # Boxing Day
]

_LME_HOLIDAYS_2026 = [
    datetime.date(2026, 1, 1),   # New Year's Day
    datetime.date(2026, 4, 3),   # Good Friday
    datetime.date(2026, 4, 6),   # Easter Monday
    datetime.date(2026, 5, 4),   # Early May Bank Holiday
    datetime.date(2026, 5, 25),  # Spring Bank Holiday
    datetime.date(2026, 8, 31),  # Summer Bank Holiday
    datetime.date(2026, 12, 25), # Christmas Day
    datetime.date(2026, 12, 28), # Boxing Day (substitute)
]

_COMEX_HOLIDAYS_2025 = [
    datetime.date(2025, 1, 1),   # New Year's Day
    datetime.date(2025, 1, 20),  # Martin Luther King Jr. Day
    datetime.date(2025, 2, 17),  # Presidents' Day
    datetime.date(2025, 4, 18),  # Good Friday
    datetime.date(2025, 5, 26),  # Memorial Day
    datetime.date(2025, 6, 19),  # Juneteenth
    datetime.date(2025, 7, 4),   # Independence Day
    datetime.date(2025, 9, 1),   # Labor Day
    datetime.date(2025, 11, 27), # Thanksgiving
    datetime.date(2025, 12, 25), # Christmas Day
]

_COMEX_HOLIDAYS_2026 = [
    datetime.date(2026, 1, 1),   # New Year's Day
    datetime.date(2026, 1, 19),  # Martin Luther King Jr. Day
    datetime.date(2026, 2, 16),  # Presidents' Day
    datetime.date(2026, 4, 3),   # Good Friday
    datetime.date(2026, 5, 25),  # Memorial Day
    datetime.date(2026, 6, 19),  # Juneteenth
    datetime.date(2026, 7, 3),   # Independence Day (observed)
    datetime.date(2026, 9, 7),   # Labor Day
    datetime.date(2026, 11, 26), # Thanksgiving
    datetime.date(2026, 12, 25), # Christmas Day
]

LME_HOLIDAYS: set[datetime.date] = set(_LME_HOLIDAYS_2025 + _LME_HOLIDAYS_2026)
COMEX_HOLIDAYS: set[datetime.date] = set(_COMEX_HOLIDAYS_2025 + _COMEX_HOLIDAYS_2026)
ALL_HOLIDAYS: set[datetime.date] = LME_HOLIDAYS | COMEX_HOLIDAYS


# ─────────────────────────────────────────────────────────────────────────────
# CALENDAR UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def is_lme_trading_day(d: datetime.date) -> bool:
    """True if LME is open on this date."""
    return d.weekday() < 5 and d not in LME_HOLIDAYS   # Mon–Fri, not holiday


def is_comex_trading_day(d: datetime.date) -> bool:
    """True if COMEX is open on this date."""
    return d.weekday() < 5 and d not in COMEX_HOLIDAYS


def is_joint_trading_day(d: datetime.date) -> bool:
    """True if BOTH LME and COMEX are open (required for cross-metal signals)."""
    return is_lme_trading_day(d) and is_comex_trading_day(d)


def next_lme_trading_day(d: datetime.date) -> datetime.date:
    """Next LME trading day after date d (exclusive)."""
    candidate = d + datetime.timedelta(days=1)
    while not is_lme_trading_day(candidate):
        candidate += datetime.timedelta(days=1)
    return candidate


def prev_lme_trading_day(d: datetime.date) -> datetime.date:
    """Previous LME trading day before date d (exclusive)."""
    candidate = d - datetime.timedelta(days=1)
    while not is_lme_trading_day(candidate):
        candidate -= datetime.timedelta(days=1)
    return candidate


def count_trading_days(start: datetime.date, end: datetime.date) -> int:
    """
    Count LME trading days between start (inclusive) and end (exclusive).
    Used for staleness checks and carry calculation.
    """
    count = 0
    d = start
    while d < end:
        if is_lme_trading_day(d):
            count += 1
        d += datetime.timedelta(days=1)
    return count


@lru_cache(maxsize=512)
def compute_lme_3m_prompt(cash_date: datetime.date) -> datetime.date:
    """
    Compute the LME 3-Month prompt date for a given Cash settle date.

    LME 3-Month = business day closest to exactly 3 calendar months forward.
    This rolls DAILY — it is not a fixed monthly date.

    Args:
        cash_date: The Cash settlement date (typically T+2)

    Returns:
        The 3-Month prompt date
    """
    target = cash_date + datetime.timedelta(days=91)   # approx 3 calendar months

    # Find nearest LME business day
    forward = target
    while not is_lme_trading_day(forward):
        forward += datetime.timedelta(days=1)

    backward = target
    while not is_lme_trading_day(backward):
        backward -= datetime.timedelta(days=1)

    # Return the closer of the two
    if (forward - target).days <= (target - backward).days:
        return forward
    return backward


def get_cash_settle_date(trade_date: datetime.date) -> datetime.date:
    """
    LME Cash settles T+2 business days.
    Accounts for LME holidays.
    """
    t1 = next_lme_trading_day(trade_date)
    return next_lme_trading_day(t1)


def get_lme_3m_dte(trade_date: datetime.date) -> int:
    """
    Actual calendar days between Cash and 3M settle on a given trade date.
    CRITICAL: use this, not a fixed 91 — it changes every day.
    """
    cash = get_cash_settle_date(trade_date)
    three_month = compute_lme_3m_prompt(cash)
    return (three_month - cash).days


def trading_days_between(
    start: datetime.date,
    end: datetime.date,
    exchange: str = "LME",
) -> list[datetime.date]:
    """Return list of all trading days between start and end (inclusive)."""
    is_trading: callable
    if exchange == "LME":
        is_trading = is_lme_trading_day
    elif exchange == "COMEX":
        is_trading = is_comex_trading_day
    else:
        is_trading = is_joint_trading_day

    days = []
    d = start
    while d <= end:
        if is_trading(d):
            days.append(d)
        d += datetime.timedelta(days=1)
    return days