"""
gex_engine.greek_calc
---------------------
Internal Black-Scholes gamma calculator.

Falls back to vendor_gamma if IV is missing or the BS calculation fails.
Logs all fallbacks to stdout (capped to avoid log spam).
"""
from __future__ import annotations

import math
from datetime import date, datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

_MARKET_CLOSE_HOUR = 16
_MARKET_CLOSE_MIN  = 0
_TRADING_HOURS_PER_DAY = 6.5   # 9:30 – 16:00 ET
_TRADING_DAYS_PER_YEAR = 252

_fallback_count = 0


# ── Math helpers ──────────────────────────────────────────────────────────────

def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


# ── Time-to-expiry ────────────────────────────────────────────────────────────

def time_to_expiry_years(expiration: str,
                         reference_date: date | None = None) -> float:
    """
    Precise time-to-expiry in years.

    For 0DTE (expiry == today) uses remaining market hours as the time measure,
    expressed as a fraction of a trading year:
        t = hours_remaining / (252 × 6.5)

    For future dates uses calendar days / 365.

    Args:
        expiration:      ISO date string "YYYY-MM-DD"
        reference_date:  override today (useful in tests)

    Returns:
        float ≥ 0.0
    """
    try:
        exp_date = date.fromisoformat(expiration)
    except ValueError:
        return 0.0

    today = reference_date or date.today()

    if exp_date < today:
        return 0.0

    if exp_date == today:
        now_et   = datetime.now(ET)
        close_et = now_et.replace(hour=_MARKET_CLOSE_HOUR,
                                  minute=_MARKET_CLOSE_MIN,
                                  second=0, microsecond=0)
        hours_remaining = max(0.0, (close_et - now_et).total_seconds() / 3600.0)
        return hours_remaining / (_TRADING_DAYS_PER_YEAR * _TRADING_HOURS_PER_DAY)

    return (exp_date - today).days / 365.0


# ── Black-Scholes gamma ───────────────────────────────────────────────────────

def bs_gamma(spot: float, strike: float, t: float,
             iv: float, rate: float = 0.05) -> float | None:
    """
    Black-Scholes gamma (per-share, per $1 spot move).

    Args:
        spot:   current underlying price
        strike: option strike price
        t:      time to expiry in years (precise, ≥ 0)
        iv:     implied volatility annualised (e.g. 0.20 = 20%)
        rate:   risk-free rate (default 0.05)

    Returns:
        gamma float, or None if inputs are invalid / result is non-finite.
    """
    if spot <= 0 or strike <= 0 or t <= 0 or iv <= 0:
        return None
    try:
        d1 = (math.log(spot / strike) + (rate + 0.5 * iv ** 2) * t) / (iv * math.sqrt(t))
        gamma = _norm_pdf(d1) / (spot * iv * math.sqrt(t))
        return gamma if math.isfinite(gamma) else None
    except (ValueError, ZeroDivisionError):
        return None


# ── Public entry point ────────────────────────────────────────────────────────

def get_gamma(contract: dict, spot: float, rate: float = 0.05) -> float:
    """
    Return the best available gamma for a normalized contract dict.

    Priority:
    1. Black-Scholes using contract['iv'] and computed time-to-expiry
    2. contract['vendor_gamma'] (fallback, logged)
    3. 0.0  (no gamma available)

    Args:
        contract:  normalized dict from vendor_ingest (must have keys:
                   ticker, strike, expiration, iv, vendor_gamma)
        spot:      current underlying price
        rate:      risk-free rate (default 0.05)
    """
    global _fallback_count

    iv           = contract.get("iv", 0.0)
    expiration   = contract.get("expiration", "")
    vendor_gamma = contract.get("vendor_gamma", 0.0)

    if iv > 0:
        t = time_to_expiry_years(expiration)
        if t > 0:
            g = bs_gamma(spot, contract["strike"], t, iv, rate)
            if g is not None:
                return g

    # Fallback to vendor gamma
    if vendor_gamma != 0.0:
        _fallback_count += 1
        if _fallback_count <= 20 or _fallback_count % 500 == 0:
            reason = ("missing IV" if iv == 0.0
                      else "t=0 (0DTE after close)" if time_to_expiry_years(expiration) <= 0
                      else "BS failed")
            print(f"  [greek_calc] fallback→vendor ({reason}): "
                  f"{contract.get('ticker','')} {contract.get('option_type','')} "
                  f"K={contract['strike']} exp={expiration} "
                  f"iv={iv:.4f}")
        return float(vendor_gamma)

    return 0.0


def reset_fallback_counter() -> None:
    """Reset the fallback log counter (call once per scan cycle)."""
    global _fallback_count
    _fallback_count = 0
