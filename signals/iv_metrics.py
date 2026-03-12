"""
Compute ATM IV, 20-day realized volatility, IV Rank, and IV/RV ratio.
Called from signals/gex.py after the options chain is already fetched — zero extra API calls.
"""
from __future__ import annotations
import math
from datetime import datetime, date


def get_atm_iv(spot: float, contracts: list[dict]) -> float | None:
    """
    Find ATM IV from the nearest 21-45 DTE expiry.
    Uses the call and put closest to spot, averages their IVs.
    Returns None if no suitable contracts exist.
    """
    if not spot or not contracts:
        return None

    today = date.today()
    # Filter to 21-45 DTE expiries with valid IV
    candidates: list[dict] = []
    for c in contracts:
        exp_str = c.get("expiration", "")
        iv = c.get("iv", 0.0)
        if not exp_str or not iv:
            continue
        try:
            exp_date = date.fromisoformat(exp_str)
        except ValueError:
            continue
        dte = (exp_date - today).days
        if 21 <= dte <= 45:
            candidates.append(c)

    if not candidates:
        # Fall back to 10-60 DTE if nothing in preferred range
        for c in contracts:
            exp_str = c.get("expiration", "")
            iv = c.get("iv", 0.0)
            if not exp_str or not iv:
                continue
            try:
                exp_date = date.fromisoformat(exp_str)
            except ValueError:
                continue
            dte = (exp_date - today).days
            if 10 <= dte <= 60:
                candidates.append(c)

    if not candidates:
        return None

    # Pick the expiry with median DTE in the candidate set
    today_dt = date.today()
    expiries = sorted({c["expiration"] for c in candidates})
    target_exp = expiries[len(expiries) // 2]
    exp_contracts = [c for c in candidates if c["expiration"] == target_exp]

    # Separate calls and puts, find nearest-ATM for each
    calls = [c for c in exp_contracts if c.get("option_type") == "call" and c.get("iv")]
    puts  = [c for c in exp_contracts if c.get("option_type") == "put"  and c.get("iv")]

    ivs = []
    for pool in (calls, puts):
        if pool:
            closest = min(pool, key=lambda c: abs(c.get("strike", 0) - spot))
            iv = closest.get("iv", 0.0)
            if iv and 0.01 <= iv <= 5.0:  # sanity: 1% – 500% annualised
                ivs.append(iv)

    if not ivs:
        return None
    return sum(ivs) / len(ivs)


def compute_rv_20d(ticker: str) -> float | None:
    """
    Compute 20-day realized volatility (annualized) from gex_history spot prices.
    Picks the most-recent reading per calendar day, then log-returns, annualized by sqrt(252).
    Returns None if fewer than 5 distinct trading days are available.
    """
    from db.database import get_gex_history
    rows = get_gex_history(ticker, days=30)
    if len(rows) < 5:
        return None

    # One price per calendar day (most recent reading)
    daily: dict[str, float] = {}
    for r in rows:
        day = r["timestamp"][:10]
        price = r.get("spot_price", 0.0)
        if price and price > 0:
            daily[day] = price  # rows are in ascending ts order; last wins

    prices = [v for _, v in sorted(daily.items())]
    if len(prices) < 5:
        return None

    # Trim to most-recent 21 prices (20 returns)
    prices = prices[-21:]
    log_returns = [
        math.log(prices[i] / prices[i - 1])
        for i in range(1, len(prices))
        if prices[i - 1] > 0
    ]
    if len(log_returns) < 4:
        return None

    n = len(log_returns)
    mean = sum(log_returns) / n
    variance = sum((r - mean) ** 2 for r in log_returns) / (n - 1)
    daily_vol = math.sqrt(variance)
    return daily_vol * math.sqrt(252)


def compute_iv_rank(ticker: str, current_iv: float) -> float | None:
    """
    IV Rank = percentile of current ATM IV vs stored history.
    Uses up to 365 days of iv_metrics records; labels itself accordingly.
    Returns None if fewer than 5 history points.
    """
    from db.database import get_iv_history
    hist = get_iv_history(ticker, days=365)
    if len(hist) < 5:
        return None
    values = [h["atm_iv"] for h in hist if h.get("atm_iv")]
    if len(values) < 5:
        return None
    below = sum(1 for v in values if v < current_iv)
    return (below / len(values)) * 100.0


def compute_and_store_iv_metrics(ticker: str, spot: float, contracts: list[dict]) -> dict | None:
    """
    Compute and persist IV metrics. Returns dict with keys:
      atm_iv, rv_20d, iv_rank, iv_rv_ratio
    or None if ATM IV can't be computed.
    """
    from db.database import save_iv_metrics

    atm_iv = get_atm_iv(spot, contracts)
    if atm_iv is None:
        return None

    rv_20d = compute_rv_20d(ticker)

    # IV Rank: compute from prior history BEFORE saving today's reading
    iv_rank = compute_iv_rank(ticker, atm_iv)

    iv_rv_ratio = None
    if rv_20d and rv_20d > 0:
        iv_rv_ratio = atm_iv / rv_20d

    try:
        save_iv_metrics(ticker, atm_iv, rv_20d, iv_rank, iv_rv_ratio)
    except Exception as e:
        print(f"  [iv_metrics] save error {ticker}: {e}")

    return {
        "atm_iv": atm_iv,
        "rv_20d": rv_20d,
        "iv_rank": iv_rank,
        "iv_rv_ratio": iv_rv_ratio,
    }
