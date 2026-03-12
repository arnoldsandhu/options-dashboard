"""Signal: Put/Call Skew Extremes — 25-delta skew vs 30-day history."""
import calendar
from datetime import date, datetime, timedelta

from polygon import RESTClient

from config import POLYGON_API_KEY
from db.database import save_skew, get_skew_history

SKEW_HISTORY_DAYS = 30
SKEW_PERCENTILE_HIGH = 90   # ELEVATED: put premium extreme
SKEW_PERCENTILE_LOW  = 10   # COMPRESSED: complacency extreme
SKEW_MIN_HISTORY     = 5    # Don't alert until we have this many readings
SKEW_MIN_DTE         = 21
SKEW_MAX_DTE         = 90
DELTA_TARGET_PUT     = -0.25
DELTA_TARGET_CALL    =  0.25
DELTA_TOLERANCE      = 0.10  # Accept deltas within ±0.10 of target

_client: RESTClient | None = None


def _get_client() -> RESTClient:
    global _client
    if _client is None:
        _client = RESTClient(api_key=POLYGON_API_KEY)
    return _client


def _fmt_expiry(expiry: str) -> str:
    try:
        parts = expiry.split("-")
        return f"{calendar.month_abbr[int(parts[1])]}'{parts[0][2:]}"
    except Exception:
        return expiry


def _select_expiry(contracts: list) -> str | None:
    """
    Pick the expiry closest to 30 DTE that is > SKEW_MIN_DTE and < SKEW_MAX_DTE.
    Prefers standard monthly expiries (15th–21st of month = 3rd Friday range).
    """
    today = date.today()
    candidates: dict[str, int] = {}  # expiry_str -> dte

    for c in contracts:
        details = getattr(c, "details", None)
        if not details:
            continue
        exp_str = getattr(details, "expiration_date", None)
        if not exp_str or exp_str in candidates:
            continue
        try:
            exp_date = date.fromisoformat(exp_str)
        except ValueError:
            continue
        dte = (exp_date - today).days
        if SKEW_MIN_DTE <= dte <= SKEW_MAX_DTE:
            candidates[exp_str] = dte

    if not candidates:
        return None

    # Prefer expiries where day-of-month is 15-21 (3rd Friday range)
    monthly = {k: v for k, v in candidates.items()
               if 15 <= date.fromisoformat(k).day <= 21}
    pool = monthly if monthly else candidates

    # Pick closest to 30 DTE
    return min(pool, key=lambda e: abs(pool[e] - 30))


def _get_spot(contracts: list, ticker: str) -> float | None:
    """Get underlying spot price from snapshot or fallback."""
    for c in contracts:
        ua = getattr(c, "underlying_asset", None)
        if ua:
            price = getattr(ua, "price", None)
            if price and price > 0:
                return float(price)
    # Fallback: last trade
    try:
        client = _get_client()
        trade = client.get_last_trade(ticker)
        if trade and trade.price:
            return float(trade.price)
    except Exception:
        pass
    return None


def check_skew(ticker: str) -> list[dict]:
    """
    Check put/call 25-delta skew for ticker against 30-day history.
    Saves every reading to skew_history for future percentile calculations.
    Returns alert dicts when an extreme or crossover condition is met.
    """
    client = _get_client()

    try:
        contracts = list(client.list_snapshot_options_chain(
            ticker, params={"limit": 250}
        ))
    except Exception as e:
        print(f"  [skew] Error fetching chain for {ticker}: {e}")
        return []

    if not contracts:
        return []

    spot = _get_spot(contracts, ticker)
    if not spot:
        return []

    target_expiry = _select_expiry(contracts)
    if not target_expiry:
        return []

    # Filter to target expiry
    exp_contracts = [
        c for c in contracts
        if getattr(getattr(c, "details", None), "expiration_date", None) == target_expiry
    ]

    # Find 25-delta put and call (closest delta within tolerance)
    put_iv: float | None = None
    call_iv: float | None = None
    put_delta_dist = float("inf")
    call_delta_dist = float("inf")

    for c in exp_contracts:
        greeks = getattr(c, "greeks", None)
        if not greeks:
            continue
        delta = getattr(greeks, "delta", None)
        iv = getattr(c, "implied_volatility", None)
        if delta is None or iv is None or iv <= 0:
            continue

        ctype = getattr(getattr(c, "details", None), "contract_type", "")
        if ctype == "put":
            dist = abs(delta - DELTA_TARGET_PUT)
            if dist < put_delta_dist and dist <= DELTA_TOLERANCE:
                put_delta_dist = dist
                put_iv = float(iv)
        elif ctype == "call":
            dist = abs(delta - DELTA_TARGET_CALL)
            if dist < call_delta_dist and dist <= DELTA_TOLERANCE:
                call_delta_dist = dist
                call_iv = float(iv)

    if put_iv is None or call_iv is None:
        return []  # Greeks not available on this subscription tier or insufficient chain

    skew = put_iv - call_iv  # positive = fear premium; puts richer than calls

    # Always persist the reading so history builds up
    save_skew(ticker, skew, spot, target_expiry)

    # Retrieve history for percentile and crossover checks
    history = get_skew_history(ticker, days=SKEW_HISTORY_DAYS)
    values = [h["skew_value"] for h in history]

    if len(values) < SKEW_MIN_HISTORY:
        return []  # Not enough history yet

    sorted_vals = sorted(values)
    n = len(sorted_vals)
    percentile = sum(1 for v in sorted_vals if v <= skew) / n * 100
    avg = sum(values) / len(values)

    # Previous reading (second-to-last, since current is already appended)
    prev_skew = values[-2] if len(values) >= 2 else None

    # Determine signal
    signal: str | None = None
    if percentile >= SKEW_PERCENTILE_HIGH:
        signal = "ELEVATED"
    elif percentile <= SKEW_PERCENTILE_LOW:
        signal = "COMPRESSED"
    elif prev_skew is not None:
        crossed_up   = prev_skew < avg <= skew
        crossed_down = prev_skew > avg >= skew
        if crossed_up or crossed_down:
            signal = "CROSSED"

    if signal is None:
        return []

    skew_vol = skew * 100  # convert to vol-point percentage
    exp_fmt = _fmt_expiry(target_expiry)
    dte = (date.fromisoformat(target_expiry) - date.today()).days

    alert_text = (
        f"{ticker} {exp_fmt} Skew {signal} — "
        f"25d Skew: {skew_vol:+.1f}vol ({percentile:.0f}th pct, 30d)"
    )
    claude_prompt = (
        f"You are an options sales trader. {ticker} 25-delta put/call skew is "
        f"{skew_vol:+.1f} vol points ({percentile:.0f}th percentile over 30 days), "
        f"signal: {signal} (put IV {put_iv:.1%}, call IV {call_iv:.1%}, {dte}d expiry). "
        f"Write one sentence about whether this is a buying or selling opportunity "
        f"for vol, referencing the skew level directly."
    )

    return [{
        "alert_text": alert_text,
        "claude_prompt": claude_prompt,
        "signal_data": {
            "skew": round(skew, 4),
            "skew_vol_pts": round(skew_vol, 2),
            "percentile": round(percentile, 1),
            "put_iv": round(put_iv, 4),
            "call_iv": round(call_iv, 4),
            "spot": round(spot, 2),
            "expiry": target_expiry,
            "dte": dte,
            "signal": signal,
            "history_count": n,
        },
    }]
