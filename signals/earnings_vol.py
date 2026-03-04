"""Signal 4: Earnings Vol Mispricing"""
from datetime import date, datetime, timedelta
from typing import Any

from polygon import RESTClient

from config import (
    POLYGON_API_KEY,
    EARNINGS_DAYS_AHEAD,
    EARNINGS_HISTORY_COUNT,
    EARNINGS_MISPRICE_THRESHOLD,
)
from claude_draft import draft_observation

_client: RESTClient | None = None


def _get_client() -> RESTClient:
    global _client
    if _client is None:
        _client = RESTClient(api_key=POLYGON_API_KEY)
    return _client


def _get_earnings_date(ticker: str) -> date | None:
    """Fetch next earnings date from Polygon."""
    client = _get_client()
    try:
        results = list(client.vx.list_stock_financials(
            ticker=ticker, timeframe="quarterly", limit=1
        ))
        # Fall back to ticker details
        details = client.get_ticker_details(ticker)
        # Try the events endpoint
        today_str = date.today().strftime("%Y-%m-%d")
        events = list(client.list_ticker_events(
            params={"ticker": ticker, "types": "earnings", "date.gte": today_str, "limit": 1}
        ))
        if events:
            ev = events[0]
            event_date = getattr(ev, "date", None)
            if event_date:
                return datetime.strptime(event_date, "%Y-%m-%d").date()
    except Exception as e:
        print(f"  [earnings] Could not fetch earnings date for {ticker}: {e}")
    return None


def _fetch_spot(ticker: str) -> float:
    client = _get_client()
    today_str = date.today().strftime("%Y-%m-%d")
    try:
        aggs = list(client.get_aggs(ticker, 1, "day", today_str, today_str))
        if aggs:
            return aggs[0].close or 0.0
    except Exception:
        pass
    return 0.0


def _fetch_atm_straddle(ticker: str, spot: float, expiry: str) -> float:
    """
    Return (ATM call price + ATM put price) for a given expiry.
    ATM = strike closest to spot.
    """
    client = _get_client()
    contracts = []
    try:
        for c in client.list_snapshot_options_chain(
            ticker,
            params={"expiration_date": expiry, "limit": 100},
        ):
            contracts.append(c)
    except Exception as e:
        print(f"  [earnings] Error fetching chain for {ticker} expiry {expiry}: {e}")
        return 0.0

    # Find ATM call and put
    best_call = None
    best_put = None
    best_call_dist = float("inf")
    best_put_dist = float("inf")

    for c in contracts:
        details = getattr(c, "details", None)
        if details is None:
            continue
        cp = getattr(details, "contract_type", "").lower()
        strike = getattr(details, "strike_price", None)
        if strike is None:
            continue
        dist = abs(strike - spot)
        day = getattr(c, "day", None)
        last = (day.close if day and day.close else None) or (day.last if day and hasattr(day, 'last') else None)
        if last is None or last == 0:
            continue
        if cp == "call" and dist < best_call_dist:
            best_call = last
            best_call_dist = dist
        elif cp == "put" and dist < best_put_dist:
            best_put = last
            best_put_dist = dist

    if best_call and best_put:
        return best_call + best_put
    return 0.0


def _get_historical_moves(ticker: str, count: int = EARNINGS_HISTORY_COUNT) -> list[float]:
    """
    Return list of absolute % moves on earnings days (last `count` earnings).
    Uses daily OHLC from Polygon — moves from prior close to earnings day close.
    """
    client = _get_client()
    moves = []
    try:
        # Pull ~2 years of daily data and find earnings date moves
        end = date.today()
        start = end - timedelta(days=365 * 2)
        aggs = list(client.get_aggs(
            ticker, 1, "day",
            start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
            limit=500,
        ))
        # We don't have precise earnings dates here, so we look for big gap moves
        # as a proxy — top N moves by absolute % change
        daily_moves = []
        for i in range(1, len(aggs)):
            prev_close = aggs[i - 1].close
            curr_close = aggs[i].close
            if prev_close and curr_close and prev_close > 0:
                pct = abs((curr_close - prev_close) / prev_close)
                daily_moves.append(pct)
        daily_moves.sort(reverse=True)
        moves = daily_moves[:count]
    except Exception as e:
        print(f"  [earnings] Could not fetch historical moves for {ticker}: {e}")
    return moves


def check_earnings_vol(ticker: str) -> list[dict]:
    """
    Returns alert dicts for earnings vol mispricing.
    """
    earnings_date = _get_earnings_date(ticker)
    if earnings_date is None:
        return []

    today = date.today()
    days_to_earnings = (earnings_date - today).days
    if days_to_earnings < 0 or days_to_earnings > EARNINGS_DAYS_AHEAD:
        return []

    spot = _fetch_spot(ticker)
    if spot == 0.0:
        return []

    # Find the first expiry on or just after earnings
    expiry = earnings_date.strftime("%Y-%m-%d")
    straddle = _fetch_atm_straddle(ticker, spot, expiry)
    if straddle == 0.0:
        return []

    implied_move = straddle / spot

    hist_moves = _get_historical_moves(ticker)
    if len(hist_moves) < 3:
        return []

    hist_avg = sum(hist_moves) / len(hist_moves)
    diff = implied_move - hist_avg

    if abs(diff) < EARNINGS_MISPRICE_THRESHOLD:
        return []

    cheap_rich = "CHEAP" if diff < 0 else "RICH"
    signal_desc = (
        f"{ticker} earnings in {days_to_earnings} days: implied move {implied_move*100:.1f}% "
        f"vs {hist_avg*100:.1f}% historical avg — vol is {cheap_rich}"
    )
    observation = draft_observation(signal_desc)
    alert_text = (
        f"{ticker} EARNINGS VOL — Implied move: {implied_move*100:.1f}% "
        f"vs {hist_avg*100:.1f}% historical avg — {cheap_rich} | {observation}"
    )

    return [{
        "ticker": ticker,
        "earnings_date": earnings_date.strftime("%Y-%m-%d"),
        "days_to_earnings": days_to_earnings,
        "implied_move": implied_move,
        "hist_avg": hist_avg,
        "cheap_rich": cheap_rich,
        "alert_text": alert_text,
        "signal_data": {
            "earnings_date": earnings_date.strftime("%Y-%m-%d"),
            "days_to_earnings": days_to_earnings,
            "implied_move": implied_move,
            "hist_avg": hist_avg,
            "cheap_rich": cheap_rich,
            "straddle": straddle,
            "spot": spot,
        },
    }]
