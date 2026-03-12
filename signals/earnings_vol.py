"""Signal 4: Earnings Vol Mispricing"""
from datetime import date, datetime, timedelta

from polygon import RESTClient

from config import (
    POLYGON_API_KEY,
    EARNINGS_DAYS_AHEAD,
    EARNINGS_HISTORY_COUNT,
    EARNINGS_MISPRICE_THRESHOLD,
)

_client: RESTClient | None = None


def _get_client() -> RESTClient:
    global _client
    if _client is None:
        _client = RESTClient(api_key=POLYGON_API_KEY)
    return _client


def _get_earnings_date(ticker: str) -> date | None:
    """
    Fetch next upcoming earnings date via Polygon vX ticker events.
    Returns None silently if the endpoint is unavailable on this plan.
    """
    client = _get_client()
    today_str = date.today().strftime("%Y-%m-%d")
    try:
        vx = getattr(client, "vx", None)
        if vx is None:
            return None
        fn = getattr(vx, "list_ticker_events", None)
        if fn is None:
            return None
        events = list(fn(ticker=ticker, types="earnings", date_gte=today_str, limit=1))
        if events:
            ev = events[0]
            event_date = getattr(ev, "date", None)
            if event_date:
                return datetime.strptime(str(event_date)[:10], "%Y-%m-%d").date()
    except Exception:
        pass  # Silently skip — earnings events require a higher Polygon plan tier
    return None


def _fetch_spot(ticker: str) -> float:
    """Fetch spot from options chain underlying, then last trade, then prev close."""
    client = _get_client()
    try:
        for contract in client.list_snapshot_options_chain(ticker, params={"limit": 1}):
            ua = getattr(contract, "underlying_asset", None)
            if ua:
                price = getattr(ua, "price", None)
                if price:
                    return float(price)
            break
    except Exception:
        pass
    try:
        trade = client.get_last_trade(ticker)
        price = getattr(trade, "price", None) or getattr(trade, "p", None)
        if price:
            return float(price)
    except Exception:
        pass
    try:
        pc = list(client.get_previous_close(ticker))
        if pc and hasattr(pc[0], "close") and pc[0].close:
            return float(pc[0].close)
    except Exception:
        pass
    return 0.0


def _fetch_atm_straddle(ticker: str, spot: float, expiry: str) -> float:
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

    best_call = best_put = None
    best_call_dist = best_put_dist = float("inf")

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
        last = (day.close if day and day.close else None)
        if last is None or last == 0:
            continue
        if cp == "call" and dist < best_call_dist:
            best_call, best_call_dist = last, dist
        elif cp == "put" and dist < best_put_dist:
            best_put, best_put_dist = last, dist

    return (best_call + best_put) if best_call and best_put else 0.0


def _get_historical_moves(ticker: str, count: int = EARNINGS_HISTORY_COUNT) -> list[float]:
    """Top N daily absolute % moves as proxy for historical earnings moves."""
    client = _get_client()
    try:
        end = date.today()
        start = end - timedelta(days=365 * 2)
        aggs = list(client.get_aggs(
            ticker, 1, "day",
            start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
            limit=500,
        ))
        daily_moves = []
        for i in range(1, len(aggs)):
            prev_close = aggs[i - 1].close
            curr_close = aggs[i].close
            if prev_close and curr_close and prev_close > 0:
                daily_moves.append(abs((curr_close - prev_close) / prev_close))
        daily_moves.sort(reverse=True)
        return daily_moves[:count]
    except Exception:
        return []  # Plan restriction — skip historical move check


def check_earnings_vol(ticker: str) -> list[dict]:
    """
    Returns alert dicts for earnings vol mispricing.
    Each dict has 'alert_text' (base, no Claude) and 'claude_prompt'.
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
    claude_prompt = (
        f"{ticker} earnings in {days_to_earnings} days: implied move {implied_move*100:.1f}% "
        f"vs {hist_avg*100:.1f}% historical avg — vol is {cheap_rich}"
    )
    alert_text = (
        f"{ticker} EARNINGS VOL — Implied: {implied_move*100:.1f}% "
        f"vs {hist_avg*100:.1f}% hist — {cheap_rich}"
    )

    return [{
        "ticker": ticker,
        "earnings_date": earnings_date.strftime("%Y-%m-%d"),
        "days_to_earnings": days_to_earnings,
        "implied_move": implied_move,
        "hist_avg": hist_avg,
        "cheap_rich": cheap_rich,
        "alert_text": alert_text,
        "claude_prompt": claude_prompt,
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
