"""Signal 1: Unusual Options Volume"""
from datetime import date, timedelta
from typing import Any

from polygon import RESTClient

from config import (
    POLYGON_API_KEY,
    VOL_OI_RATIO_THRESHOLD,
    VOL_MIN_CONTRACTS,
    VOL_AVG_DAYS,
    VOL_AVG_MULTIPLIER,
)
from claude_draft import draft_observation

_client: RESTClient | None = None


def _get_client() -> RESTClient:
    global _client
    if _client is None:
        _client = RESTClient(api_key=POLYGON_API_KEY)
    return _client


def _fetch_options_chain(ticker: str, as_of: str) -> list[dict]:
    """Return all option contracts for ticker as of as_of date."""
    client = _get_client()
    contracts = []
    try:
        for contract in client.list_snapshot_options_chain(
            ticker,
            params={"limit": 250},
        ):
            contracts.append(contract)
    except Exception as e:
        print(f"  [unusual_vol] Error fetching chain for {ticker}: {e}")
    return contracts


def _get_historical_volume(ticker: str, days: int = VOL_AVG_DAYS) -> float:
    """Return average daily options volume over the last N trading days."""
    client = _get_client()
    today = date.today()
    volumes = []
    for i in range(1, days + 10):  # extra days to account for weekends/holidays
        d = today - timedelta(days=i)
        if d.weekday() >= 5:
            continue
        day_str = d.strftime("%Y-%m-%d")
        try:
            aggs = list(client.get_aggs(f"O:{ticker}", 1, "day", day_str, day_str))
            if aggs:
                volumes.append(aggs[0].volume or 0)
        except Exception:
            pass
        if len(volumes) >= days:
            break
    return sum(volumes) / len(volumes) if volumes else 0.0


def check_unusual_vol(ticker: str) -> list[dict]:
    """
    Returns a list of alert dicts for unusual-volume signals on ticker.
    Each dict: {ticker, contract, ratio, volume, oi, alert_text}
    """
    today = date.today().strftime("%Y-%m-%d")
    contracts = _fetch_options_chain(ticker, today)
    if not contracts:
        return []

    alerts = []
    total_vol = 0

    for c in contracts:
        details = getattr(c, "details", None)
        day = getattr(c, "day", None)
        greeks = getattr(c, "greeks", None)

        volume = (day.volume if day and day.volume else 0) or 0
        oi = getattr(c, "open_interest", 0) or 0
        total_vol += volume

        if volume < VOL_MIN_CONTRACTS or oi == 0:
            continue

        ratio = volume / oi
        if ratio >= VOL_OI_RATIO_THRESHOLD:
            strike = getattr(details, "strike_price", "?") if details else "?"
            expiry = getattr(details, "expiration_date", "?") if details else "?"
            cp = getattr(details, "contract_type", "?").upper() if details else "?"
            expiry_short = expiry[2:].replace("-", "") if expiry != "?" else "?"

            signal_desc = (
                f"{ticker} {cp} {strike} exp {expiry}: "
                f"vol/OI ratio {ratio:.1f}x ({int(volume):,} contracts vs {int(oi):,} OI)"
            )
            observation = draft_observation(signal_desc)
            alert_text = (
                f"{ticker} {cp} {strike}{expiry_short} — "
                f"vol/OI ratio: {ratio:.1f}x | {observation}"
            )
            alerts.append({
                "ticker": ticker,
                "contract": f"{strike}{cp[0]}{expiry_short}",
                "ratio": ratio,
                "volume": volume,
                "oi": oi,
                "alert_text": alert_text,
                "signal_data": {
                    "strike": strike,
                    "expiry": expiry,
                    "contract_type": cp,
                    "volume": volume,
                    "oi": oi,
                    "ratio": ratio,
                },
            })

    # Check total daily volume vs 20-day average
    avg_vol = _get_historical_volume(ticker)
    if avg_vol > 0 and total_vol > avg_vol * VOL_AVG_MULTIPLIER:
        signal_desc = (
            f"{ticker} total options volume today: {int(total_vol):,} contracts "
            f"vs {int(avg_vol):,} {VOL_AVG_DAYS}-day average ({total_vol/avg_vol:.1f}x)"
        )
        observation = draft_observation(signal_desc)
        alert_text = (
            f"{ticker} TOTAL VOL {total_vol/avg_vol:.1f}x avg — "
            f"{int(total_vol):,} vs {int(avg_vol):,} ({VOL_AVG_DAYS}d) | {observation}"
        )
        alerts.append({
            "ticker": ticker,
            "contract": "TOTAL",
            "ratio": total_vol / avg_vol,
            "volume": total_vol,
            "oi": avg_vol,
            "alert_text": alert_text,
            "signal_data": {
                "total_vol": total_vol,
                "avg_vol": avg_vol,
                "ratio": total_vol / avg_vol,
            },
        })

    return alerts
