"""Signal 2: GEX (Gamma Exposure) Extreme"""
import statistics
from datetime import date

from polygon import RESTClient

from config import (
    POLYGON_API_KEY,
    GEX_HISTORY_DAYS,
    GEX_PERCENTILE_EXTREME,
    GEX_SINGLE_STRIKE_PCT,
)
from claude_draft import draft_observation
from db.database import save_gex, get_gex_history, get_latest_gex

_client: RESTClient | None = None


def _get_client() -> RESTClient:
    global _client
    if _client is None:
        _client = RESTClient(api_key=POLYGON_API_KEY)
    return _client


def _fetch_spot(ticker: str) -> float:
    client = _get_client()
    try:
        snap = client.get_snapshot_all("stocks", tickers=[ticker])
        if snap and len(snap) > 0:
            return snap[0].day.close or snap[0].prev_day.close or 0.0
    except Exception:
        pass
    try:
        aggs = list(client.get_aggs(ticker, 1, "day",
                                    date.today().strftime("%Y-%m-%d"),
                                    date.today().strftime("%Y-%m-%d")))
        if aggs:
            return aggs[0].close or 0.0
    except Exception as e:
        print(f"  [gex] Could not fetch spot for {ticker}: {e}")
    return 0.0


def _compute_gex(contracts: list, spot: float) -> tuple[float, dict[float, float]]:
    """
    Returns (net_gex_dollars, strike_gex_map).
    net_gex = sum(call_gex) - sum(put_gex) across all contracts.
    GEX per contract = gamma × OI × 100 × spot_price
    Calls add positive GEX, puts add negative GEX.
    """
    strike_gex: dict[float, float] = {}
    net_gex = 0.0

    for c in contracts:
        details = getattr(c, "details", None)
        greeks = getattr(c, "greeks", None)
        oi = getattr(c, "open_interest", 0) or 0
        gamma = (greeks.gamma if greeks and greeks.gamma else 0.0) or 0.0
        cp = (getattr(details, "contract_type", "").lower() if details else "")
        strike = (getattr(details, "strike_price", 0.0) if details else 0.0) or 0.0

        if gamma == 0.0 or oi == 0:
            continue

        gex = gamma * oi * 100 * spot
        if cp == "call":
            net_gex += gex
        elif cp == "put":
            net_gex -= gex

        strike_gex[strike] = strike_gex.get(strike, 0.0) + abs(gex)

    return net_gex, strike_gex


def _percentile_rank(value: float, history: list[float]) -> float:
    if not history:
        return 50.0
    below = sum(1 for v in history if v < value)
    return (below / len(history)) * 100.0


def check_gex(ticker: str) -> list[dict]:
    """
    Returns a list of alert dicts for GEX extreme signals on ticker.
    """
    client = _get_client()
    contracts = []
    try:
        for contract in client.list_snapshot_options_chain(
            ticker, params={"limit": 250}
        ):
            contracts.append(contract)
    except Exception as e:
        print(f"  [gex] Error fetching chain for {ticker}: {e}")
        return []

    spot = _fetch_spot(ticker)
    if spot == 0.0:
        print(f"  [gex] No spot price for {ticker}, skipping GEX.")
        return []

    net_gex, strike_gex = _compute_gex(contracts, spot)

    # Save to DB
    save_gex(ticker, net_gex, spot)

    history = get_gex_history(ticker, GEX_HISTORY_DAYS)
    prev_row = None
    if len(history) >= 2:
        prev_row = history[-2]  # second-to-last (current is already saved)

    gex_values = [r["net_gex"] for r in history]
    pct_rank = _percentile_rank(net_gex, gex_values[:-1]) if len(gex_values) > 1 else 50.0

    alerts = []
    net_gex_m = net_gex / 1_000_000  # in millions

    def _make_alert(reason: str, signal_data: dict) -> dict:
        signal_desc = (
            f"{ticker} GEX {reason}: Net GEX ${net_gex_m:.1f}M "
            f"({pct_rank:.0f}th percentile, {GEX_HISTORY_DAYS}d)"
        )
        observation = draft_observation(signal_desc)
        alert_text = (
            f"{ticker} GEX EXTREME — Net GEX: ${net_gex_m:.1f}M "
            f"({pct_rank:.0f}th pct, {GEX_HISTORY_DAYS}d) | {observation}"
        )
        return {
            "ticker": ticker,
            "net_gex": net_gex,
            "net_gex_m": net_gex_m,
            "pct_rank": pct_rank,
            "alert_text": alert_text,
            "signal_data": signal_data,
        }

    # (a) Zero cross
    if prev_row is not None:
        prev_gex = prev_row["net_gex"]
        if (prev_gex > 0 and net_gex < 0) or (prev_gex < 0 and net_gex > 0):
            direction = "flipped positive" if net_gex > 0 else "flipped negative"
            alerts.append(_make_alert(
                f"zero-cross ({direction})",
                {"reason": "zero_cross", "prev_gex": prev_gex, "net_gex": net_gex},
            ))

    # (b) Percentile extreme
    if len(gex_values) >= 10:
        if pct_rank <= GEX_PERCENTILE_EXTREME or pct_rank >= (100 - GEX_PERCENTILE_EXTREME):
            label = "EXTREME LOW" if pct_rank <= GEX_PERCENTILE_EXTREME else "EXTREME HIGH"
            alerts.append(_make_alert(
                label,
                {"reason": "percentile_extreme", "pct_rank": pct_rank, "net_gex": net_gex},
            ))

    # (c) Single-strike concentration
    total_abs_gex = sum(strike_gex.values())
    if total_abs_gex > 0:
        for strike, sgex in strike_gex.items():
            if sgex / total_abs_gex >= GEX_SINGLE_STRIKE_PCT:
                pct_conc = sgex / total_abs_gex * 100
                alerts.append({
                    "ticker": ticker,
                    "net_gex": net_gex,
                    "net_gex_m": net_gex_m,
                    "pct_rank": pct_rank,
                    "alert_text": (
                        f"{ticker} GEX CONCENTRATION — Strike ${strike} has "
                        f"{pct_conc:.0f}% of total GEX (${sgex/1e6:.1f}M) | "
                        f"[gamma pin risk at {strike}]"
                    ),
                    "signal_data": {
                        "reason": "strike_concentration",
                        "strike": strike,
                        "pct": pct_conc,
                        "strike_gex": sgex,
                    },
                })

    return alerts
