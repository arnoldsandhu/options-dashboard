"""Signal 2: GEX (Gamma Exposure) Extreme"""
from datetime import date

from config import (
    POLYGON_API_KEY,
    GEX_HISTORY_DAYS,
    GEX_PERCENTILE_EXTREME,
    GEX_SINGLE_STRIKE_PCT,
    GEX_MIN_ABS_M,
)
from db.database import save_gex, get_gex_history
from gex_engine.vendor_ingest import fetch_chain
from gex_engine.greek_calc import get_gamma, reset_fallback_counter
from gex_engine.contract_model import normalize_chain


def _compute_gex(contracts: list[dict], spot: float) -> tuple[float, dict[float, float]]:
    """
    Compute net GEX and per-strike absolute GEX from normalized contract dicts.

    Uses get_gamma() which prioritises Black-Scholes (IV-based) over vendor gamma.
    Formula: gamma × OI × multiplier × spot  (per $1 move, signed by dealer convention)
    """
    strike_gex: dict[float, float] = {}
    net_gex = 0.0

    for c in contracts:
        oi     = c.get("oi", 0)
        cp     = c.get("option_type", "")
        strike = c.get("strike", 0.0)
        mult   = c.get("multiplier", 100)

        if oi == 0 or strike == 0.0:
            continue

        gamma = get_gamma(c, spot)
        if gamma == 0.0:
            continue

        gex = gamma * oi * mult * spot
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
    Returns alert dicts for GEX extreme signals.
    Each dict has 'alert_text' (base, no Claude) and 'claude_prompt'.
    """
    reset_fallback_counter()

    try:
        spot, raw_contracts = fetch_chain(ticker)
    except Exception as e:
        print(f"  [gex] Error fetching chain for {ticker}: {e}")
        return []

    if spot == 0.0:
        print(f"  [gex] No spot price for {ticker}, skipping GEX.")
        return []

    contracts = normalize_chain(raw_contracts, ticker)
    net_gex, strike_gex = _compute_gex(contracts, spot)

    save_gex(ticker, net_gex, spot)

    # Compute and store IV metrics using already-fetched chain (no extra API calls)
    try:
        from signals.iv_metrics import compute_and_store_iv_metrics
        compute_and_store_iv_metrics(ticker, spot, raw_contracts)
    except Exception as _iv_err:
        print(f"  [gex] IV metrics error {ticker}: {_iv_err}")

    history = get_gex_history(ticker, GEX_HISTORY_DAYS)
    prev_row = history[-2] if len(history) >= 2 else None
    gex_values = [r["net_gex"] for r in history]
    pct_rank = _percentile_rank(net_gex, gex_values[:-1]) if len(gex_values) > 1 else 50.0
    net_gex_m = net_gex / 1_000_000

    # Skip alerting for near-zero GEX (noise from tickers with sparse options activity)
    if abs(net_gex_m) < GEX_MIN_ABS_M:
        return []

    alerts = []

    def _make_alert(reason: str, signal_data: dict) -> dict:
        claude_prompt = (
            f"{ticker} GEX {reason}: Net GEX ${net_gex_m:.1f}M "
            f"({pct_rank:.0f}th percentile, {GEX_HISTORY_DAYS}d)"
        )
        alert_text = (
            f"{ticker} GEX EXTREME — Net GEX: ${net_gex_m:.1f}M "
            f"({pct_rank:.0f}th pct, {GEX_HISTORY_DAYS}d)"
        )
        return {
            "ticker": ticker,
            "net_gex": net_gex,
            "net_gex_m": net_gex_m,
            "pct_rank": pct_rank,
            "alert_text": alert_text,
            "claude_prompt": claude_prompt,
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
                        f"{pct_conc:.0f}% of total GEX (${sgex/1e6:.1f}M)"
                    ),
                    "claude_prompt": (
                        f"{ticker}: Strike ${strike} accounts for {pct_conc:.0f}% of total "
                        f"GEX (${sgex/1e6:.1f}M) — potential gamma pin at {strike}"
                    ),
                    "signal_data": {
                        "reason": "strike_concentration",
                        "strike": strike,
                        "pct": pct_conc,
                        "strike_gex": sgex,
                    },
                })

    return alerts
