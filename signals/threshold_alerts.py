"""
Threshold alert checker — evaluates extreme conditions across all signal types
and returns a list of triggered alert dicts for logging and email delivery.
"""
from __future__ import annotations

# Thresholds
GEX_PCT_HIGH    = 95.0   # percentile above which GEX is "extreme positive"
GEX_PCT_LOW     =  5.0   # percentile below which GEX is "extreme negative"
OVI_SPIKE_PCT   = 500.0  # % above 20d avg to classify as extreme flow spike
SKEW_PCT_HIGH   = 90.0   # percentile above which skew is extreme


def check_thresholds(
    ovi_report: dict,
    latest_gex: list[dict],
    latest_skew: list[dict],
    prior_snap: dict | None,
) -> list[dict]:
    """
    Evaluate all threshold conditions.

    Returns list of dicts:
      {ticker, alert_type, detail, severity}

    Severity: CRITICAL | HIGH | MEDIUM
    """
    from db.database import get_gex_history, get_skew_history, GEX_HISTORY_DAYS

    alerts: list[dict] = []
    prior_gex = (prior_snap or {}).get("gex", {})

    # ── 1. GEX percentile extremes + gamma flip ─────────────────────────────
    for g in latest_gex:
        tk = g["ticker"]
        hist = get_gex_history(tk, days=GEX_HISTORY_DAYS)
        if len(hist) < 5:
            continue
        values = [h["net_gex"] for h in hist]
        below = sum(1 for v in values if v < g["net_gex"])
        pct = (below / len(values)) * 100.0
        net_gex_m = g["net_gex"] / 1_000_000

        if pct >= GEX_PCT_HIGH:
            alerts.append({
                "ticker": tk,
                "alert_type": "GEX_EXTREME_HIGH",
                "detail": (
                    f"GEX at {pct:.0f}th pctile (30d) — "
                    f"Net ${net_gex_m:+.1f}M, dealers long gamma — "
                    f"pinning regime near spot"
                ),
                "severity": "HIGH",
            })
        elif pct <= GEX_PCT_LOW:
            alerts.append({
                "ticker": tk,
                "alert_type": "GEX_EXTREME_LOW",
                "detail": (
                    f"GEX at {pct:.0f}th pctile (30d) — "
                    f"Net ${net_gex_m:+.1f}M, dealers short gamma — "
                    f"potential volatility amplification"
                ),
                "severity": "HIGH",
            })

        # Gamma flip: prior was opposite sign
        prior_entry = prior_gex.get(tk, {})
        prior_gex_m = prior_entry.get("net_gex_m")
        if prior_gex_m is not None:
            if prior_gex_m > 0 and net_gex_m < 0:
                alerts.append({
                    "ticker": tk,
                    "alert_type": "GAMMA_FLIP_NEG",
                    "detail": (
                        f"Gamma FLIPPED NEGATIVE — "
                        f"was ${prior_gex_m:+.1f}M, now ${net_gex_m:+.1f}M — "
                        f"dealer hedging regime shift, vol may expand"
                    ),
                    "severity": "CRITICAL",
                })
            elif prior_gex_m < 0 and net_gex_m > 0:
                alerts.append({
                    "ticker": tk,
                    "alert_type": "GAMMA_FLIP_POS",
                    "detail": (
                        f"Gamma FLIPPED POSITIVE — "
                        f"was ${prior_gex_m:+.1f}M, now ${net_gex_m:+.1f}M — "
                        f"dealers now long, expect mean-reversion dampening"
                    ),
                    "severity": "CRITICAL",
                })

    # ── 2. OVI spike > 500% ──────────────────────────────────────────────────
    for r in ovi_report.get("top_calls", []):
        pct = r.get("c_vol_pct")
        if pct is not None and pct >= OVI_SPIKE_PCT:
            spot = r.get("spot")
            spot_str = f" @ ${spot:,.0f}" if spot else ""
            alerts.append({
                "ticker": r["ticker"],
                "alert_type": "OVI_CALL_SPIKE",
                "detail": (
                    f"CALL volume +{pct:.0f}% vs 20d avg{spot_str} — "
                    f"extreme directional flow, potential informed positioning"
                ),
                "severity": "HIGH",
            })

    for r in ovi_report.get("top_puts", []):
        pct = r.get("p_vol_pct")
        if pct is not None and pct >= OVI_SPIKE_PCT:
            spot = r.get("spot")
            spot_str = f" @ ${spot:,.0f}" if spot else ""
            alerts.append({
                "ticker": r["ticker"],
                "alert_type": "OVI_PUT_SPIKE",
                "detail": (
                    f"PUT volume +{pct:.0f}% vs 20d avg{spot_str} — "
                    f"extreme directional flow, potential informed hedging"
                ),
                "severity": "HIGH",
            })

    # ── 3. Skew crosses 90th percentile ──────────────────────────────────────
    for r in latest_skew:
        tk = r["ticker"]
        hist = get_skew_history(tk, days=30)
        if len(hist) < 5:
            continue
        values = [h["skew_value"] for h in hist]
        below = sum(1 for v in values if v < r["skew_value"])
        pct = (below / len(values)) * 100.0
        if pct >= SKEW_PCT_HIGH:
            skew_vol = r["skew_value"] * 100
            alerts.append({
                "ticker": tk,
                "alert_type": "SKEW_EXTREME",
                "detail": (
                    f"25d Skew at {pct:.0f}th pctile (30d) — "
                    f"{skew_vol:+.1f}vol, extreme put premium — "
                    f"tail risk hedging elevated"
                ),
                "severity": "MEDIUM",
            })

    return alerts
