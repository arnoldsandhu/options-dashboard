"""
gex_engine.spot_scan
---------------------
True spot-scan gamma flip point.

Builds a spot grid from spot×0.90 → spot×1.10 in 0.5% increments (~40 points).
For each hypothetical spot, recalculates gamma for every contract using Black-Scholes
at that spot level, then computes total net GEX.  The zero-crossing of the net GEX
curve is the TRUE gamma flip (vs the legacy strike-level approximation).

Also returns the full GEX-vs-spot curve for the JPM-style "gamma imbalance profile" chart.
"""
from __future__ import annotations

import math

from gex_engine.greek_calc import bs_gamma, time_to_expiry_years


# Grid: ±10% around spot in 0.5% steps
_GRID_HALF_RANGE = 0.10
_GRID_STEP_PCT   = 0.005   # 0.5%


def _build_grid(spot: float) -> list[float]:
    lo   = spot * (1 - _GRID_HALF_RANGE)
    hi   = spot * (1 + _GRID_HALF_RANGE)
    step = spot * _GRID_STEP_PCT
    pts  = []
    s = lo
    while s <= hi + 1e-9:
        pts.append(round(s, 4))
        s += step
    return pts


def _net_gex_at_spot(contracts: list[dict], hypo_spot: float, mode: str = "1pct") -> float:
    """Net GEX across the entire chain if spot were at hypo_spot."""
    net = 0.0
    for c in contracts:
        oi      = c.get("oi", 0)
        cp      = c.get("option_type", "")
        strike  = c.get("strike", 0.0)
        expiry  = c.get("expiration", "")
        mult    = c.get("multiplier", 100)
        iv      = c.get("iv", 0.0)
        rate    = c.get("rate", 0.05)

        if oi == 0 or strike == 0.0 or cp not in ("call", "put"):
            continue

        t = time_to_expiry_years(expiry)
        if t <= 0 or iv <= 0:
            gamma = c.get("vendor_gamma", 0.0) or 0.0
        else:
            gamma = bs_gamma(hypo_spot, strike, t, iv, rate) or 0.0

        if gamma == 0.0:
            continue

        sign = 1.0 if cp == "call" else -1.0
        if mode == "1pct":
            contrib = gamma * oi * mult * hypo_spot * hypo_spot * 0.01 * sign
        else:
            contrib = gamma * oi * mult * hypo_spot * sign

        net += contrib
    return net


def run_spot_scan(
    contracts: list[dict],
    spot: float,
    mode: str = "1pct",
) -> dict:
    """
    Run a full spot-scan and find the true gamma flip point.

    Args:
        contracts:  list of normalized+model-enriched contract dicts
        spot:       current spot price
        mode:       "1pct" (default) or "1usd"

    Returns dict with:
        flip_point      float | None   — spot level where net GEX crosses zero
        flip_found      bool
        flip_note       str            — human-readable description
        nearest_boundary float | None  — nearest grid edge if flip not found
        curve           list[dict]     — [{spot, net_gex}, ...] for chart
        grid_lo         float
        grid_hi         float
        current_net_gex float          — net GEX at current spot
    """
    grid = _build_grid(spot)
    curve: list[dict] = []

    for hypo in grid:
        net = _net_gex_at_spot(contracts, hypo, mode)
        curve.append({"spot": hypo, "net_gex": net})

    # Find zero-crossing
    flip_point = None
    for i in range(1, len(curve)):
        g0 = curve[i - 1]["net_gex"]
        g1 = curve[i]["net_gex"]
        s0 = curve[i - 1]["spot"]
        s1 = curve[i]["spot"]

        if g0 == 0.0:
            flip_point = s0
            break
        if (g0 > 0) != (g1 > 0):
            # Linear interpolation
            frac = abs(g0) / abs(g1 - g0) if abs(g1 - g0) > 1e-12 else 0.5
            flip_point = round(s0 + frac * (s1 - s0), 2)
            break

    # Current net GEX at actual spot
    current_net = _net_gex_at_spot(contracts, spot, mode)

    if flip_point is not None:
        flip_found = True
        flip_note  = f"Gamma Flip ${flip_point:,.2f} (spot-scan)"
    else:
        flip_found = False
        # Find which boundary net GEX is closer to zero
        lo_net = curve[0]["net_gex"]
        hi_net = curve[-1]["net_gex"]
        if abs(lo_net) < abs(hi_net):
            nearest = curve[0]["spot"]
            flip_note = f"Flip not found in ±10% range — nearest: ${nearest:,.2f} (low boundary)"
        else:
            nearest = curve[-1]["spot"]
            flip_note = f"Flip not found in ±10% range — nearest: ${nearest:,.2f} (high boundary)"
        flip_point = None

    return {
        "flip_point":       flip_point,
        "flip_found":       flip_found,
        "flip_note":        flip_note,
        "curve":            curve,
        "grid_lo":          grid[0],
        "grid_hi":          grid[-1],
        "grid_step_pct":    _GRID_STEP_PCT * 100,
        "current_net_gex":  current_net,
        "mode":             mode,
    }
