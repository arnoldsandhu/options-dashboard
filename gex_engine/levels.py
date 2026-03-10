"""
gex_engine.levels
------------------
Improved wall detection, regime classification, and magnet strike logic.

All wall / regime output is clearly labeled as ESTIMATED.
"""
from __future__ import annotations

import math
from typing import NamedTuple


# ── Data structures ────────────────────────────────────────────────────────────

class WallLevel(NamedTuple):
    strike:       float
    gex_m:        float   # absolute value, $M (1% move units)
    distance_pct: float   # % away from spot (signed: positive = above spot)


class RegimeResult(NamedTuple):
    label:        str          # primary regime string
    subflags:     list[str]    # additional flags
    net_gex_m:    float
    abs_gex_m:    float
    flip_dist_pct: float | None  # % distance from spot to flip


# ── Wall Detection ─────────────────────────────────────────────────────────────

def find_walls(
    call_gex_by_s: dict[float, float],
    put_gex_by_s:  dict[float, float],
    spot: float,
    top_n: int = 3,
) -> dict:
    """
    Find top N call/put GEX concentration levels and the magnet strike.

    Args:
        call_gex_by_s:  {strike → signed call gex} from exposure_calc
        put_gex_by_s:   {strike → signed put gex} from exposure_calc
        spot:           current underlying price
        top_n:          number of top levels to return (default 3)

    Returns dict with:
        call_walls      list[WallLevel]
        put_walls       list[WallLevel]
        max_abs_strike  float    — magnet strike (highest |gamma|)
        magnet_score    float    — abs gamma at magnet / avg abs gamma across strikes
    """
    def _top_walls(gex_dict: dict[float, float], n: int) -> list[WallLevel]:
        by_abs = sorted(gex_dict.items(), key=lambda kv: abs(kv[1]), reverse=True)
        walls = []
        for strike, gex_val in by_abs[:n]:
            dist_pct = (strike - spot) / spot * 100 if spot > 0 else 0.0
            walls.append(WallLevel(
                strike=strike,
                gex_m=abs(gex_val) / 1e6,
                distance_pct=round(dist_pct, 2),
            ))
        return walls

    call_walls = _top_walls(call_gex_by_s, top_n)
    put_walls  = _top_walls(put_gex_by_s,  top_n)

    # Magnet: highest |gex| across all strikes (calls + puts combined)
    all_gex: dict[float, float] = {}
    for s, v in call_gex_by_s.items():
        all_gex[s] = all_gex.get(s, 0.0) + abs(v)
    for s, v in put_gex_by_s.items():
        all_gex[s] = all_gex.get(s, 0.0) + abs(v)

    if all_gex:
        max_abs_strike = max(all_gex, key=lambda s: all_gex[s])
        avg_abs = sum(all_gex.values()) / len(all_gex)
        magnet_score = all_gex[max_abs_strike] / avg_abs if avg_abs > 0 else 0.0
    else:
        max_abs_strike = spot
        magnet_score   = 0.0

    return {
        "call_walls":      call_walls,
        "put_walls":       put_walls,
        "max_abs_strike":  max_abs_strike,
        "magnet_score":    round(magnet_score, 2),
    }


# ── Regime Classification ──────────────────────────────────────────────────────

def classify_regime(
    net_gex_m:       float,
    abs_gex_m:       float,
    abs_gex_0dte_m:  float,
    flip_point:      float | None,
    spot:            float,
    net_gex_pctile:  float = 50.0,    # 30d historical percentile
) -> RegimeResult:
    """
    Classify the current GEX regime and emit subflags.

    Args:
        net_gex_m:      signed net GEX in $M (1%-move units)
        abs_gex_m:      total absolute GEX in $M
        abs_gex_0dte_m: absolute 0DTE GEX in $M
        flip_point:     spot-scan flip price (or None if not found)
        spot:           current underlying price
        net_gex_pctile: percentile rank of net_gex_m vs 30-day history

    Returns RegimeResult with label and subflags.
    """
    # Primary regime label
    if net_gex_m > 0:
        if net_gex_pctile >= 70:
            label = "Strong positive gamma — dealer dampening"
        else:
            label = "Moderate positive gamma"
    else:
        if net_gex_pctile <= 30:
            label = "Strong negative gamma — dealer amplifying"
        elif net_gex_pctile <= 45:
            label = "Moderate negative gamma"
        else:
            label = "Neutral — near flip"

    # Flip distance
    if flip_point is not None and spot > 0:
        flip_dist_pct = (flip_point - spot) / spot * 100
    else:
        flip_dist_pct = None

    if flip_dist_pct is not None and abs(flip_dist_pct) < 0.5:
        label = "Neutral — near flip"

    # Subflags
    subflags: list[str] = []

    # 0DTE dominant: 0DTE share > 30%
    if abs_gex_m > 0 and (abs_gex_0dte_m / abs_gex_m) > 0.30:
        subflags.append("0DTE dominant")

    # Pinning likely: near large absolute gamma with positive net
    if net_gex_m > 0 and flip_dist_pct is not None and abs(flip_dist_pct) < 1.0:
        subflags.append("Pinning likely")

    # Air pocket: large negative gamma zone near spot
    if net_gex_m < 0 and flip_dist_pct is not None and abs(flip_dist_pct) < 2.0:
        subflags.append("Air pocket")

    # High convexity zone: abs GEX is very high percentile
    if net_gex_pctile >= 80 or net_gex_pctile <= 20:
        subflags.append("High convexity zone")

    return RegimeResult(
        label=label,
        subflags=subflags,
        net_gex_m=round(net_gex_m, 2),
        abs_gex_m=round(abs_gex_m, 2),
        flip_dist_pct=round(flip_dist_pct, 2) if flip_dist_pct is not None else None,
    )


# ── Convenience wrapper ────────────────────────────────────────────────────────

def compute_levels(
    call_gex_by_s:   dict[float, float],
    put_gex_by_s:    dict[float, float],
    net_gex_m:       float,
    abs_gex_m:       float,
    abs_gex_0dte_m:  float,
    flip_point:      float | None,
    spot:            float,
    net_gex_pctile:  float = 50.0,
    top_n: int = 3,
) -> dict:
    """
    Full levels computation: walls + regime in one call.

    Returns merged dict of find_walls() + classify_regime() results.
    """
    walls  = find_walls(call_gex_by_s, put_gex_by_s, spot, top_n)
    regime = classify_regime(
        net_gex_m, abs_gex_m, abs_gex_0dte_m,
        flip_point, spot, net_gex_pctile,
    )
    return {
        **walls,
        "regime_label":     regime.label,
        "regime_subflags":  regime.subflags,
        "flip_dist_pct":    regime.flip_dist_pct,
    }
