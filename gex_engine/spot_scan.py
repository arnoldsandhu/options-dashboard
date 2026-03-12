"""
gex_engine.spot_scan -- fast spot-scan with pre-computed TTE per contract.
"""
from __future__ import annotations
from gex_engine.greek_calc import bs_gamma, time_to_expiry_years

_GRID_HALF_RANGE = 0.10
_GRID_STEP_PCT   = 0.005

def _build_grid(spot):
    lo, hi, step = spot*(1-_GRID_HALF_RANGE), spot*(1+_GRID_HALF_RANGE), spot*_GRID_STEP_PCT
    pts, s = [], lo
    while s <= hi + 1e-9:
        pts.append(round(s, 4))
        s += step
    return pts

def _precompute(contracts):
    """Pre-compute TTE once per contract. Filter invalid entries."""
    result = []
    for c in contracts:
        oi, cp, strike = c.get("oi", 0), c.get("option_type", ""), c.get("strike", 0.0)
        if oi == 0 or strike == 0.0 or cp not in ("call", "put"):
            continue
        iv, rate, expiry = c.get("iv", 0.0), c.get("rate", 0.05), c.get("expiration", "")
        mult = c.get("multiplier", 100)
        sign = 1.0 if cp == "call" else -1.0
        t = time_to_expiry_years(expiry)
        if t > 0 and iv > 0:
            result.append({"k": strike, "t": t, "iv": iv, "r": rate,
                           "oi": oi, "m": mult, "s": sign, "bs": True, "vg": 0.0})
        else:
            vg = float(c.get("vendor_gamma", 0.0) or 0.0)
            if vg != 0.0:
                result.append({"k": strike, "t": 0, "iv": 0, "r": rate,
                               "oi": oi, "m": mult, "s": sign, "bs": False, "vg": vg})
    return result

def _net(pre, hypo, mode):
    net = 0.0
    s2 = hypo * hypo * 0.01
    for p in pre:
        g = (bs_gamma(hypo, p["k"], p["t"], p["iv"], p["r"]) or 0.0) if p["bs"] else p["vg"]
        if g == 0.0:
            continue
        net += g * p["oi"] * p["m"] * (s2 if mode == "1pct" else hypo) * p["s"]
    return net

def run_spot_scan(contracts, spot, mode="1pct"):
    """Run spot-scan and find the true gamma flip point via zero-crossing."""
    grid = _build_grid(spot)
    pre  = _precompute(contracts)
    curve = [{"spot": h, "net_gex": round(_net(pre, h, mode), 0)} for h in grid]

    flip_point = None
    for i in range(1, len(curve)):
        g0, g1 = curve[i-1]["net_gex"], curve[i]["net_gex"]
        s0, s1 = curve[i-1]["spot"],    curve[i]["spot"]
        if g0 == 0.0:
            flip_point = s0
            break
        if (g0 > 0) != (g1 > 0):
            frac = abs(g0) / abs(g1 - g0) if abs(g1 - g0) > 1e-12 else 0.5
            flip_point = round(s0 + frac * (s1 - s0), 2)
            break

    cur = _net(pre, spot, mode)
    if flip_point is not None:
        ff, fn = True, f"Gamma Flip ${flip_point:,.2f} (spot-scan)"
    else:
        ff = False
        all_vals = [c["net_gex"] for c in curve]
        if all(v <= 0 for v in all_vals):
            fn = "No flip in range — all negative gamma"
        elif all(v >= 0 for v in all_vals):
            fn = "No flip in range — all positive gamma"
        else:
            lo_n, hi_n = curve[0]["net_gex"], curve[-1]["net_gex"]
            near = curve[0]["spot"] if abs(lo_n) < abs(hi_n) else curve[-1]["spot"]
            side = "low" if abs(lo_n) < abs(hi_n) else "high"
            fn = f"No flip in ±10% range — nearest: ${near:,.2f} ({side} boundary)"

    return {"flip_point": flip_point, "flip_found": ff, "flip_note": fn, "curve": curve,
            "grid_lo": grid[0], "grid_hi": grid[-1], "grid_step_pct": _GRID_STEP_PCT * 100,
            "current_net_gex": cur, "mode": mode}
