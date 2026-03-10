"""
Shared synthetic options chain fixture for GEX engine tests.

Design: 20 contracts on a fictional underlying at spot=500.0.
All 30-DTE contracts use IV so Black-Scholes gamma is applicable.

Fixture is intentionally structured so we can compute expected
net_gex, abs_gex, and 0DTE metrics analytically.

Structure
---------
Contracts 1-8  : 30-DTE calls + puts with IV — BS gamma used
Contracts 9-10 : 0DTE calls + puts with IV   — 0DTE filter tested
Contracts 11-13: No IV — vendor_gamma fallback used
Contracts 14-15: OI=0  — must be excluded from all calculations
Contract  16   : Invalid (zero strike) — excluded
Contracts 17-20: Additional strikes at wider range for wall ranking
"""
from __future__ import annotations

import math
from datetime import date, timedelta

from gex_engine.greek_calc import time_to_expiry_years as _tte


SPOT        = 500.0
RATE        = 0.05
MULTIPLIER  = 100
T_30        = 30 / 365.0          # 30 calendar days


def _expiry(offset_days: int) -> str:
    return (date.today() + timedelta(days=offset_days)).isoformat()


TODAY_STR = date.today().isoformat()
EXP_30D   = _expiry(30)
EXP_0DTE  = TODAY_STR


# ── Pure BS gamma helper (mirrors greek_calc.bs_gamma exactly) ─────────────

def _bs_gamma(spot: float, strike: float, t: float, iv: float, rate: float = 0.05) -> float | None:
    if spot <= 0 or strike <= 0 or t <= 0 or iv <= 0:
        return None
    try:
        d1 = (math.log(spot / strike) + (rate + 0.5 * iv ** 2) * t) / (iv * math.sqrt(t))
        npdf = math.exp(-0.5 * d1 * d1) / math.sqrt(2.0 * math.pi)
        gamma = npdf / (spot * iv * math.sqrt(t))
        return gamma if math.isfinite(gamma) else None
    except (ValueError, ZeroDivisionError):
        return None


def _gex_1pct(gamma: float, oi: int, mult: int, spot: float, sign: float) -> float:
    return gamma * oi * mult * spot * spot * 0.01 * sign


# ── Contract factory ───────────────────────────────────────────────────────────

def _c(option_type, strike, oi, iv, expiry, vendor_gamma=0.0) -> dict:
    return {
        "ticker":       "TEST",
        "option_type":  option_type,
        "strike":       float(strike),
        "oi":           oi,
        "volume":       max(1, oi // 10),
        "iv":           iv,
        "vendor_gamma": vendor_gamma,
        "multiplier":   MULTIPLIER,
        "expiration":   expiry,
        "rate":         RATE,
    }


# ── The 20-contract chain ─────────────────────────────────────────────────────

CHAIN: list[dict] = [
    # --- 30-DTE BS contracts (will use BS gamma) ---
    # id  type     strike  oi     iv
    _c("call",  490,  1200, 0.20, EXP_30D),   #  0 — slightly ITM call
    _c("call",  500,  3000, 0.20, EXP_30D),   #  1 — ATM call (highest gamma)
    _c("call",  510,  2000, 0.20, EXP_30D),   #  2 — OTM call
    _c("call",  520,   800, 0.21, EXP_30D),   #  3 — further OTM call
    _c("put",   510,  1000, 0.22, EXP_30D),   #  4 — slightly ITM put
    _c("put",   500,  2500, 0.22, EXP_30D),   #  5 — ATM put
    _c("put",   490,  1800, 0.23, EXP_30D),   #  6 — OTM put
    _c("put",   480,   700, 0.25, EXP_30D),   #  7 — further OTM put

    # --- 0DTE contracts (expiry = today) ---
    _c("call",  500,   400, 0.30, EXP_0DTE),  #  8 — 0DTE ATM call
    _c("put",   495,   350, 0.35, EXP_0DTE),  #  9 — 0DTE OTM put

    # --- Vendor-gamma-only contracts (no IV) ---
    _c("call",  550,   100, 0.0,  EXP_30D, vendor_gamma=0.0008),  # 10
    _c("put",   450,   100, 0.0,  EXP_30D, vendor_gamma=0.0012),  # 11
    _c("call",  600,    50, 0.0,  EXP_30D, vendor_gamma=0.0003),  # 12

    # --- Zero-OI contracts (must be excluded) ---
    _c("call",  505,     0, 0.20, EXP_30D),   # 13 — zero OI
    _c("put",   498,     0, 0.22, EXP_30D),   # 14 — zero OI

    # --- Invalid contract (zero strike) ---
    _c("call",    0,   100, 0.20, EXP_30D),   # 15 — excluded by strike=0 check

    # --- Wide-range contracts for wall ranking ---
    _c("call",  530,   600, 0.22, EXP_30D),   # 16
    _c("call",  540,   400, 0.23, EXP_30D),   # 17
    _c("put",   470,   500, 0.27, EXP_30D),   # 18
    _c("put",   460,   300, 0.30, EXP_30D),   # 19
]


# ── Pre-computed expected values ──────────────────────────────────────────────
# These are derived analytically so tests can assert against them independently.
# Re-derive via compute_expected() if SPOT / RATE / fixture changes.

def _sign(option_type: str) -> float:
    return 1.0 if option_type == "call" else -1.0


def compute_expected(chain: list[dict] = CHAIN, spot: float = SPOT) -> dict:
    """
    Compute ground-truth GEX metrics from the fixture using only standard math.
    This mirrors the calc_chain_metrics logic but is implemented independently.
    """
    net_gex        = 0.0
    abs_gex        = 0.0
    net_gex_0dte   = 0.0
    abs_gex_0dte   = 0.0
    n_included     = 0
    n_zero_oi      = 0
    n_vendor       = 0
    n_bs           = 0

    for c in chain:
        oi      = c.get("oi", 0)
        iv      = c.get("iv", 0.0)
        cp      = c.get("option_type", "")
        strike  = c.get("strike", 0.0)
        expiry  = c.get("expiration", "")
        mult    = c.get("multiplier", 100)
        vg      = c.get("vendor_gamma", 0.0)

        if oi == 0:
            n_zero_oi += 1
            continue
        if strike == 0.0 or cp not in ("call", "put"):
            continue

        # Determine gamma — use the same TTE function as calc_chain_metrics
        t = _tte(expiry) if expiry else 0.0
        gamma = None
        if iv > 0 and t > 0:
            gamma = _bs_gamma(spot, strike, t, iv, RATE)
            if gamma is not None:
                n_bs += 1
        if gamma is None and vg != 0.0:
            gamma = float(vg)
            n_vendor += 1
        if gamma is None or gamma == 0.0:
            continue

        sign    = _sign(cp)
        contrib = _gex_1pct(gamma, oi, mult, spot, sign)
        abs_c   = abs(contrib)

        net_gex += contrib
        abs_gex += abs_c
        n_included += 1

        if expiry == TODAY_STR:
            net_gex_0dte += contrib
            abs_gex_0dte += abs_c

    return {
        "net_gex":       net_gex,
        "abs_gex":       abs_gex,
        "net_gex_0dte":  net_gex_0dte,
        "abs_gex_0dte":  abs_gex_0dte,
        "n_included":    n_included,
        "n_zero_oi":     n_zero_oi,
        "n_vendor":      n_vendor,
        "n_bs":          n_bs,
    }


EXPECTED = compute_expected()
