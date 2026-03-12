"""
gex_engine.exposure_calc
-------------------------
Clearly-labeled GEX exposure functions with explicit unit annotations.

All functions operate on a single contract's contribution.  Aggregate by
summing across contracts (see calc_net_gex / calc_chain_metrics below).

Sign convention (dealer-position model):
    calls → +sign  (dealer hedges by selling as spot rises)
    puts  → -sign  (dealer hedges by buying as spot falls)

Primary metric (default for dashboard):
    gex_1pct — notional exposure for a 1% spot move
"""
from __future__ import annotations

from datetime import date

from gex_engine.greek_calc import get_gamma


# ── Per-contract building blocks ──────────────────────────────────────────────

def hedge_shares_1usd(gamma: float, oi: int, multiplier: int, sign: float) -> float:
    """
    Shares the dealer must buy/sell per $1 spot move.

    Units: shares  (delta-neutral hedge size, per $1 move)
    Formula: gamma × OI × multiplier × sign
    """
    return gamma * oi * multiplier * sign


def hedge_notional_1usd(gamma: float, oi: int, multiplier: int,
                        spot: float, sign: float) -> float:
    """
    Dollar notional the dealer must transact per $1 spot move.

    Units: $ (notional dollars, per $1 move)
    Formula: gamma × OI × multiplier × spot × sign
    """
    return gamma * oi * multiplier * spot * sign


def gex_1pct(gamma: float, oi: int, multiplier: int,
             spot: float, sign: float) -> float:
    """
    GEX for a 1% spot move — PRIMARY METRIC.

    Units: $ (notional dollars, per 1% spot move)
    Formula: gamma × OI × multiplier × spot² × 0.01 × sign
    """
    return gamma * oi * multiplier * spot * spot * 0.01 * sign


def gex_1usd(gamma: float, oi: int, multiplier: int,
             spot: float, sign: float) -> float:
    """
    Alias for hedge_notional_1usd.  Use for '$1 move' display mode.
    Units: $ (notional dollars, per $1 move)
    """
    return hedge_notional_1usd(gamma, oi, multiplier, spot, sign)


# ── Sign helper ────────────────────────────────────────────────────────────────

def dealer_sign(option_type: str) -> float:
    """Classic sign: calls = +1, puts = -1."""
    return 1.0 if option_type == "call" else -1.0


# ── Chain-level aggregation ────────────────────────────────────────────────────

def calc_chain_metrics(
    contracts: list[dict],
    spot: float,
    mode: str = "1pct",
    today_str: str | None = None,
    sign_mode: str = "classic",
) -> dict:
    """
    Aggregate GEX metrics across an entire chain.

    Args:
        contracts:  list of normalized+model-enriched contract dicts
        spot:       current underlying price
        mode:       "1pct" (default) or "1usd"
        today_str:  ISO date string for 0DTE filter; defaults to today
        sign_mode:  "classic" (calls +1 puts -1) or "unsigned" (all +1)

    Returns dict with:
        net_gex         float  — signed sum (primary, in selected units)
        abs_gex         float  — sum of absolute values
        strike_gex      dict[float, float]  — |gex| per strike
        call_gex_by_s   dict[float, float]  — call gex per strike
        put_gex_by_s    dict[float, float]  — put gex per strike
        net_gex_0dte    float  — net GEX filtered to today's expiry (OI-based)
        flow_gex_0dte   float  — net GEX filtered to today (volume-weighted)
        abs_gex_0dte    float
        diagnostics     dict   — counts for transparency panel
    """
    if today_str is None:
        today_str = date.today().isoformat()

    calc_fn = gex_1pct if mode == "1pct" else gex_1usd

    net_gex       = 0.0
    abs_gex       = 0.0
    net_gex_0dte  = 0.0
    flow_gex_0dte = 0.0
    abs_gex_0dte  = 0.0

    strike_gex:    dict[float, float] = {}
    call_gex_by_s: dict[float, float] = {}
    put_gex_by_s:  dict[float, float] = {}

    n_included       = 0
    n_missing_iv     = 0
    n_zero_oi        = 0
    n_bs_computed    = 0   # contracts where Black-Scholes gamma was used
    n_vendor_fallback = 0  # contracts where vendor gamma was the fallback

    for c in contracts:
        oi      = c.get("oi", 0)
        vol     = c.get("volume", 0)
        cp      = c.get("option_type", "")
        strike  = c.get("strike", 0.0)
        expiry  = c.get("expiration", "")
        mult    = c.get("multiplier", 100)
        iv      = c.get("iv", 0.0)

        if oi == 0:
            n_zero_oi += 1
            continue
        if strike == 0.0 or cp not in ("call", "put"):
            continue

        if iv == 0.0:
            n_missing_iv += 1

        gamma = get_gamma(c, spot)
        if gamma == 0.0:
            continue

        # Track gamma source: BS when IV available and t > 0, else vendor
        from gex_engine.greek_calc import time_to_expiry_years
        if iv > 0 and time_to_expiry_years(c.get("expiration", "")) > 0:
            n_bs_computed += 1
        else:
            n_vendor_fallback += 1

        if sign_mode == "unsigned":
            sign = 1.0
        elif sign_mode == "heuristic":
            from gex_engine.sign_engine import get_sign as _gs
            _raw, _conf = _gs(c, spot, "heuristic")
            sign = _raw * (_conf / 100.0)
        elif sign_mode == "probabilistic":
            from gex_engine.sign_engine import get_p_dealer_short as _gp
            p = _gp(c, spot)
            sign = 2.0 * p - 1.0
        else:  # "classic" or "customer_long"
            sign = dealer_sign(cp)

        contrib = calc_fn(gamma, oi, mult, spot, sign)
        abs_c   = abs(contrib)

        net_gex += contrib
        abs_gex += abs_c
        n_included += 1

        strike_gex[strike]    = strike_gex.get(strike, 0.0) + abs_c
        if cp == "call":
            call_gex_by_s[strike] = call_gex_by_s.get(strike, 0.0) + contrib
        else:
            put_gex_by_s[strike]  = put_gex_by_s.get(strike, 0.0) + contrib

        # 0DTE (OI-based)
        if expiry == today_str:
            net_gex_0dte += contrib
            abs_gex_0dte += abs_c

            # Flow GEX: same formula but weighted by volume instead of OI
            if vol and vol > 0:
                flow_contrib   = calc_fn(gamma, vol, mult, spot, sign)
                flow_gex_0dte += flow_contrib

    return {
        "net_gex":        net_gex,
        "abs_gex":        abs_gex,
        "strike_gex":     strike_gex,
        "call_gex_by_s":  call_gex_by_s,
        "put_gex_by_s":   put_gex_by_s,
        "net_gex_0dte":   net_gex_0dte,
        "flow_gex_0dte":  flow_gex_0dte,
        "abs_gex_0dte":   abs_gex_0dte,
        "mode":           mode,
        "diagnostics": {
            "included":         n_included,
            "zero_oi":          n_zero_oi,
            "missing_iv":       n_missing_iv,
            "bs_computed":      n_bs_computed,
            "vendor_fallback":  n_vendor_fallback,
            "total":            n_included + n_zero_oi,
        },
    }


def calc_probabilistic_range(
    contracts: list[dict],
    spot: float,
    mode: str = "1pct",
    today_str: str | None = None,
) -> dict:
    """
    Compute GEX under three probabilistic dealer-short scenarios.

    Returns:
        net_gex_base   — per-contract heuristic p_dealer_short
        net_gex_low    — conservative: all contracts at p = 0.40 (expected_sign = -0.20)
        net_gex_high   — aggressive:   all contracts at p = 0.95 (expected_sign = +0.90)
        uncertainty_width — |net_gex_high - net_gex_low|
        strike_gex_base   — per-strike |gex| dict for the base scenario
        call_gex_by_s_base, put_gex_by_s_base — for bar chart rendering
    """
    from gex_engine.sign_engine import get_p_dealer_short

    if today_str is None:
        today_str = date.today().isoformat()

    calc_fn = gex_1pct if mode == "1pct" else gex_1usd

    P_LOW  = 0.40
    P_HIGH = 0.95
    S_LOW  = 2.0 * P_LOW  - 1.0   # -0.20
    S_HIGH = 2.0 * P_HIGH - 1.0   # +0.90

    net_base = net_low = net_high = 0.0
    strike_gex_base:    dict[float, float] = {}
    call_gex_by_s_base: dict[float, float] = {}
    put_gex_by_s_base:  dict[float, float] = {}

    for c in contracts:
        oi     = c.get("oi", 0)
        cp     = c.get("option_type", "")
        strike = c.get("strike", 0.0)
        mult   = c.get("multiplier", 100)

        if oi == 0 or strike == 0.0 or cp not in ("call", "put"):
            continue

        gamma = get_gamma(c, spot)
        if gamma == 0.0:
            continue

        p_base   = get_p_dealer_short(c, spot)
        s_base   = 2.0 * p_base - 1.0

        contrib_base = calc_fn(gamma, oi, mult, spot, s_base)
        contrib_low  = calc_fn(gamma, oi, mult, spot, S_LOW)
        contrib_high = calc_fn(gamma, oi, mult, spot, S_HIGH)

        net_base += contrib_base
        net_low  += contrib_low
        net_high += contrib_high

        abs_b = abs(contrib_base)
        strike_gex_base[strike] = strike_gex_base.get(strike, 0.0) + abs_b
        if cp == "call":
            call_gex_by_s_base[strike] = call_gex_by_s_base.get(strike, 0.0) + contrib_base
        else:
            put_gex_by_s_base[strike]  = put_gex_by_s_base.get(strike, 0.0) + contrib_base

    return {
        "net_gex_base":        net_base,
        "net_gex_low":         net_low,
        "net_gex_high":        net_high,
        "uncertainty_width":   abs(net_high - net_low),
        "strike_gex_base":     strike_gex_base,
        "call_gex_by_s_base":  call_gex_by_s_base,
        "put_gex_by_s_base":   put_gex_by_s_base,
    }
