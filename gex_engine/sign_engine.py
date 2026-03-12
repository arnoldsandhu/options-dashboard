"""
gex_engine.sign_engine
-----------------------
Five configurable sign models for dealer GEX calculations.

IMPORTANT: Dealer positioning is NEVER known from public options data.
All signed metrics produced here are ESTIMATES under assumptions.
Labels are always marked "estimated" in the UI.

Mode 1  "classic"          — calls +1, puts -1
Mode 2  "customer_long"    — same signs, explicit note & confidence=80
Mode 3  "heuristic"        — moneyness/TTE/volume pattern scoring (0–100 confidence)
Mode 4  "unsigned"         — |gamma| only, no directional sign
Mode 5  "probabilistic"    — continuous expected_sign = 2*p_dealer_short - 1,
                             with uncertainty band [p=0.4 floor, p=0.95 ceiling]
"""
from __future__ import annotations

import math
from datetime import date


# ── Public API ─────────────────────────────────────────────────────────────────

MODES = ("classic", "customer_long", "heuristic", "unsigned", "probabilistic")


def get_sign(contract: dict, spot: float, mode: str = "classic") -> tuple[float, float]:
    """
    Return (sign, confidence) for a contract under the selected mode.

    Args:
        contract:  normalized+model-enriched dict
        spot:      current spot price
        mode:      one of MODES

    Returns:
        (sign, confidence)
        sign        ∈ {-1.0, 0.0, +1.0}   (unsigned mode returns +1.0 with note)
        confidence  ∈ [0.0, 100.0]
    """
    if mode == "unsigned":
        return 1.0, 100.0
    elif mode == "classic":
        return _classic_sign(contract), 70.0
    elif mode == "customer_long":
        return _classic_sign(contract), 80.0
    elif mode == "heuristic":
        return _heuristic_sign(contract, spot)
    elif mode == "probabilistic":
        p = get_p_dealer_short(contract, spot)
        return 2.0 * p - 1.0, p * 100.0
    else:
        raise ValueError(f"Unknown sign mode: {mode!r}. Choose from {MODES}")


def mode_label(mode: str) -> str:
    return {
        "classic":       "Classic (calls +1, puts -1)",
        "customer_long": "Customer Long Premium",
        "heuristic":     "Heuristic (moneyness/TTE/volume)",
        "unsigned":      "Unsigned (absolute gamma only)",
        "probabilistic": "Probabilistic (p_dealer_short)",
    }.get(mode, mode)


def mode_note(mode: str) -> str:
    return {
        "classic":       "Assumed: standard dealer hedging convention. Estimated.",
        "customer_long": "Assumed: customers net long options. Dealer is net short. Estimated.",
        "heuristic":     "Heuristic confidence scoring by moneyness, TTE, and volume. Estimated.",
        "unsigned":      "No dealer sign applied — shows absolute gamma concentration only.",
        "probabilistic": "Per-contract p_dealer_short via delta/DTE/vol heuristics. Band shows p=[0.40,0.95] range. Estimated.",
    }.get(mode, "")


def apply_sign_to_chain(
    contracts: list[dict],
    spot: float,
    mode: str = "classic",
) -> tuple[list[dict], float]:
    """
    Attach 'sign' and 'sign_confidence' to every contract dict.

    Returns:
        (enriched_contracts, avg_confidence)
    """
    total_conf = 0.0
    enriched = []
    for c in contracts:
        sign, conf = get_sign(c, spot, mode)
        enriched.append({**c, "sign": sign, "sign_confidence": conf})
        total_conf += conf
    avg_conf = total_conf / len(contracts) if contracts else 0.0
    return enriched, avg_conf


def get_p_dealer_short(contract: dict, spot: float) -> float:
    """
    Probabilistic estimate that the dealer is SHORT this contract.

    Returns p ∈ [0.0, 1.0].
    expected_sign = 2*p - 1  maps to [-1.0, +1.0].

    Heuristics (from user spec):
      • far OTM puts  (delta < -0.15)      → 0.90  (customers buy protection)
      • far OTM calls (delta >  0.85)      → 0.85  (customers hold, dealer short)
      • near ATM      (0.4 < |delta| < 0.6)→ 0.55  (balanced, uncertain)
      • default                             → 0.70
      Adjustments:
      • DTE < 7                            → +0.05 (short-dated speculative)
      • DTE > 90                           → -0.05 (long-dated institutional)
      • vol/OI > 2                         → -0.05 (fresh opening, less certain)
    """
    from gex_engine.greek_calc import get_delta

    cp     = contract.get("option_type", "")
    oi     = max(contract.get("oi", 1), 1)
    vol    = contract.get("volume", 0) or 0
    expiry = contract.get("expiration", "")

    try:
        exp_date = date.fromisoformat(expiry)
        dte = max(0, (exp_date - date.today()).days)
    except ValueError:
        dte = 30

    vol_oi = vol / oi if oi > 0 else 0.0

    delta = get_delta(contract, spot)

    # Base p from delta thresholds
    p = 0.70  # default
    if delta is not None:
        if cp == "put" and delta < -0.15:
            p = 0.90
        elif cp == "call" and delta > 0.85:
            p = 0.85
        elif 0.4 < abs(delta) < 0.6:
            p = 0.55

    # Adjustments
    if dte < 7:
        p += 0.05
    if dte > 90:
        p -= 0.05
    if vol_oi > 2:
        p -= 0.05

    return max(0.0, min(1.0, p))


# ── Internal helpers ───────────────────────────────────────────────────────────

def _classic_sign(contract: dict) -> float:
    cp = contract.get("option_type", "")
    return 1.0 if cp == "call" else -1.0


def _heuristic_sign(contract: dict, spot: float) -> tuple[float, float]:
    """
    Heuristic confidence scoring.

    High confidence (customer long) → sign = classic
    Low confidence → confidence approaches 50 (uncertain)

    Factors:
    - Moneyness: far OTM puts → customers typically long protection → high conf
    - ATM covered call zone → calls often sold by customers → lower conf
    - TTE: very short TTE (0DTE) with high volume → speculative → moderate conf
    - Volume/OI ratio: high vol/OI → fresh activity → slightly higher conf
    """
    cp     = contract.get("option_type", "")
    strike = contract.get("strike", spot)
    oi     = max(contract.get("oi", 1), 1)
    vol    = contract.get("volume", 0) or 0
    expiry = contract.get("expiration", "")

    sign = _classic_sign(contract)

    # Moneyness: how far is strike from spot?
    moneyness = (strike - spot) / spot if spot > 0 else 0.0

    # Days to expiry
    try:
        exp_date = date.fromisoformat(expiry)
        dte = max(0, (exp_date - date.today()).days)
    except ValueError:
        dte = 30  # assume mid-term if unknown

    # Vol/OI ratio
    vol_oi = vol / oi if oi > 0 else 0.0

    confidence = 70.0  # base

    if cp == "put":
        if moneyness < -0.05:          # far OTM put: protection buyer
            confidence += 15.0
        elif moneyness < -0.02:        # moderately OTM put
            confidence += 8.0
        elif abs(moneyness) < 0.01:    # near ATM put: could be hedge or speculative
            confidence -= 5.0
    else:  # call
        if moneyness > 0.05:           # far OTM call: speculative buyer
            confidence += 5.0
        elif -0.02 <= moneyness <= 0.02:  # ATM call: covered call territory → lower conf
            confidence -= 10.0

    # 0DTE speculative activity: more uncertain
    if dte == 0:
        if vol_oi > 0.5:
            confidence -= 8.0
        else:
            confidence -= 3.0

    # Fresh flow (vol > OI would be roll/opening): slight confidence bump
    if vol_oi > 1.0:
        confidence += 5.0

    confidence = max(30.0, min(95.0, confidence))
    return sign, confidence
