"""Signal: 13F Positioning + Flow Match — cross-reference options flow with known fund positions."""
from db.positions_13f import POSITIONS_13F

# ── Match type definitions ────────────────────────────────────────────────────

_MATCH_TYPES = {
    "HEDGE":       {"desc": "Likely hedging existing long — collar opportunity",    "color": "neutral"},
    "MOMENTUM":    {"desc": "Adding upside on existing long — momentum confirmation", "color": "bullish"},
    "DIRECTIONAL": {"desc": "No long to hedge — directional bearish conviction",    "color": "bearish"},
    "NO_DATA":     {"desc": "No 13F positioning data — possible new build",         "color": "neutral"},
}


def _classify(position_direction: str, flow_direction: str) -> str:
    """Classify the interaction between fund positioning and options flow."""
    pd = position_direction.lower()
    fd = flow_direction.lower()
    if pd == "long" and fd == "put":
        return "HEDGE"
    if pd == "long" and fd == "call":
        return "MOMENTUM"
    if pd in ("short", "none") and fd == "put":
        return "DIRECTIONAL"
    # Long + package, or any other combo
    return "HEDGE"  # Default for package/mixed flows against long positions


def check_positioning(ticker: str, flow_direction: str) -> list[dict]:
    """
    Given a ticker and flow direction ('call', 'put', or 'package'),
    look up 13F holdings and return a positioning context alert.

    Returns [] if no 13F data exists for this ticker (silent — no noise).
    Returns a single aggregated alert showing the top holders.
    """
    fund_positions = POSITIONS_13F.get(ticker)
    if not fund_positions:
        return []  # No data — skip silently

    fd_clean = flow_direction.lower()
    if fd_clean == "package":
        fd_clean = "put"  # Treat package flows as put-like for positioning analysis

    long_funds = [
        (name, pos) for name, pos in fund_positions.items()
        if pos.get("direction", "long") == "long"
    ]
    if not long_funds:
        return []

    # Sort by share count descending; show top 3 in alert
    long_funds_sorted = sorted(long_funds, key=lambda x: x[1]["shares"], reverse=True)
    match_type = _classify("long", fd_clean)
    match_info = _MATCH_TYPES[match_type]

    # Build holders string (top 3 names + sizes)
    top3 = long_funds_sorted[:3]
    holders_parts = [
        f"{name} ({pos['shares'] / 1e6:.1f}M shs)"
        for name, pos in top3
    ]
    holders_str = ", ".join(holders_parts)
    extra = f" +{len(long_funds_sorted) - 3} more" if len(long_funds_sorted) > 3 else ""

    total_shares_m = sum(p["shares"] for _, p in long_funds_sorted) / 1e6
    alert_text = (
        f"{ticker} | {fd_clean.upper()} flow | "
        f"{holders_str}{extra} | "
        f"{match_info['desc']}"
    )

    claude_prompt = (
        f"You are an options sales trader. {ticker} is seeing large {fd_clean} options flow. "
        f"Known 13F long holders include {holders_str} (total ~{total_shares_m:.1f}M shs). "
        f"Context: {match_info['desc']}. "
        f"Write one sentence a sales trader would say to a hedge fund PM about this positioning angle."
    )

    return [{
        "alert_text": alert_text,
        "claude_prompt": claude_prompt,
        "match_type": match_type,
        "color": match_info["color"],
        "signal_data": {
            "ticker": ticker,
            "flow_direction": fd_clean,
            "match_type": match_type,
            "holders": [name for name, _ in long_funds_sorted],
            "total_shares_m": round(total_shares_m, 2),
        },
    }]
