"""
13F Flow Match engine — cross-references live OVI alerts against institutional
13F positions to surface possible smart-money alignment.
"""
from __future__ import annotations
from db.positions_13f import POSITIONS_13F

# Per-ticker fund context — specific Q4 2025 positioning signals
# sig_type: "conviction" | "exit" | "reversal" | "options" | "hedge" | "contrarian"
_FUND_CONTEXT: dict[str, list[dict]] = {
    "NVDA": [
        {"fund": "Millennium",    "detail": "Added 93% Q4, calls overlay — puts → conviction flip",  "sig_type": "conviction"},
        {"fund": "Point72",       "detail": "#1 holding, 3-quarter persistent build",                "sig_type": "conviction"},
        {"fund": "D.E. Shaw",     "detail": "#1 equity maintained, consistent add Q3+Q4",            "sig_type": "conviction"},
        {"fund": "Citadel",       "detail": "Trimmed Q3 → reclaimed #1 equity Q4",                  "sig_type": "reversal"},
    ],
    "MSFT": [
        {"fund": "Millennium",    "detail": "Added 135% Q4 ($2.49B) — late but aggressive entry",    "sig_type": "conviction"},
        {"fund": "D.E. Shaw",     "detail": "Added $1.9B Q3, maintained top 3 Q4",                  "sig_type": "conviction"},
        {"fund": "Point72",       "detail": "#1 holding Q2 → trimmed Q4 — profit-taking",           "sig_type": "contrarian"},
        {"fund": "Citadel",       "detail": "Added $2.7B Q3, maintained top 5 Q4",                  "sig_type": "conviction"},
    ],
    "AAPL": [
        {"fund": "Millennium",    "detail": "Added $1.8B Q3 + 67% Q4 — steady build",               "sig_type": "conviction"},
        {"fund": "D.E. Shaw",     "detail": "SOLD $2.3B Q3 — major exit (vs Millennium add)",        "sig_type": "exit"},
        {"fund": "Berkshire",     "detail": "Continuing to reduce since Sep 2023",                   "sig_type": "exit"},
        {"fund": "Citadel",       "detail": "Added $1.6B Q3",                                        "sig_type": "conviction"},
    ],
    "AMZN": [
        {"fund": "Millennium",    "detail": "Reduced Q3 → added 398% Q4 — massive reversal",        "sig_type": "reversal"},
        {"fund": "Citadel",       "detail": "Consistent adds, #2 equity holding Q4",                "sig_type": "conviction"},
        {"fund": "Ackman",        "detail": "Top 3 Pershing Square holding",                         "sig_type": "conviction"},
        {"fund": "Soros",         "detail": "#1 holding Q4",                                         "sig_type": "conviction"},
    ],
    "META": [
        {"fund": "Druckenmiller", "detail": "FULL EXIT Q4 — complete mega-cap tech exit",           "sig_type": "exit"},
        {"fund": "Point72",       "detail": "Reduced $406M Q4 — not high conviction",               "sig_type": "exit"},
        {"fund": "Tepper",        "detail": "5.8% Appaloosa holding — divergence vs exits",         "sig_type": "contrarian"},
        {"fund": "Ackman",        "detail": "Top 5 Pershing Square holding",                         "sig_type": "conviction"},
    ],
    "GOOG": [
        {"fund": "Tepper",        "detail": "8.2% Appaloosa — #2 holding, high conviction",         "sig_type": "conviction"},
        {"fund": "Ackman",        "detail": "Top 5 Pershing Square holding",                         "sig_type": "conviction"},
        {"fund": "Soros",         "detail": "#5 holding Q4",                                         "sig_type": "conviction"},
        {"fund": "Bridgewater",   "detail": "Trimmed Q4",                                            "sig_type": "exit"},
    ],
    "GOOGL": [
        {"fund": "Tepper",        "detail": "8.2% Appaloosa — #2 holding, high conviction",         "sig_type": "conviction"},
        {"fund": "Ackman",        "detail": "Top 5 Pershing Square holding",                         "sig_type": "conviction"},
        {"fund": "Soros",         "detail": "#5 holding Q4",                                         "sig_type": "conviction"},
        {"fund": "Bridgewater",   "detail": "Trimmed Q4",                                            "sig_type": "exit"},
    ],
    "GS": [
        {"fund": "Druckenmiller", "detail": "NEW Q4 while exiting BAC + C — bank rotation into GS", "sig_type": "reversal"},
    ],
    "NFLX": [
        {"fund": "Citadel",       "detail": "Reduced $1.2B Q3 — notable reduction",                 "sig_type": "exit"},
    ],
    "TSLA": [
        {"fund": "Citadel",       "detail": "Calls as #3 overall holding — persistent bull overlay", "sig_type": "options"},
        {"fund": "Millennium",    "detail": "Heavy call overlay in Q4 options positions",            "sig_type": "options"},
    ],
    "AVGO": [
        {"fund": "Millennium",    "detail": "Added 146% Q4 — AI semi conviction",                   "sig_type": "conviction"},
        {"fund": "Point72",       "detail": "Added $612M Q4, consistent builds Q3+Q4",              "sig_type": "conviction"},
        {"fund": "D.E. Shaw",     "detail": "Added $1.0B Q3 — semi infrastructure",                 "sig_type": "conviction"},
    ],
    "TSM": [
        {"fund": "Point72",       "detail": "Added $992M Q4 → #2 holding — accelerating conviction", "sig_type": "conviction"},
    ],
    "PLTR": [
        {"fund": "D.E. Shaw",     "detail": "Maintained top 5 Q3+Q4 — contrarian vs peers selling", "sig_type": "contrarian"},
        {"fund": "Millennium",    "detail": "Sold $792M Q3 after Q2 add — momentum dump",           "sig_type": "exit"},
    ],
    "AMD": [
        {"fund": "D.E. Shaw",     "detail": "Entered top 5 Q4 alongside NVDA — semi pair trade?",   "sig_type": "conviction"},
        {"fund": "Point72",       "detail": "Reduced $379M Q4",                                     "sig_type": "exit"},
    ],
}


def build_13f_matches(ovi_report: dict, gex_tickers: list[str] | None = None) -> list[dict]:
    """
    Compare top OVI tickers AND GEX watchlist tickers against 13F holdings.
    Returns list of match dicts for dashboard display, sorted by fund coverage.
    """
    top_calls = ovi_report.get("top_calls", [])
    top_puts  = ovi_report.get("top_puts",  [])

    matches: list[dict] = []
    seen: set[str] = set()

    for row in top_calls[:10]:
        tk = row.get("ticker", "")
        if tk in POSITIONS_13F and tk not in seen:
            seen.add(tk)
            matches.append(_make_match(tk, "CALL", row, POSITIONS_13F[tk]))

    for row in top_puts[:10]:
        tk = row.get("ticker", "")
        if tk in POSITIONS_13F and tk not in seen:
            seen.add(tk)
            matches.append(_make_match(tk, "PUT", row, POSITIONS_13F[tk]))

    # Also check GEX watchlist tickers not already matched via OVI
    for tk in (gex_tickers or []):
        if tk in POSITIONS_13F and tk not in seen:
            seen.add(tk)
            matches.append(_make_match(tk, "GEX", {}, POSITIONS_13F[tk]))

    matches.sort(key=lambda m: m["fund_count"], reverse=True)
    return matches[:8]


def _make_match(ticker: str, side: str, row: dict, funds: dict) -> dict:
    if side == "GEX":
        pct_str = "GEX watchlist"
    else:
        vol_pct = row.get("c_vol_pct" if side == "CALL" else "p_vol_pct")
        pct_str = (f"+{vol_pct:.0f}%" if vol_pct >= 0 else f"{vol_pct:.0f}%") if vol_pct is not None else "n/a"

    fund_names = sorted(funds.keys(), key=lambda f: funds[f]["shares"], reverse=True)
    top_funds  = fund_names[:4]
    total_val  = sum(f["value_usd"] for f in funds.values())

    # Pull specific fund context entries for this ticker
    ctx = _FUND_CONTEXT.get(ticker, [])

    # Derive alignment label from context signals when available
    if side == "CALL":
        alignment, align_cls = "ALIGNED", "bull"
    elif side == "PUT":
        alignment, align_cls = "HEDGE", "bear"
    elif ctx:
        sig_types = {e["sig_type"] for e in ctx}
        if "exit" in sig_types and "conviction" in sig_types:
            alignment, align_cls = "DIVERGENCE", "warn"
        elif "exit" in sig_types and "conviction" not in sig_types:
            alignment, align_cls = "SELLING", "bear"
        elif "reversal" in sig_types:
            alignment, align_cls = "REVERSAL", "warn"
        elif "contrarian" in sig_types:
            alignment, align_cls = "MIXED", "neut"
        elif "options" in sig_types:
            alignment, align_cls = "OPTIONS", "bull"
        else:
            alignment, align_cls = "CONVICTION", "bull"
    else:
        alignment, align_cls = "13F LONG", "bull"

    return {
        "ticker":          ticker,
        "side":            side,
        "pct_str":         pct_str,
        "fund_count":      len(funds),
        "top_funds":       top_funds,
        "total_val_m":     total_val / 1_000_000,
        "alignment":       alignment,
        "align_cls":       align_cls,
        "spot":            row.get("spot"),
        "context_entries": ctx,
    }
