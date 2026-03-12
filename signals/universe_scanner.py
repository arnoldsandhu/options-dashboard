"""
Universe-wide OVI live scanner.

Aggregates options volume across the full S&P 500 + Nasdaq 100 + Russell 2000
universe using Polygon's universal snapshot API (primary) with per-ticker
fallback if the global endpoint isn't supported.

Called from scan_ovi() in unusual_vol.py.
"""
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

import requests
from polygon import RESTClient

from config import POLYGON_API_KEY
from db.database import (
    save_ovi_snapshot, get_ovi_history, get_universe_option_root_map
)

_OPT_TICKER_RE = re.compile(r"^O:([A-Z]+)(\d{6})([CP])(\d+)$")
_POLY_BASE = "https://api.polygon.io"

# Badge labels for index membership
INDEX_BADGES = {
    "in_sp500": "S",
    "in_ndx":   "N",
    "in_rut":   "R",
}

# Scan thresholds (same as unusual_vol.py for consistency)
OVI_MIN_VOL       = 1_000  # matches Bloomberg Min Call/Put Vol = 1000
OVI_MIN_AVG_VOL   = 1_000  # matches Bloomberg Min Call/Put Avg Vol = 1000
OVI_MIN_HISTORY   = 10
OVI_MIN_SPIKE_PCT = 50
OVI_TOP_N         = 10
EXTREME_PCT       = 300   # >300% = extreme alert
TWO_WAY_PCT       = 100   # both sides >100% = two-way flow flag

# Global snapshot pagination
MAX_PAGES         = 30
MIN_VOL_CUTOFF    = 500   # stop paginating when contracts below this volume

_thread_local = threading.local()


def _get_poly() -> RESTClient:
    if not hasattr(_thread_local, "poly"):
        _thread_local.poly = RESTClient(api_key=POLYGON_API_KEY)
    return _thread_local.poly


def _parse_option_ticker(ticker_str: str) -> tuple[str, str] | None:
    """Parse OCC option ticker → (option_root, 'call'|'put') or None."""
    m = _OPT_TICKER_RE.match(ticker_str)
    if not m:
        return None
    return m.group(1), ("call" if m.group(3) == "C" else "put")


# ── Primary: Global Snapshot ──────────────────────────────────────────────────

def _fetch_global_snapshot(root_map: dict[str, str]) -> dict[str, dict] | None:
    """
    Paginate the universal snapshot API filtered to options, sorted by volume.
    Returns {option_root: {call_vol, put_vol}} or None if endpoint unsupported.
    """
    vol_by_root: dict[str, dict[str, int]] = {}
    next_url = (
        f"{_POLY_BASE}/v3/snapshot"
        f"?type=options&sort=session.volume&order=desc&limit=250"
        f"&apiKey={POLYGON_API_KEY}"
    )
    pages = 0
    min_vol_hit = False

    while next_url and pages < MAX_PAGES and not min_vol_hit:
        try:
            resp = requests.get(next_url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  [universe] Global snapshot error page {pages+1}: {e}")
            return None

        results = data.get("results", [])
        if not results:
            break

        for item in results:
            ticker_str = item.get("ticker", "")
            parsed = _parse_option_ticker(ticker_str)
            if not parsed:
                continue
            root, side = parsed
            if root not in root_map:
                continue

            # Volume is nested under session or day
            session = item.get("session", {}) or {}
            day     = item.get("day", {}) or {}
            vol = int(session.get("volume", 0) or day.get("volume", 0) or 0)

            if vol < MIN_VOL_CUTOFF:
                min_vol_hit = True
                break

            if root not in vol_by_root:
                vol_by_root[root] = {"call_vol": 0, "put_vol": 0}
            if side == "call":
                vol_by_root[root]["call_vol"] += vol
            else:
                vol_by_root[root]["put_vol"] += vol

        pages += 1
        # Follow pagination cursor
        next_url_raw = data.get("next_url", "")
        if next_url_raw:
            sep = "&" if "?" in next_url_raw else "?"
            next_url = f"{next_url_raw}{sep}apiKey={POLYGON_API_KEY}"
        else:
            next_url = None

    if not vol_by_root:
        print("  [universe] Global snapshot returned 0 results — falling back to per-ticker")
        return None

    print(f"  [universe] Global snapshot: {pages} pages, {len(vol_by_root)} underlying roots")
    return vol_by_root


# ── Fallback: Per-Ticker Snapshot ─────────────────────────────────────────────

def _day_is_today(c) -> bool:
    day = getattr(c, "day", None)
    if not day:
        return False
    ns = getattr(day, "last_updated", None)
    if not ns:
        return False
    try:
        return datetime.utcfromtimestamp(ns / 1e9).date() == date.today()
    except Exception:
        return False


def _fetch_ticker_volumes(ticker: str) -> tuple[str, int, int] | None:
    """Per-ticker snapshot → (ticker, call_vol, put_vol)."""
    from data.universe import get_option_root
    client = _get_poly()
    try:
        contracts = list(client.list_snapshot_options_chain(ticker, params={"limit": 250}))
    except Exception:
        return None

    c_vol = p_vol = 0
    for c in contracts:
        details = getattr(c, "details", None)
        if not details:
            continue
        ct = getattr(details, "contract_type", "")
        vol = int(getattr(getattr(c, "day", None), "volume", 0) or 0) if _day_is_today(c) else 0
        if ct == "call":
            c_vol += vol
        elif ct == "put":
            p_vol += vol

    return get_option_root(ticker), c_vol, p_vol


def _fetch_per_ticker_fallback(universe: dict[str, dict]) -> dict[str, dict]:
    """Fall back to per-ticker snapshots with 8 workers."""
    print(f"  [universe] Per-ticker fallback: scanning {len(universe)} tickers (8 workers)...")
    vol_by_root: dict[str, dict[str, int]] = {}
    tickers = list(universe.keys())

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_ticker_volumes, t): t for t in tickers}
        done = 0
        for future in as_completed(futures):
            done += 1
            if done % 100 == 0:
                print(f"    {done}/{len(tickers)} tickers scanned...")
            result = future.result()
            if result:
                root, c_vol, p_vol = result
                vol_by_root[root] = {"call_vol": c_vol, "put_vol": p_vol}

    print(f"  [universe] Per-ticker fallback complete: {len(vol_by_root)} roots")
    return vol_by_root


# ── Enrichment + Ranking ─────────────────────────────────────────────────────

def _enrich(ticker: str, call_vol: int, put_vol: int,
            index_flags: dict) -> dict | None:
    """Add 20d history, compute pct changes. Returns enriched dict or None."""
    history = get_ovi_history(ticker, days=20)
    n = len(history)

    pc_ratio = put_vol / call_vol if call_vol > 0 else None
    # Build [S][N][R] badge string for dashboard display
    badges = "".join(
        f"[{v}]" for k, v in INDEX_BADGES.items() if index_flags.get(k)
    )
    result = {
        "ticker":      ticker,
        "c_vol":       call_vol,
        "p_vol":       put_vol,
        "c_oi":        0,
        "p_oi":        0,
        "pc_ratio":    pc_ratio,
        "spot":        None,
        "px_pct_chg":  None,
        "opt_vol":     call_vol + put_vol,
        "index_flags": index_flags,
        "index_badges": badges,
        "history_days": n,
        # defaults
        "c_avg_vol": None, "p_avg_vol": None,
        "c_avg_oi":  None, "p_avg_oi":  None,
        "c_vol_pct": None, "p_vol_pct": None,
        "c_oi_pct":  None, "p_oi_pct":  None,
    }

    if n >= OVI_MIN_HISTORY:
        c_vols = [r["call_vol"] for r in history if r["call_vol"] is not None]
        p_vols = [r["put_vol"]  for r in history if r["put_vol"]  is not None]
        c_avg = sum(c_vols) / len(c_vols) if c_vols else None
        p_avg = sum(p_vols) / len(p_vols) if p_vols else None

        def _pct(today, avg):
            return (today - avg) / avg * 100 if avg and avg > 0 else None

        result.update({
            "c_avg_vol": c_avg,
            "p_avg_vol": p_avg,
            "c_vol_pct": _pct(call_vol, c_avg),
            "p_vol_pct": _pct(put_vol,  p_avg),
        })

    return result


def _call_ok(r: dict) -> bool:
    if r["c_vol"] < OVI_MIN_VOL:
        return False
    if r.get("c_avg_vol") is None:
        return False  # require history
    if r["c_avg_vol"] < OVI_MIN_AVG_VOL:
        return False
    if r.get("c_vol_pct") is not None and r["c_vol_pct"] < OVI_MIN_SPIKE_PCT:
        return False
    return True


def _put_ok(r: dict) -> bool:
    if r["p_vol"] < OVI_MIN_VOL:
        return False
    if r.get("p_avg_vol") is None:
        return False
    if r["p_avg_vol"] < OVI_MIN_AVG_VOL:
        return False
    if r.get("p_vol_pct") is not None and r["p_vol_pct"] < OVI_MIN_SPIKE_PCT:
        return False
    return True


# ── Main Entry Point ──────────────────────────────────────────────────────────

def scan_universe(universe: dict[str, dict]) -> dict:
    """
    Run live universe-wide OVI scan.

    Args:
        universe: {ticker: {in_sp500, in_ndx, in_rut, option_root}}

    Returns structured report dict compatible with the existing OVI report schema,
    plus extra fields: extreme_alerts, two_way_flow, index_breakdown,
    universe_scanned, tickers_above_threshold, no_baseline_notable.
    """
    if not universe:
        print("  [universe] Empty universe — nothing to scan")
        return {}

    # Build option_root → {ticker, index_flags} lookup
    root_map: dict[str, str] = {}          # option_root → canonical ticker
    index_by_root: dict[str, dict] = {}    # option_root → index_flags
    for ticker, flags in universe.items():
        root = flags["option_root"]
        root_map[root] = ticker
        index_by_root[root] = {
            "in_sp500": flags["in_sp500"],
            "in_ndx":   flags["in_ndx"],
            "in_rut":   flags["in_rut"],
        }

    # Fetch volumes
    # 1. Global snapshot (paginated) — free plan returns 0, falls through
    vol_by_root = _fetch_global_snapshot(root_map)
    # 2. Per-ticker fallback (reliable, ~3 min for full universe)
    if vol_by_root is None:
        vol_by_root = _fetch_per_ticker_fallback(universe)

    today_str = date.today().isoformat()

    # Enrich and save
    all_data: list[dict] = []
    no_baseline_notable: list[dict] = []

    for root, vols in vol_by_root.items():
        ticker = root_map.get(root)
        if not ticker:
            continue
        c_vol = vols.get("call_vol", 0)
        p_vol = vols.get("put_vol", 0)
        if c_vol + p_vol == 0:
            continue

        save_ovi_snapshot(ticker, today_str, c_vol, p_vol, 0, 0, None, None)

        flags = index_by_root.get(root, {})
        enriched = _enrich(ticker, c_vol, p_vol, flags)
        if enriched:
            all_data.append(enriched)
            # Flag high raw volume with no history
            if enriched["c_avg_vol"] is None and (c_vol + p_vol) > 10_000:
                no_baseline_notable.append({
                    "ticker":  ticker,
                    "c_vol":   c_vol,
                    "p_vol":   p_vol,
                    "opt_vol": c_vol + p_vol,
                })

    # Rank
    _sorted_calls = sorted(
        [r for r in all_data if _call_ok(r)],
        key=lambda r: r["c_vol_pct"] if r["c_vol_pct"] is not None else r["c_vol"],
        reverse=True,
    )
    _sorted_puts = sorted(
        [r for r in all_data if _put_ok(r)],
        key=lambda r: r["p_vol_pct"] if r["p_vol_pct"] is not None else r["p_vol"],
        reverse=True,
    )

    top_calls  = _sorted_calls[:OVI_TOP_N]
    also_calls = _sorted_calls[OVI_TOP_N:OVI_TOP_N + 5]
    top_puts   = _sorted_puts[:OVI_TOP_N]
    also_puts  = _sorted_puts[OVI_TOP_N:OVI_TOP_N + 5]

    # Tickers above 50% threshold (total qualifying on either side)
    qualifying_tickers = set(r["ticker"] for r in _sorted_calls + _sorted_puts)
    tickers_above_threshold = len(qualifying_tickers)

    # Extreme alerts: any ticker with >300% spike on either side
    extreme_alerts: list[dict] = []
    for r in all_data:
        c_pct = r.get("c_vol_pct") or 0
        p_pct = r.get("p_vol_pct") or 0
        if c_pct >= EXTREME_PCT:
            extreme_alerts.append({
                "ticker": r["ticker"], "side": "call",
                "pct": c_pct, "vol": r["c_vol"],
                "index_flags": r["index_flags"],
            })
        if p_pct >= EXTREME_PCT:
            extreme_alerts.append({
                "ticker": r["ticker"], "side": "put",
                "pct": p_pct, "vol": r["p_vol"],
                "index_flags": r["index_flags"],
            })
    extreme_alerts.sort(key=lambda x: x["pct"], reverse=True)

    # Two-way flow: both calls and puts spiking >100%
    two_way_flow: list[dict] = []
    pct_by_ticker: dict[str, dict] = {}
    for r in all_data:
        t = r["ticker"]
        pct_by_ticker[t] = {
            "c_pct": r.get("c_vol_pct") or 0,
            "p_pct": r.get("p_vol_pct") or 0,
            "c_vol": r["c_vol"], "p_vol": r["p_vol"],
            "index_flags": r["index_flags"],
        }
    for t, p in pct_by_ticker.items():
        if p["c_pct"] >= TWO_WAY_PCT and p["p_pct"] >= TWO_WAY_PCT:
            two_way_flow.append({"ticker": t, **p})
    two_way_flow.sort(key=lambda x: max(x["c_pct"], x["p_pct"]), reverse=True)

    # Index breakdown: count of unique tickers in each index with >50% spike
    sp500_active = sum(
        1 for r in qualifying_tickers
        if any(d["ticker"] == r and d["index_flags"].get("in_sp500")
               for d in all_data)
    )
    ndx_active = sum(
        1 for r in qualifying_tickers
        if any(d["ticker"] == r and d["index_flags"].get("in_ndx")
               for d in all_data)
    )
    rut_active = sum(
        1 for r in qualifying_tickers
        if any(d["ticker"] == r and d["index_flags"].get("in_rut")
               for d in all_data)
    )

    index_breakdown = {
        "sp500_active": sp500_active,
        "ndx_active":   ndx_active,
        "rut_active":   rut_active,
    }

    print(
        f"  [universe] Scanned {len(all_data)} tickers | "
        f"{tickers_above_threshold} above 50% threshold | "
        f"{len(extreme_alerts)} extreme alerts | "
        f"{len(two_way_flow)} two-way flow"
    )
    print(
        f"  [universe] Index breakdown: SP500={sp500_active} NDX={ndx_active} RUT={rut_active}"
    )

    return {
        "top_calls":               top_calls,
        "top_puts":                top_puts,
        "also_calls":              also_calls,
        "also_puts":               also_puts,
        "extreme_alerts":          extreme_alerts[:10],  # cap at 10
        "two_way_flow":            two_way_flow[:10],
        "index_breakdown":         index_breakdown,
        "universe_scanned":        len(all_data),
        "tickers_above_threshold": tickers_above_threshold,
        "no_baseline_notable":     no_baseline_notable[:10],
    }
