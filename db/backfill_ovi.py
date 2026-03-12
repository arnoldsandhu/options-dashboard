"""
Backfill 20 trading days of historical options volume into ovi_history.

Run once from the project root:
    python db/backfill_ovi.py

APPROACH
--------
Polygon's v3 snapshot/options endpoint only returns currently-active contracts.
Historical dates are missing expired contracts (often the highest-volume ones on
expiry day), so per-day volume via `as_of` is systematically understated.

Instead, this script uses today's live snapshot (same methodology as the live
OVI scan) as a baseline proxy, saved for each of the past 20 trading days.
This immediately enables %Chg calculations. Estimates will be replaced by real
live data as the system accumulates daily readings over 10+ trading sessions.

The baseline is internally consistent: both the live scan and these backfilled
records use the same Polygon snapshot endpoint and `_day_is_today` contract
filter, so %Chg shows meaningful relative changes from day one.
"""
import os
import sys
from datetime import date, datetime, timedelta

# Allow running from db/ or project root
_here = os.path.dirname(os.path.abspath(__file__))
_root = os.path.dirname(_here)
if _root not in sys.path:
    sys.path.insert(0, _root)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from db.database import get_ovi_history, save_ovi_snapshot
from signals.unusual_vol import _fetch_ticker_data  # reuse live-scan fetch logic
from signals.watchlist_builder import TIER1, TIER2

# Backfill TIER1 (core) + TIER2 (liquid ETFs) — static tickers that need long baselines.
# TIER3/4 tickers accumulate real data over time; no proxy backfill needed.
BACKFILL_LIST = list(dict.fromkeys(TIER1 + TIER2))  # dedup, preserve order

MAX_TRADING_DAYS = 20


def _last_n_trading_days(n: int) -> list[date]:
    """Return the last N weekdays before today, newest first."""
    days: list[date] = []
    d = date.today() - timedelta(days=1)
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    return days


def main():
    trading_days = _last_n_trading_days(MAX_TRADING_DAYS)

    print(f"{'='*62}")
    print(f"OVI BACKFILL (live-proxy method) — {MAX_TRADING_DAYS} trading days")
    print(f"Date range: {trading_days[-1]} → {trading_days[0]}")
    print(f"{'='*62}")
    print()
    print("Step 1 — Fetching today's live volumes for all tickers...")
    print()

    live_data: dict[str, dict] = {}
    for ticker in BACKFILL_LIST:
        print(f"  {ticker:<6} ", end="", flush=True)
        raw = _fetch_ticker_data(ticker)
        if raw:
            live_data[ticker] = raw
            print(f"C:{raw['c_vol']:>8,}  P:{raw['p_vol']:>8,}  OI-C:{raw['c_oi']:>8,}  OI-P:{raw['p_oi']:>8,}")
        else:
            live_data[ticker] = {"c_vol": 0, "p_vol": 0, "c_oi": 0, "p_oi": 0,
                                 "spot": None, "px_pct_chg": None}
            print("no data")

    print()
    print("Step 2 — Writing baseline proxy to ovi_history for past 20 trading days...")
    print("         (today's volume saved for each historical date as estimate)")
    print()

    total_records = 0
    for day_idx, d in enumerate(trading_days, 1):
        date_str = d.isoformat()
        for ticker in BACKFILL_LIST:
            td = live_data[ticker]
            save_ovi_snapshot(
                ticker, date_str,
                td["c_vol"], td["p_vol"],
                td["c_oi"],  td["p_oi"],
                td.get("spot"), None,  # px_pct_chg not meaningful for historical
            )
            total_records += 1
        print(f"  [{day_idx:2d}/{MAX_TRADING_DAYS}] {date_str} — {len(BACKFILL_LIST)} records written")

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print(f"{'─'*62}")
    print(f"{'Ticker':<8} {'Days':>5} {'Avg C Vol':>13} {'Avg P Vol':>13}")
    print(f"{'─'*8} {'─'*5} {'─'*13} {'─'*13}")

    full_count = partial_count = zero_count = 0
    for ticker in BACKFILL_LIST:
        history = get_ovi_history(ticker, days=MAX_TRADING_DAYS + 5)
        n = len(history)
        if n >= MAX_TRADING_DAYS:
            full_count += 1
        elif n > 0:
            partial_count += 1
        else:
            zero_count += 1

        if history:
            avg_c = sum(r["call_vol"] for r in history) / len(history)
            avg_p = sum(r["put_vol"]  for r in history) / len(history)
            print(f"{ticker:<8} {n:>5}d {avg_c:>13,.0f} {avg_p:>13,.0f}")
        else:
            print(f"{ticker:<8} {0:>5}d {'—':>13} {'—':>13}")

    print()
    print(f"{'='*62}")
    print(f"Backfill complete — {total_records} records written")
    print(f"Full {MAX_TRADING_DAYS}d baseline : {full_count}/{len(BACKFILL_LIST)} tickers")
    print()
    print("NOTE: These are baseline estimates using today's live volume.")
    print("      %Chg calculations will work immediately but show deviations")
    print("      relative to today's session volume, not true historical averages.")
    print("      Real averages will emerge after 10+ daily live scans accumulate.")
    print()
    print("Next step: python main.py --poll-now")


if __name__ == "__main__":
    main()
