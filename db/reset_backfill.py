"""
Reset and re-backfill the OVI history baseline.

Run this when:
- The initial backfill proxy data is stale (written at system init with old volume levels)
- New tickers have been added to TIER1/TIER2 that need a baseline
- You want to align the 20d average baseline with current live volume

Usage (from project root):
    python db/reset_backfill.py

What it does:
1. Deletes all ovi_history rows for TIER1+TIER2 tickers (clears stale proxy data)
2. Re-runs the backfill using today's live volume as the fresh proxy baseline
   (ON CONFLICT DO UPDATE means re-running is safe even without step 1, but
   step 1 ensures a truly clean 20-day window with no stale entries)
"""
import os
import sys

_here = os.path.dirname(os.path.abspath(__file__))
_root = os.path.dirname(_here)
if _root not in sys.path:
    sys.path.insert(0, _root)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from db.database import get_conn
from signals.watchlist_builder import TIER1, TIER2

PURGE_LIST = list(dict.fromkeys(TIER1 + TIER2))


def purge_ovi_history():
    """Delete all ovi_history rows for TIER1+TIER2 tickers."""
    placeholders = ",".join("?" * len(PURGE_LIST))
    with get_conn() as conn:
        result = conn.execute(
            f"DELETE FROM ovi_history WHERE ticker IN ({placeholders})",
            PURGE_LIST,
        )
        deleted = result.rowcount
    print(f"Purged {deleted} rows from ovi_history for {len(PURGE_LIST)} tickers.")
    return deleted


def main():
    print("=" * 62)
    print("OVI HISTORY RESET + RE-BACKFILL")
    print("=" * 62)
    print()
    print(f"Step 1 — Purging stale proxy data for {len(PURGE_LIST)} TIER1+TIER2 tickers...")
    purge_ovi_history()
    print()
    print("Step 2 — Re-backfilling with fresh live-volume proxy...")
    print()

    # Import and run backfill
    from db.backfill_ovi import main as backfill_main
    backfill_main()

    print()
    print("Reset complete. New 20d averages will reflect today's live volume.")
    print("Run 'python main.py --poll-now' to begin accumulating real daily data.")


if __name__ == "__main__":
    main()
