"""
Backfill 20 trading days of OVI history for the full investable universe.

Uses Polygon's per-ticker options chain snapshot — one call per ticker.
Stores today's snapshot volume as a live-proxy baseline for all 20 past dates.

WHY PER-TICKER: The grouped daily options endpoint (/v2/aggs/grouped/...options)
is not available on this Polygon plan. Per-ticker snapshots are the only
working options data source.

Checkpoint table in SQLite lets you resume after a crash.

Run from project root:
    python db/backfill_universe.py             # full run (or resume if checkpoint exists)
    python db/backfill_universe.py --resume    # skip already-completed tickers
    python db/backfill_universe.py --test      # first 10 tickers only
    python db/backfill_universe.py --test --resume
"""
import argparse
import os
import sys
import time
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Optional

_here = os.path.dirname(os.path.abspath(__file__))
_root = os.path.dirname(_here)
if _root not in sys.path:
    sys.path.insert(0, _root)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from polygon import RESTClient
from config import POLYGON_API_KEY, DB_PATH
from db.database import save_ovi_snapshot, init_db
from data.universe import build_universe

# ── Tuning ────────────────────────────────────────────────────────────────────
BATCH_SIZE       = 50    # tickers per batch
MAX_WORKERS      = 5     # concurrent Polygon calls
SLEEP_AFTER_CALL = 0.1   # seconds between API calls per thread
PROGRESS_EVERY   = 10    # print progress every N tickers
BACKFILL_DAYS    = 20    # trading days to backfill


# ── Checkpoint helpers ────────────────────────────────────────────────────────

def _ensure_checkpoint_table() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS backfill_checkpoint (
                ticker       TEXT PRIMARY KEY,
                completed_at TEXT NOT NULL,
                call_vol     INTEGER,
                put_vol      INTEGER
            )
        """)
        conn.commit()


def _get_completed_tickers() -> set:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT ticker FROM backfill_checkpoint"
        ).fetchall()
    return {r[0] for r in rows}


def _mark_completed(ticker: str, call_vol: int, put_vol: int) -> None:
    ts = datetime.now(datetime.timezone.utc if hasattr(datetime, 'timezone') else None).isoformat() if False else datetime.utcnow().isoformat()
    # Python 3.11+ compatible timestamp
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        ts = datetime.utcnow().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """INSERT INTO backfill_checkpoint (ticker, completed_at, call_vol, put_vol)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(ticker) DO UPDATE SET
                 completed_at = excluded.completed_at,
                 call_vol     = excluded.call_vol,
                 put_vol      = excluded.put_vol""",
            (ticker, ts, call_vol, put_vol),
        )
        conn.commit()


# ── Trading day helpers ───────────────────────────────────────────────────────

def _last_n_trading_days(n: int) -> list:
    """Return last N weekdays before today as ISO strings, oldest first."""
    days = []
    d = date.today() - timedelta(days=1)
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.isoformat())
        d -= timedelta(days=1)
    return list(reversed(days))


# ── Per-ticker fetch ──────────────────────────────────────────────────────────

def _fetch_and_save(ticker: str, trading_days: list) -> tuple:
    """
    Fetch options chain snapshot for ticker, sum call/put volume,
    save as live-proxy OVI rows for every trading day.
    Returns (ticker, call_vol, put_vol, status).
    """
    call_vol = put_vol = 0
    try:
        client = RESTClient(api_key=POLYGON_API_KEY)
        contracts = client.list_snapshot_options_chain(underlying_asset=ticker)
        for contract in contracts:
            try:
                details  = getattr(contract, "details", None)
                cp       = getattr(details, "contract_type", None)
                day_data = getattr(contract, "day", None)
                vol      = int(getattr(day_data, "volume", 0) or 0)
                if cp == "call":
                    call_vol += vol
                elif cp == "put":
                    put_vol += vol
            except Exception:
                continue
    except Exception as e:
        return (ticker, 0, 0, f"snapshot_error: {e}")

    time.sleep(SLEEP_AFTER_CALL)

    for date_str in trading_days:
        try:
            save_ovi_snapshot(ticker, date_str, call_vol, put_vol, 0, 0, None, None)
        except Exception as e:
            return (ticker, call_vol, put_vol, f"save_error ({date_str}): {e}")

    try:
        _mark_completed(ticker, call_vol, put_vol)
    except Exception as e:
        return (ticker, call_vol, put_vol, f"checkpoint_warn: {e}")

    return (ticker, call_vol, put_vol, "ok")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Crash-safe OVI universe backfill")
    parser.add_argument("--test",   action="store_true", help="First 10 tickers only")
    parser.add_argument("--resume", action="store_true", help="Skip completed tickers")
    args = parser.parse_args()

    init_db()
    _ensure_checkpoint_table()

    print("=" * 66)
    print("OVI UNIVERSE BACKFILL — per-ticker snapshot, crash-safe")
    print("=" * 66)
    flags = []
    if args.test:   flags.append("--test (first 10)")
    if args.resume: flags.append("--resume (skip completed)")
    if flags:
        print(f"  Mode: {', '.join(flags)}")
    print()

    print("Loading universe...")
    universe    = build_universe()
    all_tickers = sorted(universe.keys())
    print(f"  {len(all_tickers)} tickers in universe")

    if args.test:
        all_tickers = all_tickers[:10]

    if args.resume:
        done    = _get_completed_tickers()
        pending = [t for t in all_tickers if t not in done]
        print(f"  Checkpoint: {len(done)} done, {len(pending)} remaining")
    else:
        pending = all_tickers
        print(f"  Processing {len(pending)} tickers (--resume to skip completed)")

    if not pending:
        print("\nAll tickers already complete.")
        return

    print()
    trading_days = _last_n_trading_days(BACKFILL_DAYS)
    print(f"Trading days: {trading_days[0]} → {trading_days[-1]} ({len(trading_days)} days)")
    print(f"Workers: {MAX_WORKERS} | Batch: {BATCH_SIZE} | Sleep: {SLEEP_AFTER_CALL}s")
    print()

    total = len(pending)
    processed = errors = total_calls = total_puts = 0
    total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_start in range(0, total, BATCH_SIZE):
        batch     = pending[batch_start : batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        print(f"--- Batch {batch_num}/{total_batches} [{batch[0]} … {batch[-1]}] ---")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_fetch_and_save, t, trading_days): t for t in batch}
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    t, cv, pv, status = future.result()
                except Exception as exc:
                    processed += 1; errors += 1
                    print(f"  [ERROR] {ticker}: {exc}")
                    continue

                processed += 1
                if status == "ok":
                    total_calls += cv; total_puts += pv
                else:
                    errors += 1
                    print(f"  [WARN] {t}: {status}")

                if processed % PROGRESS_EVERY == 0 or processed == total:
                    print(f"  {processed}/{total} ({processed/total*100:.0f}%) | errors: {errors}")
        print()

    print("─" * 66)
    print(f"Complete: {processed} tickers | {errors} errors | "
          f"call vol {total_calls:,} | put vol {total_puts:,}")
    print(f"Live-proxy baseline set for {len(trading_days)} days.")
    print("Spike detection improves after ~10 real daily scans accumulate.")
    print()
    print("Next step: python main.py --poll-now")


if __name__ == "__main__":
    main()
