"""Options Alert System — main entry point."""
import sys
import time

# Force UTF-8 output on Windows so box-drawing characters and em-dashes print cleanly
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
import json
from datetime import datetime, date
from zoneinfo import ZoneInfo

import schedule

from config import (
    WATCHLIST, DRY_RUN,
    MARKET_OPEN_ET, MARKET_CLOSE_ET, POLL_INTERVAL_MINUTES,
)
from db.database import (
    init_db,
    is_already_alerted, record_alert, log_signal,
    get_day_signals,
)
from signals.unusual_vol import check_unusual_vol
from signals.gex import check_gex
from signals.rolls import check_rolls
from signals.earnings_vol import check_earnings_vol
from delivery.gmail import send_digest, send_market_open_digest, send_eod_summary
from dashboard.publisher import publish as publish_dashboard

ET = ZoneInfo("America/New_York")


def _now_et() -> datetime:
    return datetime.now(ET)


def _is_market_day() -> bool:
    return _now_et().weekday() < 5  # Mon–Fri


def poll_cycle(is_open_bell: bool = False, is_close_bell: bool = False):
    if not _is_market_day():
        print(f"[{_now_et().strftime('%H:%M')}] Weekend/holiday — skipping poll.")
        return

    now_str = _now_et().strftime("%H:%M ET")
    print(f"\n{'='*60}")
    print(f"POLL CYCLE — {now_str} {'[OPEN BELL]' if is_open_bell else '[CLOSE BELL]' if is_close_bell else ''}")
    print(f"{'='*60}")

    alerts_by_type: dict[str, list[str]] = {
        "UNUSUAL_VOL": [],
        "GEX": [],
        "ROLL": [],
        "EARNINGS": [],
    }
    new_alert_count = 0

    for ticker in WATCHLIST:
        print(f"  Checking {ticker}...", end="", flush=True)
        ticker_alerts = 0

        # ── Signal 1: Unusual Vol ───────────────────────────────────────
        try:
            vol_alerts = check_unusual_vol(ticker)
            for a in vol_alerts:
                sig_type = "UNUSUAL_VOL"
                log_signal(ticker, sig_type, a.get("signal_data", {}), alerted=False)
                if not is_already_alerted(ticker, sig_type):
                    record_alert(ticker, sig_type, a["alert_text"])
                    log_signal(ticker, sig_type, a.get("signal_data", {}), alerted=True)
                    alerts_by_type["UNUSUAL_VOL"].append(a["alert_text"])
                    new_alert_count += 1
                    ticker_alerts += 1
        except Exception as e:
            print(f"\n    [unusual_vol] ERROR: {e}")

        # ── Signal 2: GEX ───────────────────────────────────────────────
        try:
            gex_alerts = check_gex(ticker)
            for a in gex_alerts:
                sig_type = "GEX"
                log_signal(ticker, sig_type, a.get("signal_data", {}), alerted=False)
                if not is_already_alerted(ticker, sig_type):
                    record_alert(ticker, sig_type, a["alert_text"])
                    log_signal(ticker, sig_type, a.get("signal_data", {}), alerted=True)
                    alerts_by_type["GEX"].append(a["alert_text"])
                    new_alert_count += 1
                    ticker_alerts += 1
        except Exception as e:
            print(f"\n    [gex] ERROR: {e}")

        # ── Signal 3: Rolls ─────────────────────────────────────────────
        try:
            roll_alerts = check_rolls(ticker)
            for a in roll_alerts:
                sig_type = "ROLL"
                log_signal(ticker, sig_type, a.get("signal_data", {}), alerted=False)
                key = f"ROLL_{a.get('contract_key', ticker)}"
                if not is_already_alerted(ticker, key):
                    record_alert(ticker, key, a["alert_text"])
                    log_signal(ticker, sig_type, a.get("signal_data", {}), alerted=True)
                    alerts_by_type["ROLL"].append(a["alert_text"])
                    new_alert_count += 1
                    ticker_alerts += 1
        except Exception as e:
            print(f"\n    [rolls] ERROR: {e}")

        # ── Signal 4: Earnings Vol ──────────────────────────────────────
        try:
            earnings_alerts = check_earnings_vol(ticker)
            for a in earnings_alerts:
                sig_type = "EARNINGS"
                log_signal(ticker, sig_type, a.get("signal_data", {}), alerted=False)
                if not is_already_alerted(ticker, sig_type):
                    record_alert(ticker, sig_type, a["alert_text"])
                    log_signal(ticker, sig_type, a.get("signal_data", {}), alerted=True)
                    alerts_by_type["EARNINGS"].append(a["alert_text"])
                    new_alert_count += 1
                    ticker_alerts += 1
        except Exception as e:
            print(f"\n    [earnings] ERROR: {e}")

        print(f" {ticker_alerts} alerts")

    # ── Summary ──────────────────────────────────────────────────────────
    total = sum(len(v) for v in alerts_by_type.values())
    print(f"\nPoll complete — {total} signals, {new_alert_count} new")
    for sig_type, items in alerts_by_type.items():
        if items:
            print(f"  {sig_type}: {len(items)}")

    # ── Delivery ─────────────────────────────────────────────────────────
    if is_open_bell:
        send_market_open_digest(alerts_by_type)
    elif is_close_bell:
        # EOD: rank all day's alerted signals by type
        day_sigs = get_day_signals()
        alerted = [s for s in day_sigs if s["alerted"]]
        ranked = sorted(alerted, key=lambda s: (
            {"EARNINGS": 0, "GEX": 1, "UNUSUAL_VOL": 2, "ROLL": 3}.get(s["signal_type"], 9),
            s["timestamp"],
        ))
        top_lines = []
        for s in ranked[:20]:
            data = json.loads(s["signal_data_json"])
            top_lines.append(f"[{s['signal_type']}] {s['ticker']} — {json.dumps(data)[:80]}")
        send_eod_summary(top_lines)
    elif new_alert_count > 0:
        send_digest(alerts_by_type)
    else:
        print("No new alerts — skipping email.")

    # ── Dashboard ─────────────────────────────────────────────────────────
    publish_dashboard()


def run_scheduler():
    mo_h, mo_m = MARKET_OPEN_ET
    mc_h, mc_m = MARKET_CLOSE_ET

    open_time = f"{mo_h:02d}:{mo_m:02d}"
    close_time = f"{mc_h:02d}:{mc_m + 5:02d}"  # 4:05 PM

    # Market open bell
    schedule.every().monday.at(open_time).do(lambda: poll_cycle(is_open_bell=True))
    schedule.every().tuesday.at(open_time).do(lambda: poll_cycle(is_open_bell=True))
    schedule.every().wednesday.at(open_time).do(lambda: poll_cycle(is_open_bell=True))
    schedule.every().thursday.at(open_time).do(lambda: poll_cycle(is_open_bell=True))
    schedule.every().friday.at(open_time).do(lambda: poll_cycle(is_open_bell=True))

    # Market close summary
    schedule.every().monday.at(close_time).do(lambda: poll_cycle(is_close_bell=True))
    schedule.every().tuesday.at(close_time).do(lambda: poll_cycle(is_close_bell=True))
    schedule.every().wednesday.at(close_time).do(lambda: poll_cycle(is_close_bell=True))
    schedule.every().thursday.at(close_time).do(lambda: poll_cycle(is_close_bell=True))
    schedule.every().friday.at(close_time).do(lambda: poll_cycle(is_close_bell=True))

    # Regular polls every 20 minutes, weekdays only
    # We check market hours inside poll_cycle, so we schedule broadly and filter
    schedule.every(POLL_INTERVAL_MINUTES).minutes.do(poll_cycle)

    print(f"Scheduler started. Polling every {POLL_INTERVAL_MINUTES} min.")
    print(f"Market open bell: {open_time} ET | Close summary: {close_time} ET")
    print(f"DRY_RUN = {DRY_RUN}")
    print(f"Watchlist ({len(WATCHLIST)} tickers): {', '.join(WATCHLIST)}\n")

    while True:
        schedule.run_pending()
        time.sleep(30)


def main():
    init_db()

    if len(sys.argv) > 1 and sys.argv[1] == "--poll-now":
        print("Running single poll cycle (--poll-now)...")
        poll_cycle()
        return

    if len(sys.argv) > 1 and sys.argv[1] == "--open-bell":
        poll_cycle(is_open_bell=True)
        return

    if len(sys.argv) > 1 and sys.argv[1] == "--close-bell":
        poll_cycle(is_close_bell=True)
        return

    run_scheduler()


if __name__ == "__main__":
    main()
