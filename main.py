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
    POLL_TOKEN, NTFY_TOPIC,
)
from db.database import (
    init_db,
    is_already_alerted, record_alert, log_signal,
    get_day_signals, save_positioning_match,
    log_threshold_alert, is_threshold_alerted,
)
from signals.unusual_vol import scan_ovi
from signals.gex import check_gex
from signals.skew import check_skew
from signals.positioning import check_positioning
from delivery.gmail import send_digest, send_market_open_digest, send_eod_summary, send_threshold_alert
from dashboard.publisher import publish as publish_dashboard
from dashboard.alpha_signals import generate_alpha_signals
from claude_draft import draft_observation
from ntfy_trigger import start_ntfy_listener
from db.positions_13f import print_holdings_summary

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
        "UNUSUAL_VOL":  [],
        "GEX":          [],
        "SKEW":         [],
        "POSITIONING":  [],
    }
    new_alert_count = 0

    # ── Signal 1: OVI Scan (full watchlist) ─────────────────────────────────
    ovi_report: dict = {}
    try:
        print("[OVI] Scanning full universe (SP500 + NDX + Russell 2000)...")
        ovi_report = scan_ovi()
        n_c = len(ovi_report.get("top_calls", []))
        n_p = len(ovi_report.get("top_puts", []))
        print(f"  OVI complete — {n_c} top calls, {n_p} top puts")
    except Exception as e:
        print(f"[OVI] ERROR: {e}")

    # ── Alpha Signals — generate BEFORE per-ticker API calls to avoid rate limits ──
    # Uses OVI report + DB state (latest GEX/skew/13F) for cross-source synthesis.
    alpha_theses: list = []
    try:
        from db.database import get_latest_gex_all, get_latest_skew_all
        from dashboard.template import _gex_extreme_rows, _skew_extreme_rows
        from dashboard.match_13f import build_13f_matches
        _latest_gex = get_latest_gex_all()
        _latest_skew = get_latest_skew_all()
        _gex_rows = _gex_extreme_rows(_latest_gex)
        _skew_rows = _skew_extreme_rows(_latest_skew)
        _f13 = build_13f_matches(ovi_report, [g["ticker"] for g in _latest_gex])
        alpha_theses = generate_alpha_signals(ovi_report, _gex_rows, _skew_rows, _f13)
    except Exception as e:
        print(f"[alpha_signals] Pre-generation error: {e}")

    # ── Threshold Alerts ─────────────────────────────────────────────────────
    try:
        from signals.threshold_alerts import check_thresholds
        from db.database import get_prior_snapshot
        _prior = get_prior_snapshot()
        raw_threshold_alerts = check_thresholds(
            ovi_report, _latest_gex, _latest_skew, _prior
        )
        # Dedup: skip if same ticker+type fired within 4h
        new_threshold = [
            a for a in raw_threshold_alerts
            if not is_threshold_alerted(a["ticker"], a["alert_type"], hours=4)
        ]
        if new_threshold:
            for ta in new_threshold:
                log_threshold_alert(ta["ticker"], ta["alert_type"],
                                    ta["detail"], ta.get("severity", "MEDIUM"))
            send_threshold_alert(new_threshold)
            print(f"  [threshold] {len(new_threshold)} new alert(s) triggered")
        elif raw_threshold_alerts:
            print(f"  [threshold] {len(raw_threshold_alerts)} condition(s) active (deduped)")
    except Exception as e:
        print(f"[threshold] Error: {e}")

    for ticker in WATCHLIST:
        print(f"  Checking {ticker}...", end="", flush=True)
        ticker_alerts = 0

        # Derive vol_fired / flow_dir from OVI results
        ticker_flag = ovi_report.get("per_ticker_flags", {}).get(ticker, {})
        vol_fired = ticker_flag.get("vol_fired", False)
        vol_flow_dir = ticker_flag.get("flow_dir", "unknown")

        # ── Signal 2: GEX ───────────────────────────────────────────────
        try:
            gex_alerts = check_gex(ticker)
            for a in gex_alerts:
                sig_type = "GEX"
                log_signal(ticker, sig_type, a.get("signal_data", {}), alerted=False)
                if not is_already_alerted(ticker, sig_type):
                    obs = draft_observation(a.get("claude_prompt", ""))
                    final_text = a["alert_text"] + (f" | {obs}" if obs else "")
                    record_alert(ticker, sig_type, final_text)
                    log_signal(ticker, sig_type, a.get("signal_data", {}), alerted=True)
                    alerts_by_type["GEX"].append(final_text)
                    new_alert_count += 1
                    ticker_alerts += 1
                    break  # one alert per ticker per cycle
        except Exception as e:
            print(f"\n    [gex] ERROR: {e}")

        # ── Signal 3: Skew Extremes ─────────────────────────────────────
        try:
            skew_alerts = check_skew(ticker)
            for a in skew_alerts:
                sig_type = "SKEW"
                log_signal(ticker, sig_type, a.get("signal_data", {}), alerted=False)
                if not is_already_alerted(ticker, sig_type):
                    obs = draft_observation(a.get("claude_prompt", "")) if a.get("claude_prompt") else ""
                    final_text = a["alert_text"] + (f" | {obs}" if obs else "")
                    record_alert(ticker, sig_type, final_text)
                    log_signal(ticker, sig_type, a.get("signal_data", {}), alerted=True)
                    alerts_by_type["SKEW"].append(final_text)
                    new_alert_count += 1
                    ticker_alerts += 1
                    break  # one alert per ticker per cycle
        except Exception as e:
            print(f"\n    [skew] ERROR: {e}")

        # ── Signal 4: 13F Positioning (triggered by unusual vol) ────────
        if vol_fired and vol_flow_dir != "unknown":
            try:
                pos_alerts = check_positioning(ticker, vol_flow_dir)
                for a in pos_alerts:
                    sig_type = "POSITIONING"
                    pos_key = f"POSITIONING_{ticker}"
                    if not is_already_alerted(ticker, pos_key):
                        obs = draft_observation(a.get("claude_prompt", "")) if a.get("claude_prompt") else ""
                        final_text = a["alert_text"] + (f" | {obs}" if obs else "")
                        record_alert(ticker, pos_key, final_text)
                        log_signal(ticker, sig_type, a.get("signal_data", {}), alerted=True)
                        save_positioning_match(
                            ticker, sig_type,
                            ", ".join(a["signal_data"].get("holders", [])[:3]),
                            "long", vol_flow_dir,
                            a.get("match_type", ""),
                        )
                        alerts_by_type["POSITIONING"].append(final_text)
                        new_alert_count += 1
                        ticker_alerts += 1
                        break
            except Exception as e:
                print(f"\n    [positioning] ERROR: {e}")

        print(f" {ticker_alerts} alerts")

    # ── Summary ──────────────────────────────────────────────────────────
    total = sum(len(v) for v in alerts_by_type.values())
    print(f"\nPoll complete — {total} signals, {new_alert_count} new")
    for sig_type, items in alerts_by_type.items():
        if items:
            print(f"  {sig_type}: {len(items)}")

    # ── Delivery ─────────────────────────────────────────────────────────
    if is_open_bell:
        send_market_open_digest(alerts_by_type, ovi_report=ovi_report)
    elif is_close_bell:
        day_sigs = get_day_signals()
        alerted = [s for s in day_sigs if s["alerted"]]
        ranked = sorted(alerted, key=lambda s: (
            {"SKEW": 0, "GEX": 1, "UNUSUAL_VOL": 2, "POSITIONING": 3}.get(s["signal_type"], 9),
            s["timestamp"],
        ))
        top_lines = []
        for s in ranked[:20]:
            data = json.loads(s["signal_data_json"])
            top_lines.append(f"[{s['signal_type']}] {s['ticker']} — {json.dumps(data)[:80]}")
        send_eod_summary(top_lines, ovi_report=ovi_report)
    elif new_alert_count > 0:
        send_digest(alerts_by_type, ovi_report=ovi_report)
    else:
        print("No new alerts — skipping email.")

    # ── Dashboard ─────────────────────────────────────────────────────────
    publish_dashboard(alpha_theses=alpha_theses)


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
    schedule.every(POLL_INTERVAL_MINUTES).minutes.do(poll_cycle)

    # Start ntfy.sh listener for remote poll triggers from dashboard
    start_ntfy_listener(NTFY_TOPIC, POLL_TOKEN, poll_cycle)

    print(f"Scheduler started. Polling every {POLL_INTERVAL_MINUTES} min.")
    print(f"Market open bell: {open_time} ET | Close summary: {close_time} ET")
    print(f"DRY_RUN = {DRY_RUN}")
    print(f"Watchlist ({len(WATCHLIST)} tickers): {', '.join(WATCHLIST)}\n")

    while True:
        schedule.run_pending()
        time.sleep(30)


def main():
    init_db()

    # Print 13F holdings summary on startup for verification
    print_holdings_summary()

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
