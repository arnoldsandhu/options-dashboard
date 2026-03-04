"""Build the self-contained dashboard HTML from live data."""
from __future__ import annotations

import html
import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from config import WATCHLIST, DASHBOARD_TITLE, MARKET_OPEN_ET, MARKET_CLOSE_ET
from db.database import (
    get_today_alerts,
    get_latest_gex_all,
    get_gex_percentile,
    GEX_HISTORY_DAYS,
)

ET = ZoneInfo("America/New_York")
_TWO_HOURS_SECS = 7200


# ── Helpers ──────────────────────────────────────────────────────────────────

def _now_et() -> datetime:
    return datetime.now(ET)


def _is_market_open() -> bool:
    now = _now_et()
    if now.weekday() >= 5:
        return False
    t = (now.hour, now.minute)
    return MARKET_OPEN_ET <= t <= MARKET_CLOSE_ET


def _parse_ts(ts_str: str) -> datetime:
    """Parse UTC ISO timestamp from DB."""
    try:
        return datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def _age_secs(ts_str: str) -> float:
    now_utc = datetime.now(timezone.utc)
    return (now_utc - _parse_ts(ts_str)).total_seconds()


def _fmt_ts_et(ts_str: str) -> str:
    dt = _parse_ts(ts_str).astimezone(ET)
    return dt.strftime("%H:%M ET")


def _sentiment(signal_type: str, alert_text: str) -> str:
    """Return 'bullish', 'bearish', or 'neutral'."""
    txt = alert_text.upper()
    if signal_type == "UNUSUAL_VOL":
        if " CALL" in txt or "C2" in txt or "C3" in txt:
            return "bullish"
        if " PUT" in txt or "P2" in txt or "P3" in txt:
            return "bearish"
        return "neutral"
    if signal_type == "GEX":
        if "FLIPPED NEGATIVE" in txt or "EXTREME LOW" in txt or "NEGATIVE" in txt:
            return "bearish"
        if "FLIPPED POSITIVE" in txt or "EXTREME HIGH" in txt:
            return "bullish"
        return "neutral"
    if signal_type == "EARNINGS":
        if "CHEAP" in txt:
            return "bullish"
        if "RICH" in txt:
            return "bearish"
        return "neutral"
    return "neutral"  # ROLL


def _gex_trend(history_gex: list[float]) -> str:
    """↑ ↓ → based on last 3 readings."""
    if len(history_gex) < 2:
        return "→"
    recent = history_gex[-3:]
    if recent[-1] > recent[0] * 1.05:
        return "↑"
    if recent[-1] < recent[0] * 0.95:
        return "↓"
    return "→"


# ── Data assembly ─────────────────────────────────────────────────────────────

def _build_signal_rows(alerts: list[dict], sig_type: str, max_rows: int = 10) -> list[dict]:
    filtered = [a for a in alerts if a["signal_type"] == sig_type][:max_rows]
    rows = []
    for a in filtered:
        age = _age_secs(a["timestamp"])
        rows.append({
            "ticker": a["ticker"],
            "alert_text": a["alert_text"],
            "time_str": _fmt_ts_et(a["timestamp"]),
            "sentiment": _sentiment(sig_type, a["alert_text"]),
            "faded": age > _TWO_HOURS_SECS,
        })
    return rows


def _build_gex_table(latest_gex: list[dict]) -> list[dict]:
    rows = []
    for g in latest_gex:
        net_gex_m = g["net_gex"] / 1_000_000
        pct = get_gex_percentile(g["ticker"], g["net_gex"])
        # Trend: pull short history inline
        from db.database import get_gex_history
        hist = get_gex_history(g["ticker"], days=3)
        trend = _gex_trend([h["net_gex"] for h in hist])
        rows.append({
            "ticker": g["ticker"],
            "net_gex_m": net_gex_m,
            "pct": pct,
            "trend": trend,
            "negative": net_gex_m < 0,
        })
    rows.sort(key=lambda r: abs(r["net_gex_m"]), reverse=True)
    return rows[:15]


# ── HTML fragments ────────────────────────────────────────────────────────────

def _e(s) -> str:
    return html.escape(str(s))


def _signal_row_html(row: dict) -> str:
    sentiment_cls = {"bullish": "bull", "bearish": "bear", "neutral": "neut"}.get(row["sentiment"], "neut")
    faded = ' style="opacity:0.45"' if row["faded"] else ""
    ticker = _e(row["ticker"])
    time_str = _e(row["time_str"])
    # Split alert_text: ticker | rest | AI note (after last " | ")
    parts = row["alert_text"].split(" | ", 1)
    main_text = _e(parts[0])
    ai_note = _e(parts[1]) if len(parts) > 1 else ""
    return f"""
      <div class="sig-row {sentiment_cls}"{faded}>
        <span class="ticker-badge">{ticker}</span>
        <span class="sig-main">{main_text}</span>
        {f'<span class="ai-note">💬 {ai_note}</span>' if ai_note else ""}
        <span class="sig-time">{time_str}</span>
      </div>"""


def _gex_table_html(rows: list[dict]) -> str:
    if not rows:
        return '<p class="empty">No GEX data yet.</p>'
    inner = ""
    for r in rows:
        neg_cls = " neg-gex" if r["negative"] else " pos-gex"
        inner += (
            f'<tr class="gex-tr{neg_cls}">'
            f'<td class="ticker-badge">{_e(r["ticker"])}</td>'
            f'<td>${r["net_gex_m"]:+.1f}M</td>'
            f'<td>{r["pct"]:.0f}th</td>'
            f'<td class="trend">{r["trend"]}</td>'
            f"</tr>"
        )
    return f'<table class="gex-tbl"><thead><tr><th>Ticker</th><th>Net GEX</th><th>Pct</th><th></th></tr></thead><tbody>{inner}</tbody></table>'


def _card_html(title: str, rows: list[dict], extra_html: str = "") -> str:
    if rows:
        body = "".join(_signal_row_html(r) for r in rows)
    else:
        body = '<p class="empty">No signals this session.</p>'
    return f"""
    <div class="card">
      <div class="card-header">{title}</div>
      <div class="card-body scroll">
        {body}
        {extra_html}
      </div>
    </div>"""


def _watchlist_badges() -> str:
    return " ".join(f'<span class="wl-badge">{_e(t)}</span>' for t in WATCHLIST)


# ── Main renderer ─────────────────────────────────────────────────────────────

def render(title: str | None = None) -> str:
    title = title or DASHBOARD_TITLE
    now_et = _now_et()
    updated = now_et.strftime("%b %d %Y %H:%M ET")
    market_open = _is_market_open()
    market_label = "OPEN" if market_open else "CLOSED"
    market_cls = "status-open" if market_open else "status-closed"

    alerts = get_today_alerts()
    total_today = len(alerts)

    vol_rows = _build_signal_rows(alerts, "UNUSUAL_VOL")
    gex_rows = _build_signal_rows(alerts, "GEX")
    roll_rows = _build_signal_rows(alerts, "ROLL")
    earn_rows = _build_signal_rows(alerts, "EARNINGS")

    latest_gex = get_latest_gex_all()
    gex_table = _build_gex_table(latest_gex)

    gex_extra = _gex_table_html(gex_table)

    vol_card = _card_html("📊 UNUSUAL VOL", vol_rows)
    gex_card = _card_html("⚡ GEX EXTREMES", gex_rows, gex_extra)
    roll_card = _card_html("🔄 ROLL OPPORTUNITIES", roll_rows)
    earn_card = _card_html("📅 EARNINGS VOL MISPRICING", earn_rows)

    wl_badges = _watchlist_badges()

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="refresh" content="300">
  <title>{_e(title)}</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

    :root {{
      --bg:       #0d1117;
      --bg2:      #161b22;
      --bg3:      #21262d;
      --border:   #30363d;
      --text:     #e6edf3;
      --muted:    #8b949e;
      --bull:     #238636;
      --bull-fg:  #3fb950;
      --bear:     #6e1a1a;
      --bear-fg:  #f85149;
      --neut:     #7d6a00;
      --neut-fg:  #e3b341;
      --badge-bg: #1f2937;
      --badge-fg: #93c5fd;
      --ai-fg:    #a78bfa;
    }}

    body {{
      background: var(--bg);
      color: var(--text);
      font-family: 'Segoe UI', system-ui, sans-serif;
      font-size: 13px;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
    }}

    /* ── Header ─────────────────────────── */
    header {{
      background: var(--bg2);
      border-bottom: 1px solid var(--border);
      padding: 14px 24px;
      display: flex;
      align-items: center;
      gap: 16px;
      flex-wrap: wrap;
    }}
    header h1 {{ font-size: 18px; font-weight: 600; letter-spacing: .3px; flex: 1; }}
    .meta {{ display: flex; align-items: center; gap: 12px; }}
    .updated {{ color: var(--muted); font-size: 11px; }}
    .status-open  {{ background: #1c3a20; color: #4ade80; padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; }}
    .status-closed{{ background: #3a1c1c; color: #f87171; padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; }}
    .count-badge  {{ background: #1e3a5f; color: #60a5fa;  padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; }}

    /* ── Grid ───────────────────────────── */
    .grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      grid-template-rows: auto auto;
      gap: 16px;
      padding: 16px 24px;
      flex: 1;
    }}
    @media (max-width: 800px) {{ .grid {{ grid-template-columns: 1fr; }} }}

    /* ── Card ───────────────────────────── */
    .card {{
      background: var(--bg2);
      border: 1px solid var(--border);
      border-radius: 8px;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }}
    .card-header {{
      background: var(--bg3);
      padding: 10px 14px;
      font-weight: 600;
      font-size: 12px;
      letter-spacing: .5px;
      border-bottom: 1px solid var(--border);
      text-transform: uppercase;
    }}
    .card-body {{ padding: 10px; flex: 1; overflow-y: auto; }}
    .scroll {{ max-height: 340px; }}
    .empty {{ color: var(--muted); font-style: italic; padding: 8px 4px; font-size: 12px; }}

    /* ── Signal rows ────────────────────── */
    .sig-row {{
      display: grid;
      grid-template-columns: 60px 1fr auto;
      grid-template-rows: auto auto;
      column-gap: 8px;
      row-gap: 2px;
      padding: 7px 8px;
      border-radius: 6px;
      margin-bottom: 5px;
      border-left: 3px solid transparent;
    }}
    .sig-row.bull {{ background: #0f2d14; border-left-color: var(--bull-fg); }}
    .sig-row.bear {{ background: #2d0f0f; border-left-color: var(--bear-fg); }}
    .sig-row.neut {{ background: #2a2000; border-left-color: var(--neut-fg); }}

    .ticker-badge {{
      background: var(--badge-bg);
      color: var(--badge-fg);
      font-weight: 700;
      font-size: 11px;
      padding: 2px 6px;
      border-radius: 4px;
      white-space: nowrap;
      align-self: center;
      text-align: center;
    }}
    .sig-main  {{ font-size: 12px; line-height: 1.4; align-self: center; }}
    .sig-time  {{ color: var(--muted); font-size: 10px; white-space: nowrap; align-self: center; }}
    .ai-note   {{
      grid-column: 2 / 4;
      color: var(--ai-fg);
      font-size: 11px;
      font-style: italic;
      line-height: 1.3;
    }}

    /* ── GEX table ──────────────────────── */
    .gex-tbl {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 12px;
      font-size: 11px;
    }}
    .gex-tbl th {{
      color: var(--muted);
      text-align: left;
      padding: 4px 6px;
      border-bottom: 1px solid var(--border);
      font-weight: 500;
    }}
    .gex-tr td {{ padding: 4px 6px; border-bottom: 1px solid #1e2530; }}
    .gex-tr.neg-gex {{ background: #2a0f0f; }}
    .gex-tr.pos-gex {{ background: #0f2a12; }}
    .trend {{ font-size: 14px; }}

    /* ── Footer ─────────────────────────── */
    footer {{
      background: var(--bg2);
      border-top: 1px solid var(--border);
      padding: 12px 24px;
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 10px;
    }}
    .wl-badge {{
      background: #1c2840;
      color: #94a3b8;
      font-size: 10px;
      font-weight: 600;
      padding: 2px 7px;
      border-radius: 10px;
    }}
    .footer-right {{
      margin-left: auto;
      color: var(--muted);
      font-size: 11px;
    }}
  </style>
</head>
<body>

<header>
  <h1>📈 {_e(title)}</h1>
  <div class="meta">
    <span class="updated">Updated {_e(updated)}</span>
    <span class="{market_cls}">{market_label}</span>
    <span class="count-badge">{total_today} signals today</span>
  </div>
</header>

<div class="grid">
  {vol_card}
  {gex_card}
  {roll_card}
  {earn_card}
</div>

<footer>
  {wl_badges}
  <span class="footer-right">Powered by Polygon.io + Claude</span>
</footer>

</body>
</html>"""
