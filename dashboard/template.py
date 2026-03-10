"""Build the self-contained dashboard HTML from live data."""
from __future__ import annotations

import html
import json
import math
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from config import WATCHLIST, DASHBOARD_TITLE, MARKET_OPEN_ET, MARKET_CLOSE_ET, POLL_TOKEN, NTFY_TOPIC
from dashboard.match_13f import build_13f_matches
from db.database import (
    get_today_alerts,
    get_latest_gex_all,
    get_gex_percentile,
    get_latest_ovi_report,
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
        if "PACKAGE TRADE" in txt:
            return "neutral"
        if "CALL \u2014" in txt:
            return "bullish"
        if "PUT \u2014" in txt:
            return "bearish"
        return "neutral"
    if signal_type == "GEX":
        if "FLIPPED NEGATIVE" in txt or "EXTREME LOW" in txt or "NEGATIVE" in txt:
            return "bearish"
        if "FLIPPED POSITIVE" in txt or "EXTREME HIGH" in txt:
            return "bullish"
        return "neutral"
    if signal_type == "SKEW":
        if "ELEVATED" in txt:
            return "bearish"
        if "COMPRESSED" in txt:
            return "bullish"
        return "neutral"  # CROSSED
    if signal_type == "POSITIONING":
        if "MOMENTUM" in txt:
            return "bullish"
        if "DIRECTIONAL" in txt:
            return "bearish"
        return "neutral"  # HEDGE / NO_DATA
    return "neutral"


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


# ── OVI helpers ───────────────────────────────────────────────────────────────

def _fmt_k(v) -> str:
    if v is None:
        return "—"
    v = int(v)
    if v >= 1_000_000:
        return f"{v / 1_000_000:.1f}M"
    if v >= 1_000:
        return f"{v / 1_000:.0f}K"
    return str(v)


def _z_color(v: float | None) -> str:
    """Return a Z-Score diverging hex color for a directional percentage."""
    if v is None: return "#484F58"
    if v >= 200:  return "#207020"
    if v >= 100:  return "#409030"
    if v >= 50:   return "#70B030"
    if v >= 20:   return "#A8C840"
    if v >= 0:    return "#D0D860"
    if v >= -20:  return "#E8D040"
    if v >= -50:  return "#E88830"
    if v >= -70:  return "#E06040"
    if v >= -100: return "#CC3333"
    return "#8B0000"


def _fmt_vol_pct(v) -> str:
    if v is None:
        return '<span class="pct-na">n/a</span>'
    sign = "+" if v >= 0 else ""
    color = _z_color(v)
    cls = "vol-pos" if v >= 0 else "vol-neg"
    return f'<span class="{cls}" style="color:{color}">{sign}{v:.0f}%</span>'


def _fmt_oi_pct(v) -> str:
    if v is None:
        return '<span class="pct-na">n/a</span>'
    sign = "+" if v >= 0 else ""
    color = _z_color(v)
    cls = "vol-pos" if v >= 0 else "vol-neg"
    return f'<span class="{cls}" style="color:{color}">{sign}{v:.0f}%</span>'


def _fmt_px_pct(v) -> str:
    if v is None:
        return '<span class="pct-na">—</span>'
    sign = "+" if v >= 0 else ""
    cls = "pct-pos" if v > 0 else ("pct-neg" if v < 0 else "pct-na")
    return f'<span class="{cls}">{sign}{v:.1f}%</span>'


def _fmt_pc(v) -> str:
    if v is None:
        return "—"
    return f"{v:.0f}" if v >= 99 else f"{v:.2f}"


def _fmt_ts_short(ts_str: str) -> str:
    if not ts_str:
        return "—"
    try:
        dt = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
        return dt.astimezone(ET).strftime("%H:%M ET")
    except Exception:
        return ts_str


def _idx_badges_html(r: dict) -> str:
    """Render [S][N][R] index membership badges for a row."""
    flags = r.get("index_flags", {})
    parts = []
    if flags.get("in_sp500"):
        parts.append('<span class="idx-badge sp500">S</span>')
    if flags.get("in_ndx"):
        parts.append('<span class="idx-badge ndx">N</span>')
    if flags.get("in_rut"):
        parts.append('<span class="idx-badge rut">R</span>')
    return "".join(parts)


def _ovi_row_html(r: dict, side: str) -> str:
    vol     = r["c_vol"]    if side == "call" else r["p_vol"]
    avg_vol = r.get("c_avg_vol") if side == "call" else r.get("p_avg_vol")
    vol_pct = r.get("c_vol_pct") if side == "call" else r.get("p_vol_pct")
    oi      = r["c_oi"]     if side == "call" else r["p_oi"]
    avg_oi  = r.get("c_avg_oi")  if side == "call" else r.get("p_avg_oi")
    oi_pct  = r.get("c_oi_pct")  if side == "call" else r.get("p_oi_pct")
    spot    = r.get("spot")
    price_str = ("$" + f"{spot:.2f}") if spot else "—"
    badges = _idx_badges_html(r)
    tk = r["ticker"]
    _na = '<span class="pct-na">—</span>'
    oi_disp     = _na if (oi     is None or int(oi)     == 0) else _fmt_k(oi)
    avg_oi_disp = _na if (avg_oi is None or int(avg_oi) == 0) else _fmt_k(avg_oi)
    return (
        f'<tr>'
        f'<td><span class="ticker-badge tk-{tk}">{_e(tk)}</span>{badges}</td>'
        f'<td>{_fmt_k(vol)}</td>'
        f'<td>{_fmt_k(avg_vol)}</td>'
        f'<td>{_fmt_vol_pct(vol_pct)}</td>'
        f'<td>{_fmt_pc(r.get("pc_ratio"))}</td>'
        f'<td>{price_str}</td>'
        f'<td>{_fmt_px_pct(r.get("px_pct_chg"))}</td>'
        f'<td>{oi_disp}</td>'
        f'<td>{avg_oi_disp}</td>'
        f'<td>{_fmt_oi_pct(oi_pct)}</td>'
        f'<td>{_fmt_k(r.get("opt_vol"))}</td>'
        f'</tr>'
    )


def _ovi_table_html(rows: list[dict], side: str) -> str:
    vol_h = "C Vol" if side == "call" else "P Vol"
    oi_h  = "C OI"  if side == "call" else "P OI"
    if not rows:
        return '<p class="empty">No unusual activity exceeding threshold today.</p>'
    for i, r in enumerate(rows):
        r["_alt"] = bool(i % 2)
    tbody = "".join(_ovi_row_html(r, side) for r in rows)
    return (
        f'<div class="ovi-scroll"><table class="ovi-tbl">'
        f'<thead><tr>'
        f'<th>Security</th><th>{vol_h}</th><th>Avg</th><th>%Chg</th>'
        f'<th>P/C</th><th>Price</th><th>Px%Chg</th>'
        f'<th>{oi_h}</th><th>Avg OI</th><th>OI%Chg</th><th>Opt Vol</th>'
        f'</tr></thead><tbody>{tbody}</tbody></table></div>'
    )


def _also_notable_html(rows: list[dict], side: str) -> str:
    """Compact 'Also notable: TICKER+X%, ...' line for positions 11-15."""
    if not rows:
        return ""
    parts = []
    for r in rows:
        ticker = r["ticker"]
        pct = r.get("c_vol_pct") if side == "call" else r.get("p_vol_pct")
        if pct is not None:
            sign = "+" if pct >= 0 else ""
            parts.append(f"{ticker} {sign}{pct:.0f}%")
        else:
            vol = r["c_vol"] if side == "call" else r["p_vol"]
            parts.append(f"{ticker} {_fmt_k(vol)}")
    return f'<p class="also-notable">Also notable: {", ".join(_e(p) for p in parts)}</p>'


def _extreme_alerts_html(extreme_alerts: list) -> str:
    """Render extreme alert rows (>300% spike) above OVI tables."""
    if not extreme_alerts:
        return ""
    rows = []
    for a in extreme_alerts[:5]:
        tk   = a["ticker"]
        pct  = f"+{a['pct']:.0f}%"
        side = a["side"].upper()
        color = _z_color(a["pct"])
        rows.append(
            f'<div class="extreme-alert">'
            f'<span class="ticker-badge tk-{tk}">{_e(tk)}</span>'
            f'<div>'
            f'<strong style="color:{color}">{pct}</strong> {side} vs 20d avg'
            f'<span class="extreme-badge">EXTREME SPIKE</span>'
            f'</div>'
            f'</div>'
        )
    return '<div class="extreme-alerts-box">' + "".join(rows) + "</div>"


def _universe_coverage_html(ovi_report: dict) -> str:
    """Render universe coverage line below OVI tables."""
    scanned   = ovi_report.get("universe_scanned", 0)
    threshold = ovi_report.get("tickers_above_threshold", 0)
    idx       = ovi_report.get("index_breakdown", {})
    if not scanned:
        return ""
    sp  = idx.get("sp500_active", 0)
    ndx = idx.get("ndx_active", 0)
    rut = idx.get("rut_active", 0)
    idx_str = f"SP500: {sp} | NDX: {ndx} | RUT: {rut}" if idx else ""
    return (
        f'<p class="universe-coverage">'
        f'Scanned {scanned:,} tickers | '
        f'{threshold} exceeded 50% threshold | '
        f'Showing top 10 each side'
        f'{(" | " + idx_str) if idx_str else ""}'
        f'</p>'
    )


def _ovi_card_html(ovi_report: dict, copy_ts: str) -> str:
    top_calls      = ovi_report.get("top_calls", [])
    top_puts       = ovi_report.get("top_puts", [])
    also_calls     = ovi_report.get("also_calls", [])
    also_puts      = ovi_report.get("also_puts", [])
    analysis       = ovi_report.get("analysis", "")
    analysis_ts    = _fmt_ts_short(ovi_report.get("analysis_ts", ""))
    market_context = ovi_report.get("market_context", "")
    extreme_alerts = ovi_report.get("extreme_alerts", [])

    extreme_html    = _extreme_alerts_html(extreme_alerts)
    coverage_html   = _universe_coverage_html(ovi_report)
    calls_html      = _ovi_table_html(top_calls, "call")
    also_calls_html = _also_notable_html(also_calls, "call")
    puts_html       = _ovi_table_html(top_puts,  "put")
    also_puts_html  = _also_notable_html(also_puts, "put")
    analysis_escaped = _e(analysis)

    ctx_section = ""
    if market_context:
        ctx_escaped = _e(market_context)
        ctx_section = (
            f'<div class="ctx-section">'
            f'<button class="ctx-toggle" onclick="toggleCtx(event)">'
            f'\U0001F4F0 Context fed to AI \u25bc</button>'
            f'<pre id="ctx-text" class="ctx-text">{ctx_escaped}</pre>'
            f'</div>'
        )

    return f"""
    <div id="ovi-panel" class="card ovi-full">
      <div class="card-header" style="display:flex;align-items:center;justify-content:space-between;">
        <span>📊 OPTION VOLUME INTELLIGENCE</span>
        <button onclick="exportPanel('ovi-panel','OVI_Table')" class="export-btn">📸 Save Table</button>
      </div>
      <div class="card-body">
        {extreme_html}
        {coverage_html}
        <div class="ovi-section-lbl call-lbl">🟢 Top By Calls — Equity / ETF</div>
        {calls_html}
        {also_calls_html}
        <div class="ovi-section-lbl put-lbl">🔴 Top By Puts — Equity / ETF</div>
        {puts_html}
        {also_puts_html}
        {ctx_section}
        <div id="flow-pulse-panel" class="ai-box">
          <div class="ai-box-hdr">
            <span class="ai-box-title">FLOW PULSE — AI ANALYSIS</span>
            <div style="display:flex;gap:6px;align-items:center;">
              <button id="copy-btn" class="copy-btn" data-ts="{_e(analysis_ts)}" onclick="copyAnalysis()">📋 Copy Analysis</button>
              <button onclick="exportPanel('flow-pulse-panel','Flow_Pulse')" class="export-btn">📸 Save Analysis</button>
            </div>
          </div>
          <pre id="ai-text" class="ai-text">{analysis_escaped}</pre>
          <div class="ai-ts">Analysis as of {_e(analysis_ts)}</div>
          <div class="ai-disclaimer">Flow observations only. Inferences require confirmation from strike, tenor, and opening/closing data.</div>
        </div>
      </div>
    </div>"""


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
        <span class="ticker-badge tk-{row['ticker']}">{ticker}</span>
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


def _card_html(title: str, rows: list[dict], extra_html: str = "", extra_class: str = "") -> str:
    cls = f'card{" " + extra_class if extra_class else ""}'
    if rows:
        body = "".join(_signal_row_html(r) for r in rows)
    else:
        body = '<p class="empty">No signals this session.</p>'
    return f"""
    <div class="{cls}">
      <div class="card-header">{title}</div>
      <div class="card-body scroll">
        {body}
        {extra_html}
      </div>
    </div>"""


def _watchlist_badges() -> str:
    return " ".join(f'<span class="ticker-badge wl-badge tk-{t}">{_e(t)}</span>' for t in WATCHLIST)


def _13f_match_card_html(matches: list[dict]) -> str:
    """Render the 13F Flow Match panel with styled match cards."""
    if not matches:
        body = (
            '<p class="empty">No 13F overlap with today\'s OVI tickers. '
            'Check back as more names print.</p>'
        )
    else:
        cards = []
        for m in matches:
            side_cls   = "bull" if m["side"] == "CALL" else "bear"
            side_color = "#4ade80" if m["side"] == "CALL" else "#f87171"
            align_bg   = "rgba(74,222,128,0.06)" if m["side"] == "CALL" else "rgba(248,113,113,0.06)"
            align_border = "#4ade80" if m["side"] == "CALL" else "#f87171"
            funds_html = " ".join(
                f'<span class="f13-fund">{_e(f)}</span>' for f in m["top_funds"]
            )
            more = f' <span class="f13-more">+{m["fund_count"] - len(m["top_funds"])} more</span>' \
                   if m["fund_count"] > len(m["top_funds"]) else ""
            spot_str = f'${m["spot"]:.2f}' if m.get("spot") else ""
            cards.append(
                f'<div class="f13-card" style="border-left-color:{align_border};background:{align_bg}">'
                f'<div class="f13-top">'
                f'<span class="ticker-badge tk-{m["ticker"]}">{_e(m["ticker"])}</span>'
                f'<span class="f13-side" style="color:{side_color}">{m["side"]}</span>'
                f'<span class="f13-pct" style="color:{side_color}">{_e(m["pct_str"])}</span>'
                f'{"<span class=f13-spot>" + _e(spot_str) + "</span>" if spot_str else ""}'
                f'<span class="f13-align f13-{side_cls}">{m["alignment"]}</span>'
                f'</div>'
                f'<div class="f13-funds">{funds_html}{more}'
                f'<span class="f13-aum">${m["total_val_m"]:,.0f}M AUM</span>'
                f'</div>'
                f'</div>'
            )
        body = "\n".join(cards)
    return f"""
    <div class="card pos-full">
      <div class="card-header">&#128202; 13F FLOW MATCH</div>
      <div class="card-body scroll">
        {body}
      </div>
    </div>"""


# ── GEX Chart Panel ───────────────────────────────────────────────────────────

def _sanitize(obj):
    """Recursively replace NaN/Inf floats with 0 for safe JSON serialization."""
    if isinstance(obj, float):
        return 0.0 if not math.isfinite(obj) else obj
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj


def _gex_panel_html(gex_chart_data: dict | None, gex_heatmap_data: dict | None = None) -> str:
    if not gex_chart_data:
        return (
            '<div id="gex-chart-panel" class="card gex-viz-full">'
            '<div class="card-header">\u26a1 GAMMA EXPOSURE \u2014 STRIKE BREAKDOWN</div>'
            '<div class="card-body"><p class="empty">No GEX chart data available.</p></div>'
            '</div>'
        )

    data_json = json.dumps(_sanitize(gex_chart_data))
    tickers = list(gex_chart_data.keys())
    default_ticker = tickers[0] if tickers else ""

    tabs_html = "".join(
        f'<button class="gex-tab{"  gex-tab-active" if i == 0 else ""}" '
        f'data-t="{_e(t)}" onclick="switchGexTicker(\'{_e(t)}\')">{_e(t)}</button>'
        for i, t in enumerate(tickers)
    )

    # Data script — f-string safe (data_json may contain { } but that's fine as a substituted value)
    heatmap_json = json.dumps(_sanitize(gex_heatmap_data or {}))
    data_script = (
        f'<script>const GEX_DATA = {data_json};'
        f' const GEX_DEFAULT = "{_e(default_ticker)}";'
        f' const GEX_HEATMAP = {heatmap_json};</script>'
    )

    # D3 rendering script — plain string so { } in JS are literal, no f-string escaping needed
    d3_script = """<script>
(function() {
  if (typeof d3 === 'undefined') { console.warn('[gex] D3 not loaded'); return; }
  var curTicker = GEX_DEFAULT || Object.keys(GEX_DATA)[0] || 'SPY';

  window.switchGexTicker = function(t) {
    curTicker = t;
    document.querySelectorAll('.gex-tab').forEach(function(b) { b.classList.remove('gex-tab-active'); });
    var btn = document.querySelector('.gex-tab[data-t="' + t + '"]');
    if (btn) btn.classList.add('gex-tab-active');
    renderAll();
  };

  // Toggle state
  var _showNet    = true;   // Net GEX bars vs split call/put
  var _showLevels = true;   // Key gamma levels overlay
  var _showZdte   = true;   // 0DTE column

  window.gexToggle = function(which) {
    if (which === 'net')    { _showNet    = !_showNet;    _syncBtn('gex-toggle-net',    _showNet);    }
    if (which === 'levels') { _showLevels = !_showLevels; _syncBtn('gex-toggle-levels', _showLevels); }
    if (which === 'zdte')   { _showZdte   = !_showZdte;   _syncBtn('gex-toggle-zdte',   _showZdte);
      var col = document.getElementById('gex-zdte-col');
      if (col) col.style.display = _showZdte ? '' : 'none';
    }
    renderAll();
  };

  function _syncBtn(id, active) {
    var b = document.getElementById(id);
    if (!b) return;
    if (active) b.classList.add('gex-toggle-active');
    else b.classList.remove('gex-toggle-active');
  }

  function renderAll() {
    var d = GEX_DATA[curTicker] || {};
    renderRegime(d);
    renderKeyLevels(d);
    renderDiagnostics(d);
    drawStrikeBars('gex-strike-chart', d.strikes || [], d);
    drawPriceLine(d);
    drawStrikeBars('gex-0dte-chart', d.zdtes || [], d);
    if (typeof drawHeatmap === 'function') drawHeatmap('gex-heatmap-chart', curTicker);
  }

  function renderRegime(d) {
    var gex = d.net_gex_m || 0;
    var sign = gex >= 0 ? '+' : '';
    var isPos = gex >= 0;
    var gexColor = isPos ? '#4169E1' : '#DC5078';
    var stab = d.stability_pct != null ? Math.round(d.stability_pct) : null;
    var stabColor = stab != null ? (stab > 60 ? '#4169E1' : stab < 30 ? '#DC5078' : '#D0D860') : '#8B949E';

    var spotEl = document.getElementById('gex-ss-spot');
    if (spotEl) spotEl.textContent = d.spot ? '$' + Number(d.spot).toLocaleString(undefined, {minimumFractionDigits:2,maximumFractionDigits:2}) : '\u2014';

    var netEl = document.getElementById('gex-ss-netgex');
    if (netEl) { netEl.textContent = '$' + sign + gex.toFixed(1) + 'M'; netEl.style.color = gexColor; }

    var flipEl = document.getElementById('gex-ss-flip');
    if (flipEl) {
      var flipTxt = d.flip_note || (d.flip_point ? 'Gamma Flip $' + d.flip_point + ' (spot-scan)' : '\u2014');
      flipEl.textContent = d.flip_point ? '$' + d.flip_point : '\u2014';
      flipEl.title = flipTxt;
    }

    var stabEl = document.getElementById('gex-ss-stab');
    var p30 = d.pctiles_30d || {};
    if (stabEl) {
      var stabTxt = stab != null ? 'Net GEX: ' + stab + 'th pct (30d)' : '\u2014';
      if (p30.abs_gex_pctile != null) stabTxt += ' | Abs: ' + Math.round(p30.abs_gex_pctile) + 'th';
      stabEl.textContent = stab != null ? stab + 'th pct' : '\u2014';
      stabEl.title = stabTxt;
      stabEl.style.color = stabColor;
    }

    var regEl = document.getElementById('gex-ss-regime');
    if (regEl) {
      var regLabel = d.regime_label || d.regime_summary || (isPos ? 'Positive GEX' : 'Negative GEX');
      var subflags = (d.regime_subflags || []).join(' · ');
      regEl.textContent = regLabel.length > 35 ? regLabel.slice(0, 35) + '\u2026' : regLabel;
      regEl.title = regLabel + (subflags ? '  [' + subflags + ']' : '') + '  (estimated)';
      regEl.style.color = gexColor;
    }

    var tkEl = document.getElementById('gex-ss-ticker');
    if (tkEl) tkEl.textContent = d.ticker || '';
  }

  function renderKeyLevels(d) {
    var bar = document.getElementById('gex-key-levels-bar');
    if (!bar) return;
    if (!_showLevels) { bar.innerHTML = ''; bar.style.display = 'none'; return; }
    bar.style.display = 'flex';
    var items = [];
    if (d.flip_point != null) items.push(
      '<span class="kl-item kl-flip" title="Gamma Flip (spot-scan)">\u21c6 Gamma Flip (spot-scan) <b>$' + d.flip_point + '</b></span>'
    );
    if (d.call_wall != null) items.push(
      '<span class="kl-item kl-call" title="Top Call Wall — highest call GEX strike">Top Call Wall <b>$' + d.call_wall + '</b></span>'
    );
    if (d.put_wall != null) items.push(
      '<span class="kl-item kl-put" title="Top Put Wall — highest put GEX strike">Top Put Wall <b>$' + d.put_wall + '</b></span>'
    );
    if (d.max_gamma_strike != null) items.push(
      '<span class="kl-item kl-max" title="Max |Γ| — highest absolute gamma strike (magnet)">Max |\u0393| <b>$' + d.max_gamma_strike + '</b> (magnet)</span>'
    );
    bar.innerHTML = items.length
      ? '<span class="kl-label">Key Levels:</span> ' + items.join('')
      : '';
  }

  function renderDiagnostics(d) {
    var el = document.getElementById('gex-diag-body');
    if (!el) return;
    var diag = d.diagnostics || {};
    var p30  = d.pctiles_30d || {};
    var now  = d._fetched_at ? new Date(d._fetched_at) : null;
    var ageS = now ? Math.round((Date.now() - now.getTime()) / 1000) : null;
    var stale = ageS != null && ageS > 300;
    var rows = [
      ['Contracts included', diag.included != null ? diag.included : '—'],
      ['Contracts excluded (zero OI)', diag.zero_oi != null ? diag.zero_oi : '—'],
      ['Missing IV (vendor gamma used)', diag.missing_iv != null ? diag.missing_iv : '—'],
      ['GEX formula', 'γ × OI × multiplier × spot² × 0.01 × sign (1% move)'],
      ['Flip method', 'Spot-scan zero-crossing (±10% grid, 0.5% steps)'],
      ['Sign model', 'Classic (calls +1, puts −1) — ESTIMATED'],
      ['Multiplier', '100 (standard equity/ETF)'],
      ['Net GEX pctile (30d)', p30.net_gex_pctile != null ? Math.round(p30.net_gex_pctile) + 'th  (z=' + p30.net_gex_zscore + ')' : '—'],
      ['Abs GEX pctile (30d)', p30.abs_gex_pctile != null ? Math.round(p30.abs_gex_pctile) + 'th  (z=' + p30.abs_gex_zscore + ')' : '—'],
      ['0DTE share pctile (30d)', p30.zdte_share_pctile != null ? Math.round(p30.zdte_share_pctile) + 'th  (z=' + p30.zdte_share_zscore + ')' : '—'],
      ['History snapshots (30d)', p30.history_count != null ? p30.history_count : '—'],
      ['Data age', ageS != null ? ageS + 's' + (stale ? ' ⚠ STALE (>5 min)' : '') : '—'],
    ];
    var html = '<table class="diag-tbl">';
    rows.forEach(function(r) {
      html += '<tr><td class="diag-k">' + r[0] + '</td><td class="diag-v">' + r[1] + '</td></tr>';
    });
    html += '</table>';
    if (stale) html = '<div class="diag-stale">\u26a0 Data is stale (&gt;5 min old)</div>' + html;
    el.innerHTML = html;
  }

  function drawStrikeBars(elemId, strikes, d) {
    var container = document.getElementById(elemId);
    if (!container) return;
    container.innerHTML = '';
    if (!strikes || !strikes.length) {
      container.innerHTML = '<p class="empty">' +
        (elemId === 'gex-0dte-chart' ? 'No 0DTE gamma (non-expiry or weekend)' : 'No strike data') +
        '</p>';
      return;
    }

    var barH = strikes.length > 35 ? 5 : strikes.length > 20 ? 7 : 9;
    var m = {top: 10, right: 68, bottom: 22, left: 50};
    var W = container.offsetWidth || 340;
    var w = W - m.left - m.right;
    var H = Math.min(460, Math.max(140, strikes.length * (barH + 2.5) + m.top + m.bottom));
    var h = H - m.top - m.bottom;

    var strikeVals = strikes.map(function(s) { return s.strike; });
    var minS = d3.min(strikeVals), maxS = d3.max(strikeVals);
    var pad = strikeVals.length > 1 ? (maxS - minS) / (strikeVals.length - 1) * 0.55 : 3;

    var yScale = d3.scaleLinear()
      .domain([minS - pad, maxS + pad])
      .range([h, 0]);

    var svg = d3.select(container).append('svg')
      .attr('width', W).attr('height', H).style('overflow', 'visible');
    var g = svg.append('g').attr('transform', 'translate(' + m.left + ',' + m.top + ')');

    if (_showNet) {
      // ── Net GEX mode: single bar, positive right (blue), negative left (red) ──
      var maxAbsNet = d3.max(strikes, function(s) { return Math.abs(s.net_gex_m || 0); }) || 1;
      var xZero = w / 2;
      var xNetScale = d3.scaleLinear().domain([-maxAbsNet * 1.1, maxAbsNet * 1.1]).range([0, w]);

      g.selectAll('.nbar').data(strikes).enter().append('rect')
        .attr('class', 'nbar')
        .attr('x', function(s) { var v = s.net_gex_m || 0; return v >= 0 ? xZero : xNetScale(v); })
        .attr('y', function(s) { return yScale(s.strike) - barH / 2; })
        .attr('width', function(s) {
          var v = s.net_gex_m || 0;
          return Math.max(0, v >= 0 ? xNetScale(v) - xZero : xZero - xNetScale(v));
        })
        .attr('height', barH)
        .attr('fill', function(s) { return (s.net_gex_m || 0) >= 0 ? '#4169E1' : '#DC5078'; })
        .attr('opacity', 0.85);

      // Zero divider
      g.append('line')
        .attr('x1', xZero).attr('y1', 0).attr('x2', xZero).attr('y2', h)
        .attr('stroke', '#4b5563').attr('stroke-width', 1);

      // X axis labels
      g.append('text').attr('x', xZero / 2).attr('y', h + 15)
        .attr('text-anchor', 'middle').attr('fill', '#DC5078').attr('font-size', '8px')
        .text('\u2190 Net Neg GEX');
      g.append('text').attr('x', xZero + xZero / 2).attr('y', h + 15)
        .attr('text-anchor', 'middle').attr('fill', '#4169E1').attr('font-size', '8px')
        .text('Net Pos GEX \u2192');
    } else {
      // ── Split mode: calls right (blue), puts left (red) ──
      var maxGex = d3.max(strikes, function(s) {
        return Math.max(s.call_gex_m || 0, s.put_gex_m || 0);
      }) || 1;
      var xHalf = w / 2;
      var xScale = d3.scaleLinear().domain([0, maxGex * 1.1]).range([0, xHalf]);

      g.selectAll('.cbar').data(strikes).enter().append('rect')
        .attr('class', 'cbar')
        .attr('x', xHalf)
        .attr('y', function(s) { return yScale(s.strike) - barH / 2; })
        .attr('width', function(s) { return Math.max(0, xScale(s.call_gex_m || 0)); })
        .attr('height', barH)
        .attr('fill', '#4169E1').attr('opacity', 0.85);

      g.selectAll('.pbar').data(strikes).enter().append('rect')
        .attr('class', 'pbar')
        .attr('x', function(s) { return xHalf - xScale(s.put_gex_m || 0); })
        .attr('y', function(s) { return yScale(s.strike) - barH / 2; })
        .attr('width', function(s) { return Math.max(0, xScale(s.put_gex_m || 0)); })
        .attr('height', barH)
        .attr('fill', '#DC5078').attr('opacity', 0.85);

      g.append('line')
        .attr('x1', xHalf).attr('y1', 0).attr('x2', xHalf).attr('y2', h)
        .attr('stroke', '#4b5563').attr('stroke-width', 1);

      g.append('text').attr('x', xHalf / 2).attr('y', h + 15)
        .attr('text-anchor', 'middle').attr('fill', '#DC5078').attr('font-size', '8px')
        .text('\u2190 Puts');
      g.append('text').attr('x', xHalf + xHalf / 2).attr('y', h + 15)
        .attr('text-anchor', 'middle').attr('fill', '#4169E1').attr('font-size', '8px')
        .text('Calls \u2192');
    }

    // ── Spot price dashed line ──────────────────────────────────────────────
    if (d.spot && d.spot > 0) {
      var spotY = yScale(d.spot);
      g.append('line')
        .attr('x1', 0).attr('y1', spotY).attr('x2', w).attr('y2', spotY)
        .attr('stroke', '#f9fafb').attr('stroke-width', 1.5)
        .attr('stroke-dasharray', '4,3');
      g.append('text')
        .attr('x', w + 4).attr('y', spotY + 4)
        .attr('fill', '#f9fafb').attr('font-size', '9px').text('SPOT');
    }

    // ── Key level reference lines (when Key Levels is on) ──────────────────
    if (_showLevels) {
      var levelDefs = [
        { val: d.flip_point,       label: 'Flip',     color: '#a855f7' },
        { val: d.call_wall,        label: 'Call Wall', color: '#60a5fa' },
        { val: d.put_wall,         label: 'Put Wall',  color: '#f87171' },
        { val: d.max_gamma_strike, label: 'Max\u0393', color: '#fbbf24' },
      ];
      levelDefs.forEach(function(lv) {
        if (lv.val == null) return;
        var dom = yScale.domain();
        if (lv.val < dom[0] || lv.val > dom[1]) return;
        var ly = yScale(lv.val);
        g.append('line')
          .attr('x1', 0).attr('y1', ly).attr('x2', w).attr('y2', ly)
          .attr('stroke', lv.color).attr('stroke-width', 0.8)
          .attr('stroke-dasharray', '3,3').attr('opacity', 0.7);
        g.append('text')
          .attr('x', -3).attr('y', ly + 3)
          .attr('text-anchor', 'end')
          .attr('fill', lv.color).attr('font-size', '7px').attr('font-weight', '700')
          .text(lv.label);
      });
    }

    // ── Y axis ─────────────────────────────────────────────────────────────
    var tickCount = Math.min(10, strikeVals.length);
    var step = Math.ceil(strikeVals.length / tickCount);
    var tickVals = strikeVals.filter(function(_, i) { return i % step === 0; });
    g.append('g')
      .call(d3.axisLeft(yScale).tickValues(tickVals).tickFormat(function(v) { return '$' + v; }))
      .selectAll('text').attr('fill', '#9ca3af').attr('font-size', '8px');
    g.selectAll('.domain').attr('stroke', '#374151');
    g.selectAll('.tick line').attr('stroke', '#374151');
  }

  function drawPriceLine(d) {
    var bars = d.price_bars || [];
    var container = document.getElementById('gex-price-chart');
    if (!container) return;
    container.innerHTML = '';
    if (!bars.length) {
      container.innerHTML = '<p class="empty">No intraday data (market closed or weekend)</p>';
      return;
    }

    var m = {top: 10, right: 62, bottom: 28, left: 55};
    var W = container.offsetWidth || 340;
    var w = W - m.left - m.right;
    var H = 220, h = H - m.top - m.bottom;

    var allC = bars.map(function(b) { return b.c; });
    var keyS = (d.key_strikes || []).filter(function(ks) {
      return ks > d3.min(allC) * 0.998 && ks < d3.max(allC) * 1.002;
    });
    var prices = allC.concat(keyS);
    var minP = d3.min(prices) * 0.9995, maxP = d3.max(prices) * 1.0005;
    var minT = d3.min(bars, function(b) { return b.t; });
    var maxT = d3.max(bars, function(b) { return b.t; });

    var xScale = d3.scaleLinear().domain([minT, maxT]).range([0, w]);
    var yScale = d3.scaleLinear().domain([minP, maxP]).range([h, 0]);

    var svg = d3.select(container).append('svg').attr('width', W).attr('height', H);
    var g = svg.append('g').attr('transform', 'translate(' + m.left + ',' + m.top + ')');

    // Light grid
    g.append('g').call(
      d3.axisLeft(yScale).ticks(5).tickSize(-w).tickFormat('')
    ).selectAll('line').attr('stroke', '#1e2530').attr('stroke-width', 0.5);
    g.selectAll('.grid .domain').attr('stroke', 'none');

    // Key strike reference lines (legacy key_strikes list)
    if (_showLevels) {
      keyS.forEach(function(ks) {
        g.append('line')
          .attr('x1', 0).attr('y1', yScale(ks)).attr('x2', w).attr('y2', yScale(ks))
          .attr('stroke', '#fbbf24').attr('stroke-width', 0.8)
          .attr('stroke-dasharray', '3,3');
        g.append('text')
          .attr('x', w + 3).attr('y', yScale(ks) + 3)
          .attr('fill', '#fbbf24').attr('font-size', '8px')
          .text('$' + ks);
      });

      // Named key levels overlay on the price chart
      var priceLevels = [
        { val: d.flip_point,       label: 'Flip',      color: '#a855f7' },
        { val: d.call_wall,        label: 'Call Wall',  color: '#60a5fa' },
        { val: d.put_wall,         label: 'Put Wall',   color: '#f87171' },
        { val: d.max_gamma_strike, label: 'Max\u0393',  color: '#fbbf24' },
      ];
      priceLevels.forEach(function(lv) {
        if (lv.val == null) return;
        if (lv.val < minP || lv.val > maxP) return;
        var ly = yScale(lv.val);
        g.append('line')
          .attr('x1', 0).attr('y1', ly).attr('x2', w).attr('y2', ly)
          .attr('stroke', lv.color).attr('stroke-width', 0.8)
          .attr('stroke-dasharray', '5,3').attr('opacity', 0.8);
        g.append('text')
          .attr('x', w + 3).attr('y', ly + 3)
          .attr('fill', lv.color).attr('font-size', '7px').attr('font-weight', '600')
          .text(lv.label);
      });
    }

    var firstC = bars[0].c, lastC = bars[bars.length - 1].c;
    var lineColor = lastC >= firstC ? '#22c55e' : '#ef4444';

    var line = d3.line()
      .x(function(b) { return xScale(b.t); })
      .y(function(b) { return yScale(b.c); });

    g.append('path').datum(bars)
      .attr('fill', 'none').attr('stroke', lineColor)
      .attr('stroke-width', 1.5).attr('d', line);

    // X axis (time)
    g.append('g').attr('transform', 'translate(0,' + h + ')')
      .call(d3.axisBottom(xScale).ticks(4).tickFormat(function(t) {
        var dt = new Date(t);
        return dt.getHours().toString().padStart(2,'0') + ':' +
               dt.getMinutes().toString().padStart(2,'0');
      }))
      .selectAll('text').attr('fill', '#9ca3af').attr('font-size', '9px');

    // Y axis (price)
    g.append('g').call(d3.axisLeft(yScale).ticks(5).tickFormat(function(v) {
      return '$' + (v >= 1000 ? v.toFixed(0) : v.toFixed(1));
    }))
      .selectAll('text').attr('fill', '#9ca3af').attr('font-size', '9px');

    g.selectAll('.domain').attr('stroke', '#374151');
    g.selectAll('.tick line').attr('stroke', '#374151');
  }

  // Kick off on load
  renderAll();
})();
</script>"""

    panel_html = (
        f'<div id="gex-chart-panel" class="card gex-viz-full">'
        f'<div class="card-header" style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:6px;">'
        f'<span>\u26a1 GAMMA EXPOSURE \u2014 STRIKE BREAKDOWN</span>'
        f'<div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">'
        f'<button id="gex-toggle-net" class="gex-toggle-btn gex-toggle-active" onclick="gexToggle(\'net\')" title="Toggle net GEX vs split call/put bars">Net GEX</button>'
        f'<button id="gex-toggle-levels" class="gex-toggle-btn gex-toggle-active" onclick="gexToggle(\'levels\')" title="Show/hide key gamma levels">Key Levels</button>'
        f'<button id="gex-toggle-zdte" class="gex-toggle-btn gex-toggle-active" onclick="gexToggle(\'zdte\')" title="Show/hide 0DTE gamma">0DTE</button>'
        f'<button onclick="exportPanel(\'gex-chart-panel\',\'GEX_Chart\')" class="export-btn">\U0001F4F8 Save Chart</button>'
        f'</div>'
        f'</div>'
        f'<div class="card-body">'
        f'<div class="gex-tabs">{tabs_html}</div>'
        f'<div class="gex-stats-strip" id="gex-stats-strip">'
        f'<div class="gex-ss-item"><div class="gex-ss-lbl">TICKER</div><div class="gex-ss-val" id="gex-ss-ticker">\u2014</div></div>'
        f'<div class="gex-ss-sep"></div>'
        f'<div class="gex-ss-item"><div class="gex-ss-lbl">SPOT</div><div class="gex-ss-val" id="gex-ss-spot">\u2014</div></div>'
        f'<div class="gex-ss-sep"></div>'
        f'<div class="gex-ss-item"><div class="gex-ss-lbl">NET GEX (1% move)</div><div class="gex-ss-val" id="gex-ss-netgex">\u2014</div></div>'
        f'<div class="gex-ss-sep"></div>'
        f'<div class="gex-ss-item"><div class="gex-ss-lbl">GAMMA FLIP (spot-scan)</div><div class="gex-ss-val" id="gex-ss-flip">\u2014</div></div>'
        f'<div class="gex-ss-sep"></div>'
        f'<div class="gex-ss-item"><div class="gex-ss-lbl">STABILITY</div><div class="gex-ss-val" id="gex-ss-stab">\u2014</div></div>'
        f'<div class="gex-ss-sep"></div>'
        f'<div class="gex-ss-item gex-ss-regime-item"><div class="gex-ss-lbl">REGIME</div><div class="gex-ss-val" id="gex-ss-regime">\u2014</div></div>'
        f'</div>'
        f'<div id="gex-key-levels-bar" class="gex-key-levels-bar"></div>'
        f'<div class="gex-chart-grid">'
        f'<div><div class="gex-sub-lbl">GEX by Strike</div>'
        f'<div id="gex-strike-chart"></div></div>'
        f'<div><div class="gex-sub-lbl">Intraday Price Path</div>'
        f'<div id="gex-price-chart"></div></div>'
        f'<div id="gex-zdte-col"><div class="gex-sub-lbl">0DTE Gamma</div>'
        f'<div id="gex-0dte-chart"></div></div>'
        f'</div>'
        f'<div class="gex-heatmap-section">'
        f'<div class="gex-sub-lbl">GEX Heatmap \u2014 Strike Profile Over Time</div>'
        f'<div id="gex-heatmap-chart" class="gex-heatmap-canvas-wrap"></div>'
        f'</div>'
        f'<details class="gex-diag-details" id="gex-diagnostics-panel">'
        f'<summary class="gex-diag-summary">Diagnostics &amp; Methodology</summary>'
        f'<div id="gex-diag-body" class="gex-diag-body">Loading\u2026</div>'
        f'</details>'
        f'</div></div>'
        f'{data_script}'
    ) + d3_script + _HEATMAP_SCRIPT

    return panel_html


# ── GEX Heatmap JS ────────────────────────────────────────────────────────────

_HEATMAP_SCRIPT = """<script>
function drawHeatmap(elemId, ticker) {
  var container = document.getElementById(elemId);
  if (!container) return;
  container.innerHTML = '';
  var hd = (typeof GEX_HEATMAP !== 'undefined' ? GEX_HEATMAP : {})[ticker] || {};
  var strikes = hd.strikes || [];
  var timestamps = hd.timestamps || [];
  var matrix = hd.matrix || [];
  var spotTrace = hd.spot_trace || [];
  if (!strikes.length || timestamps.length < 2) {
    container.innerHTML = '<p class="empty" style="padding:10px 4px;">Heatmap builds throughout the trading day (needs \u22652 poll cycles)</p>';
    return;
  }
  var m = {top: 8, right: 52, bottom: 22, left: 54};
  var W = container.offsetWidth || 500;
  var cellH = Math.max(3, Math.min(10, Math.floor(340 / strikes.length)));
  var H = Math.max(140, Math.min(480, strikes.length * cellH + m.top + m.bottom));
  var w = W - m.left - m.right;
  var h = H - m.top - m.bottom;
  var dpr = window.devicePixelRatio || 1;
  var canvas = document.createElement('canvas');
  canvas.width = W * dpr; canvas.height = H * dpr;
  canvas.style.width = W + 'px'; canvas.style.height = H + 'px';
  canvas.style.display = 'block';
  container.style.position = 'relative';
  container.appendChild(canvas);
  var ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.fillStyle = '#0d1117'; ctx.fillRect(0, 0, W, H);
  var minS = Math.min.apply(null, strikes), maxS = Math.max.apply(null, strikes);
  var yScale = d3.scaleLinear().domain([minS, maxS]).range([h, 0]);
  var cellW = w / timestamps.length;
  var absMax = 0.01;
  for (var si2 = 0; si2 < matrix.length; si2++) {
    for (var ti2 = 0; ti2 < (matrix[si2] || []).length; ti2++) {
      var av = Math.abs(matrix[si2][ti2] || 0); if (av > absMax) absMax = av;
    }
  }
  function cellColor(v) {
    if (v === 0) return null;
    var norm = Math.pow(Math.min(1, Math.abs(v) / absMax), 0.6);
    var r, g, b;
    if (v > 0) {
      // white-blue (#e8f0ff → #4169e1)
      r = Math.round(232 - norm * (232 - 65));
      g = Math.round(240 - norm * (240 - 105));
      b = Math.round(255 - norm * (255 - 225));
    } else {
      // white-pink (#ffe8f0 → #dc5078)
      r = Math.round(255 - norm * (255 - 220));
      g = Math.round(232 - norm * (232 - 80));
      b = Math.round(240 - norm * (240 - 120));
    }
    return 'rgba(' + r + ',' + g + ',' + b + ',0.92)';
  }
  ctx.save(); ctx.translate(m.left, m.top);
  for (var si = 0; si < strikes.length; si++) {
    var sy = yScale(strikes[si]);
    var sy2 = si + 1 < strikes.length ? yScale(strikes[si + 1]) : sy - cellH;
    var rh = Math.max(1, Math.abs(sy2 - sy)); var ry = Math.min(sy, sy2);
    if (!matrix[si]) continue;
    for (var ti = 0; ti < timestamps.length; ti++) {
      var v = matrix[si][ti] || 0;
      var cc = cellColor(v); if (!cc) continue;
      ctx.fillStyle = cc;
      ctx.fillRect(ti * cellW, ry, cellW + 0.5, rh + 0.5);
    }
  }
  if (spotTrace.length > 1) {
    ctx.strokeStyle = 'rgba(255,255,255,0.9)'; ctx.lineWidth = 1.5; ctx.lineJoin = 'round';
    ctx.beginPath(); var started = false;
    for (var i = 0; i < spotTrace.length; i++) {
      if (!spotTrace[i]) continue;
      var px = (i + 0.5) * cellW, py = yScale(spotTrace[i]);
      if (py < 0 || py > h) { started = false; continue; }
      if (!started) { ctx.moveTo(px, py); started = true; } else ctx.lineTo(px, py);
    }
    if (started) ctx.stroke();
  }
  ctx.restore();
  var svg = d3.select(container).append('svg')
    .style('position', 'absolute').style('top', 0).style('left', 0)
    .attr('width', W).attr('height', H).style('pointer-events', 'none');
  var ga = svg.append('g').attr('transform', 'translate(' + m.left + ',' + m.top + ')');
  ga.append('g').call(d3.axisLeft(yScale).ticks(7).tickFormat(function(v) { return '$' + v; }))
    .selectAll('text').attr('fill', '#9ca3af').attr('font-size', '8px');
  ga.selectAll('.domain').attr('stroke', 'none');
  ga.selectAll('.tick line').attr('stroke', '#374151').attr('stroke-width', 0.5);
  function fmtTs(ts) {
    try {
      var d = new Date(ts);
      var etH = (d.getUTCHours() - 4 + 24) % 24;
      return etH.toString().padStart(2, '0') + ':' + d.getUTCMinutes().toString().padStart(2, '0');
    } catch(e) { return ''; }
  }
  var tg = svg.append('g').attr('transform', 'translate(' + m.left + ',' + (m.top + h + 14) + ')');
  tg.append('text').attr('x', 0).attr('fill', '#9ca3af').attr('font-size', '8px').text(fmtTs(timestamps[0]));
  tg.append('text').attr('x', w).attr('text-anchor', 'end').attr('fill', '#9ca3af').attr('font-size', '8px').text(fmtTs(timestamps[timestamps.length - 1]) + ' ET');
  var cb = svg.append('g').attr('transform', 'translate(' + (m.left + w + 4) + ',' + m.top + ')');
  var cbSteps = 40, cbW = 8;
  for (var cs = 0; cs < cbSteps; cs++) {
    var frac = cs / (cbSteps - 1);       // 0 = top (+GEX), 1 = bottom (-GEX)
    var norm2 = Math.pow(frac <= 0.5 ? (1 - frac * 2) : (frac * 2 - 1), 0.6);
    var cr, cg, cb2;
    if (frac <= 0.5) {
      cr = Math.round(232 - norm2 * (232 - 65));
      cg = Math.round(240 - norm2 * (240 - 105));
      cb2 = Math.round(255 - norm2 * (255 - 225));
    } else {
      cr = Math.round(255 - norm2 * (255 - 220));
      cg = Math.round(232 - norm2 * (232 - 80));
      cb2 = Math.round(240 - norm2 * (240 - 120));
    }
    cb.append('rect').attr('x', 0).attr('y', Math.round(frac * h))
      .attr('width', cbW).attr('height', Math.ceil(h / cbSteps) + 1)
      .attr('fill', 'rgba(' + cr + ',' + cg + ',' + cb2 + ',0.92)');
  }
  cb.append('text').attr('x', cbW + 2).attr('y', 8).attr('fill', '#9ca3af').attr('font-size', '7px').text('+GEX');
  cb.append('text').attr('x', cbW + 2).attr('y', h).attr('fill', '#9ca3af').attr('font-size', '7px').text('\u2212GEX');
}
</script>"""


# ── Main renderer ─────────────────────────────────────────────────────────────

def render(title: str | None = None, gex_chart_data: dict | None = None,
           gex_heatmap_data: dict | None = None) -> str:
    title = title or DASHBOARD_TITLE
    now_et = _now_et()
    updated = now_et.strftime("%b %d %Y %H:%M ET")
    market_open = _is_market_open()
    market_label = "OPEN" if market_open else "CLOSED"
    market_cls = "status-open" if market_open else "status-closed"

    alerts = get_today_alerts()
    total_today = len(alerts)

    gex_rows = _build_signal_rows(alerts, "GEX")
    skew_rows = _build_signal_rows(alerts, "SKEW")
    pos_rows = _build_signal_rows(alerts, "POSITIONING")

    latest_gex = get_latest_gex_all()
    gex_table = _build_gex_table(latest_gex)
    gex_extra = _gex_table_html(gex_table)

    ovi_report = get_latest_ovi_report() or {}
    analysis_ts = _fmt_ts_short(ovi_report.get("analysis_ts", ""))

    ovi_card  = _ovi_card_html(ovi_report, analysis_ts)
    gex_panel = _gex_panel_html(gex_chart_data, gex_heatmap_data)
    gex_card  = _card_html("⚡ GEX EXTREMES", gex_rows, gex_extra)
    skew_card = _card_html("⚡ SKEW EXTREMES", skew_rows)
    f13_matches = build_13f_matches(ovi_report)
    pos_card  = _13f_match_card_html(f13_matches)

    wl_badges = _watchlist_badges()

    # ── Header stat card computation ──────────────────────────────────────
    spy_gex = next((r for r in gex_table if r["ticker"] == "SPY"), gex_table[0] if gex_table else None)
    hdr_gex_val = f'${spy_gex["net_gex_m"]:+.1f}M' if spy_gex else "—"
    hdr_gex_ctx = "put-dominated" if (spy_gex and spy_gex["negative"]) else "call-dominated"
    hdr_gex_cls = "stat-neg" if (spy_gex and spy_gex["negative"]) else "stat-pos"

    top_calls = ovi_report.get("top_calls", [])
    pc_vals = [r.get("pc_ratio") for r in top_calls if r.get("pc_ratio") is not None]
    avg_pc = sum(pc_vals) / len(pc_vals) if pc_vals else None
    hdr_pc_val = f"{avg_pc:.2f}" if avg_pc else "—"
    hdr_pc_ctx = "put bias" if (avg_pc and avg_pc > 1) else "call bias"
    hdr_pc_cls = "stat-neg" if (avg_pc and avg_pc > 1) else "stat-pos"

    new_signals = sum(1 for a in alerts if _age_secs(a["timestamp"]) < 3600)

    # refresh_btn must be defined before header_html (header embeds it)
    refresh_btn = (
        '<div class="refresh-wrap">'
        '<button id="refresh-btn" onclick="triggerPoll()">&#9889; Refresh Now</button>'
        '<span id="refresh-status" class="refresh-status"></span>'
        '</div>'
    )

    header_html = f"""<header>
  <div class="hdr-top">
    <div class="hdr-brand">
      <img src="assets/LNlogo2.png" alt="Liquidnet" class="hdr-logo" onerror="this.style.display='none'">
      <div class="hdr-title-wrap">
        <div class="hdr-title">Options Flow Dashboard</div>
        <div class="hdr-subtitle">Powered by Polygon.io + Claude</div>
      </div>
    </div>
    <div class="hdr-right">
      <span class="hdr-time">{_e(updated)}</span>
      <span class="{market_cls}"><span class="market-dot"></span>{market_label}</span>
    </div>
  </div>
  <div class="hdr-stats">
    <div class="hdr-stat">
      <div class="hdr-stat-label">Net GEX (1% move, SPY)</div>
      <div class="hdr-stat-val {hdr_gex_cls}">{_e(hdr_gex_val)}</div>
      <div class="hdr-stat-ctx">{_e(hdr_gex_ctx)}</div>
    </div>
    <div class="hdr-stat">
      <div class="hdr-stat-label">Avg P/C Ratio</div>
      <div class="hdr-stat-val {hdr_pc_cls}">{_e(hdr_pc_val)}</div>
      <div class="hdr-stat-ctx">{_e(hdr_pc_ctx)}</div>
    </div>
    <div class="hdr-stat">
      <div class="hdr-stat-label">Signals Today</div>
      <div class="hdr-stat-val stat-neut">{total_today}</div>
      <div class="hdr-stat-ctx">{new_signals} in last hour</div>
    </div>
    <div class="hdr-stat" style="flex:2;">
      <div class="hdr-stat-label">Last Analysis</div>
      <div class="hdr-stat-val" style="font-size:13px;color:var(--text);">{_e(analysis_ts) if analysis_ts else '—'}</div>
      <div class="hdr-stat-ctx">Flow Pulse scan</div>
    </div>
    <div class="hdr-stat" style="justify-content:center;align-items:center;display:flex;flex-direction:row;gap:0;">
      {refresh_btn}
    </div>
  </div>
</header>"""

    # Build button / JS as plain strings (avoid f-string brace escaping in JS)
    poll_meta = (
        f'<meta name="poll-token" content="{_e(POLL_TOKEN)}">'
        f'<meta name="ntfy-topic" content="{_e(NTFY_TOPIC)}">'
    )
    refresh_js = """<script>
async function triggerPoll() {
  const btn = document.getElementById('refresh-btn');
  const status = document.getElementById('refresh-status');
  btn.disabled = true;
  btn.innerHTML = '&#9203; Polling&hellip;';
  status.textContent = '';
  status.className = 'refresh-status';
  try {
    const token = document.querySelector('meta[name="poll-token"]').content;
    const topic = document.querySelector('meta[name="ntfy-topic"]').content;
    const res = await fetch('https://ntfy.sh/' + topic, {
      method: 'POST',
      body: token
    });
    if (res.ok) {
      status.textContent = '\u2713 Poll triggered \u2014 reloading in 90s\u2026';
      status.className = 'refresh-status refresh-ok';
      setTimeout(() => location.reload(), 90000);
    } else {
      throw new Error('ntfy HTTP ' + res.status);
    }
  } catch(e) {
    status.textContent = '\u2717 ' + (e.message || 'offline');
    status.className = 'refresh-status refresh-err';
    btn.disabled = false; btn.innerHTML = '&#9889; Refresh Now';
  }
}
</script>"""

    copy_js = """<script>
function copyAnalysis() {
  var btn = document.getElementById('copy-btn');
  var ts = btn.getAttribute('data-ts') || '';
  var text = document.getElementById('ai-text').innerText;
  var ctxEl = document.getElementById('ctx-text');
  var ctxLines = '';
  if (ctxEl) {
    ctxLines = ctxEl.innerText.split('\\n').filter(function(l){ return l.startsWith('-'); }).slice(0,5).join('\\n');
  }
  var now = new Date();
  var dateStr = now.toLocaleDateString('en-US', {month:'short',day:'numeric',year:'numeric'});
  var full = 'OPTIONS FLOW PULSE \u2014 ' + dateStr + ' ' + ts + '\\n\\n' + text;
  if (ctxLines) { full += '\\n\\nContext:\\n' + ctxLines; }
  full += '\\n\\nData: Polygon.io | Analysis: Claude AI';
  navigator.clipboard.writeText(full).then(function() {
    btn.textContent = '\u2713 Copied!';
    setTimeout(function() { btn.textContent = '\U0001F4CB Copy Analysis'; }, 2000);
  }).catch(function() {
    btn.textContent = '\u2717 Failed';
    setTimeout(function() { btn.textContent = '\U0001F4CB Copy Analysis'; }, 2000);
  });
}
function toggleCtx(evt) {
  var el = document.getElementById('ctx-text');
  var btn = evt.currentTarget;
  if (el.style.display === 'block') {
    el.style.display = 'none';
    btn.innerHTML = btn.innerHTML.replace('\u25b2', '\u25bc');
  } else {
    el.style.display = 'block';
    btn.innerHTML = btn.innerHTML.replace('\u25bc', '\u25b2');
  }
}
</script>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="refresh" content="300">
  {poll_meta}
  <title>{_e(title)}</title>
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

    :root {{
      /* ── Backgrounds ── */
      --bg:        #0D1117;
      --bg2:       #161B22;
      --bg3:       #1C2128;
      --bg-hover:  #21262D;
      --border:    #30363D;
      --border2:   #2D333B;

      /* ── Text ── */
      --text:      #E6EDF3;
      --muted:     #8B949E;
      --muted2:    #484F58;

      /* ── Liquidnet Accent ── */
      --accent:    #E8588C;
      --accent2:   #F5A623;
      --accent3:   #C13584;

      /* ── Z-Score diverging scale ── */
      --z-xneg:   #8B0000;
      --z-sneg:   #CC3333;
      --z-mneg:   #E06040;
      --z-lneg:   #E88830;
      --z-slneg:  #F0B030;
      --z-neut1:  #E8D040;
      --z-neut:   #D0D860;
      --z-spos:   #A8C840;
      --z-mpos:   #70B030;
      --z-spos2:  #409030;
      --z-xpos:   #207020;

      /* ── Legacy aliases (keep existing code working) ── */
      --bull:      #1a3d20;
      --bull-fg:   #70B030;
      --bear:      #3d1a1a;
      --bear-fg:   #CC3333;
      --neut:      #2a2200;
      --neut-fg:   #E8D040;
      --badge-bg:  #1C2128;
      --badge-fg:  #93c5fd;
      --ai-fg:     #E8588C;
    }}

    body {{
      background: var(--bg);
      color: var(--text);
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      font-size: 13px;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
    }}

    /* Monospace for all data values */
    td, .data-num, .ai-text, .hero-val {{
      font-family: 'JetBrains Mono', 'SF Mono', 'Consolas', monospace;
    }}

    /* ── Header ─────────────────────────── */
    header {{
      background: var(--bg2);
      border-bottom: 1px solid var(--border);
      padding: 0;
    }}
    .hdr-top {{
      display: flex; align-items: center; justify-content: space-between;
      padding: 12px 24px 10px; gap: 16px; flex-wrap: wrap;
    }}
    .hdr-brand {{ display: flex; align-items: center; gap: 12px; }}
    .hdr-logo {{ height: 28px; width: auto; display: block; object-fit: contain; }}
    .hdr-logo-text {{
      font-size: 13px; font-weight: 700; letter-spacing: 2px;
      color: var(--accent); text-transform: uppercase;
    }}
    .hdr-title-wrap {{ display: flex; flex-direction: column; gap: 1px; }}
    .hdr-title {{
      font-size: 16px; font-weight: 700; letter-spacing: .5px; color: var(--text);
      text-transform: uppercase;
    }}
    .hdr-subtitle {{ font-size: 10px; color: var(--muted); letter-spacing: .3px; }}
    .hdr-right {{ display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }}
    .hdr-time {{ color: var(--muted); font-size: 11px; font-family: 'JetBrains Mono', monospace; }}
    .market-dot {{
      display: inline-block; width: 7px; height: 7px;
      border-radius: 50%; margin-right: 5px; vertical-align: middle;
    }}
    .status-open  {{
      background: rgba(112,176,48,0.12); color: var(--z-mpos);
      padding: 3px 10px; border-radius: 10px; font-size: 11px; font-weight: 600;
      border: 1px solid rgba(112,176,48,0.3);
    }}
    .status-open .market-dot  {{ background: var(--z-mpos); box-shadow: 0 0 5px var(--z-mpos); }}
    .status-closed {{
      background: rgba(204,51,51,0.12); color: var(--z-sneg);
      padding: 3px 10px; border-radius: 10px; font-size: 11px; font-weight: 600;
      border: 1px solid rgba(204,51,51,0.3);
    }}
    .status-closed .market-dot {{ background: var(--z-sneg); }}
    .count-badge {{
      background: rgba(232,88,140,0.1); color: var(--accent);
      padding: 3px 10px; border-radius: 10px; font-size: 11px; font-weight: 600;
      border: 1px solid rgba(232,88,140,0.25);
    }}
    /* ── Header Stat Cards ── */
    .hdr-stats {{
      display: flex; gap: 1px; background: var(--border);
      border-top: 1px solid var(--border); overflow-x: auto;
    }}
    .hdr-stat {{
      flex: 1; min-width: 110px; background: var(--bg2);
      padding: 8px 16px; display: flex; flex-direction: column; gap: 2px;
    }}
    .hdr-stat-label {{
      font-size: 9px; font-weight: 600; letter-spacing: .08em;
      text-transform: uppercase; color: var(--muted);
    }}
    .hdr-stat-val {{
      font-family: 'JetBrains Mono', monospace;
      font-size: 18px; font-weight: 700; line-height: 1.1;
    }}
    .hdr-stat-ctx {{ font-size: 10px; color: var(--muted); }}
    .stat-neg {{ color: var(--z-sneg); }}
    .stat-pos {{ color: var(--z-mpos); }}
    .stat-neut {{ color: var(--z-neut1); }}
    .updated {{ color: var(--muted); font-size: 11px; }}
    .meta {{ display: flex; align-items: center; gap: 12px; }}

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
      border-radius: 6px;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }}
    .card-header {{
      background: var(--bg3);
      padding: 9px 14px;
      font-weight: 700;
      font-size: 11px;
      letter-spacing: .07em;
      border-bottom: 1px solid var(--border);
      text-transform: uppercase;
      color: var(--muted);
    }}
    .card-body {{ padding: 10px; flex: 1; overflow-y: auto; }}
    .scroll {{ max-height: 340px; }}
    .empty {{ color: var(--muted2); font-style: italic; padding: 10px 4px; font-size: 12px; }}

    /* ── Signal / Alert rows ────────────────────── */
    .sig-row {{
      display: grid;
      grid-template-columns: 54px 1fr auto;
      grid-template-rows: auto auto;
      column-gap: 10px;
      row-gap: 2px;
      padding: 8px 10px 8px 0;
      border-radius: 0 4px 4px 0;
      margin-bottom: 4px;
      border-left: 3px solid transparent;
      transition: background .12s;
    }}
    .sig-row:hover {{ background: var(--bg-hover); }}
    .sig-row.bull {{ background: rgba(112,176,48,0.06); border-left-color: var(--z-mpos); }}
    .sig-row.bear {{ background: rgba(204,51,51,0.06); border-left-color: var(--z-sneg); }}
    .sig-row.neut {{ background: rgba(232,208,64,0.05); border-left-color: var(--z-neut1); }}

    /* ── Ticker badges (base + sector overrides) ── */
    .ticker-badge {{
      display: inline-block;
      background: var(--badge-bg);
      color: var(--badge-fg);
      font-family: 'Inter', sans-serif;
      font-weight: 700;
      font-size: 11px;
      padding: 2px 7px;
      border-radius: 3px;
      white-space: nowrap;
      text-align: center;
      border: 1px solid rgba(148,163,184,0.2);
      min-width: 44px;
    }}
    /* Mega-cap tech */
    .tk-NVDA,.tk-AAPL,.tk-MSFT,.tk-GOOG,.tk-GOOGL,.tk-AMZN,.tk-META,.tk-ARM,.tk-MRVL {{
      background: rgba(59,130,246,0.13); color: #60A5FA;
      border-color: rgba(59,130,246,0.28);
    }}
    /* ETFs / Index */
    .tk-SPY,.tk-QQQ,.tk-IWM,.tk-XLK,.tk-XLE,.tk-XLF,.tk-XLRE,.tk-XLV,.tk-XLI,.tk-XLP,.tk-XLU,.tk-XLB,.tk-XLY,.tk-TQQQ,.tk-SQQQ,.tk-SPXL,.tk-SPXS {{
      background: rgba(139,92,246,0.13); color: #A78BFA;
      border-color: rgba(139,92,246,0.28);
    }}
    /* High-vol singles */
    .tk-TSLA,.tk-COIN,.tk-AMD,.tk-UBER,.tk-NFLX,.tk-GS,.tk-JPM {{
      background: rgba(245,158,11,0.13); color: #FBBF24;
      border-color: rgba(245,158,11,0.28);
    }}

    .sig-main  {{ font-size: 12px; line-height: 1.4; align-self: center; color: var(--text); }}
    .sig-time  {{ color: var(--muted); font-size: 10px; font-family: 'JetBrains Mono', monospace; white-space: nowrap; align-self: center; text-align: right; }}
    .ai-note   {{
      grid-column: 2 / 4;
      color: var(--accent);
      font-size: 11px;
      font-style: italic;
      line-height: 1.4;
    }}

    /* ── GEX Extremes table ──────────────────────── */
    .gex-tbl {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 12px;
      font-size: 11px;
      font-family: 'JetBrains Mono', monospace;
    }}
    .gex-tbl th {{
      color: var(--muted);
      text-align: left;
      padding: 4px 8px;
      border-bottom: 1px solid var(--border);
      font-family: 'Inter', sans-serif;
      font-size: 9px; font-weight: 600; letter-spacing: .06em; text-transform: uppercase;
    }}
    .gex-tr {{ transition: background .1s; cursor: default; }}
    .gex-tr:hover {{ background: var(--bg-hover) !important; }}
    .gex-tr td {{ padding: 5px 8px; border-bottom: 1px solid var(--bg3); }}
    .gex-tr.neg-gex {{ background: rgba(204,51,51,0.07); border-left: 2px solid var(--z-sneg); }}
    .gex-tr.pos-gex {{ background: rgba(112,176,48,0.07); border-left: 2px solid var(--z-mpos); }}
    .trend {{ font-size: 13px; }}

    /* ── Footer ─────────────────────────── */
    footer {{
      background: var(--bg2);
      border-top: 1px solid var(--border);
    }}
    .footer-tickers {{
      padding: 10px 24px 8px;
      display: flex; flex-wrap: wrap; gap: 6px;
      border-bottom: 1px solid var(--border2);
    }}
    .footer-bar {{
      padding: 10px 24px;
      display: flex; align-items: center; gap: 16px; flex-wrap: wrap;
    }}
    .footer-credits {{ color: var(--muted); font-size: 11px; }}
    .footer-logo {{ height: 20px; width: auto; opacity: 0.8; margin-left: auto; object-fit: contain; }}
    .footer-disc {{ color: var(--muted2); font-size: 10px; }}
    .wl-badge {{ font-size: 10px; min-width: unset; padding: 1px 6px; }}

    /* ── OVI full-width card ───────────────── */
    .ovi-full  {{ grid-column: 1 / -1; }}
    .pos-full  {{ grid-column: 1 / -1; }}

    .ovi-section-lbl {{
      font-size: 11px; font-weight: 700; letter-spacing: .5px;
      padding: 6px 4px 4px; text-transform: uppercase;
    }}
    .call-lbl {{ color: #3fb950; }}
    .put-lbl  {{ color: #f85149; margin-top: 10px; }}

    .ovi-scroll {{ overflow-x: auto; }}
    .ovi-tbl {{
      width: 100%; border-collapse: separate; border-spacing: 0;
      font-family: 'JetBrains Mono', monospace;
      font-size: 12px; white-space: nowrap;
    }}
    .ovi-tbl th {{
      font-family: 'Inter', sans-serif;
      font-size: 9px; font-weight: 600; letter-spacing: .06em;
      text-transform: uppercase; color: #8b9ab5;
      padding: 6px 10px; text-align: right;
      border-bottom: 2px solid #2d5a8e;
      background: linear-gradient(90deg, #1a2332, #1e2a3a); position: sticky; top: 0;
    }}
    .ovi-tbl th:first-child {{ text-align: left; }}
    .ovi-tbl td {{ padding: 5px 10px; text-align: right; border-bottom: 1px solid var(--bg3); }}
    .ovi-tbl td:first-child {{ font-family: 'Inter', sans-serif; font-weight: 600; text-align: left; }}
    .ovi-tbl tr:nth-child(even) {{ background: rgba(255,255,255,0.012); }}
    .ovi-tbl tr:hover {{ background: var(--bg-hover); }}
    .ovi-alt {{ background: transparent; }}
    /* %Chg cell tints */
    .pct-pos {{ color: #4ade80; font-weight: 600; }}
    .pct-neg {{ color: #f87171; font-weight: 600; }}
    .pct-na  {{ color: #374151; }}
    .vol-pos {{ font-weight: 600; }}
    .vol-neg {{ font-weight: 600; }}
    .ovi-tbl td:has(.pct-pos), .ovi-tbl td:has(.vol-pos) {{ background: rgba(34,197,94,0.08); }}
    .ovi-tbl td:has(.pct-neg), .ovi-tbl td:has(.vol-neg) {{ background: rgba(239,68,68,0.08); }}
    .also-notable {{ color: var(--muted); font-size: 10px; padding: 2px 4px 6px; font-style: italic; }}
    /* Index badges */
    .idx-badge {{ display: inline-block; margin-left: 3px; padding: 0 4px;
                  border-radius: 3px; font-size: 9px; font-weight: 700; vertical-align: middle; }}
    .idx-badge.sp500 {{ background: #1a3a5c; color: #60a5fa; }}
    .idx-badge.ndx   {{ background: #1a3a2a; color: #4ade80; }}
    .idx-badge.rut   {{ background: #3a1a3a; color: #c084fc; }}
    /* Extreme alerts */
    .extreme-alerts-box {{ margin-bottom: 10px; }}
    .extreme-alert {{
      background: var(--bg2); border-left: 3px solid var(--accent2);
      border-radius: 0 4px 4px 0; padding: 8px 12px; margin-bottom: 5px;
      display: flex; align-items: center; gap: 12px; font-size: 12px;
      transition: background .12s;
    }}
    .extreme-alert:hover {{ background: var(--bg-hover); }}
    .extreme-badge {{
      margin-left: 8px; padding: 1px 6px; border-radius: 3px; font-size: 9px;
      font-weight: 700; letter-spacing: .06em; text-transform: uppercase;
      background: rgba(245,166,35,0.15); color: var(--accent2);
      border: 1px solid rgba(245,166,35,0.3);
    }}
    /* Universe coverage line */
    .universe-coverage {{ color: var(--muted); font-size: 10px; padding: 2px 4px 6px;
                          font-style: italic; }}

    /* ── 13F Match Cards ──────────────────────── */
    .f13-card {{
      border-left: 3px solid #4ade80; border-radius: 0 4px 4px 0;
      padding: 7px 12px; margin-bottom: 6px; font-size: 12px;
      transition: filter .12s;
    }}
    .f13-card:hover {{ filter: brightness(1.08); }}
    .f13-top {{
      display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
      margin-bottom: 4px;
    }}
    .f13-side {{
      font-family: 'Inter', sans-serif; font-size: 10px; font-weight: 700;
      letter-spacing: .06em; text-transform: uppercase;
    }}
    .f13-pct {{ font-family: 'JetBrains Mono', monospace; font-weight: 700; font-size: 12px; }}
    .f13-spot {{ font-family: 'JetBrains Mono', monospace; font-size: 11px; color: var(--muted); }}
    .f13-align {{
      margin-left: auto; font-size: 9px; font-weight: 700; letter-spacing: .08em;
      text-transform: uppercase; padding: 1px 6px; border-radius: 3px;
    }}
    .f13-bull {{ background: rgba(74,222,128,0.12); color: #4ade80; border: 1px solid rgba(74,222,128,0.25); }}
    .f13-bear {{ background: rgba(248,113,113,0.12); color: #f87171; border: 1px solid rgba(248,113,113,0.25); }}
    .f13-funds {{
      display: flex; align-items: center; flex-wrap: wrap; gap: 5px; font-size: 10px;
    }}
    .f13-fund {{
      background: var(--bg3); color: var(--muted); border: 1px solid var(--border);
      border-radius: 3px; padding: 1px 6px; font-size: 9px; white-space: nowrap;
    }}
    .f13-more {{ color: var(--muted2); font-size: 9px; }}
    .f13-aum {{
      margin-left: auto; font-family: 'JetBrains Mono', monospace;
      font-size: 9px; color: var(--muted2);
    }}

    /* ── AI Analysis box (research note style) ─── */
    .ai-box {{
      margin-top: 12px; border: 1px solid var(--border);
      border-radius: 6px; background: var(--bg);
    }}
    .ai-box-hdr {{
      display: flex; align-items: center; justify-content: space-between;
      padding: 10px 16px; border-bottom: 1px solid var(--border);
      background: var(--bg3); border-radius: 6px 6px 0 0;
    }}
    .ai-box-title {{
      font-size: 10px; font-weight: 700; letter-spacing: .08em;
      text-transform: uppercase; color: var(--accent);
    }}
    .ai-text {{
      margin: 0; padding: 14px 16px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 12px; line-height: 1.65; color: var(--text);
      white-space: pre-wrap; word-wrap: break-word;
    }}
    .ai-formatted {{ white-space: normal; }}
    .ai-section-lbl {{
      font-family: 'Inter', sans-serif; font-size: 9px; font-weight: 700;
      letter-spacing: .1em; text-transform: uppercase;
      color: var(--accent); margin-top: 14px; margin-bottom: 4px; display: block;
    }}
    .ai-trade-lbl {{ color: var(--accent2) !important; }}
    .ai-trade-box {{
      background: rgba(232,88,140,0.07); border: 1px solid rgba(232,88,140,0.2);
      border-radius: 4px; padding: 10px 14px; margin-top: 4px; margin-bottom: 8px;
      font-size: 12px; line-height: 1.6; white-space: pre-wrap;
    }}
    .ai-body-line {{ font-size: 12px; color: var(--text); line-height: 1.65; }}
    .ai-ts {{ padding: 4px 16px; font-size: 10px; color: var(--muted); font-family: 'JetBrains Mono', monospace; }}
    .ai-disclaimer {{
      padding: 4px 16px 10px; font-size: 9px; color: var(--muted2);
      font-style: italic; border-top: 1px solid var(--border2); margin-top: 4px;
    }}
    .copy-btn {{
      background: #21262d; color: #8b949e; border: 1px solid #30363d;
      border-radius: 4px; padding: 3px 10px; font-size: 10px;
      cursor: pointer; white-space: nowrap;
    }}
    .copy-btn:hover {{ background: #30363d; color: var(--text); }}

    /* ── Context section ───────────────────── */
    .ctx-section {{ margin: 10px 0 4px; }}
    .ctx-toggle {{
      background: none; border: 1px solid #2d333b; color: var(--muted);
      font-size: 10px; padding: 4px 10px; border-radius: 4px; cursor: pointer;
    }}
    .ctx-toggle:hover {{ color: var(--text); border-color: #8b949e; }}
    .ctx-text {{
      display: none; font-family: 'Consolas', 'Courier New', monospace;
      font-size: 10px; color: var(--muted); white-space: pre-wrap;
      padding: 8px 12px; margin-top: 4px; border: 1px solid #2d333b;
      border-radius: 4px; background: #080c10;
    }}

    /* ── Refresh button ────────────────────── */
    .refresh-wrap {{ display: flex; align-items: center; gap: 10px; }}
    #refresh-btn {{
      background: #0d7377; color: #e6edf3; border: none;
      border-radius: 6px; padding: 5px 14px; font-size: 12px;
      font-weight: 600; cursor: pointer; white-space: nowrap;
    }}
    #refresh-btn:hover:not(:disabled) {{ background: #14a085; }}
    #refresh-btn:disabled {{ opacity: 0.6; cursor: not-allowed; }}
    .refresh-status {{ font-size: 11px; color: var(--muted); }}
    .refresh-ok {{ color: #4ade80; }}
    .refresh-warn {{ color: #e3b341; }}
    .refresh-err {{ color: #f85149; }}

    /* ── Export button ──────────────────────── */
    .export-btn {{
      background: #1a1a2e; color: #e6edf3;
      border: 1px solid #444; border-radius: 4px;
      padding: 6px 12px; font-size: 12px;
      cursor: pointer; white-space: nowrap; flex-shrink: 0;
    }}
    .export-btn:hover {{ background: #252545; }}
    .export-btn:disabled {{ opacity: 0.6; cursor: not-allowed; }}

    /* ── GEX Chart Panel ─────────────────────── */
    .gex-viz-full {{ grid-column: 1 / -1; }}
    .gex-tabs {{ display: flex; gap: 6px; margin-bottom: 10px; }}
    .gex-tab {{
      background: var(--bg3); color: var(--muted);
      border: 1px solid var(--border); border-radius: 4px;
      padding: 4px 14px; font-size: 11px; font-weight: 600; cursor: pointer;
    }}
    .gex-tab:hover {{ color: var(--text); }}
    .gex-tab-active {{ background: #1a2f4a; color: #60a5fa; border-color: #3b82f6; }}
    /* ── GEX Stats Strip (SpotGamma-style header) ── */
    .gex-stats-strip {{
      display: flex; align-items: stretch; gap: 0;
      background: var(--bg3); border: 1px solid var(--border);
      border-radius: 4px; margin-bottom: 10px; overflow: hidden;
    }}
    .gex-ss-item {{
      flex: 1; padding: 7px 12px; display: flex; flex-direction: column; gap: 2px;
    }}
    .gex-ss-regime-item {{ flex: 2; }}
    .gex-ss-sep {{
      width: 1px; background: var(--border); flex-shrink: 0;
    }}
    .gex-ss-lbl {{
      font-family: 'Inter', sans-serif; font-size: 8px; font-weight: 700;
      letter-spacing: .08em; text-transform: uppercase; color: var(--muted2);
    }}
    .gex-ss-val {{
      font-family: 'JetBrains Mono', monospace; font-size: 13px;
      font-weight: 700; color: var(--text); line-height: 1.1;
    }}
    .gex-chart-grid {{
      display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 14px;
    }}
    @media (max-width: 900px) {{ .gex-chart-grid {{ grid-template-columns: 1fr; }} }}
    .gex-sub-lbl {{
      font-size: 10px; font-weight: 600; letter-spacing: .5px;
      color: var(--muted); text-transform: uppercase; margin-bottom: 6px;
    }}

    /* ── GEX Toggle Buttons ──────────────────── */
    .gex-toggle-btn {{
      background: var(--bg3); color: var(--muted);
      border: 1px solid var(--border); border-radius: 4px;
      padding: 3px 10px; font-size: 10px; font-weight: 600; cursor: pointer;
      transition: background .15s, color .15s;
    }}
    .gex-toggle-btn:hover {{ color: var(--text); border-color: #8b949e; }}
    .gex-toggle-active {{ background: #1a2f4a; color: #60a5fa; border-color: #3b82f6; }}

    /* ── Key Levels Bar ──────────────────────── */
    .gex-key-levels-bar {{
      display: flex; align-items: center; flex-wrap: wrap; gap: 8px;
      margin-bottom: 10px; font-size: 11px;
    }}
    .kl-label {{ color: var(--muted); font-weight: 600; margin-right: 2px; }}
    .kl-item {{
      padding: 2px 8px; border-radius: 3px; font-size: 10px; font-weight: 500;
      border: 1px solid;
    }}
    .kl-flip  {{ color: #c084fc; border-color: #a855f7; background: #1a0a2a; }}
    .kl-call  {{ color: #93c5fd; border-color: #3b82f6; background: #0a1a2a; }}
    .kl-put   {{ color: #fca5a5; border-color: #ef4444; background: #2a0a0a; }}
    .kl-max   {{ color: #fcd34d; border-color: #f59e0b; background: #1a1500; }}

    /* ── GEX Heatmap ─────────────────────────── */
    .gex-heatmap-section {{
      margin-top: 16px; border-top: 1px solid var(--border); padding-top: 12px;
    }}
    .gex-heatmap-canvas-wrap {{
      position: relative; width: 100%; min-height: 80px;
      background: #0d1117; border-radius: 4px; overflow: hidden;
    }}

    /* ── GEX Diagnostics ─────────────────────── */
    .gex-diag-details {{
      margin-top: 16px; border-top: 1px solid var(--border); padding-top: 8px;
    }}
    .gex-diag-summary {{
      font-size: 11px; color: #8B949E; cursor: pointer; user-select: none;
      letter-spacing: 0.04em; text-transform: uppercase; padding: 4px 0;
    }}
    .gex-diag-summary:hover {{ color: #c9d1d9; }}
    .gex-diag-body {{ margin-top: 8px; font-size: 11px; }}
    .diag-stale {{
      background: rgba(251,191,36,0.12); color: #fbbf24;
      padding: 4px 8px; border-radius: 3px; margin-bottom: 6px;
    }}
    .diag-tbl {{ width: 100%; border-collapse: collapse; }}
    .diag-tbl tr:nth-child(even) {{ background: rgba(255,255,255,0.02); }}
    .diag-k {{
      color: #8B949E; padding: 3px 8px 3px 0; width: 55%; vertical-align: top;
    }}
    .diag-v {{
      color: #c9d1d9; font-family: 'JetBrains Mono', monospace; padding: 3px 0;
    }}
  </style>
</head>
<body>

{header_html}

<div class="grid">
  {ovi_card}
  {gex_panel}
  {gex_card}
  {skew_card}
  {pos_card}
</div>

<footer>
  <div class="footer-tickers">{wl_badges}</div>
  <div class="footer-bar">
    <span class="footer-credits">Powered by Polygon.io &times; Claude</span>
    <img src="assets/LNlogo2.png" alt="Liquidnet" class="footer-logo" onerror="this.style.display='none'">
    <span class="footer-disc">v2.0 &nbsp;|&nbsp; Data delayed. Not investment advice. &copy; 2026</span>
  </div>
</footer>

{refresh_js}
{copy_js}
<script>
(function() {{
  var pre = document.getElementById('ai-text');
  if (!pre) return;
  var raw = pre.textContent || '';
  if (!raw.trim()) return;
  var lines = raw.split('\\n');
  var out = '';
  var inTradeIdea = false;
  for (var i = 0; i < lines.length; i++) {{
    var line = lines[i];
    var trimmed = line.trim();
    if (!trimmed) {{ if (inTradeIdea) out += '</div>'; inTradeIdea = false; out += '<br>'; continue; }}
    var upper = trimmed.toUpperCase();
    if (upper.indexOf('TRADE IDEA') === 0 || upper.indexOf('TRADE IDEAS') === 0) {{
      if (inTradeIdea) out += '</div>';
      out += '<div class="ai-section-lbl ai-trade-lbl">' + _esc(trimmed) + '</div>';
      out += '<div class="ai-trade-box">';
      inTradeIdea = true;
    }} else if (/^(HEADLINE|CALL FLOW|PUT FLOW|SECOND ORDER|KEY RISK|POSITIONING|SUMMARY|MARKET CONTEXT)/i.test(upper)) {{
      if (inTradeIdea) {{ out += '</div>'; inTradeIdea = false; }}
      out += '<div class="ai-section-lbl">' + _esc(trimmed) + '</div>';
    }} else {{
      out += (inTradeIdea ? '' : '<span class="ai-body-line">') + _esc(line) + (inTradeIdea ? '\\n' : '</span><br>');
    }}
  }}
  if (inTradeIdea) out += '</div>';
  function _esc(s) {{ return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }}
  pre.outerHTML = '<div id="ai-text" class="ai-text ai-formatted">' + out + '</div>';
}})();
</script>
<script>
async function exportPanel(panelId, filename) {{
  var btn = event.target;
  var originalText = btn.innerHTML;
  btn.innerHTML = '\u23f3 Generating...';
  btn.disabled = true;

  var panel = document.getElementById(panelId);

  var watermark = document.createElement('div');
  watermark.style.cssText = [
    'position:absolute', 'bottom:12px', 'right:16px',
    'color:rgba(255,255,255,0.4)', 'font-size:11px',
    'font-family:monospace', 'pointer-events:none', 'z-index:999'
  ].join(';');
  watermark.textContent = 'Liquidnet Options Flow | ' +
    new Date().toLocaleDateString('en-US', {{month:'short', day:'numeric', year:'numeric'}});
  var prevPos = panel.style.position;
  panel.style.position = 'relative';
  panel.appendChild(watermark);

  try {{
    var canvas = await html2canvas(panel, {{
      backgroundColor: '#0d0d1a',
      scale: 2,
      useCORS: true,
      logging: false,
      width: panel.offsetWidth,
      height: panel.offsetHeight,
      windowWidth: panel.offsetWidth
    }});

    panel.removeChild(watermark);
    panel.style.position = prevPos;

    var link = document.createElement('a');
    link.download = filename + '_' + new Date().toISOString().slice(0,10) + '.png';
    link.href = canvas.toDataURL('image/png', 1.0);
    link.click();

    btn.innerHTML = '\u2713 Saved!';
  }} catch(e) {{
    if (panel.contains(watermark)) panel.removeChild(watermark);
    panel.style.position = prevPos;
    btn.innerHTML = '\u2717 Error';
    console.error('[export]', e);
  }}

  setTimeout(function() {{ btn.innerHTML = originalText; btn.disabled = false; }}, 2000);
}}
</script>
</body>
</html>"""
