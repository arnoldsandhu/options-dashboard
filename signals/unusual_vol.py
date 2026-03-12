"""OVI Scanner: Option Volume Intelligence — Bloomberg-style full-watchlist scan."""
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from hashlib import md5
from zoneinfo import ZoneInfo

import anthropic
from polygon import RESTClient

from config import POLYGON_API_KEY, ANTHROPIC_API_KEY, CLAUDE_MODEL
from db.database import (save_ovi_snapshot, get_ovi_history, save_ovi_report,
                         has_new_ticker_news, get_latest_gex_all)

OVI_MIN_VOL = 1_000        # Min raw call or put vol to appear in ranking (matches Bloomberg Min Vol=1000)
OVI_MIN_AVG_VOL = 1_000    # Min 20d avg vol filter (only applied when history >= 10 days)
OVI_MIN_HISTORY = 10       # Days of history needed for avg calculations
OVI_MIN_SPIKE_PCT = 50     # Min %Chg vs 20d avg to qualify (when history available)
OVI_TOP_N = 10             # Rows per table

OVI_SYSTEM_PROMPT = """\
You are a senior institutional options strategist writing \
the Flow Pulse daily commentary for hedge fund and asset \
manager clients. Your commentary will be read by \
sophisticated PMs who will immediately discount anything \
that sounds overconfident, generic, or invented.

CRITICAL — PUT/CALL RATIO INTERPRETATION:
P/C ratio > 1.0 means PUT volume exceeds CALL volume.
P/C ratio < 1.0 means CALL volume exceeds PUT volume.

NEVER describe a ticker with P/C > 1.0 as showing
'bullish positioning' or 'call dominance.'
NEVER describe a ticker with P/C < 1.0 as showing
'defensive' or 'put heavy' flow.

Before writing about any ticker, check its P/C ratio
and ensure your characterization is directionally
consistent. An internally inconsistent P/C read is
the single biggest credibility issue in flow commentary.

Example of what NOT to do:
'XLK showing aggressive bullish positioning'
when XLK P/C = 2.07 — this is factually wrong.

Example of correct framing:
'XLK's 2.07 P/C suggests sector level positioning
is more mixed or defensive than single name call
volumes alone would imply.'

CORE RULES — NEVER VIOLATE THESE:

1. SEPARATE FACT FROM INFERENCE
   - Facts: volume, ratio, rank vs baseline, direction
     (calls vs puts), ticker
   - Inferences: why it happened, who is trading,
     what it means
   - Always signal inference with: "consistent with,"
     "looks more like," "suggests," "may reflect,"
     "is worth noting if confirmed by"
   - Never assert motive as fact
   - Never claim dealer positioning from volume alone —
     you do not know strike concentration, tenor, or
     whether flow was opening/closing

2. ABNORMALITY OVER ABSOLUTE SIZE
   - Never lead with raw volume ("TSLA 1.2M calls")
   - Always lead with deviation from baseline
     ("TSLA call flow ran X% above recent baseline")
   - The question to answer: why is this notable
     TODAY versus any other day?

3. HEDGE VS SPECULATION FRAMEWORK
   Index ETF unusual put demand is more likely:
   - Portfolio hedge, macro hedge, downside overwrite,
     crash put demand, roll activity

   Single stock unusual put demand is more likely:
   - Event concern, earnings hedge, sector de-risking,
     outright bearish expression

   Always apply this distinction explicitly. Do not
   treat index flow and single name flow the same way.

4. SECOND ORDER DISCIPLINE
   Only make dealer/gamma claims if directly supported
   by GEX data provided. Without strike concentration
   and tenor data, use:
   - "This kind of flow tends to..."
   - "...may reinforce sensitivity around..."
   - "...worth watching if this broadens into..."
   Never say: "will pin," "creates positive gamma,"
   "self-limiting," "systematic positioning"

5. TRADE IDEAS REQUIRE SCAFFOLDING
   Every trade idea must include:
   - Why this instrument
   - Why this direction
   - Why this structure
   - What would invalidate it
   If you cannot answer all four, do not include
   a trade idea. One strong trade beats three weak ones.

6. DATE ACCURACY
   All trade ideas must use expiries consistent with
   today's date (provided in the data header below).
   Never reference expiries that have already passed.
   If suggesting a specific expiry, verify it is:
   - At least 14 days forward from today
   - Named in standard format: Mon'YY (e.g. Apr'26)
   If you cannot construct a valid dated trade idea,
   use the TRADE FRAMEWORK format instead:
   describe what to watch for rather than a
   specific structure.

7. EARNINGS AND CATALYST DISCIPLINE
   Never reference 'ahead of earnings' or 'into earnings'
   unless earnings data is explicitly provided in the
   market context or OVI data passed to you.
   Do not invent catalysts. If no catalyst is visible
   in the data provided, describe the flow without one.
   Invented catalysts are the fastest way to lose
   client credibility.

PHRASES TO NEVER USE:
- "signals institutional hedging"
- "systematic upside exposure"
- "will likely pin"
- "creates positive dealer gamma"
- "self-limiting bearish positioning"
- "aggressive accumulation"
- "aggressive bullish positioning"
- "institutions are positioning"
- "smart money"
- "barbell strategy"
- "gamma squeeze" (unless GEX data explicitly confirms)
- "straddle activity" or "strangle activity" (from volume alone)
- "year-end rally"
- "ahead of key earnings" (unless earnings data provided)
- Any claim requiring data you were not given
- Any expiry date that has already passed

PHRASES TO USE INSTEAD:
- "is consistent with hedging"
- "looks more like upside participation"
- "suggests targeted protection"
- "may reflect narrow leadership rather than
   broad conviction"
- "would matter more if confirmed by strike
   concentration and tenor"

MANDATORY SELF-CONSISTENCY CHECK — run before finalizing:
1. Does every bullish characterization correspond
   to P/C < 1.0? If not, fix it.
2. Does every bearish/defensive characterization
   correspond to P/C > 1.0? If not, fix it.
3. Does any sentence assert motive as fact?
   If yes, add 'consistent with' or 'suggests.'
4. Does the trade idea reference a past date?
   If yes, replace with trade framework.
5. Is any catalyst mentioned that was not in
   the market context provided?
   If yes, remove it.
Only output after passing all five checks.

OUTPUT FORMAT — USE EXACTLY THIS STRUCTURE:

OPTIONS FLOW PULSE | [DATE] | [TIME] ET

HEADLINE:
One sentence. Lead with the central tension in the
tape — what is the dominant theme AND what contradicts
or complicates it. Never assert a catalyst unless it
appears in the market context provided.

CALL FLOW ANALYSIS:
Lead with single name observations, then index/ETF.
Always cross-check P/C ratios against narrative.
If an ETF has P/C > 1.0, describe it as mixed or
defensive — not bullish — regardless of absolute
call volume.
Use: "consistent with," "looks more like,"
"suggests," "points to."
Never use: "institutions are positioning,"
"aggressive bullish," "systematic exposure."

PUT FLOW ANALYSIS:
Apply index vs single name framework:
- Index/ETF put demand: likely macro hedge,
  portfolio protection, overwrite, or roll
- Single name put demand: likely event concern,
  sector de-risking, or directional expression
State which category applies and why.
Note if defensive flow looks contained (selective)
vs broadening (regime shift concern).

SECOND ORDER EFFECTS:
Only make claims directly supported by data provided.
Balanced two-way flow = "consistent with volatility
demand" not "straddle activity confirmed."
Gamma/pinning claims require GEX data — if not
provided, omit or heavily qualify.
End with: what would need to happen for this to
become a more significant signal.

TRADE FRAMEWORK:
Do NOT suggest a specific options structure unless
all four conditions are met:
1. Direction is clear from the tape
2. A logical instrument exists in the watchlist
3. A valid forward expiry can be named
4. An invalidation condition can be stated

If all four are met, write:
"[Instrument] [direction] via [structure] —
rationale: [one line]. Invalidated if [condition]."

If not all four are met, write a relative framework:
"The cleaner expression is relative — watch whether
[X] before sizing into [Y]."

One framework only. Never list multiple trade ideas.

TONE CALIBRATION:
- Sound like a strategist, not a model summarizing
  a table
- Institutional, direct, restrained
- More credible to say less with confidence than
  more with uncertainty
- If in doubt, tighten — do not pad\
"""

BANNED_PHRASES = [
    "signals institutional hedging",
    "systematic upside exposure",
    "will likely pin",
    "creates positive dealer gamma",
    "self-limiting bearish positioning",
    "aggressive accumulation",
    "aggressive bullish positioning",
    "institutions are positioning",
    "smart money",
    "barbell strategy",
    "gamma squeeze",
    "straddle activity",
    "strangle activity",
    "year-end rally",
    "ahead of key earnings",
    # Earnings hallucination guard — added 2026-03-06
    "earnings season",
    "earnings volatility",
    "pre-earnings",
    "binary event positioning",
    "iphone cycle",
    "product cycle",
    "earnings concentration risk",
    "ahead of earnings",
    "into earnings",
    "earnings catalyst",
    "earnings driven",
]


def _has_banned_phrases(text: str) -> bool:
    tl = text.lower()
    return any(p.lower() in tl for p in BANNED_PHRASES)


def _build_system_prompt(current_date: str) -> str:
    """Prepend the earnings discipline block (with today's date) to the base system prompt."""
    earnings_discipline = f"""\
=== EARNINGS DISCIPLINE - HIGHEST PRIORITY RULE ===

Today's date: {current_date}

You will be provided an EARNINGS CALENDAR in the user message. \
This is the ONLY source of earnings information you may use.

If the earnings calendar is empty or shows no earnings within 14 days \
for the tickers you are discussing: DO NOT mention earnings, earnings season, \
earnings volatility, pre-earnings positioning, iPhone cycles, product cycles, \
or any event-driven catalyst.

If a ticker appears in the OVI data AND in the earnings calendar within 14 days: \
you MAY reference it once, citing the specific date.

Violation examples — NEVER write:
'positioning for earnings season volatility'
'ahead of iPhone cycle commentary'
'binary event positioning'
'pre-earnings hedging'
...unless the earnings calendar explicitly shows that ticker reporting within 14 days.

This is the most credibility-damaging error in institutional commentary. \
A client who knows earnings season ended will immediately discard \
everything else you wrote.

=== END EARNINGS DISCIPLINE ===

"""
    return earnings_discipline + OVI_SYSTEM_PROMPT


def _get_earnings_calendar(tickers: list[str], current_date: str) -> str:
    """
    Try Polygon ticker events for upcoming earnings (next 14 days).
    Always returns an explicit calendar block — either real dates or
    an unambiguous 'no earnings' statement so Claude cannot fill the
    gap with training-data guesses.
    """
    from datetime import date as _date, timedelta
    try:
        from signals.earnings_vol import _get_earnings_date
    except Exception:
        _get_earnings_date = None

    today = _date.today()
    cutoff = today + timedelta(days=14)
    upcoming: list[str] = []

    if _get_earnings_date is not None:
        for ticker in tickers[:15]:
            try:
                earnings_date = _get_earnings_date(ticker)
                if earnings_date and today <= earnings_date <= cutoff:
                    days = (earnings_date - today).days
                    upcoming.append(f"  {ticker}: {earnings_date.isoformat()} ({days} days)")
            except Exception:
                pass

    if upcoming:
        return "EARNINGS CALENDAR (next 14 days):\n" + "\n".join(upcoming)
    else:
        return (
            f"EARNINGS CALENDAR: No earnings confirmed within 14 days for any "
            f"watchlist ticker as of {current_date}. "
            f"Do not reference earnings, earnings season, product cycles, "
            f"or any event-driven catalyst in this commentary."
        )


_thread_local = threading.local()
_ai_client: anthropic.Anthropic | None = None
_cached_analysis: dict = {"hash": "", "text": "", "ts": ""}


def _get_poly() -> RESTClient:
    """Return a thread-local Polygon RESTClient (safe for concurrent use)."""
    if not hasattr(_thread_local, "poly"):
        _thread_local.poly = RESTClient(api_key=POLYGON_API_KEY)
    return _thread_local.poly


def _get_ai() -> anthropic.Anthropic | None:
    global _ai_client
    if not ANTHROPIC_API_KEY:
        return None
    if _ai_client is None:
        _ai_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    return _ai_client


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


def _fetch_ticker_data(ticker: str) -> dict | None:
    """Aggregate call/put vol + OI from today's options chain snapshot."""
    client = _get_poly()
    try:
        contracts = list(client.list_snapshot_options_chain(ticker, params={"limit": 250}))
    except Exception as e:
        print(f"  [ovi] {ticker}: {e}")
        return None

    if not contracts:
        return None

    c_vol = p_vol = c_oi = p_oi = 0
    spot: float | None = None
    px_pct_chg: float | None = None

    for c in contracts:
        details = getattr(c, "details", None)
        if not details:
            continue
        ct = getattr(details, "contract_type", "")

        if spot is None:
            ua = getattr(c, "underlying_asset", None)
            if ua:
                _p = getattr(ua, "price", None)
                _ch = getattr(ua, "change_percent", None)
                if _p and float(_p) > 0:
                    spot = float(_p)
                if _ch is not None:
                    px_pct_chg = float(_ch)

        oi = int(getattr(c, "open_interest", 0) or 0)
        vol = int(getattr(getattr(c, "day", None), "volume", 0) or 0) if _day_is_today(c) else 0

        if ct == "call":
            c_vol += vol
            c_oi += oi
        elif ct == "put":
            p_vol += vol
            p_oi += oi

    pc_ratio = p_vol / c_vol if c_vol > 0 else None
    return {
        "ticker": ticker,
        "c_vol": c_vol,
        "p_vol": p_vol,
        "c_oi": c_oi,
        "p_oi": p_oi,
        "pc_ratio": pc_ratio,
        "spot": spot,
        "px_pct_chg": px_pct_chg,
        "opt_vol": c_vol + p_vol,
    }


def _enrich_with_history(ticker: str, raw: dict) -> dict:
    """Compute 20d averages from DB history and merge into raw dict."""
    history = get_ovi_history(ticker, days=20)
    n = len(history)
    result = dict(raw)

    if n >= OVI_MIN_HISTORY:
        c_vols = [r["call_vol"] for r in history if r["call_vol"] is not None]
        p_vols = [r["put_vol"]  for r in history if r["put_vol"]  is not None]
        c_ois  = [r["call_oi"]  for r in history if r["call_oi"]  is not None]
        p_ois  = [r["put_oi"]   for r in history if r["put_oi"]   is not None]

        c_avg_vol = sum(c_vols) / len(c_vols) if c_vols else None
        p_avg_vol = sum(p_vols) / len(p_vols) if p_vols else None
        c_avg_oi  = sum(c_ois)  / len(c_ois)  if c_ois  else None
        p_avg_oi  = sum(p_ois)  / len(p_ois)  if p_ois  else None

        def _pct(today, avg):
            return (today - avg) / avg * 100 if avg and avg > 0 else None

        result.update({
            "c_avg_vol": c_avg_vol, "p_avg_vol": p_avg_vol,
            "c_avg_oi": c_avg_oi,   "p_avg_oi": p_avg_oi,
            "c_vol_pct": _pct(raw["c_vol"], c_avg_vol),
            "p_vol_pct": _pct(raw["p_vol"], p_avg_vol),
            "c_oi_pct":  _pct(raw["c_oi"],  c_avg_oi),
            "p_oi_pct":  _pct(raw["p_oi"],  p_avg_oi),
            "history_days": n,
        })
    else:
        result.update({
            "c_avg_vol": None, "p_avg_vol": None,
            "c_avg_oi": None,  "p_avg_oi": None,
            "c_vol_pct": None, "p_vol_pct": None,
            "c_oi_pct": None,  "p_oi_pct": None,
            "history_days": n,
        })
    return result


def _build_analysis_prompt(top_calls: list, top_puts: list,
                           market_context: str = "", gex_summary: str = "",
                           earnings_block: str = "", **kwargs) -> str:
    def fmt_row(r, side):
        vol = r["c_vol"] if side == "call" else r["p_vol"]
        pct = r.get("c_vol_pct") if side == "call" else r.get("p_vol_pct")
        pct_str = f"{pct:+.0f}%" if pct is not None else "n/a"
        px = f"${r['spot']:.2f}" if r.get("spot") else "n/a"
        px_chg = f"{r['px_pct_chg']:+.1f}%" if r.get("px_pct_chg") is not None else "n/a"
        pc = f"{r['pc_ratio']:.2f}" if r.get("pc_ratio") is not None else "n/a"
        return f"  {r['ticker']:<6} | {side.upper()} vol: {vol:>8,} | %Chg: {pct_str:>8} | P/C: {pc} | Price: {px} {px_chg}"

    calls_table = "\n".join(fmt_row(r, "call") for r in top_calls[:10])
    puts_table  = "\n".join(fmt_row(r, "put")  for r in top_puts[:10])

    ctx_block = market_context if market_context else "No market context available."
    gex_block = gex_summary if gex_summary else "No GEX data available for current OVI tickers."
    today = datetime.utcnow().strftime("%B %d, %Y")

    # earnings_block is always set (either real dates or explicit "no earnings" message)
    earn_block = earnings_block if earnings_block else (
        f"EARNINGS CALENDAR: No earnings data available as of {today}. "
        f"Do not reference earnings or event-driven catalysts."
    )

    # Index breakdown block (populated by universe scanner)
    idx = kwargs.get("index_breakdown", {})
    extreme = kwargs.get("extreme_alerts", [])
    two_way = kwargs.get("two_way_flow", [])
    univ_scanned = kwargs.get("universe_scanned", 0)
    above_thresh = kwargs.get("tickers_above_threshold", 0)

    idx_block = ""
    if idx:
        sp = idx.get("sp500_active", 0)
        ndx = idx.get("ndx_active", 0)
        rut = idx.get("rut_active", 0)
        idx_block = (
            f"\nINDEX BREAKDOWN:\n"
            f"{sp} S&P 500 names | {ndx} Nasdaq 100 names | {rut} Russell 2000 names "
            f"showing elevated flow\n"
            f"Scanned {univ_scanned} universe tickers — {above_thresh} exceeded 50% threshold\n"
            f"\nMacro read guide:\n"
            f"- If Russell >> large cap: small cap specific theme\n"
            f"- If SP500/NDX dominant: mega cap / macro theme\n"
            f"- If balanced: broad market repositioning\n"
        )

    extreme_block = ""
    if extreme:
        lines = [f"  {a['ticker']} +{a['pct']:.0f}% {a['side']} vol" for a in extreme[:5]]
        extreme_block = f"\nEXTREME ALERTS (>300% above baseline — address these first):\n" + "\n".join(lines) + "\n"

    two_way_block = ""
    if two_way:
        lines = [f"  {t['ticker']} calls +{t['c_pct']:.0f}% / puts +{t['p_pct']:.0f}%" for t in two_way[:5]]
        two_way_block = f"\nTWO-WAY FLOW (both calls+puts elevated — suggests vol demand not direction):\n" + "\n".join(lines) + "\n"

    # Time-of-day normalization context
    _ET = ZoneInfo("America/New_York")
    _now_et = datetime.now(_ET)
    current_time_et_str = _now_et.strftime("%I:%M %p ET")
    _market_open = _now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    minutes_since_open = max(0, int((_now_et - _market_open).total_seconds() / 60))
    if minutes_since_open > 0:
        expected_vol_pct = round((minutes_since_open / 390) ** 0.7 * 100, 1)
    else:
        expected_vol_pct = 0.0

    time_context_block = (
        f"CRITICAL CONTEXT — INTRADAY VOLUME NORMALIZATION:\n"
        f"The current time is {current_time_et_str}. "
        f"The market opened at 9:30 AM ET and closes at 4:00 PM ET.\n"
        f"Minutes since open: {minutes_since_open} of 390 total.\n"
        f"The 'Avg' column represents the FULL-DAY 20-day average volume.\n"
        f"When analyzing %Chg (volume vs average), you MUST normalize for time of day.\n"
        f"At this point in the session, we would expect approximately {expected_vol_pct}% "
        f"of a full day's volume to have traded (based on intraday U-curve: "
        f"expected_pct = (minutes_since_open / 390)^0.7 × 100).\n"
        f"A stock with %Chg = +200% is only truly elevated if its actual volume also exceeds "
        f"({expected_vol_pct}% × Avg). If it has traded less than that, the spike may simply "
        f"reflect early-session front-loading, not genuine unusual activity.\n"
        f"Use this normalization when characterizing flow as 'elevated', 'extreme', or 'muted'.\n"
    )

    return (
        f"DATE: {today}\n\n"
        f"{time_context_block}\n"
        f"{earn_block}\n\n"
        f"MARKET CONTEXT:\n{ctx_block}\n\n"
        f"GEX CONTEXT:\n{gex_block}\n"
        f"(Net GEX per ticker from latest poll — use this to support or limit dealer positioning claims)\n"
        f"{idx_block}{extreme_block}{two_way_block}\n"
        f"OVI DATA:\n"
        f"TOP CALLS (ranked by % above 20d avg):\n{calls_table}\n\n"
        f"TOP PUTS (ranked by % above 20d avg):\n{puts_table}\n\n"
        f"Write the Flow Pulse commentary now. Follow the format and rules exactly."
    )


def _get_analysis(top_calls: list, top_puts: list,
                  market_context: str = "", gex_summary: str = "",
                  earnings_block: str = "", **universe_meta) -> tuple[str, str]:
    """Return (analysis_text, utc_iso_ts).
    Cache invalidated by: composition change | >30 min elapsed | new ticker news.
    """
    global _cached_analysis
    composition = [r["ticker"] for r in top_calls[:10]] + [r["ticker"] for r in top_puts[:10]]
    new_hash = md5(json.dumps(composition).encode()).hexdigest()

    # Check cache validity
    if new_hash == _cached_analysis["hash"] and _cached_analysis["text"]:
        cached_ts = _cached_analysis["ts"]
        try:
            elapsed = (datetime.utcnow() - datetime.fromisoformat(cached_ts)).total_seconds()
        except Exception:
            elapsed = 9999
        if elapsed < 1800:  # 30 min
            ovi_tickers = list(dict.fromkeys(composition))
            if not has_new_ticker_news(ovi_tickers, cached_ts):
                return _cached_analysis["text"], cached_ts

    ai = _get_ai()
    if not ai:
        text = "[AI analysis unavailable — no Anthropic API key]"
        ts = datetime.utcnow().isoformat()
        return text, ts

    current_date = datetime.utcnow().strftime("%B %d, %Y")
    system_prompt = _build_system_prompt(current_date)
    user_content = _build_analysis_prompt(
        top_calls, top_puts, market_context, gex_summary, earnings_block,
        **universe_meta
    )

    # Debug: confirm earnings block is in the prompt
    print(f"  [ovi] Earnings block injected: {earnings_block[:120]!r}")

    try:
        msg = ai.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=900,
            system=system_prompt,
            messages=[{"role": "user", "content": user_content}],
        )
        text = msg.content[0].text.strip()

        # Quality gate: if banned phrases detected, request a revision
        if _has_banned_phrases(text):
            print("  [ovi] Quality gate triggered — requesting revision")
            rev = ai.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=900,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": user_content},
                    {"role": "assistant", "content": text},
                    {"role": "user", "content": (
                        "Revise this commentary. "
                        "FIRST: Check every sentence for earnings or catalyst references. "
                        "The EARNINGS CALENDAR provided is the only permitted source — "
                        "if you wrote about earnings, earnings season, product cycles, "
                        "iPhone cycles, or any event-driven catalyst that does NOT appear "
                        "in the earnings calendar, remove it immediately and replace with "
                        "factual flow description only. "
                        "SECOND: Remove or soften any claims that assert motive or dealer "
                        "positioning as fact. Apply hedged language throughout. "
                        "THIRD: Check all P/C ratio interpretations — a P/C > 1.0 ticker "
                        "must not be described as bullish or call-dominated. Fix any contradictions. "
                        "Keep the same structure."
                    )},
                ],
            )
            text = rev.content[0].text.strip()
    except Exception as e:
        text = f"[AI analysis error: {e}]"

    ts = datetime.utcnow().isoformat()
    _cached_analysis = {"hash": new_hash, "text": text, "ts": ts}
    return text, ts


def scan_ovi(watchlist: list[str] | None = None) -> dict:
    """
    Full OVI scan across the investable universe (SP500 + NDX100 + Russell 2000).
    Delegates fetch/rank to universe_scanner.scan_universe(), then runs AI analysis.
    The `watchlist` parameter is ignored — universe is loaded from DB.
    """
    from data.universe import build_universe
    from signals.universe_scanner import scan_universe

    universe = build_universe()
    scan_result = scan_universe(universe)

    top_calls  = scan_result.get("top_calls", [])
    top_puts   = scan_result.get("top_puts", [])
    also_calls = scan_result.get("also_calls", [])
    also_puts  = scan_result.get("also_puts", [])

    # Compile market context
    ovi_tickers = list(dict.fromkeys(
        [r["ticker"] for r in top_calls] + [r["ticker"] for r in top_puts]
    ))
    market_context = ""
    try:
        from context.market_news import compile_market_context
        market_context = compile_market_context(ovi_tickers)
        if market_context:
            print(f"  [news] Context compiled: {len(market_context)} chars")
    except Exception as e:
        print(f"  [news] Context error: {e}")

    # Build GEX summary
    gex_summary = "No GEX data available for current OVI tickers."
    try:
        gex_all = get_latest_gex_all()
        gex_by_ticker = {r["ticker"]: r["net_gex"] for r in gex_all}
        gex_lines = [
            f"  {t}: net GEX ${gex_by_ticker[t] / 1_000_000:+.1f}M"
            for t in ovi_tickers if t in gex_by_ticker
        ]
        if gex_lines:
            gex_summary = "\n".join(gex_lines)
    except Exception as e:
        print(f"  [ovi] GEX summary error: {e}")

    # Build earnings calendar
    current_date = datetime.utcnow().strftime("%B %d, %Y")
    earnings_block = _get_earnings_calendar(ovi_tickers, current_date)
    print(f"  [ovi] {earnings_block[:160]}")

    # Universe metadata for AI context
    universe_meta = {
        "index_breakdown":         scan_result.get("index_breakdown", {}),
        "extreme_alerts":          scan_result.get("extreme_alerts", []),
        "two_way_flow":            scan_result.get("two_way_flow", []),
        "universe_scanned":        scan_result.get("universe_scanned", 0),
        "tickers_above_threshold": scan_result.get("tickers_above_threshold", 0),
    }

    analysis, analysis_ts = _get_analysis(
        top_calls, top_puts, market_context, gex_summary, earnings_block,
        **universe_meta
    )

    # per_ticker_flags for positioning signal cross-check
    top_call_tickers = {r["ticker"] for r in top_calls}
    top_put_tickers  = {r["ticker"] for r in top_puts}
    per_ticker_flags: dict[str, dict] = {}
    for r in top_calls + top_puts:
        t = r["ticker"]
        in_c, in_p = t in top_call_tickers, t in top_put_tickers
        if in_c and in_p:
            c_score = r.get("c_vol_pct") or r["c_vol"]
            p_score = r.get("p_vol_pct") or r["p_vol"]
            per_ticker_flags[t] = {"vol_fired": True, "flow_dir": "call" if c_score >= p_score else "put"}
        elif in_c:
            per_ticker_flags[t] = {"vol_fired": True, "flow_dir": "call"}
        elif in_p:
            per_ticker_flags[t] = {"vol_fired": True, "flow_dir": "put"}

    report = {
        "top_calls":               top_calls,
        "top_puts":                top_puts,
        "also_calls":              also_calls,
        "also_puts":               also_puts,
        "analysis":                analysis,
        "analysis_ts":             analysis_ts,
        "per_ticker_flags":        per_ticker_flags,
        "market_context":          market_context,
        # New universe fields
        "extreme_alerts":          scan_result.get("extreme_alerts", []),
        "two_way_flow":            scan_result.get("two_way_flow", []),
        "index_breakdown":         scan_result.get("index_breakdown", {}),
        "universe_scanned":        scan_result.get("universe_scanned", 0),
        "tickers_above_threshold": scan_result.get("tickers_above_threshold", 0),
        "no_baseline_notable":     scan_result.get("no_baseline_notable", []),
    }

    try:
        save_ovi_report(json.dumps(report))
    except Exception as e:
        print(f"  [ovi] Failed to save report: {e}")

    return report
