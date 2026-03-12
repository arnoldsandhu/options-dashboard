"""
Cross-source alpha signal synthesis — calls Claude to generate 2-3 trade theses
connecting OVI flow, GEX regime, skew extremes, and 13F institutional positioning.
"""
from __future__ import annotations

_SYSTEM_PROMPT = (
    "You are a senior options sales trader at a tier-1 broker-dealer writing intraday "
    "commentary for hedge fund PMs. Synthesize the signal data below into exactly 2-3 "
    "distinct, actionable trade theses. Each thesis MUST connect at least 2 different "
    "signal sources (OVI unusual flow, GEX dealer regime, skew extremes, 13F positioning). "
    "Output ONLY plain text — NO markdown, NO asterisks, NO bold, NO bullet points. "
    "Format each thesis on exactly these 3 labeled lines, then a separator:\n"
    "NAME: <short label, max 5 words>\n"
    "THESIS: <one sentence connecting the signals, max 30 words>\n"
    "EXPRESSION: <specific options trade or ETF expression, max 15 words>\n"
    "---\n"
    "Repeat for each thesis. "
    "Prioritize contrarian 13F divergences, gamma regime breaks, and cross-sector rotation signals. "
    "Be direct, institutional, no filler. Use real tickers. No disclaimers."
)

_MAX_TOKENS = 600


def _build_prompt(
    ovi_report: dict,
    gex_rows: list[dict],
    skew_rows: list[dict],
    f13_matches: list[dict],
) -> str:
    lines: list[str] = ["=== TODAY'S CROSS-SOURCE SIGNAL SUMMARY ===\n"]

    # OVI unusual flow
    top_calls = ovi_report.get("top_calls", [])[:6]
    top_puts  = ovi_report.get("top_puts",  [])[:6]
    if top_calls:
        lines.append("OVI UNUSUAL CALL FLOW (vs 20d avg):")
        for r in top_calls:
            pct = r.get("c_vol_pct")
            pct_str = f"+{pct:.0f}%" if pct is not None else "elevated"
            spot = f" @ ${r['spot']:.0f}" if r.get("spot") else ""
            lines.append(f"  {r['ticker']}{spot}: {pct_str}")
    if top_puts:
        lines.append("OVI UNUSUAL PUT FLOW:")
        for r in top_puts:
            pct = r.get("p_vol_pct")
            pct_str = f"+{pct:.0f}%" if pct is not None else "elevated"
            spot = f" @ ${r['spot']:.0f}" if r.get("spot") else ""
            lines.append(f"  {r['ticker']}{spot}: {pct_str}")

    # GEX extremes — dealer regime
    if gex_rows:
        lines.append("\nGEX DEALER REGIME EXTREMES:")
        for r in gex_rows[:5]:
            # Strip the ai-note portion — just use the primary signal text
            raw = r.get("alert_text", "")
            main_part = raw.split(" | ")[0] if " | " in raw else raw
            lines.append(f"  {r['ticker']}: {main_part[:100]}")

    # Skew extremes
    if skew_rows:
        lines.append("\nSKEW EXTREMES (put/call skew vs history):")
        for r in skew_rows[:5]:
            raw = r.get("alert_text", "")
            main_part = raw.split(" | ")[0] if " | " in raw else raw
            lines.append(f"  {r['ticker']}: {main_part[:100]}")

    # 13F institutional positioning context
    if f13_matches:
        lines.append("\n13F INSTITUTIONAL POSITIONING:")
        for m in f13_matches[:5]:
            ctx = m.get("context_entries", [])
            if ctx:
                ctx_parts = []
                for e in ctx[:3]:
                    ctx_parts.append(f"{e['fund']} [{e['sig_type'].upper()}]: {e['detail'][:50]}")
                lines.append(f"  {m['ticker']} ({m['alignment']}): " + " | ".join(ctx_parts))
            else:
                lines.append(f"  {m['ticker']} ({m['alignment']}): {m['fund_count']} funds long")

    # Market context / news
    market_ctx = ovi_report.get("market_context", "").strip()
    if market_ctx:
        lines.append(f"\nMARKET CONTEXT: {market_ctx[:300]}")

    # OVI overall analysis
    analysis = ovi_report.get("analysis", "").strip()
    if analysis:
        lines.append(f"\nOVI PULSE ANALYSIS: {analysis[:300]}")

    return "\n".join(lines)


def _parse_theses(raw: str) -> list[dict]:
    """Parse Claude's output into thesis dicts.

    Handles multiple Claude formatting styles:
    - Plain:    NAME: text
    - Bold:     **NAME:** text  or  **NAME: text**
    - Header:   **THESIS TITLE**  (followed by THESIS: on next line)
    """
    import re

    def _clean(s: str) -> str:
        return re.sub(r"\*+", "", s).strip(" :-")

    theses: list[dict] = []
    current: dict[str, str] = {}

    def _flush():
        if current.get("name") and current.get("thesis"):
            theses.append(dict(current))

    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("---") or line.startswith("==="):
            _flush()
            current = {}
            continue

        # Normalize: strip all asterisks and extra punctuation from the line
        # to detect field labels reliably
        normed = re.sub(r"\*+", "", line).strip()

        if re.match(r"(?i)^name\s*:", normed):
            _flush()
            current = {"name": normed.split(":", 1)[1].strip(), "thesis": "", "expression": ""}
        elif re.match(r"(?i)^thesis\s*:", normed):
            if not current:
                current = {"name": "Signal", "thesis": "", "expression": ""}
            current["thesis"] = normed.split(":", 1)[1].strip()
        elif re.match(r"(?i)^expression\s*:", normed):
            if current:
                current["expression"] = normed.split(":", 1)[1].strip()
        elif re.match(r"(?i)^\d+[\.\)]\s+", normed) and not current.get("name"):
            # Numbered list item like "1. NAME:" — treat number+text as name
            name_part = re.sub(r"^\d+[\.\)]\s+", "", normed).rstrip(":")
            _flush()
            current = {"name": name_part, "thesis": "", "expression": ""}

    _flush()
    return [t for t in theses if t.get("thesis")][:3]


def generate_alpha_signals(
    ovi_report: dict,
    gex_rows: list[dict],
    skew_rows: list[dict],
    f13_matches: list[dict],
) -> list[dict]:
    """
    Call Claude to synthesize 2-3 cross-source alpha trade theses.
    Returns list of dicts: [{name, thesis, expression}, ...].
    Returns empty list on any failure — panel degrades gracefully.
    """
    # Need at least some signals to synthesize from
    has_data = (
        ovi_report.get("top_calls") or ovi_report.get("top_puts")
        or gex_rows or skew_rows
    )
    if not has_data:
        return []

    try:
        from config import ANTHROPIC_API_KEY, CLAUDE_MODEL
        if not ANTHROPIC_API_KEY:
            return []
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        prompt = _build_prompt(ovi_report, gex_rows, skew_rows, f13_matches)
        msg = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=_MAX_TOKENS,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        theses = _parse_theses(raw)
        if theses:
            print(f"  [alpha_signals] Generated {len(theses)} theses")
        return theses
    except Exception as e:
        print(f"  [alpha_signals] Error: {type(e).__name__}")
        return []
