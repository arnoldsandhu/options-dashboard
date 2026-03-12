"""
Generate personalized Bloomberg IB chat-format morning notes for specific funds.
Uses Claude API to synthesize the fund's 13F positions with current GEX / skew / OVI flow.
"""
from __future__ import annotations

_SYSTEM_PROMPT = (
    "You are a senior equity derivatives sales trader at a tier-1 broker-dealer. "
    "You are writing a personalized morning note to a hedge fund PM over Bloomberg IB chat. "
    "Based on the fund's specific 13F positions and today's market structure data, "
    "write exactly 3 numbered bullet points. Each bullet is one concise sentence (max 25 words) "
    "connecting the fund's specific holding or positioning to what is happening in today's "
    "options flow, gamma regime, or skew. "
    "Format: plain text only, no markdown, no asterisks, no bold. "
    "Start with 'GM [FUND],' on its own line. "
    "Then 3 numbered bullets, each on its own line. "
    "End with 'Happy to discuss. — Desk' on a final line. "
    "Be specific, institutional, direct. Reference real tickers and dollar amounts. "
    "No disclaimers, no filler."
)

_MAX_TOKENS = 350


def _build_prompt(
    fund: str,
    relevant_matches: list[dict],
    gex_rows: list[dict],
    skew_rows: list[dict],
    ovi_report: dict,
) -> str:
    lines: list[str] = [f"FUND: {fund}", ""]

    if relevant_matches:
        lines.append(f"{fund.upper()} POSITIONS IN PLAY TODAY:")
        for m in relevant_matches:
            ctx = [e for e in m.get("context_entries", []) if e["fund"] == fund]
            if ctx:
                c = ctx[0]
                lines.append(
                    f"  {m['ticker']} ({m['side']} flow {m['pct_str']}) — "
                    f"{c['sig_type'].upper()}: {c['detail']}"
                )
            else:
                lines.append(f"  {m['ticker']} ({m['side']} flow {m['pct_str']})")
        lines.append("")

    if gex_rows:
        lines.append("GEX DEALER REGIME TODAY:")
        for r in gex_rows[:4]:
            raw = r.get("alert_text", "")
            main = raw.split(" | ")[0] if " | " in raw else raw
            lines.append(f"  {r['ticker']}: {main[:90]}")
        lines.append("")

    if skew_rows:
        lines.append("SKEW EXTREMES:")
        for r in skew_rows[:3]:
            raw = r.get("alert_text", "")
            main = raw.split(" | ")[0] if " | " in raw else raw
            lines.append(f"  {r['ticker']}: {main[:90]}")
        lines.append("")

    ovi_analysis = ovi_report.get("analysis", "").strip()
    if ovi_analysis:
        lines.append(f"OVI FLOW SUMMARY: {ovi_analysis[:250]}")
        lines.append("")

    top_calls = ovi_report.get("top_calls", [])[:5]
    if top_calls:
        lines.append("TOP CALL FLOW:")
        for r in top_calls:
            pct = r.get("c_vol_pct")
            pct_str = f"+{pct:.0f}%" if pct is not None else "elevated"
            lines.append(f"  {r['ticker']}: {pct_str}")
        lines.append("")

    return "\n".join(lines)


def generate_client_note(
    fund: str,
    relevant_matches: list[dict],
    gex_rows: list[dict],
    skew_rows: list[dict],
    ovi_report: dict,
) -> str:
    """
    Generate a 3-point Bloomberg IB format morning note for a specific fund.
    Returns plain text ready to paste into Bloomberg IB chat.
    Returns an error string on failure — never raises.
    """
    if not fund:
        return "Error: no fund specified"
    try:
        from config import ANTHROPIC_API_KEY, CLAUDE_MODEL
        if not ANTHROPIC_API_KEY:
            return "Error: ANTHROPIC_API_KEY not configured"
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        prompt = _build_prompt(fund, relevant_matches, gex_rows, skew_rows, ovi_report)
        system = _SYSTEM_PROMPT.replace("[FUND]", fund)
        msg = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=_MAX_TOKENS,
            system=system,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except Exception as e:
        return f"Error generating note: {type(e).__name__}: {e}"
