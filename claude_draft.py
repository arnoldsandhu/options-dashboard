import anthropic
from config import ANTHROPIC_API_KEY, CLAUDE_MODEL, CLAUDE_MAX_TOKENS, CLAUDE_SYSTEM_PROMPT

_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    return _client


def draft_observation(signal_description: str) -> str:
    """Return a punchy one-sentence sales-trader observation for the given signal."""
    if not ANTHROPIC_API_KEY:
        return "[Claude observation unavailable — no API key]"
    try:
        client = _get_client()
        message = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=CLAUDE_MAX_TOKENS,
            system=CLAUDE_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": signal_description}],
        )
        return message.content[0].text.strip()
    except Exception as e:
        return f"[Claude error: {e}]"
