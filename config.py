import os
from dotenv import load_dotenv

load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

WATCHLIST = [
    "SPY", "QQQ", "NVDA", "AAPL", "MSFT", "META", "GOOG", "AMZN",
    "TSLA", "GS", "JPM", "XLF", "XLK", "XLE", "IWM", "COIN",
    "AMD", "NFLX", "UBER", "ARM", "MRVL",
]

CLAUDE_MODEL = "claude-sonnet-4-20250514"
CLAUDE_MAX_TOKENS = 100
CLAUDE_SYSTEM_PROMPT = (
    "You are an options sales trader at an institutional broker-dealer. "
    "Write a single punchy sentence (max 20 words) that a sales trader "
    "would say to a hedge fund PM about this signal. Be direct, no fluff."
)

# Signal thresholds
VOL_OI_RATIO_THRESHOLD = 3.0
VOL_MIN_CONTRACTS = 100   # Minimum volume contracts (pre-filter before ratio calc)
VOL_MIN_OI = 500          # Minimum open interest (prevents tiny OI inflating ratio to absurd levels)
VOL_MAX_RATIO = 500.0     # Sanity cap — ratios above this are almost certainly data artifacts
VOL_AVG_DAYS = 20
VOL_AVG_MULTIPLIER = 2.0

GEX_HISTORY_DAYS = 30
GEX_PERCENTILE_EXTREME = 5
GEX_SINGLE_STRIKE_PCT = 0.25
GEX_MIN_ABS_M = 1.0  # Minimum |net GEX| in $M to generate an alert (filters noise for low-OI stocks)

ROLL_DTE_THRESHOLD = 7
ROLL_OI_THRESHOLD = 1000

EARNINGS_DAYS_AHEAD = 5
EARNINGS_HISTORY_COUNT = 8
EARNINGS_MISPRICE_THRESHOLD = 0.015  # 1.5%

DEDUP_HOURS = 4

DB_PATH = os.path.join(os.path.dirname(__file__), "options_alerts.db")

GMAIL_MCP_URL = "https://gmail.mcp.claude.com/mcp"

GITHUB_REPO_PATH = os.getenv("GITHUB_REPO_PATH", "")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
DASHBOARD_TITLE = os.getenv("DASHBOARD_TITLE", "Gamma Mate — Options Flow Dashboard")
NGROK_AUTH_TOKEN = os.getenv("NGROK_AUTH_TOKEN", "")
POLL_TOKEN = os.getenv("POLL_TOKEN", "")
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "")
FLASK_API_URL = os.getenv("FLASK_API_URL", "http://localhost:5000")

MARKET_OPEN_ET = (9, 35)
MARKET_CLOSE_ET = (16, 0)
POLL_INTERVAL_MINUTES = 20
