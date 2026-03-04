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
    "AMD", "NFLX", "UBER", "ARM",
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
VOL_MIN_CONTRACTS = 500
VOL_AVG_DAYS = 20
VOL_AVG_MULTIPLIER = 2.0

GEX_HISTORY_DAYS = 30
GEX_PERCENTILE_EXTREME = 5
GEX_SINGLE_STRIKE_PCT = 0.25

ROLL_DTE_THRESHOLD = 7
ROLL_OI_THRESHOLD = 1000

EARNINGS_DAYS_AHEAD = 5
EARNINGS_HISTORY_COUNT = 8
EARNINGS_MISPRICE_THRESHOLD = 0.015  # 1.5%

DEDUP_HOURS = 4

DB_PATH = os.path.join(os.path.dirname(__file__), "options_alerts.db")

GMAIL_MCP_URL = "https://gmail.mcp.claude.com/mcp"

GITHUB_REPO_PATH = os.getenv("GITHUB_REPO_PATH", "")
DASHBOARD_TITLE = os.getenv("DASHBOARD_TITLE", "Options Flow Dashboard")

MARKET_OPEN_ET = (9, 35)
MARKET_CLOSE_ET = (16, 0)
POLL_INTERVAL_MINUTES = 20
