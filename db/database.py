import sqlite3
import json
from datetime import datetime, timedelta
from contextlib import contextmanager
from config import DB_PATH, DEDUP_HOURS, GEX_HISTORY_DAYS


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS gex_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                net_gex REAL NOT NULL,
                spot_price REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS alerts_sent (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                alert_text TEXT NOT NULL,
                timestamp TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS signal_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                signal_data_json TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                alerted INTEGER NOT NULL DEFAULT 0
            );
        """)
    print("[DB] Initialized database.")


# ── GEX History ──────────────────────────────────────────────────────────────

def save_gex(ticker: str, net_gex: float, spot_price: float):
    ts = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO gex_history (ticker, timestamp, net_gex, spot_price) VALUES (?, ?, ?, ?)",
            (ticker, ts, net_gex, spot_price),
        )


def get_gex_history(ticker: str, days: int = GEX_HISTORY_DAYS) -> list[dict]:
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM gex_history WHERE ticker = ? AND timestamp >= ? ORDER BY timestamp",
            (ticker, since),
        ).fetchall()
    return [dict(r) for r in rows]


def get_latest_gex(ticker: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM gex_history WHERE ticker = ? ORDER BY timestamp DESC LIMIT 1",
            (ticker,),
        ).fetchone()
    return dict(row) if row else None


# ── Deduplication ─────────────────────────────────────────────────────────────

def is_already_alerted(ticker: str, signal_type: str) -> bool:
    cutoff = (datetime.utcnow() - timedelta(hours=DEDUP_HOURS)).isoformat()
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM alerts_sent WHERE ticker = ? AND signal_type = ? AND timestamp >= ?",
            (ticker, signal_type, cutoff),
        ).fetchone()
    return row is not None


def record_alert(ticker: str, signal_type: str, alert_text: str):
    ts = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO alerts_sent (ticker, signal_type, alert_text, timestamp) VALUES (?, ?, ?, ?)",
            (ticker, signal_type, alert_text, ts),
        )


# ── Signal Log ────────────────────────────────────────────────────────────────

def log_signal(ticker: str, signal_type: str, signal_data: dict, alerted: bool):
    ts = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO signal_log (ticker, signal_type, signal_data_json, timestamp, alerted) VALUES (?, ?, ?, ?, ?)",
            (ticker, signal_type, json.dumps(signal_data), ts, int(alerted)),
        )


def get_day_signals(date_str: str | None = None) -> list[dict]:
    if date_str is None:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM signal_log WHERE timestamp LIKE ? ORDER BY timestamp DESC",
            (f"{date_str}%",),
        ).fetchall()
    return [dict(r) for r in rows]


# ── Roll Dedup ────────────────────────────────────────────────────────────────

# ── Dashboard Queries ─────────────────────────────────────────────────────────

def get_today_alerts(date_str: str | None = None) -> list[dict]:
    """All alerts sent today, newest first."""
    if date_str is None:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM alerts_sent WHERE timestamp LIKE ? ORDER BY timestamp DESC",
            (f"{date_str}%",),
        ).fetchall()
    return [dict(r) for r in rows]


def get_latest_gex_all() -> list[dict]:
    """Most recent GEX row for every ticker that has history."""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT g.*
            FROM gex_history g
            INNER JOIN (
                SELECT ticker, MAX(timestamp) AS max_ts
                FROM gex_history
                GROUP BY ticker
            ) latest ON g.ticker = latest.ticker AND g.timestamp = latest.max_ts
            ORDER BY g.ticker
            """,
        ).fetchall()
    return [dict(r) for r in rows]


def get_gex_percentile(ticker: str, net_gex: float, days: int = GEX_HISTORY_DAYS) -> float:
    """Percentile rank of net_gex within the last `days` days for ticker."""
    history = get_gex_history(ticker, days)
    values = [r["net_gex"] for r in history]
    if not values:
        return 50.0
    below = sum(1 for v in values if v < net_gex)
    return (below / len(values)) * 100.0


# ── Roll Dedup ────────────────────────────────────────────────────────────────

def has_roll_alert(ticker: str, contract_key: str) -> bool:
    """contract_key = '{ticker}_{strike}_{expiry}_{call_put}'"""
    cutoff = (datetime.utcnow() - timedelta(hours=DEDUP_HOURS)).isoformat()
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM alerts_sent WHERE ticker = ? AND signal_type = 'ROLL' AND alert_text LIKE ? AND timestamp >= ?",
            (ticker, f"%{contract_key}%", cutoff),
        ).fetchone()
    return row is not None
