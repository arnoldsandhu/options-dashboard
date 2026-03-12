"""Flask API server — exposes POST /poll-now for remote triggering via ngrok."""
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

_poll_lock = threading.Lock()
_poll_func = None
_poll_token = None


def _build_app():
    from flask import Flask, request, jsonify

    app = Flask(__name__)

    @app.after_request
    def _cors(response):
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "X-Poll-Token, Content-Type, ngrok-skip-browser-warning"
        response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        return response

    @app.route("/poll-now", methods=["OPTIONS"])
    def _preflight():
        return "", 204

    @app.route("/poll-now", methods=["POST"])
    def _poll_now():
        token = request.headers.get("X-Poll-Token", "")
        if _poll_token and token != _poll_token:
            return jsonify({"status": "error", "message": "Unauthorized"}), 401

        if not _poll_lock.acquire(blocking=False):
            return jsonify({"status": "busy", "message": "Poll already in progress"}), 429

        def _run():
            try:
                _poll_func()
            finally:
                _poll_lock.release()

        threading.Thread(target=_run, daemon=True).start()
        now_str = datetime.now(ET).strftime("%H:%M ET")
        return jsonify({"status": "ok", "triggered": True, "timestamp": now_str})

    @app.route("/generate-client-note", methods=["OPTIONS"])
    def _note_preflight():
        return "", 204

    @app.route("/generate-client-note", methods=["POST"])
    def _generate_client_note():
        token = request.headers.get("X-Poll-Token", "")
        if _poll_token and token != _poll_token:
            return jsonify({"error": "Unauthorized"}), 401

        data = request.get_json(silent=True) or {}
        fund = (data.get("fund") or "").strip()
        if not fund:
            return jsonify({"error": "fund parameter required"}), 400

        try:
            from db.database import get_latest_gex_all, get_latest_skew_all, get_latest_ovi_report
            from dashboard.match_13f import build_13f_matches
            from dashboard.template import _gex_extreme_rows, _skew_extreme_rows
            from dashboard.client_note import generate_client_note

            ovi_report   = get_latest_ovi_report() or {}
            latest_gex   = get_latest_gex_all()
            latest_skew  = get_latest_skew_all()
            gex_rows     = _gex_extreme_rows(latest_gex)
            skew_rows    = _skew_extreme_rows(latest_skew)
            f13_matches  = build_13f_matches(ovi_report, [g["ticker"] for g in latest_gex])

            # Filter matches relevant to this fund
            relevant = [
                m for m in f13_matches
                if any(e["fund"] == fund for e in m.get("context_entries", []))
            ]

            note = generate_client_note(fund, relevant, gex_rows, skew_rows, ovi_report)
            return jsonify({"note": note, "fund": fund})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    return app


def start_server(poll_fn, token: str, port: int = 5000):
    """Start Flask API server in a background daemon thread."""
    global _poll_func, _poll_token
    _poll_func = poll_fn
    _poll_token = token
    app = _build_app()

    def _run():
        import logging
        logging.getLogger("werkzeug").setLevel(logging.ERROR)
        app.logger.setLevel(logging.ERROR)
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

    t = threading.Thread(target=_run, daemon=True, name="flask-api")
    t.start()
    print(f"[api] Flask server listening on port {port}")
