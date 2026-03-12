"""Subscribe to ntfy.sh topic and trigger poll cycles when signalled from the dashboard."""
import json
import threading
import time

_poll_lock = threading.Lock()


def start_ntfy_listener(topic: str, expected_token: str, poll_fn, base: str = "https://ntfy.sh"):
    """Start a background thread that listens for ntfy messages and triggers poll_fn."""
    if not topic:
        print("[ntfy] NTFY_TOPIC not set — remote trigger disabled.", flush=True)
        return

    def _listen():
        url = f"{base}/{topic}/json"
        print(f"[ntfy] Listening for remote triggers on topic: {topic}", flush=True)
        while True:
            try:
                import requests
                since = str(int(time.time()))
                r = requests.get(url, params={"since": since}, stream=True, timeout=120)
                for raw in r.iter_lines():
                    if not raw:
                        continue
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if msg.get("event") != "message":
                        continue
                    if msg.get("message", "").strip() != expected_token:
                        print("[ntfy] Ignored message with wrong token.", flush=True)
                        continue
                    if not _poll_lock.acquire(blocking=False):
                        print("[ntfy] Poll already in progress — skipping.", flush=True)
                        continue

                    def _run():
                        try:
                            print("[ntfy] Remote poll triggered.", flush=True)
                            poll_fn()
                        finally:
                            _poll_lock.release()

                    threading.Thread(target=_run, daemon=True).start()

            except Exception as e:
                print(f"[ntfy] Connection error: {e} — retrying in 15s", flush=True)
                time.sleep(15)

    threading.Thread(target=_listen, daemon=True, name="ntfy-listener").start()
