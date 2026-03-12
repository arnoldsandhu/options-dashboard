"""Opens an ngrok tunnel to the Flask API and publishes the URL to GitHub Pages."""
import subprocess
from pathlib import Path


def _push_url_file(url: str) -> bool:
    """Write docs/ngrok_url.txt to the dashboard repo and push."""
    from config import GITHUB_REPO_PATH, GITHUB_TOKEN

    if not GITHUB_REPO_PATH:
        print("[ngrok] GITHUB_REPO_PATH not set — skipping URL push.")
        return False

    repo = Path(GITHUB_REPO_PATH)
    url_file = repo / "docs" / "ngrok_url.txt"
    url_file.write_text(url + "\n", encoding="utf-8")

    def run(cmd):
        return subprocess.run(cmd, cwd=str(repo), capture_output=True, text=True)

    run(["git", "add", "docs/ngrok_url.txt"])

    # Skip commit if nothing changed
    diff = run(["git", "diff", "--cached", "--quiet"])
    if diff.returncode == 0:
        print("[ngrok] URL unchanged — no push needed.")
        return True

    run(["git", "commit", "-m", "Update ngrok tunnel URL"])

    url_res = run(["git", "remote", "get-url", "origin"])
    clean_url = url_res.stdout.strip()
    authed_url = (
        clean_url.replace("https://", f"https://{GITHUB_TOKEN}@", 1)
        if GITHUB_TOKEN and clean_url.startswith("https://github.com/")
        else clean_url
    )

    try:
        run(["git", "remote", "set-url", "origin", authed_url])
        result = run(["git", "push", "origin", "main"])
        if result.returncode != 0:
            print(f"[ngrok] Push failed: {result.stderr.strip()}")
            return False
        print("[ngrok] URL pushed to GitHub Pages.")
        return True
    finally:
        run(["git", "remote", "set-url", "origin", clean_url])


def start_tunnel(port: int = 5000) -> str | None:
    """Open ngrok tunnel on given port. Returns public HTTPS URL or None on failure."""
    import sys
    try:
        from pyngrok import ngrok, conf
        from config import NGROK_AUTH_TOKEN

        if NGROK_AUTH_TOKEN:
            conf.get_default().auth_token = NGROK_AUTH_TOKEN

        # Kill any stale ngrok processes from previous runs before connecting
        try:
            ngrok.kill()
        except Exception:
            pass

        tunnel = ngrok.connect(port, "http")
        url = tunnel.public_url
        if url.startswith("http://"):
            url = "https://" + url[7:]

        print(f"\n[ngrok] Poll endpoint live: {url}/poll-now\n", flush=True)
        _push_url_file(url)
        return url

    except ImportError:
        print("[ngrok] pyngrok not installed — run: pip install pyngrok", flush=True)
        return None
    except Exception as e:
        print(f"[ngrok] Failed to start tunnel: {e}", flush=True)
        return None
