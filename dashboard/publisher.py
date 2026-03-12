"""Generate dashboard HTML and push to GitHub Pages."""
from __future__ import annotations

import os
import shutil
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

from config import GITHUB_REPO_PATH, GITHUB_TOKEN, DASHBOARD_TITLE
from dashboard.template import render
from dashboard.gex_chart import get_gex_chart_data, build_heatmap_payload

ET = ZoneInfo("America/New_York")
DOCS_SUBDIR = "docs"
INDEX_FILE = "index.html"


def _docs_path() -> str | None:
    if not GITHUB_REPO_PATH:
        print("[dashboard] GITHUB_REPO_PATH not set — skipping publish.")
        return None
    docs = os.path.join(GITHUB_REPO_PATH, DOCS_SUBDIR)
    os.makedirs(docs, exist_ok=True)
    return os.path.join(docs, INDEX_FILE)


def _sync_assets() -> list[str]:
    """Copy assets/ from project root into docs/assets/. Returns relative paths staged."""
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    src_dir = os.path.join(here, "assets")
    dst_dir = os.path.join(GITHUB_REPO_PATH, DOCS_SUBDIR, "assets")
    staged = []
    if not os.path.isdir(src_dir):
        return staged
    os.makedirs(dst_dir, exist_ok=True)
    for fname in os.listdir(src_dir):
        src = os.path.join(src_dir, fname)
        dst = os.path.join(dst_dir, fname)
        if os.path.isfile(src):
            shutil.copy2(src, dst)
            staged.append(os.path.join(DOCS_SUBDIR, "assets", fname))
    return staged


def _write_html(path: str, alpha_theses: list | None = None) -> bool:
    try:
        gex_chart_data = get_gex_chart_data()
    except Exception as e:
        print(f"[dashboard] gex_chart fetch error: {e}")
        gex_chart_data = {}
    try:
        gex_heatmap_data = build_heatmap_payload()
    except Exception as e:
        print(f"[dashboard] heatmap payload error: {e}")
        gex_heatmap_data = {}
    try:
        html = render(DASHBOARD_TITLE, gex_chart_data=gex_chart_data,
                      gex_heatmap_data=gex_heatmap_data,
                      alpha_theses=alpha_theses)
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"[dashboard] Wrote {path}")
        # Also keep local preview in sync
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        preview_path = os.path.join(here, "dashboard_preview.html")
        with open(preview_path, "w", encoding="utf-8") as f:
            f.write(html)
        return True
    except Exception as e:
        print(f"[dashboard] Failed to write HTML: {e}")
        traceback.print_exc()
        return False


def _authed_url(url: str) -> str:
    """Inject PAT into an https:// GitHub URL if GITHUB_TOKEN is set."""
    if GITHUB_TOKEN and url.startswith("https://github.com/"):
        return url.replace("https://", f"https://{GITHUB_TOKEN}@", 1)
    return url


def _git_push_gitpython(repo_path: str, commit_msg: str, extra_paths: list[str] | None = None) -> bool:
    try:
        import git  # type: ignore
        repo = git.Repo(repo_path)
        rel_path = os.path.join(DOCS_SUBDIR, INDEX_FILE)
        paths_to_add = [rel_path] + (extra_paths or [])
        repo.index.add(paths_to_add)
        if not repo.index.diff("HEAD"):
            print("[dashboard] No changes to commit.")
            return True
        repo.index.commit(commit_msg)
        origin = repo.remote(name="origin")
        clean_url = origin.url
        authed_url = _authed_url(clean_url)
        try:
            origin.set_url(authed_url)
            origin.push(refspec="refs/heads/main:refs/heads/main")
        finally:
            origin.set_url(clean_url)
        print(f"[dashboard] Pushed: {commit_msg}")
        return True
    except Exception as e:
        print(f"[dashboard] GitPython push failed: {e}")
        return False


def _git_push_subprocess(repo_path: str, commit_msg: str, extra_paths: list[str] | None = None) -> bool:
    import subprocess

    def run(cmd: list[str]) -> tuple[int, str]:
        result = subprocess.run(
            cmd, cwd=repo_path, capture_output=True, text=True
        )
        return result.returncode, (result.stdout + result.stderr).strip()

    rel_path = os.path.join(DOCS_SUBDIR, INDEX_FILE)
    paths_to_add = [rel_path] + (extra_paths or [])
    code, out = run(["git", "add"] + paths_to_add)
    if code != 0:
        print(f"[dashboard] git add failed: {out}")
        return False

    # Check if there's actually something staged
    code, diff_out = run(["git", "diff", "--cached", "--quiet"])
    if code == 0:
        print("[dashboard] No changes staged — nothing to commit.")
        return True

    code, out = run(["git", "commit", "-m", commit_msg])
    if code != 0:
        print(f"[dashboard] git commit failed: {out}")
        return False

    # Get current remote URL and temporarily inject PAT for push
    _, clean_url = run(["git", "remote", "get-url", "origin"])
    authed_url = _authed_url(clean_url)
    try:
        run(["git", "remote", "set-url", "origin", authed_url])
        code, out = run(["git", "push", "-u", "origin", "main"])
        if code != 0:
            code, out = run(["git", "push", "-u", "origin", "master"])
    finally:
        run(["git", "remote", "set-url", "origin", clean_url])

    if code != 0:
        print(f"[dashboard] git push failed: {out}")
        return False

    print(f"[dashboard] Pushed: {commit_msg}")
    return True


def publish(alpha_theses: list | None = None) -> bool:
    """
    Generate dashboard HTML, write to docs/index.html in the repo,
    and push to GitHub.  Failures are logged but never propagate.
    """
    path = _docs_path()
    if path is None:
        return False

    if not _write_html(path, alpha_theses=alpha_theses):
        return False

    asset_paths = _sync_assets()
    if asset_paths:
        print(f"[dashboard] Synced assets: {asset_paths}")

    now_et = datetime.now(ET).strftime("%H:%M ET")
    commit_msg = f"Dashboard update [{now_et}]"

    repo_path = GITHUB_REPO_PATH

    # Try GitPython first; fall back to subprocess
    try:
        import git  # noqa: F401
        return _git_push_gitpython(repo_path, commit_msg, asset_paths)
    except ImportError:
        print("[dashboard] GitPython not installed, falling back to subprocess.")
        return _git_push_subprocess(repo_path, commit_msg, asset_paths)
    except Exception as e:
        print(f"[dashboard] Unexpected git error: {e}")
        traceback.print_exc()
        return _git_push_subprocess(repo_path, commit_msg, asset_paths)
