"""Generate dashboard HTML and push to GitHub Pages."""
from __future__ import annotations

import os
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

from config import GITHUB_REPO_PATH, DASHBOARD_TITLE
from dashboard.template import render

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


def _write_html(path: str) -> bool:
    try:
        html = render(DASHBOARD_TITLE)
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"[dashboard] Wrote {path}")
        return True
    except Exception as e:
        print(f"[dashboard] Failed to write HTML: {e}")
        traceback.print_exc()
        return False


def _git_push_gitpython(repo_path: str, commit_msg: str) -> bool:
    try:
        import git  # type: ignore
        repo = git.Repo(repo_path)
        rel_path = os.path.join(DOCS_SUBDIR, INDEX_FILE)
        repo.index.add([rel_path])
        if not repo.index.diff("HEAD"):
            print("[dashboard] No changes to commit.")
            return True
        repo.index.commit(commit_msg)
        origin = repo.remote(name="origin")
        origin.push()
        print(f"[dashboard] Pushed: {commit_msg}")
        return True
    except Exception as e:
        print(f"[dashboard] GitPython push failed: {e}")
        return False


def _git_push_subprocess(repo_path: str, commit_msg: str) -> bool:
    import subprocess

    def run(cmd: list[str]) -> tuple[int, str]:
        result = subprocess.run(
            cmd, cwd=repo_path, capture_output=True, text=True
        )
        return result.returncode, (result.stdout + result.stderr).strip()

    rel_path = os.path.join(DOCS_SUBDIR, INDEX_FILE)
    code, out = run(["git", "add", rel_path])
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

    code, out = run(["git", "push", "origin", "main"])
    if code != 0:
        # Try 'master' as fallback
        code, out = run(["git", "push", "origin", "master"])
    if code != 0:
        print(f"[dashboard] git push failed: {out}")
        return False

    print(f"[dashboard] Pushed: {commit_msg}")
    return True


def publish() -> bool:
    """
    Generate dashboard HTML, write to docs/index.html in the repo,
    and push to GitHub.  Failures are logged but never propagate.
    """
    path = _docs_path()
    if path is None:
        return False

    if not _write_html(path):
        return False

    now_et = datetime.now(ET).strftime("%H:%M ET")
    commit_msg = f"Dashboard update [{now_et}]"

    repo_path = GITHUB_REPO_PATH

    # Try GitPython first; fall back to subprocess
    try:
        import git  # noqa: F401
        return _git_push_gitpython(repo_path, commit_msg)
    except ImportError:
        print("[dashboard] GitPython not installed, falling back to subprocess.")
        return _git_push_subprocess(repo_path, commit_msg)
    except Exception as e:
        print(f"[dashboard] Unexpected git error: {e}")
        traceback.print_exc()
        return _git_push_subprocess(repo_path, commit_msg)
