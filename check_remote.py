import subprocess, os
repo = os.path.dirname(os.path.abspath(__file__))
def run(cmd):
    r = subprocess.run(cmd, cwd=repo, capture_output=True, text=True)
    return (r.stdout + r.stderr).strip()
print("remotes:", run(["git", "remote", "-v"]))
print("config:", run(["git", "config", "--list"]))
