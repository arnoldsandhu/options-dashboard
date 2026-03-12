"""Run git commit and push via Python subprocess."""
import subprocess, os, sys

repo = os.path.dirname(os.path.abspath(__file__))

def run(cmd):
    r = subprocess.run(cmd, cwd=repo, capture_output=True, text=True)
    out = (r.stdout + r.stderr).strip()
    if out:
        print(out)
    return r.returncode

# Remove temp files
for f in ["check_chain.py", "test_prob.py"]:
    fp = os.path.join(repo, f)
    if os.path.exists(fp):
        os.remove(fp)
        print(f"Removed {f}")

files = [
    "gex_engine/greek_calc.py",
    "gex_engine/sign_engine.py",
    "gex_engine/exposure_calc.py",
    "gex_engine/vendor_ingest.py",
    "dashboard/gex_chart.py",
    "dashboard/template.py",
]
for f in files:
    run(["git", "add", f])

run(["git", "status"])

msg = """Add Probabilistic sign mode with per-contract p_dealer_short and uncertainty band

Per-contract p_dealer_short heuristics (delta thresholds, DTE, vol/OI adjustments).
expected_sign = 2*p_dealer_short - 1 as continuous [-1,+1] multiplier.

Uncertainty band computed at p=0.40 (conservative) and p=0.95 (aggressive).
Displayed in stat card as: $+25,412M (range: $-7,976M to $+35,891M)

New: bs_delta/get_delta in greek_calc, get_p_dealer_short in sign_engine,
calc_probabilistic_range in exposure_calc, Probabilistic button in template.

Also commits IV field location fix (vendor_ingest.py): implied_volatility is
top-level on OptionContractSnapshot, not in greeks. Fixes IV=0 for all contracts.

Generated with [Claude Code](https://claude.ai/code)
via [Happy](https://happy.engineering)

Co-Authored-By: Claude <noreply@anthropic.com>
Co-Authored-By: Happy <yesreply@happy.engineering>"""

code = run(["git", "commit", "-m", msg])
if code != 0:
    print("Commit failed or nothing to commit")
    sys.exit(1)

# Push
code = run(["git", "push", "origin", "master"])
if code != 0:
    code = run(["git", "push", "origin", "main"])
if code != 0:
    print("Push failed")
    sys.exit(1)

print("Done!")
