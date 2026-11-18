"""Writes modern/dags/daily_revenue_safe_probe.py: a copy of the safe DAG with one
change, a pause between the two os.replace calls of publish(). It makes the
window that Tim Urista described long enough to hit with SIGKILL on purpose.
The real window is microseconds; the pause does not change what the code does.

  python make_probe_dag.py [seconds]      (default 2)
"""
import sys
from pathlib import Path

secs = sys.argv[1] if len(sys.argv) > 1 else "2"
src = Path(__file__).resolve().parents[1] / "modern" / "dags" / "daily_revenue_safe.py"
t = src.read_text()
old = "                    os.replace(HOME / \"output\" / (f + \".tmp\"), HOME / \"output\" / f)\n"
assert t.count(old) == 1
new = old + f"                    time.sleep({secs})   # PROBE: widens the window between the two renames\n"
t = t.replace(old, new).replace("import os\n", "import os\nimport time\n", 1)
t = t.replace("def daily_revenue_safe():", "def daily_revenue_safe_probe():").replace(
    "daily_revenue_safe()\n", "daily_revenue_safe_probe()\n")
(src.parent / "daily_revenue_safe_probe.py").write_text(t)
print("written", src.parent / "daily_revenue_safe_probe.py")
