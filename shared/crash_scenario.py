"""Scenario 5 — visibility boundary and hard kill during publication.

Raised by Tim Urista on the article: each os.replace can be atomic without the
set of reports becoming visible atomically. This script tests two things that the
four original scenarios do not:

  observe : a reader polls both outputs while a normal run publishes them and
            records every (country, month) pair of versions it sees.
  kill    : the same reader sends SIGKILL to the whole run the moment the first
            report is today's and the second is still the previous one, then
            reports what is left on disk, what a second run does, and the state
            after the stack's own recovery command (if any).

  python crash_scenario.py <stack dir> "<run command>" observe|kill
         [--repeat N] [--kill-delay SECONDS] [--recover "<command>"] [--out file.json]

Versions are computed by scenarios.py, independently of the stacks.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import scenarios as S  # noqa: E402


class Reader(threading.Thread):
    """Reads both outputs in a loop; optionally kills the run at a given pair."""

    def __init__(self, stack: Path, vers: dict, pid_group: int | None, kill_on: tuple | None,
                 delay: float = 0.0):
        super().__init__(daemon=True)
        self.delay = delay
        self.out, self.vers, self.pgid, self.kill_on = stack / "output", vers, pid_group, kill_on
        self.stop = threading.Event()
        self.samples: list[tuple[float, str, str]] = []
        self.killed_at: float | None = None

    def run(self) -> None:
        t0 = time.monotonic()
        while not self.stop.is_set():
            pair = tuple(S.label(self.out / f"{n}.csv", self.vers, n) for n in S.OUTPUTS)
            now = time.monotonic() - t0
            self.samples.append((now, *pair))
            if self.kill_on and pair == self.kill_on and self.killed_at is None:
                time.sleep(self.delay)      # 0 = the earliest possible moment
                try:
                    os.killpg(self.pgid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self.killed_at = now
                return
            time.sleep(0.001)


def summarize(samples: list) -> dict:
    """Distinct pairs seen, and total time spent in pairs that mix generations
    or show a missing / unreadable file."""
    pairs: dict[str, dict] = {}
    for i, (t, a, b) in enumerate(samples):
        dt = (samples[i + 1][0] - t) if i + 1 < len(samples) else 0.0
        k = f"{a} | {b}"
        e = pairs.setdefault(k, {"samples": 0, "seconds": 0.0})
        e["samples"] += 1
        e["seconds"] = round(e["seconds"] + dt, 4)
    bad = {k: v for k, v in pairs.items() if k.split(" | ")[0] != k.split(" | ")[1]}
    return {"pairs": pairs, "inconsistent_seconds": round(sum(v["seconds"] for v in bad.values()), 4),
            "inconsistent_pairs": sorted(bad)}


def run_once(stack: Path, vers: dict, cmd: str, mode: str, recover: str | None,
             delay: float = 0.0) -> dict:
    S.prepare(stack, vers, "normal")
    p = subprocess.Popen(["bash", "-c", cmd], cwd=stack, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, text=True, start_new_session=True)
    kill_on = ("main", "previous") if mode == "kill" else None
    rd = Reader(stack, vers, os.getpgid(p.pid), kill_on, delay)
    rd.start()
    try:
        log, _ = p.communicate(timeout=300)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        log, _ = p.communicate()
    time.sleep(0.05)
    rd.stop.set()
    rd.join()
    after = {n: S.label(stack / "output" / f"{n}.csv", vers, n) for n in S.OUTPUTS}
    row = {"mode": mode, "exit_code": p.returncode, "killed": rd.killed_at is not None,
           "after_run": after, "consistent_after_run": len(set(after.values())) == 1,
           **summarize(rd.samples), "log_tail": log.strip().splitlines()[-6:]}
    if mode == "kill":
        # what does the next scheduled run do?
        q = subprocess.run(["bash", "-c", cmd], cwd=stack, capture_output=True, text=True, timeout=300)
        nxt = {n: S.label(stack / "output" / f"{n}.csv", vers, n) for n in S.OUTPUTS}
        row["next_run"] = {"exit_code": q.returncode, "after": nxt,
                           "log_tail": (q.stdout + q.stderr).strip().splitlines()[-6:]}
        if recover:
            r = subprocess.run(["bash", "-c", recover], cwd=stack, capture_output=True, text=True, timeout=300)
            rec = {n: S.label(stack / "output" / f"{n}.csv", vers, n) for n in S.OUTPUTS}
            row["recovery"] = {"exit_code": r.returncode, "after": rec,
                               "log_tail": (r.stdout + r.stderr).strip().splitlines()[-6:]}
    return row


def main() -> int:
    a = sys.argv[1:]
    repeat, out, recover, delay = 1, None, None, 0.0
    for flag in ("--repeat", "--out", "--recover", "--kill-delay"):
        if flag in a:
            i = a.index(flag)
            v = a[i + 1]
            del a[i:i + 2]
            if flag == "--repeat":
                repeat = int(v)
            elif flag == "--kill-delay":
                delay = float(v)
            elif flag == "--out":
                out = Path(v)
            else:
                recover = v
    stack, cmd, mode = Path(a[0]).resolve(), a[1], a[2]
    vers = S.versions()
    rows = []
    for k in range(1, repeat + 1):
        row = run_once(stack, vers, cmd, mode, recover, delay)
        rows.append(row)
        print(f"PROGRESS {k}/{repeat} {mode} killed={row['killed']} after={row['after_run']} "
              f"inconsistent={row['inconsistent_seconds']}s", flush=True)
    text = json.dumps(rows, indent=1)
    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text)
    else:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
