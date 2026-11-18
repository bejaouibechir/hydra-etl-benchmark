"""The publication recipe: write each run into its own directory, then switch ONE file.

A reader resolves output/manifest.json once, then reads both reports from the
generation it names. Since the manifest is replaced with a single os.replace, a
reader sees the previous generation or the new one, never a mix, and a run killed at
any point leaves the manifest on a complete generation.

  python publish_generation.py [<output dir> <generation>] [--keep N]

Called after both reports are in <output dir>/gen/<generation>/. It checks that the
generation is complete, switches the manifest, and prunes old generations (the
newest N are kept, and never the one the manifest names).
"""
from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path

REPORTS = ("revenue_by_country.csv", "revenue_by_month.csv")


def publish(out: Path, generation: str, keep: int = 3) -> None:
    gen = out / "gen" / generation
    missing = [f for f in REPORTS if not (gen / f).exists()]
    if missing:
        raise SystemExit(f"generation {generation} is incomplete, not published: {missing}")
    tmp = out / "manifest.json.tmp"
    tmp.write_text(json.dumps({"generation": generation}), encoding="utf-8")
    os.replace(tmp, out / "manifest.json")           # the one atomic switch
    olds = sorted((d for d in (out / "gen").iterdir() if d.is_dir() and d.name != generation),
                  key=lambda d: d.stat().st_mtime, reverse=True)
    for d in olds[max(keep - 1, 0):]:
        shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    a = sys.argv[1:]
    keep = 3
    if "--keep" in a:
        i = a.index("--keep")
        keep = int(a[i + 1])
        del a[i:i + 2]
    # no arguments (Hydra ETL's python action cannot pass any): the project's output/ folder
    # and the run's generation, which Hydra exposes as the environment variable `generation`
    publish(Path(a[0]) if a else Path("output"), a[1] if len(a) > 1 else os.environ["generation"], keep)
