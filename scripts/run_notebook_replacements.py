#!/usr/bin/env python3
"""Run the three notebook replacements in order (EDA → modeling summary → conclusion).

Same outputs as ``notebooks/01_*.ipynb`` … ``03_*.ipynb`` without Jupyter.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=None, help="Project root")
    args = parser.parse_args()
    root = args.root or Path(__file__).resolve().parents[1]
    py = sys.executable
    scripts = [
        root / "scripts" / "run_eda_targets_analysis.py",
        root / "scripts" / "run_modeling_notebook_export.py",
        root / "scripts" / "run_conclusion_notebook_export.py",
    ]
    for s in scripts:
        cmd = [py, str(s), "--root", str(root)]
        print("+", " ".join(cmd), flush=True)
        r = subprocess.run(cmd, cwd=str(root))
        if r.returncode != 0:
            return int(r.returncode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
