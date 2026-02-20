#!/usr/bin/env python
"""
Run modeling pipeline (classification + regression/forecasting).

Usage: python scripts/run_modeling.py

Requires: run_pipeline.py first (data/processed/*_with_targets.csv must exist).
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

# Execute 02_modeling logic
import subprocess
result = subprocess.run(
    [sys.executable, "-m", "jupyter", "nbconvert", "--to", "notebook", "--execute",
     "--inplace", os.path.join("notebooks", "02_modeling.ipynb")],
    cwd=PROJECT_ROOT,
)
if result.returncode != 0:
    print("Warning: nbconvert execute failed. Run 02_modeling.ipynb manually.")
sys.exit(result.returncode)
