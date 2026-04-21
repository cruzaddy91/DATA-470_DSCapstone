#!/usr/bin/env python
"""Regenerate poster PNGs from ``models/temporal_holdout_test_scores_v2_ordertime.json`` (no retrain)."""

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from src.models.poster_figures_v2 import main

if __name__ == "__main__":
    raise SystemExit(main())
