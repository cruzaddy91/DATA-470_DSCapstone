#!/usr/bin/env python3
"""Run the leakage-safe overfitting evaluation workflow."""

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from src.models.backorder_modeling import run_overfit_evaluation


if __name__ == "__main__":
    run_overfit_evaluation(PROJECT_ROOT)
