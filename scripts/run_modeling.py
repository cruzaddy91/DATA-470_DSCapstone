#!/usr/bin/env python
"""Run the reproducible modeling pipeline for presentation artifacts."""

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from src.models.backorder_modeling import run_overfit_evaluation
from scripts.generate_model_health_dashboard import generate_dashboard
from scripts.generate_threshold_frontier_report import generate_threshold_frontier_report


if __name__ == "__main__":
    run_overfit_evaluation(PROJECT_ROOT)
    generate_threshold_frontier_report(PROJECT_ROOT)
    generate_dashboard(PROJECT_ROOT)
