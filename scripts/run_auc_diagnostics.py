#!/usr/bin/env python3
"""Write leakage diagnostics for the current backorder modeling workflow."""

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from src.models.backorder_modeling import generate_diagnostics, prepare_backorder_dataset


if __name__ == "__main__":
    dataset = prepare_backorder_dataset(PROJECT_ROOT)
    generate_diagnostics(dataset, PROJECT_ROOT)
