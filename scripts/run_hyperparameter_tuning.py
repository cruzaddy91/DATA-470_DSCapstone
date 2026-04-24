#!/usr/bin/env python3
"""
Tune v2 order-time base learners (LR, LightGBM, RandomForest) via multi-phase
grid search with temporal expanding-window folds, scoring PR-AUC.

Writes: models/hyperparameters_tuned_v2_ordertime.json

The pipeline_*.py builders read that JSON (if present) and fall back to
hand-defaults otherwise — tuning is therefore optional and non-destructive.

Usage:
    .venv-v2/bin/python scripts/run_hyperparameter_tuning.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from src.models.backorder_modeling import prepare_backorder_dataset
from src.models.v2_ordertime.hyperparameter_tuning import (
    TUNED_PARAMS_FILE,
    tune_all_base_learners,
)

DATE_COLUMN = os.environ.get("MODEL_DATE_COLUMN", "order_date")


def main() -> int:
    t0 = time.time()
    print(f"[{datetime.now().isoformat(timespec='seconds')}] Loading dataset...", flush=True)
    dataset = prepare_backorder_dataset(PROJECT_ROOT)
    X = dataset.features.reset_index(drop=True)
    y = dataset.target.reset_index(drop=True).to_numpy()
    dates_col = DATE_COLUMN if DATE_COLUMN in dataset.meta.columns else dataset.meta.columns[0]
    dates = dataset.meta.reset_index(drop=True)[dates_col]
    import pandas as pd
    dates = pd.to_datetime(dates, errors="coerce")
    print(f"  rows={len(X)} positives={int(y.sum())} positive_rate={y.mean():.4f}", flush=True)
    print(f"  date range: {dates.min()} -> {dates.max()}", flush=True)

    results = tune_all_base_learners(dataset, X, y, dates)

    out = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scoring": "average_precision_score (PR-AUC)",
        "fold_strategy": "temporal_expanding_window",
        "n_folds": 3,
        "val_fraction": 0.15,
        "tuned": results,
    }
    out_path = Path(PROJECT_ROOT) / "models" / TUNED_PARAMS_FILE
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")

    total = time.time() - t0
    print(f"\n[{datetime.now().isoformat(timespec='seconds')}] Done in {total:.1f}s. Wrote {out_path}", flush=True)
    print("\nSummary (best mean PR-AUC per model):", flush=True)
    for name, info in results.items():
        print(f"  {name:22s} PR={info['best_mean_pr_auc']:.4f}  params={info['best_params']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
