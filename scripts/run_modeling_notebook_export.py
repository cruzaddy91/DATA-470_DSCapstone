#!/usr/bin/env python3
"""Replace ``notebooks/02_modeling.ipynb``: print v2 dataset + metrics summaries; verify key figures exist.

Does not retrain models (use ``scripts/run_modeling.py``). Confirms paths used by the dashboard/poster still resolve.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=None)
    args = parser.parse_args()
    root = args.root or _project_root()
    processed = root / "data" / "processed"
    models = root / "models"
    figs = root / "output" / "figures"
    tables = root / "output" / "tables"

    dataset_path = processed / "master_order_fulfillment_modeling_v2_ordertime.csv"
    classification_path = models / "classification_metrics_v2_ordertime.json"
    overfit_path = models / "overfit_eval_results_v2_ordertime.json"
    regression_path = models / "regression_metrics_v2_ordertime.json"

    order = pd.read_csv(dataset_path, low_memory=False)
    classification_metrics = json.loads(classification_path.read_text()) if classification_path.exists() else {}
    overfit_eval = json.loads(overfit_path.read_text()) if overfit_path.exists() else {}
    regression_metrics = json.loads(regression_path.read_text()) if regression_path.exists() else {}

    print("Order-time v2 dataset shape:", order.shape)
    print("Order-time target distribution:")
    print(order["target_backorder_risk"].value_counts(dropna=False))
    print("Positive rate:", round(float(order["target_backorder_risk"].mean()), 4))
    print("Selected model:", classification_metrics.get("selected_model", {}))
    print("Temporal holdout keys:", list((classification_metrics.get("temporal_holdout") or {}).keys())[:8], "...")
    print("Grouped holdout keys:", list((classification_metrics.get("group_holdout") or {}).keys())[:8], "...")
    print(
        "Recent 24-week temporal holdout (summary keys):",
        list((classification_metrics.get("recent_24_week_temporal_holdout") or {}).keys())[:12],
        "...",
    )
    if classification_metrics.get("snapshot_backorder_status", {}).get("available"):
        print("Snapshot backorder dataset summary:", classification_metrics.get("snapshot_backorder_dataset_summary", {}))
        print("Snapshot backorder temporal holdout:", classification_metrics.get("snapshot_backorder_temporal_holdout", {}))
    else:
        print("Snapshot backorder status:", classification_metrics.get("snapshot_backorder_status", {}))
    print("Regression side outputs keys:", list(regression_metrics.keys())[:10] if regression_metrics else [])

    comparison_path = tables / "classification_model_comparison_v2_ordertime.csv"
    importance_path = tables / "classification_feature_importance_v2_ordertime.csv"

    if comparison_path.exists():
        print("\n--- classification_model_comparison_v2_ordertime.csv (head) ---")
        print(pd.read_csv(comparison_path).head(20).to_string())
    else:
        print("Missing:", comparison_path)
    if importance_path.exists():
        print("\n--- classification_feature_importance_v2_ordertime.csv (head) ---")
        print(pd.read_csv(importance_path).head(15).to_string())
    else:
        print("Missing:", importance_path)

    figure_paths = [
        figs / "target_balance_v2_ordertime.png",
        figs / "classification_confusion_matrices_v2_ordertime.png",
        figs / "classification_feature_importance_v2_ordertime.png",
    ]
    print("\nFigure path check (dashboard / poster):")
    for path in figure_paths:
        ok = path.exists()
        print(f"  {'OK ' if ok else 'MISS'} {path.relative_to(root)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
