#!/usr/bin/env python3
"""Replace ``notebooks/03_conclusion.ipynb``: summarize metrics across splits; verify figures exist.

Writes ``output/tables/conclusion_model_metrics_rounded.csv`` for reuse in reports (optional artifact).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _round_metric(value):
    if value is None:
        return None
    try:
        return round(float(value), 3)
    except (TypeError, ValueError):
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=None)
    args = parser.parse_args()
    root = args.root or _project_root()
    models = root / "models"
    figs = root / "output" / "figures"
    tables = root / "output" / "tables"
    tables.mkdir(parents=True, exist_ok=True)

    cls_path = models / "classification_metrics_v2_ordertime.json"
    reg_path = models / "regression_metrics_v2_ordertime.json"
    cls_metrics = json.loads(cls_path.read_text()) if cls_path.exists() else {}
    reg_metrics = json.loads(reg_path.read_text()) if reg_path.exists() else {}

    print("Classification keys:", list(cls_metrics.keys())[:12])
    print("Regression keys:", list(reg_metrics.keys())[:12] if reg_metrics else [])

    rows = []
    if cls_metrics:
        temporal_models = cls_metrics.get("temporal_holdout", {}).get("models", {})
        grouped_models = cls_metrics.get("group_holdout", {}).get("models", {})
        recent_models = cls_metrics.get("recent_24_week_temporal_holdout", {}).get("models", {})
        split_blocks = [
            ("temporal_holdout", temporal_models),
            ("group_holdout", grouped_models),
            ("recent_24_week_temporal_holdout", recent_models),
        ]
        if cls_metrics.get("snapshot_backorder_status", {}).get("available"):
            split_blocks.extend(
                [
                    ("snapshot_backorder_temporal_holdout", cls_metrics.get("snapshot_backorder_temporal_holdout", {}).get("models", {})),
                    ("snapshot_backorder_group_holdout", cls_metrics.get("snapshot_backorder_group_holdout", {}).get("models", {})),
                    (
                        "snapshot_backorder_recent_24_week_temporal_holdout",
                        cls_metrics.get("snapshot_backorder_recent_24_week_temporal_holdout", {}).get("models", {}),
                    ),
                ]
            )
        for split_name, split_models in split_blocks:
            for model_name, metrics in split_models.items():
                rows.append(
                    {
                        "Split": split_name,
                        "Model": model_name.replace("_", " ").title(),
                        "F1": _round_metric(metrics.get("f1")),
                        "PR-AUC": _round_metric(metrics.get("pr_auc")),
                        "ROC-AUC": _round_metric(metrics.get("roc_auc")),
                    }
                )
        df = pd.DataFrame(rows)
        print("\nModel metrics (rounded):\n", df.to_string(index=False))
        print("\nSelected model:", cls_metrics.get("selected_model", {}))
        out_csv = tables / "conclusion_model_metrics_rounded.csv"
        df.to_csv(out_csv, index=False)
        print("Wrote:", out_csv)

    figure_paths = [
        figs / "target_balance_v2_ordertime.png",
        figs / "classification_confusion_matrices_v2_ordertime.png",
        figs / "classification_feature_importance_v2_ordertime.png",
    ]
    print("\nFigure path check:")
    for path in figure_paths:
        ok = path.exists()
        print(f"  {'OK ' if ok else 'MISS'} {path.relative_to(root)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
