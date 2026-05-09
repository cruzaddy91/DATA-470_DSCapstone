#!/usr/bin/env python3
"""
Extract a single canonical YAML of all numbers/paths needed for poster visuals.

Truth sources (only these are read):
  - data/processed/master_order_fulfillment_modeling_v2_ordertime.csv
  - output/tables/classification_model_comparison_v2_ordertime.csv
  - models/temporal_holdout_test_scores_v2_ordertime.json
  - optional: models/auc_diagnostics_v2_ordertime.json, inventory CSV for second panel

Output: output/poster_visual_spec/canonical_poster_visual_spec.yaml

Downstream: scripts/render_poster_visuals_from_canonical_spec.py applies poster_template_style
and writes the same PNG filenames expected by build_poster.py.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
SPEC_PATH = REPO_ROOT / "output" / "poster_visual_spec" / "canonical_poster_visual_spec.yaml"


def _load_showcase_constants():
    path = REPO_ROOT / "scripts" / "generate_showcase_metrics_visuals.py"
    spec = importlib.util.spec_from_file_location("gsm", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _rel(p: Path) -> str:
    return str(p.relative_to(REPO_ROOT))


def main() -> int:
    gsm = _load_showcase_constants()
    TABLE_PATH = gsm.TABLE_PATH
    MODEL_LABELS = gsm.MODEL_LABELS
    MODEL_ORDER = gsm.MODEL_ORDER
    POSTER_MODEL_ORDER = gsm.POSTER_MODEL_ORDER
    POSTER_SPLIT_ORDER = gsm.POSTER_SPLIT_ORDER
    SPLIT_LABELS = gsm.SPLIT_LABELS
    SPLIT_ORDER = gsm.SPLIT_ORDER

    master = REPO_ROOT / "data" / "processed" / "master_order_fulfillment_modeling_v2_ordertime.csv"
    temporal_json = REPO_ROOT / "models" / "temporal_holdout_test_scores_v2_ordertime.json"
    diag_json = REPO_ROOT / "models" / "auc_diagnostics_v2_ordertime.json"

    if not master.is_file():
        print(f"Missing {master}", file=sys.stderr)
        return 1
    if not Path(TABLE_PATH).is_file():
        print(f"Missing {TABLE_PATH}", file=sys.stderr)
        return 1
    if not temporal_json.is_file():
        print(f"Missing {temporal_json}", file=sys.stderr)
        return 1

    order = pd.read_csv(master, low_memory=False)
    bo_counts = order["target_backorder_risk"].value_counts(dropna=False).sort_index()
    target_balance = {
        "order_lines_total": int(len(order)),
        "counts": {
            "no_backorder": int(bo_counts.get(0, 0)),
            "backorder": int(bo_counts.get(1, 0)),
            "unresolved_na": int(order["target_backorder_risk"].isna().sum()),
        },
        "bar_labels": ["No", "Yes", "Unresolved"],
    }

    inv_path = REPO_ROOT / "data" / "processed" / "master_inventory_material_with_targets.csv"
    overstock = None
    if inv_path.is_file():
        inv = pd.read_csv(inv_path, low_memory=False)
        if "target_overstock_risk" in inv.columns:
            ov_counts = inv["target_overstock_risk"].value_counts().sort_index()
            overstock = {
                "inventory_rows_total": int(len(inv)),
                "counts": {
                    "no_overstock": int(ov_counts.get(0, 0)),
                    "overstock": int(ov_counts.get(1, 0)),
                },
                "bar_labels": ["No (no overstock)", "Yes (overstock)"],
            }

    df = pd.read_csv(TABLE_PATH)
    df = df[df["split"].isin(SPLIT_ORDER) & df["model"].isin(MODEL_ORDER)].copy()
    df = df[df["model"].isin(POSTER_MODEL_ORDER)].copy()

    heatmap_grid = {"roc_auc": {}, "pr_auc": {}}
    for m in POSTER_MODEL_ORDER:
        heatmap_grid["roc_auc"][m] = {}
        heatmap_grid["pr_auc"][m] = {}
        for sp in POSTER_SPLIT_ORDER:
            row = df[(df["model"] == m) & (df["split"] == sp)]
            if len(row) == 1:
                heatmap_grid["roc_auc"][m][sp] = float(row.iloc[0]["roc_auc"])
                heatmap_grid["pr_auc"][m][sp] = float(row.iloc[0]["pr_auc"])

    payload = json.loads(temporal_json.read_text(encoding="utf-8"))
    temporal_meta = {
        "n_test": len(payload.get("y_true", [])),
        "n_positive": int(sum(payload.get("y_true", []))),
        "baseline_positive_rate": float(payload.get("baseline_positive_rate", 0)),
    }

    spec = {
        "schema_version": 1,
        "template_id": "westminster_brand_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "description": (
            "Canonical facts extracted from pipeline outputs. Renders use poster_template_style; "
            "numbers are not invented—only copied from the listed sources."
        ),
        "truth_paths": {
            "master_order_csv": _rel(master),
            "classification_model_comparison_csv": _rel(Path(TABLE_PATH)),
            "temporal_holdout_scores_json": _rel(temporal_json),
            "auc_diagnostics_json": _rel(diag_json) if diag_json.is_file() else None,
        },
        "target_balance": target_balance,
        "overstock_optional": overstock,
        "model_comparison_heatmap": {
            "models_order": POSTER_MODEL_ORDER,
            "splits_order": POSTER_SPLIT_ORDER,
            "model_labels": {k: MODEL_LABELS[k] for k in POSTER_MODEL_ORDER},
            "split_labels": {k: SPLIT_LABELS[k] for k in POSTER_SPLIT_ORDER},
            "metrics": ["roc_auc", "pr_auc"],
            "values": heatmap_grid,
        },
        "temporal_holdout_meta": temporal_meta,
    }

    SPEC_PATH.parent.mkdir(parents=True, exist_ok=True)
    SPEC_PATH.write_text(
        yaml.dump(spec, default_flow_style=False, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    print(f"Wrote {SPEC_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
