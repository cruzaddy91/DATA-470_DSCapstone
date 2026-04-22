#!/usr/bin/env python3
"""Recompute deployment_readiness in metrics JSON from frozen split metrics (no retrain).

Use after changing MODEL_DEPLOY_* policy or when the metrics file was produced with
different env than you want for the dashboard GO banner. Reads and writes
models/classification_metrics_v2_ordertime.json.
"""

from __future__ import annotations

import json
import os
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    path = root / "models" / "classification_metrics_v2_ordertime.json"
    if not path.exists():
        print(f"Missing {path}", flush=True)
        return 1

    precision_floor = float(os.environ.get("MODEL_DEPLOY_PRECISION_FLOOR", "0.15"))
    recall_floor = float(os.environ.get("MODEL_DEPLOY_RECALL_FLOOR", "0.35"))
    min_recent_train = int(os.environ.get("MODEL_DEPLOY_MIN_RECENT_TRAIN_POSITIVES", "25"))
    min_recent_test = int(os.environ.get("MODEL_DEPLOY_MIN_RECENT_TEST_POSITIVES", "50"))
    gate_mode = (os.environ.get("MODEL_DEPLOY_GATE_MODEL") or "selected").strip().lower()
    recent_binding = os.environ.get("MODEL_DEPLOY_RECENT_BINDING", "0") == "1"

    data = json.loads(path.read_text())
    selected_name = (data.get("selected_model") or {}).get("name")
    if not selected_name:
        print("Missing selected_model.name", flush=True)
        return 1

    temporal_metrics = (data.get("temporal_holdout") or {}).get("models") or {}
    recent_block = data.get("recent_24_week_temporal_holdout") or {}
    recent_metrics = recent_block.get("models") or {}
    gate_model = selected_name if gate_mode in ("", "selected", "selection", "inner") else gate_mode
    if gate_model not in temporal_metrics or gate_model not in recent_metrics:
        gate_model = selected_name

    selected_temporal = temporal_metrics[gate_model]
    selected_recent = recent_metrics[gate_model]
    recent_train_positives = int(recent_block.get("train_positives", 0))
    recent_test_positives = int(recent_block.get("test_positives", 0))
    recent_window_support_pass = bool(recent_block.get("target_positive_support_passed", False))
    recent_recall_reliability_pass = (
        recent_train_positives >= min_recent_train and recent_test_positives >= min_recent_test
    )
    recent_support_pass = recent_window_support_pass and recent_recall_reliability_pass

    gate_result = (data.get("diagnostics") or {}).get("label_maturity_gate") or {}
    base_gate_pass = bool(gate_result.get("passed", False))

    temporal_deploy = (
        base_gate_pass
        and float(selected_temporal.get("precision", 0.0)) >= precision_floor
        and float(selected_temporal.get("recall", 0.0)) >= recall_floor
    )
    recent_deploy = (
        base_gate_pass
        and recent_support_pass
        and float(selected_recent.get("precision", 0.0)) >= precision_floor
        and float(selected_recent.get("recall", 0.0)) >= recall_floor
    )

    data["deployment_readiness"] = {
        "rule": (
            "label_maturity_gate + precision_floor + recall_floor on MODEL_DEPLOY_GATE_MODEL "
            "(default: inner-temporal selected model). Recent window adds positive-support reliability; "
            "set MODEL_DEPLOY_RECENT_BINDING=1 to require recent GO. Recomputed by scripts/refresh_deployment_readiness.py."
        ),
        "precision_floor": precision_floor,
        "recall_floor": recall_floor,
        "gate_model": gate_model,
        "gate_model_requested": gate_mode,
        "inner_selected_model": selected_name,
        "temporal_primary": {
            "deployable": bool(temporal_deploy),
            "required": True,
            "gate_pass": base_gate_pass,
            "precision_pass": float(selected_temporal.get("precision", 0.0)) >= precision_floor,
            "recall_pass": float(selected_temporal.get("recall", 0.0)) >= recall_floor,
        },
        "recent_24_week": {
            "deployable": bool(recent_deploy),
            "required": bool(recent_binding),
            "gate_pass": base_gate_pass,
            "support_pass": recent_support_pass,
            "window_support_pass": recent_window_support_pass,
            "recall_reliability_support_pass": recent_recall_reliability_pass,
            "min_recent_train_positives_for_recall_gate": min_recent_train,
            "min_recent_test_positives_for_recall_gate": min_recent_test,
            "observed_recent_train_positives": recent_train_positives,
            "observed_recent_test_positives": recent_test_positives,
            "precision_pass": float(selected_recent.get("precision", 0.0)) >= precision_floor,
            "recall_pass": float(selected_recent.get("recall", 0.0)) >= recall_floor,
        },
    }

    path.write_text(json.dumps(data, indent=2) + "\n")
    print(
        f"Updated deployment_readiness gate_model={gate_model!r} "
        f"floors P>={precision_floor} R>={recall_floor} recent_required={recent_binding} "
        f"temporal_deploy={temporal_deploy} recent_deploy={recent_deploy}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
