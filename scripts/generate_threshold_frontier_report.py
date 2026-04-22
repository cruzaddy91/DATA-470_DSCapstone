#!/usr/bin/env python
"""Generate threshold-frontier feasibility artifacts for scored splits."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.metrics import precision_recall_curve


def _frontier_summary(y_true: np.ndarray, y_proba: np.ndarray, precision_floor: float, recall_floor: float) -> dict[str, Any]:
    precision, recall, thresholds = precision_recall_curve(y_true, y_proba)
    precision_t = precision[:-1]
    recall_t = recall[:-1]
    threshold_t = thresholds

    feasible_mask = (precision_t >= precision_floor) & (recall_t >= recall_floor)
    best_recall_mask = precision_t >= precision_floor
    best_precision_mask = recall_t >= recall_floor

    def _point(idx: int) -> dict[str, float]:
        return {
            "precision": float(precision_t[idx]),
            "recall": float(recall_t[idx]),
            "threshold": float(threshold_t[idx]),
        }

    summary: dict[str, Any] = {
        "feasible": bool(feasible_mask.any()),
        "feasible_best_point": None,
        "max_recall_at_precision_floor": None,
        "max_precision_at_recall_floor": None,
    }
    if feasible_mask.any():
        idx = np.where(feasible_mask)[0][np.argmax(recall_t[feasible_mask])]
        summary["feasible_best_point"] = _point(int(idx))
    if best_recall_mask.any():
        idx = np.where(best_recall_mask)[0][np.argmax(recall_t[best_recall_mask])]
        summary["max_recall_at_precision_floor"] = _point(int(idx))
    if best_precision_mask.any():
        idx = np.where(best_precision_mask)[0][np.argmax(precision_t[best_precision_mask])]
        summary["max_precision_at_recall_floor"] = _point(int(idx))
    return summary


def generate_threshold_frontier_report(project_root: str | Path) -> Path:
    project_root = Path(project_root)
    models_dir = project_root / "models"
    output_dir = project_root / "output" / "dashboard"
    output_dir.mkdir(parents=True, exist_ok=True)

    precision_floor = 0.30
    recall_floor = 0.40
    payload: dict[str, Any] = {
        "constraints": {"precision_floor": precision_floor, "recall_floor": recall_floor},
        "splits": {},
    }

    score_files = {
        "temporal_holdout": models_dir / "temporal_holdout_test_scores_v2_ordertime.json",
        "recent_24_week_temporal_holdout": models_dir / "recent_24_week_test_scores_v2_ordertime.json",
    }
    for split_name, score_path in score_files.items():
        if not score_path.exists():
            payload["splits"][split_name] = {"available": False, "detail": f"Missing score file: {score_path.name}"}
            continue
        scored = json.loads(score_path.read_text())
        y_true = np.asarray(scored.get("y_true", []), dtype=int)
        if y_true.size == 0:
            payload["splits"][split_name] = {"available": False, "detail": "No y_true rows in score file."}
            continue
        split_summary: dict[str, Any] = {"available": True, "models": {}}
        for model_name, model_payload in scored.get("models", {}).items():
            y_proba = np.asarray(model_payload.get("y_proba", []), dtype=float)
            if y_proba.size != y_true.size or y_proba.size == 0:
                split_summary["models"][model_name] = {"available": False, "detail": "Missing or misaligned y_proba."}
                continue
            summary = _frontier_summary(y_true, y_proba, precision_floor, recall_floor)
            summary["available"] = True
            split_summary["models"][model_name] = summary
        payload["splits"][split_name] = split_summary

    out_path = output_dir / "threshold_frontier_report.json"
    out_path.write_text(json.dumps(payload, indent=2))
    return out_path


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    written = generate_threshold_frontier_report(root)
    print(f"Wrote threshold frontier report: {written}")
