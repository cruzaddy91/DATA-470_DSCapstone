#!/usr/bin/env python3
"""
Threshold-policy analysis: sweep decision thresholds for each good model on the
temporal holdout and report the precision / recall / F1 trade-off.

Different thresholds = different business decisions. The model's PR-AUC and
ROC-AUC are fixed; threshold tuning moves along the curve, not off it.

Outputs:
  - output/tables/threshold_operating_points.md     per-model operating-point table
  - output/figures/threshold_frontier_pr_curves.png full PR curve for each model
  - output/figures/threshold_frontier_f1_curves.png F1 as a function of threshold
  - output/tables/threshold_frontier_operating_points.json  raw JSON for reuse

Usage:
  .venv-v2/bin/python scripts/run_threshold_frontier_analysis.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.metrics import precision_recall_curve, average_precision_score, roc_auc_score

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

SCORE_PATH = PROJECT_ROOT / "models" / "temporal_holdout_test_scores_v2_ordertime.json"
FIGURES_DIR = PROJECT_ROOT / "output" / "figures"
TABLES_DIR = PROJECT_ROOT / "output" / "tables"

# Order of models to analyze; missing models are skipped silently.
MODELS_IN_SCOPE = [
    "logistic_regression",
    "lightgbm",
    "random_forest",
    "soft_vote_lr_lightgbm",
    "soft_vote_4_bases",
    "oof_calibrated_stack",
    "oof_calibrated_stack_all_bases",
]

MODEL_LABELS = {
    "logistic_regression": "Logistic Regression",
    "lightgbm": "LightGBM",
    "random_forest": "RandomForest",
    "knn": "kNN",
    "xgboost": "XGBoost",
    "catboost": "CatBoost",
    "soft_vote_lr_lightgbm": "Soft Vote (LR+LGBM)",
    "soft_vote_4_bases": "Vote (4 bases)",
    "oof_calibrated_stack": "Stack (4 bases)",
    "oof_calibrated_stack_all_bases": "Stack (all bases)",
}

# Target operating points for the policy table.
PRECISION_TARGETS = [0.40, 0.50, 0.60, 0.70, 0.80]
RECALL_TARGETS = [0.50, 0.60, 0.70, 0.80, 0.90]


def _operating_points(y_true: np.ndarray, y_proba: np.ndarray) -> dict[str, Any]:
    precision, recall, thresholds = precision_recall_curve(y_true, y_proba)
    # precision_recall_curve returns precision/recall of length n_thresholds+1.
    p = precision[:-1]
    r = recall[:-1]
    t = thresholds
    f1 = np.where((p + r) > 0, 2 * p * r / (p + r), 0.0)

    # F1-maximizing threshold.
    f1_argmax = int(np.argmax(f1))
    f1_max = {
        "threshold": float(t[f1_argmax]),
        "precision": float(p[f1_argmax]),
        "recall": float(r[f1_argmax]),
        "f1": float(f1[f1_argmax]),
    }

    # Best recall achievable while holding precision ≥ target.
    recall_at_precision = {}
    for target in PRECISION_TARGETS:
        mask = p >= target
        if mask.any():
            idx = int(np.where(mask)[0][np.argmax(r[mask])])
            recall_at_precision[f"precision_ge_{target:.2f}"] = {
                "threshold": float(t[idx]),
                "precision": float(p[idx]),
                "recall": float(r[idx]),
                "f1": float(f1[idx]),
            }
        else:
            recall_at_precision[f"precision_ge_{target:.2f}"] = None

    # Best precision achievable while holding recall ≥ target.
    precision_at_recall = {}
    for target in RECALL_TARGETS:
        mask = r >= target
        if mask.any():
            idx = int(np.where(mask)[0][np.argmax(p[mask])])
            precision_at_recall[f"recall_ge_{target:.2f}"] = {
                "threshold": float(t[idx]),
                "precision": float(p[idx]),
                "recall": float(r[idx]),
                "f1": float(f1[idx]),
            }
        else:
            precision_at_recall[f"recall_ge_{target:.2f}"] = None

    return {
        "pr_auc": float(average_precision_score(y_true, y_proba)),
        "roc_auc": float(roc_auc_score(y_true, y_proba)),
        "n_test": int(len(y_true)),
        "n_positive": int(y_true.sum()),
        "baseline_positive_rate": float(y_true.mean()),
        "f1_max": f1_max,
        "recall_at_precision": recall_at_precision,
        "precision_at_recall": precision_at_recall,
        "curve": {
            "precision": p.tolist(),
            "recall": r.tolist(),
            "threshold": t.tolist(),
            "f1": f1.tolist(),
        },
    }


def _render_markdown_table(report: dict[str, Any]) -> str:
    lines = ["# Threshold Operating Points — temporal holdout", ""]
    lines.append(
        f"- n_test = {report['n_test']}, n_positive = {report['n_positive']}, "
        f"base_rate = {report['baseline_positive_rate']:.4f}"
    )
    lines.append("")
    lines.append(
        "Threshold choice is a business policy. PR-AUC / ROC-AUC are fixed by the model; "
        "each row below is a point on that fixed curve. F1-max is the operating point that "
        "maximizes F1 on outer temporal holdout."
    )
    lines.append("")
    for model_key, info in report["models"].items():
        label = MODEL_LABELS.get(model_key, model_key)
        lines.append(f"## {label}  (PR-AUC {info['pr_auc']:.3f} · ROC-AUC {info['roc_auc']:.3f})")
        lines.append("")
        lines.append("| Policy | Threshold | Precision | Recall | F1 |")
        lines.append("|---|---|---|---|---|")
        fm = info["f1_max"]
        lines.append(f"| **F1-max** | {fm['threshold']:.3f} | {fm['precision']:.3f} | {fm['recall']:.3f} | **{fm['f1']:.3f}** |")
        for key, pt in info["recall_at_precision"].items():
            target = key.split("_")[-1]
            if pt is None:
                lines.append(f"| max recall @ P ≥ {target} | — | — | infeasible | — |")
            else:
                lines.append(f"| max recall @ P ≥ {target} | {pt['threshold']:.3f} | {pt['precision']:.3f} | {pt['recall']:.3f} | {pt['f1']:.3f} |")
        for key, pt in info["precision_at_recall"].items():
            target = key.split("_")[-1]
            if pt is None:
                lines.append(f"| max precision @ R ≥ {target} | — | infeasible | — | — |")
            else:
                lines.append(f"| max precision @ R ≥ {target} | {pt['threshold']:.3f} | {pt['precision']:.3f} | {pt['recall']:.3f} | {pt['f1']:.3f} |")
        lines.append("")
    return "\n".join(lines)


def _render_pr_curves(report: dict[str, Any]) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(9, 7))
    for model_key, info in report["models"].items():
        label = f"{MODEL_LABELS.get(model_key, model_key)}  (PR-AUC {info['pr_auc']:.2f})"
        p = np.asarray(info["curve"]["precision"])
        r = np.asarray(info["curve"]["recall"])
        ax.plot(r, p, linewidth=2, label=label)
        fm = info["f1_max"]
        ax.scatter(fm["recall"], fm["precision"], s=60, zorder=3, edgecolor="black", linewidth=1.0)
    ax.axhline(report["baseline_positive_rate"], color="#888", linestyle="--", linewidth=1,
               label=f"Random baseline (P = {report['baseline_positive_rate']:.4f})")
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_title("Precision–Recall frontier — temporal holdout\n(dots mark each model's F1-max operating point)")
    ax.legend(loc="upper right", fontsize=9, frameon=True)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    out = FIGURES_DIR / "threshold_frontier_pr_curves.png"
    fig.savefig(out, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Wrote {out}")


def _render_f1_vs_threshold(report: dict[str, Any]) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(9, 6))
    for model_key, info in report["models"].items():
        label = MODEL_LABELS.get(model_key, model_key)
        t = np.asarray(info["curve"]["threshold"])
        f1 = np.asarray(info["curve"]["f1"])
        ax.plot(t, f1, linewidth=2, label=f"{label}  (F1-max {info['f1_max']['f1']:.3f})")
        fm = info["f1_max"]
        ax.scatter(fm["threshold"], fm["f1"], s=60, zorder=3, edgecolor="black", linewidth=1.0)
    ax.set_xlabel("Decision threshold")
    ax.set_ylabel("F1")
    ax.set_title("F1 as a function of decision threshold — temporal holdout")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 0.6)
    ax.legend(loc="upper right", fontsize=9)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    out = FIGURES_DIR / "threshold_frontier_f1_curves.png"
    fig.savefig(out, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Wrote {out}")


def main() -> int:
    if not SCORE_PATH.is_file():
        print(f"Missing {SCORE_PATH}. Run run_modeling.py first.", file=sys.stderr)
        return 1
    payload = json.loads(SCORE_PATH.read_text(encoding="utf-8"))
    y_true = np.asarray(payload.get("y_true", []), dtype=int)
    if y_true.size == 0:
        print("No y_true in scores file.", file=sys.stderr)
        return 1

    report: dict[str, Any] = {
        "n_test": int(y_true.size),
        "n_positive": int(y_true.sum()),
        "baseline_positive_rate": float(y_true.mean()),
        "models": {},
    }
    for model_key in MODELS_IN_SCOPE:
        model_entry = payload.get("models", {}).get(model_key)
        if not model_entry:
            continue
        y_proba = np.asarray(model_entry.get("y_proba", []), dtype=float)
        if y_proba.size != y_true.size:
            continue
        report["models"][model_key] = _operating_points(y_true, y_proba)

    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    md_path = TABLES_DIR / "threshold_operating_points.md"
    md_path.write_text(_render_markdown_table(report), encoding="utf-8")
    print(f"Wrote {md_path}")

    json_path = TABLES_DIR / "threshold_frontier_operating_points.json"
    # Drop the full curve arrays from the JSON to keep it readable; keep everything else.
    trimmed = {**report, "models": {
        k: {kk: vv for kk, vv in v.items() if kk != "curve"} for k, v in report["models"].items()
    }}
    json_path.write_text(json.dumps(trimmed, indent=2), encoding="utf-8")
    print(f"Wrote {json_path}")

    _render_pr_curves(report)
    _render_f1_vs_threshold(report)

    # Quick stdout summary — the headline numbers.
    print()
    print("Summary (F1-max operating point per model):")
    for model_key, info in report["models"].items():
        label = MODEL_LABELS.get(model_key, model_key)
        fm = info["f1_max"]
        print(f"  {label:32s}  F1={fm['f1']:.3f}  P={fm['precision']:.3f}  R={fm['recall']:.3f}  thr={fm['threshold']:.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
