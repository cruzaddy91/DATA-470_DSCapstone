#!/usr/bin/env python3
"""
Business-utility threshold analysis: reframe threshold selection from F1-max
to expected-cost-min under asymmetric cost assumptions.

For each cost ratio k = cost_of_miss / cost_of_false_alarm, sweep the
decision threshold and compute expected cost. The minimum-cost threshold
is the operationally "correct" one under that assumption. Sensitivity to
k is presented as a curve so the reader sees how robust the choice is.

Outputs:
  - output/tables/business_utility_operating_points.md
  - output/figures/business_utility_curves.png
  - output/tables/business_utility_operating_points.json

This is analysis-only; no models retrained.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.metrics import precision_recall_curve

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

SCORE_PATH = PROJECT_ROOT / "models" / "temporal_holdout_test_scores_v2_ordertime.json"
FIGURES_DIR = PROJECT_ROOT / "output" / "figures"
TABLES_DIR = PROJECT_ROOT / "output" / "tables"

MODELS = [
    "logistic_regression", "soft_vote_4_bases", "oof_calibrated_stack", "oof_calibrated_stack_all_bases",
]
LABELS = {
    "logistic_regression": "Logistic",
    "soft_vote_4_bases": "Vote (4 bases)",
    "oof_calibrated_stack": "Stack (4 bases)",
    "oof_calibrated_stack_all_bases": "Stack (all bases)",
}

# Cost ratios to analyze. k = cost_of_miss / cost_of_false_alarm.
#   k=1   symmetric (equivalent to accuracy / F1-like)
#   k=3   miss is 3x worse than false alarm (moderate asymmetry)
#   k=10  miss is 10x worse (typical supply-chain stockout vs over-stock)
#   k=30  miss is 30x worse (high-value SKU, lost customer)
#   k=100 miss is 100x worse (critical line-stop risk)
COST_RATIOS = [1, 3, 10, 30, 100]


def _utility_sweep(y_true: np.ndarray, y_proba: np.ndarray, k: float) -> dict[str, Any]:
    """Expected cost per row as a function of threshold, under cost ratio k."""
    precision, recall, thresholds = precision_recall_curve(y_true, y_proba)
    p = precision[:-1]
    r = recall[:-1]
    t = thresholds
    n_pos = float(y_true.sum())
    n_neg = float(len(y_true) - y_true.sum())

    # At each threshold:
    #   TP = recall * n_pos
    #   FN = (1 - recall) * n_pos               cost = k per FN
    #   FP = TP * (1 - precision) / precision   (when precision > 0)
    #   Total expected cost = k * FN + 1 * FP
    fn = (1.0 - r) * n_pos
    with np.errstate(divide="ignore", invalid="ignore"):
        tp = r * n_pos
        fp = np.where(p > 0, tp * (1.0 - p) / p, np.inf)
    cost = k * fn + fp
    cost_per_row = cost / len(y_true)
    idx = int(np.nanargmin(cost_per_row))
    f1 = np.where((p + r) > 0, 2 * p * r / (p + r), 0.0)
    return {
        "cost_ratio_k": k,
        "min_cost_threshold": float(t[idx]),
        "precision_at_min_cost": float(p[idx]),
        "recall_at_min_cost": float(r[idx]),
        "f1_at_min_cost": float(f1[idx]),
        "expected_cost_per_row": float(cost_per_row[idx]),
        "expected_misses_per_100_orders": float(100 * fn[idx] / len(y_true)),
        "expected_false_alarms_per_100_orders": float(100 * fp[idx] / len(y_true)),
        "curve": {
            "threshold": t.tolist(),
            "expected_cost_per_row": cost_per_row.tolist(),
        },
    }


def _render_markdown(report: dict[str, Any]) -> str:
    lines = ["# Business-Utility Threshold Analysis — temporal holdout", ""]
    lines.append(
        f"- n_test = {report['n_test']}, n_positive = {report['n_positive']}, "
        f"baseline_positive_rate = {report['baseline_positive_rate']:.4f}"
    )
    lines.append("")
    lines.append(
        "Each operating point is the threshold that **minimizes expected cost** under the "
        "given cost ratio k = cost_of_miss / cost_of_false_alarm. "
        "Stockouts (missing a backorder the model should have flagged) are typically much "
        "more expensive than pre-stocking an order that didn't actually backorder, so "
        "real-world k is usually ≥ 10 for supply-chain use cases."
    )
    lines.append("")
    for model_key, per_k in report["models"].items():
        label = LABELS.get(model_key, model_key)
        lines.append(f"## {label}")
        lines.append("")
        lines.append("| Cost ratio k | Threshold | Precision | Recall | F1 | Expected misses / 100 orders | Expected false alarms / 100 orders |")
        lines.append("|---|---|---|---|---|---|---|")
        for info in per_k:
            lines.append(
                f"| {int(info['cost_ratio_k'])} | {info['min_cost_threshold']:.3f} | "
                f"{info['precision_at_min_cost']:.3f} | {info['recall_at_min_cost']:.3f} | "
                f"{info['f1_at_min_cost']:.3f} | {info['expected_misses_per_100_orders']:.2f} | "
                f"{info['expected_false_alarms_per_100_orders']:.2f} |"
            )
        lines.append("")
    return "\n".join(lines)


def _render_figure(report: dict[str, Any]) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    from poster_matplotlib_style import apply_report_matplotlib_style

    apply_report_matplotlib_style()

    models_items = list(report["models"].items())
    n = len(models_items)
    if n == 0:
        return
    ncols = 2 if n > 1 else 1
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(6.8 * ncols, 4.5 * nrows))
    if n == 1:
        axes_flat = [axes]
    else:
        axes_flat = list(np.atleast_1d(axes).ravel())

    for ax, (model_key, per_k) in zip(axes_flat, models_items):
        label = LABELS.get(model_key, model_key)
        for info in per_k:
            t = np.asarray(info["curve"]["threshold"])
            c = np.asarray(info["curve"]["expected_cost_per_row"])
            ax.plot(t, c, linewidth=2, label=f"k = {int(info['cost_ratio_k'])}")
            ax.scatter(
                info["min_cost_threshold"],
                info["expected_cost_per_row"],
                s=40,
                zorder=3,
                edgecolor="black",
                linewidth=1,
            )
        ax.set_xlabel("Decision threshold", fontsize=11)
        ax.set_ylabel("Expected cost per row", fontsize=11)
        ax.set_title(label, fontsize=12, fontweight="bold")
        ax.set_xlim(0, 1)
        ax.legend(fontsize=9, loc="upper right")
        ax.tick_params(axis="both", labelsize=10)
        ax.grid(True, alpha=0.3)

    for ax in axes_flat[n:]:
        ax.set_visible(False)

    fig.suptitle(
        "Business-utility curves — expected cost vs threshold under cost ratio k",
        fontsize=13,
        fontweight="bold",
        y=1.02,
    )
    fig.tight_layout()
    out = FIGURES_DIR / "business_utility_curves.png"
    fig.savefig(out, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Wrote {out}")


def main() -> int:
    payload = json.loads(SCORE_PATH.read_text(encoding="utf-8"))
    y_true = np.asarray(payload.get("y_true", []), dtype=int)
    if y_true.size == 0:
        print("No y_true in score file.", file=sys.stderr)
        return 1

    report: dict[str, Any] = {
        "n_test": int(len(y_true)),
        "n_positive": int(y_true.sum()),
        "baseline_positive_rate": float(y_true.mean()),
        "cost_ratios": COST_RATIOS,
        "models": {},
    }
    for key in MODELS:
        entry = payload.get("models", {}).get(key)
        if not entry:
            continue
        y_proba = np.asarray(entry.get("y_proba", []), dtype=float)
        if y_proba.size != y_true.size:
            continue
        sweeps = [_utility_sweep(y_true, y_proba, k) for k in COST_RATIOS]
        report["models"][key] = sweeps

    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    md_path = TABLES_DIR / "business_utility_operating_points.md"
    md_path.write_text(_render_markdown(report), encoding="utf-8")
    print(f"Wrote {md_path}")

    # Keep JSON slim by dropping the full sweep curves.
    slim = {**report, "models": {
        k: [{kk: vv for kk, vv in info.items() if kk != "curve"} for info in per_k]
        for k, per_k in report["models"].items()
    }}
    json_path = TABLES_DIR / "business_utility_operating_points.json"
    json_path.write_text(json.dumps(slim, indent=2), encoding="utf-8")
    print(f"Wrote {json_path}")

    _render_figure(report)

    print()
    print("Summary (min-cost operating point per model, per cost ratio):")
    for key, per_k in report["models"].items():
        label = LABELS.get(key, key)
        print(f"  {label}")
        for info in per_k:
            print(
                f"    k={int(info['cost_ratio_k']):3d}  thr={info['min_cost_threshold']:.3f}  "
                f"P={info['precision_at_min_cost']:.3f}  R={info['recall_at_min_cost']:.3f}  "
                f"F1={info['f1_at_min_cost']:.3f}  misses/100={info['expected_misses_per_100_orders']:.2f}  "
                f"FA/100={info['expected_false_alarms_per_100_orders']:.2f}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
