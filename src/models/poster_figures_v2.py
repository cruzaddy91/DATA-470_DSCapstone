"""
Poster-ready figures for v2 order-time models (temporal holdout).

Reads ``temporal_holdout_test_scores_v2_ordertime.json`` (written by
``run_overfit_evaluation``) and optional ``auc_diagnostics_v2_ordertime.json``.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,
)

from westminster_poster_palette import (
    COPPER,
    FLINT,
    NIGHT,
    SKY,
    THISTLE,
    brand_confusion_heatmap_cmap,
)

# Westminster brand mapping
COLOR_LR = NIGHT
COLOR_XGB = COPPER
COLOR_STACK = THISTLE
COLOR_BASELINE = FLINT

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
from poster_matplotlib_style import POSTER_FONT, apply_poster_matplotlib_style


def _paths(project_root: Path | None) -> dict[str, Path]:
    root = project_root or Path(__file__).resolve().parents[2]
    return {
        "root": root,
        "models": root / "models",
        "figures": root / "output" / "figures",
    }


def generate_temporal_holdout_poster_figures(project_root: str | Path | None = None) -> dict[str, str]:
    """
    Create ROC, PR, temporal drift, and score-distribution figures under ``output/figures/``.

    Returns a map of logical name -> output path.
    """
    paths = _paths(Path(project_root) if project_root else None)
    apply_poster_matplotlib_style()
    scores_path = paths["models"] / "temporal_holdout_test_scores_v2_ordertime.json"
    diag_path = paths["models"] / "auc_diagnostics_v2_ordertime.json"

    if not scores_path.exists():
        raise FileNotFoundError(
            f"Missing {scores_path}. Run scripts/run_modeling.py (run_overfit_evaluation) first."
        )

    payload = json.loads(scores_path.read_text(encoding="utf-8"))
    y_true = np.asarray(payload["y_true"], dtype=int)
    baseline_rate = float(payload.get("baseline_positive_rate", y_true.mean()))
    models = payload["models"]

    paths["figures"].mkdir(parents=True, exist_ok=True)
    out: dict[str, str] = {}

    # --- ROC (both models, one plot) ---
    fig, ax = plt.subplots(figsize=(7, 6))
    ax.plot([0, 1], [0, 1], linestyle="--", color=COLOR_BASELINE, linewidth=1.5, label="Random classifier")
    for name, color, label_short in (
        ("logistic_regression", COLOR_LR, "Logistic"),
        ("xgboost", COLOR_XGB, "XGBoost"),
        ("oof_calibrated_stack", COLOR_STACK, "Stack"),
    ):
        if name not in models:
            continue
        proba = np.asarray(models[name]["y_proba"], dtype=float)
        fpr, tpr, _ = roc_curve(y_true, proba)
        roc_auc = float(models[name].get("roc_auc", roc_auc_score(y_true, proba)))
        ax.plot(fpr, tpr, color=color, linewidth=2, label=f"{label_short} (AUC={roc_auc:.2f})")
    ax.set_xlabel("False positive rate")
    ax.set_ylabel("True positive rate")
    ax.set_title("ROC — temporal holdout test set (v2 order-time)")
    ax.legend(loc="lower right", frameon=True)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_aspect("equal")
    fig.tight_layout()
    roc_path = paths["figures"] / "roc_curves_temporal_v2_ordertime.png"
    fig.savefig(roc_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    out["roc"] = str(roc_path)

    # --- Precision–Recall (both models + baseline) ---
    fig, ax = plt.subplots(figsize=(7, 6))
    ax.axhline(
        baseline_rate,
        color=COLOR_BASELINE,
        linestyle="--",
        linewidth=1.5,
        label=f"Baseline positive rate ({baseline_rate * 100:.2f}%)",
    )
    for name, color, label_short in (
        ("logistic_regression", COLOR_LR, "Logistic"),
        ("xgboost", COLOR_XGB, "XGBoost"),
        ("oof_calibrated_stack", COLOR_STACK, "Stack"),
    ):
        if name not in models:
            continue
        proba = np.asarray(models[name]["y_proba"], dtype=float)
        prec, rec, _ = precision_recall_curve(y_true, proba)
        pr_auc = float(models[name].get("pr_auc", average_precision_score(y_true, proba)))
        ax.plot(rec, prec, color=color, linewidth=2, label=f"{label_short} (PR-AUC={pr_auc:.2f})")
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_title("Precision–recall — temporal holdout test set (v2 order-time)")
    ax.legend(loc="upper right", frameon=True)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.tight_layout()
    pr_path = paths["figures"] / "pr_curves_temporal_v2_ordertime.png"
    fig.savefig(pr_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    out["pr"] = str(pr_path)

    # --- Temporal positive-rate drift (from diagnostics JSON) ---
    if diag_path.exists():
        diag = json.loads(diag_path.read_text(encoding="utf-8"))
        monthly = (diag.get("target_stability") or {}).get("monthly_tail") or []
        if monthly:
            months = [row["order_month"] for row in monthly]
            rates = [float(row["positive_rate"]) * 100.0 for row in monthly]
            fig, ax = plt.subplots(figsize=(9, 4.5))
            x = range(len(months))
            ax.plot(x, rates, color=THISTLE, linewidth=2, marker="o", markersize=5)
            ax.set_xticks(list(x))
            ax.set_xticklabels(months, rotation=45, ha="right")
            ax.set_ylabel("Positive rate (%)")
            ax.set_xlabel("Order month")
            ax.set_title("Temporal drift — positive rate by month (labeled rows, tail window)")
            ax.axhline(baseline_rate * 100.0, color=COLOR_BASELINE, linestyle="--", label="Temporal test mean")
            ax.legend()
            ax.grid(True, alpha=0.25)
            fig.tight_layout()
            drift_path = paths["figures"] / "temporal_positive_rate_drift_v2_ordertime.png"
            fig.savefig(drift_path, dpi=200, bbox_inches="tight")
            plt.close(fig)
            out["drift"] = str(drift_path)

    # --- Score distributions (one figure, rows: LR / XGBoost / Stack) ---
    model_items: list[tuple[str, str]] = []
    for label_short, key in (("Logistic", "logistic_regression"), ("XGBoost", "xgboost"), ("Stack", "oof_calibrated_stack")):
        if key in models:
            model_items.append((key, label_short))
    if model_items:
        n_models = len(model_items)
        fig, axes = plt.subplots(n_models, 1, figsize=(8, 3.5 * n_models), squeeze=False)
        for ax, (name, label_short) in zip(axes.flat, model_items):
            proba = np.asarray(models[name]["y_proba"], dtype=float)
            thr = float(models[name].get("decision_threshold", 0.5))
            ax.hist(proba[y_true == 0], bins=40, alpha=0.65, color=SKY, label="No backorder", density=True)
            ax.hist(proba[y_true == 1], bins=40, alpha=0.65, color=NIGHT, label="Backorder", density=True)
            ax.axvline(thr, color="black", linestyle="--", linewidth=2, label=f"Threshold={thr:.3f}")
            ax.set_xlabel("Predicted P(backorder)")
            ax.set_ylabel("Density")
            ax.set_title(f"{label_short} — score distribution (temporal test)")
            ax.legend(fontsize=8)
        fig.tight_layout()
        score_path = paths["figures"] / "score_distribution_temporal_v2_ordertime.png"
        fig.savefig(score_path, dpi=200, bbox_inches="tight")
        plt.close(fig)
        out["scores"] = str(score_path)

    # --- Confusion matrices: color by row % (raw-count colormaps hide the minority class) ---
    sns.set_theme(style="white", font=POSTER_FONT)
    cm_specs = [
        ("logistic_regression", "Logistic regression"),
        ("xgboost", "XGBoost"),
        ("oof_calibrated_stack", "Stack (selected)"),
    ]
    present = [(k, lab) for k, lab in cm_specs if k in models]
    if present:
        n_p = len(present)
        fig, axes = plt.subplots(1, n_p, figsize=(5.0 * n_p + 0.5, 5.0), squeeze=False)
        n_pos = int(y_true.sum())
        for ax, (name, label_short) in zip(axes.flat, present):
            y_pred = np.asarray(models[name]["y_pred"], dtype=int)
            cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
            row_sum = cm.sum(axis=1, keepdims=True)
            row_frac = cm.astype(np.float64) / np.maximum(row_sum, 1.0)
            ann = np.empty((2, 2), dtype=object)
            for i in range(2):
                for j in range(2):
                    ann[i, j] = f"{cm[i, j]:,}\n({100.0 * row_frac[i, j]:.1f}% of row)"
            sns.heatmap(
                row_frac,
                ax=ax,
                annot=ann,
                fmt="",
                cmap=brand_confusion_heatmap_cmap(),
                vmin=0.0,
                vmax=1.0,
                linewidths=2.0,
                linecolor="white",
                cbar=True,
                cbar_kws={
                    "shrink": 0.78,
                    "label": "Share of actual class\n(row sums to 100%)",
                },
                xticklabels=["Predicted No", "Predicted Yes"],
                yticklabels=["Actual No", "Actual Yes"],
                annot_kws={"fontsize": 12, "fontweight": "bold", "color": "#111827"},
            )
            ax.set_yticklabels(ax.get_yticklabels(), rotation=0)
            thr = float(models[name].get("decision_threshold", 0.5))
            ax.set_title(f"{label_short}\n(threshold = {thr:.3f})", fontsize=12, fontweight="bold", pad=10)
        fig.suptitle(
            "Temporal holdout — confusion matrices (color = row-normalized, not raw count)",
            fontsize=13,
            fontweight="bold",
            y=1.02,
        )
        fig.text(
            0.5,
            -0.02,
            f"Test prevalence: {n_pos} positives / {len(y_true):,} rows (~{100 * n_pos / max(len(y_true), 1):.2f}%). "
            "Raw counts appear in each cell; using counts for color would wash out all but the true-negative cell.",
            ha="center",
            fontsize=10,
            color="#374151",
        )
        plt.tight_layout(rect=(0, 0.06, 1, 0.96))
        cm_path = paths["figures"] / "classification_confusion_matrices_v2_ordertime.png"
        fig.savefig(cm_path, dpi=220, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        out["confusion"] = str(cm_path)

    return out


def main() -> int:
    os.environ.setdefault("MPLBACKEND", "Agg")
    out = generate_temporal_holdout_poster_figures()
    for key, path in out.items():
        print(f"wrote {key}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
