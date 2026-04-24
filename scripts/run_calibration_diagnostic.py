#!/usr/bin/env python3
"""
Calibration diagnostic: are the model's predicted probabilities trustworthy?

Reads existing temporal_holdout scores; for each model builds a reliability
diagram (bin by predicted probability, plot observed positive rate) and
reports Brier score + ECE (expected calibration error). Output is
diagnostic-only — no models are retrained.

If calibration is meaningfully off, Step 1b wraps base learners with
sklearn's CalibratedClassifierCV and re-runs.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import numpy as np
from sklearn.calibration import calibration_curve
from sklearn.metrics import brier_score_loss

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

SCORE_PATH = PROJECT_ROOT / "models" / "temporal_holdout_test_scores_v2_ordertime.json"
FIGURES_DIR = PROJECT_ROOT / "output" / "figures"
TABLES_DIR = PROJECT_ROOT / "output" / "tables"

MODELS = [
    "logistic_regression", "lightgbm", "random_forest", "knn",
    "xgboost", "catboost",
    "soft_vote_lr_lightgbm", "soft_vote_4_bases",
    "oof_calibrated_stack", "oof_calibrated_stack_all_bases",
]
LABELS = {
    "logistic_regression": "Logistic",
    "lightgbm": "LightGBM",
    "random_forest": "Random Forest",
    "knn": "kNN",
    "xgboost": "XGBoost",
    "catboost": "CatBoost",
    "soft_vote_lr_lightgbm": "Soft Vote (LR+LGBM)",
    "soft_vote_4_bases": "Vote (4 bases)",
    "oof_calibrated_stack": "Stack (4 bases)",
    "oof_calibrated_stack_all_bases": "Stack (all bases)",
}


def _ece(y_true: np.ndarray, y_proba: np.ndarray, n_bins: int = 10) -> float:
    """Expected Calibration Error."""
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    idx = np.digitize(y_proba, bins[1:-1], right=False)
    n = len(y_true)
    ece = 0.0
    for b in range(n_bins):
        mask = idx == b
        if not mask.any():
            continue
        conf = float(y_proba[mask].mean())
        acc = float(y_true[mask].mean())
        ece += (mask.sum() / n) * abs(conf - acc)
    return float(ece)


def main() -> int:
    payload = json.loads(SCORE_PATH.read_text(encoding="utf-8"))
    y_true = np.asarray(payload.get("y_true", []), dtype=int)
    if y_true.size == 0:
        print("No y_true rows in score file.", file=sys.stderr)
        return 1

    report: dict = {"n_test": int(len(y_true)), "n_positive": int(y_true.sum()), "models": {}}
    for key in MODELS:
        entry = payload.get("models", {}).get(key)
        if not entry:
            continue
        y_proba = np.asarray(entry.get("y_proba", []), dtype=float)
        if y_proba.size != y_true.size:
            continue
        brier = float(brier_score_loss(y_true, y_proba))
        ece = _ece(y_true, y_proba, n_bins=10)
        # Reliability curve (observed vs expected per quantile bin).
        try:
            frac_pos, mean_pred = calibration_curve(y_true, y_proba, n_bins=10, strategy="quantile")
        except Exception:
            frac_pos, mean_pred = np.array([]), np.array([])
        report["models"][key] = {
            "brier_score": brier,
            "ece": ece,
            "reliability_mean_pred": mean_pred.tolist(),
            "reliability_frac_pos": frac_pos.tolist(),
        }

    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    (TABLES_DIR / "calibration_diagnostic.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    # Reliability diagram.
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(9, 7))
    ax.plot([0, 1], [0, 1], linestyle="--", color="#888", linewidth=1.5, label="Perfectly calibrated")
    for key, info in report["models"].items():
        mp = info["reliability_mean_pred"]
        fp = info["reliability_frac_pos"]
        if not mp:
            continue
        ax.plot(mp, fp, marker="o", linewidth=2, label=f"{LABELS.get(key, key)} (Brier={info['brier_score']:.4f}, ECE={info['ece']:.3f})")
    ax.set_xlabel("Mean predicted probability (per bin)")
    ax.set_ylabel("Observed positive rate (per bin)")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_title("Calibration diagnostic — reliability diagram on temporal holdout")
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "calibration_reliability.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

    print("Calibration summary (lower is better):")
    print(f"  {'model':32s}  {'Brier':>8s}  {'ECE':>8s}")
    for key, info in report["models"].items():
        print(f"  {LABELS.get(key, key):32s}  {info['brier_score']:8.4f}  {info['ece']:8.4f}")
    print()
    print("ECE reading: <0.02 = well-calibrated, 0.02-0.05 = mildly off, >0.05 = meaningfully off")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
