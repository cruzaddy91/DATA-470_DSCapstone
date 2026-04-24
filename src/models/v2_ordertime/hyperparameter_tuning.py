"""
Multi-phase hyperparameter tuning for v2 order-time base learners.

Uses temporal K-fold (expanding window) and PR-AUC scoring — appropriate for
the rare-event backorder problem. Random K-fold would leak future data into
the search; do not substitute.

Design:
  Phase 1 — coarse grid over the most impactful levers.
  Phase 2 — fine grid centered on the Phase 1 winner (± one step per lever).

kNN is intentionally omitted: on the honest baseline run it posted F1 0.119
(recall 0.069 — catches almost no positives). Tuning cannot rescue that.

Outputs: per-model best params + per-phase scores as a JSON dict, written to
models/hyperparameters_tuned_v2_ordertime.json. The pipeline_*.py builders
read that file (when present) and fall back to hand-defaults otherwise.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
from sklearn.base import clone
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score
from sklearn.model_selection import ParameterGrid
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MaxAbsScaler

from .preprocessing import build_v2_column_preprocessor

try:
    from lightgbm import LGBMClassifier
    HAS_LGBM = True
except ImportError:
    HAS_LGBM = False


TUNED_PARAMS_FILE = "hyperparameters_tuned_v2_ordertime.json"


@dataclass
class TuningResult:
    model_name: str
    best_params: dict[str, Any]
    best_mean_pr_auc: float
    best_per_fold: list[float]
    phase_history: list[dict[str, Any]]
    elapsed_seconds: float


def temporal_expanding_folds(
    dates: pd.Series,
    n_folds: int = 3,
    val_fraction: float = 0.15,
) -> list[tuple[np.ndarray, np.ndarray]]:
    """
    Expanding-window temporal folds. Each fold trains on everything before a
    cutoff and validates on the next val_fraction of rows by date.
    """
    order = dates.sort_values().index.to_numpy()
    n = len(order)
    val_size = max(1, int(n * val_fraction))
    folds: list[tuple[np.ndarray, np.ndarray]] = []
    for k in range(n_folds):
        val_end = n - k * val_size
        val_start = max(1, val_end - val_size)
        if val_start <= 0:
            break
        tr = order[:val_start]
        va = order[val_start:val_end]
        if len(tr) == 0 or len(va) == 0:
            continue
        folds.append((tr, va))
    folds.reverse()
    return folds


def _score_params(
    build_pipeline: Callable[[dict[str, Any]], Pipeline],
    params: dict[str, Any],
    X: pd.DataFrame,
    y: np.ndarray,
    folds: list[tuple[np.ndarray, np.ndarray]],
) -> tuple[float, list[float]]:
    per_fold: list[float] = []
    for tr_idx, va_idx in folds:
        pipe = build_pipeline(params)
        pipe.fit(X.iloc[tr_idx], y[tr_idx])
        p_va = pipe.predict_proba(X.iloc[va_idx])[:, 1]
        if len(np.unique(y[va_idx])) < 2:
            per_fold.append(float("nan"))
            continue
        per_fold.append(float(average_precision_score(y[va_idx], p_va)))
    mean = float(np.nanmean(per_fold)) if per_fold else 0.0
    return mean, per_fold


def _run_grid(
    label: str,
    build_pipeline: Callable[[dict[str, Any]], Pipeline],
    grid: list[dict[str, Any]],
    X: pd.DataFrame,
    y: np.ndarray,
    folds: list[tuple[np.ndarray, np.ndarray]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for params in grid:
        t0 = time.time()
        mean_pr, per_fold = _score_params(build_pipeline, params, X, y, folds)
        rows.append({
            "params": params,
            "mean_pr_auc": mean_pr,
            "per_fold_pr_auc": per_fold,
            "seconds": round(time.time() - t0, 2),
        })
        print(f"    [{label}] {params}  mean_PR={mean_pr:.4f}  ({rows[-1]['seconds']}s)", flush=True)
    rows.sort(key=lambda r: r["mean_pr_auc"], reverse=True)
    return {"label": label, "results": rows, "best": rows[0] if rows else None}


def _fine_around(winner: dict[str, Any], axes: dict[str, list[Any]]) -> list[dict[str, Any]]:
    """Build a fine grid of ± 1 step on each axis around the winner."""
    fine_axes: dict[str, list[Any]] = {}
    for k, values in axes.items():
        if k not in winner:
            continue
        try:
            idx = values.index(winner[k])
        except ValueError:
            idx = min(range(len(values)), key=lambda i: abs(values[i] - winner[k]) if isinstance(values[i], (int, float)) and isinstance(winner[k], (int, float)) else 1e18)
        neighbors = [values[i] for i in (idx - 1, idx, idx + 1) if 0 <= i < len(values)]
        fine_axes[k] = list(dict.fromkeys(neighbors))
    grid = list(ParameterGrid(fine_axes))
    fixed = {k: v for k, v in winner.items() if k not in axes}
    return [{**fixed, **p} for p in grid]


# ---- Logistic Regression ---------------------------------------------------

def _build_lr(dataset: Any, params: dict[str, Any]) -> Pipeline:
    return Pipeline(steps=[
        ("preprocess", build_v2_column_preprocessor(dataset.numeric_features, dataset.categorical_features)),
        ("scale", MaxAbsScaler()),
        ("model", LogisticRegression(
            C=params.get("C", 1.0),
            max_iter=2000,
            class_weight="balanced",
            solver="lbfgs",
            random_state=42,
        )),
    ])


def tune_logistic_regression(dataset: Any, X: pd.DataFrame, y: np.ndarray, folds) -> TuningResult:
    t0 = time.time()
    print("Tuning LogisticRegression...", flush=True)
    grid = [{"C": c} for c in [0.01, 0.1, 1.0, 10.0]]
    phase1 = _run_grid("LR_phase1", lambda p: _build_lr(dataset, p), grid, X, y, folds)
    best = phase1["best"]["params"]
    return TuningResult(
        model_name="logistic_regression",
        best_params=best,
        best_mean_pr_auc=phase1["best"]["mean_pr_auc"],
        best_per_fold=phase1["best"]["per_fold_pr_auc"],
        phase_history=[phase1],
        elapsed_seconds=round(time.time() - t0, 2),
    )


# ---- LightGBM --------------------------------------------------------------

def _build_lgbm(dataset: Any, params: dict[str, Any]) -> Pipeline:
    from .pipeline_lightgbm import build_v2_lightgbm_pipeline
    pipe = build_v2_lightgbm_pipeline(dataset)
    if pipe is None:
        raise RuntimeError("LightGBM pipeline unavailable")
    pipe.named_steps["model"].set_params(**params)
    return pipe


def tune_lightgbm(dataset: Any, X: pd.DataFrame, y: np.ndarray, folds) -> TuningResult:
    t0 = time.time()
    if not HAS_LGBM:
        print("LightGBM not installed — skipping tuning.", flush=True)
        return TuningResult("lightgbm", {}, 0.0, [], [], 0.0)
    print("Tuning LightGBM (phase 1 coarse)...", flush=True)
    coarse_axes = {
        "num_leaves": [31, 63],
        "learning_rate": [0.05, 0.1],
        "n_estimators": [200, 400, 800],
    }
    coarse_grid = list(ParameterGrid(coarse_axes))
    phase1 = _run_grid("LGBM_phase1", lambda p: _build_lgbm(dataset, p), coarse_grid, X, y, folds)

    print("Tuning LightGBM (phase 2 fine)...", flush=True)
    fine_axes = {
        "num_leaves": [15, 31, 63, 127],
        "learning_rate": [0.03, 0.05, 0.1],
        "n_estimators": [200, 400, 600, 800, 1200],
        "min_child_samples": [20, 50, 100],
    }
    fine_grid = _fine_around(phase1["best"]["params"], fine_axes)
    phase2 = _run_grid("LGBM_phase2", lambda p: _build_lgbm(dataset, p), fine_grid, X, y, folds)

    best = phase2["best"] if phase2["best"]["mean_pr_auc"] >= phase1["best"]["mean_pr_auc"] else phase1["best"]
    return TuningResult(
        model_name="lightgbm",
        best_params=best["params"],
        best_mean_pr_auc=best["mean_pr_auc"],
        best_per_fold=best["per_fold_pr_auc"],
        phase_history=[phase1, phase2],
        elapsed_seconds=round(time.time() - t0, 2),
    )


# ---- Random Forest ---------------------------------------------------------

def _build_rf(dataset: Any, params: dict[str, Any]) -> Pipeline:
    return Pipeline(steps=[
        ("preprocess", build_v2_column_preprocessor(dataset.numeric_features, dataset.categorical_features)),
        ("model", RandomForestClassifier(
            n_estimators=params.get("n_estimators", 400),
            max_depth=params.get("max_depth", None),
            min_samples_leaf=params.get("min_samples_leaf", 5),
            max_features=params.get("max_features", "sqrt"),
            class_weight="balanced_subsample",
            n_jobs=4,
            random_state=42,
        )),
    ])


def tune_random_forest(dataset: Any, X: pd.DataFrame, y: np.ndarray, folds) -> TuningResult:
    t0 = time.time()
    print("Tuning RandomForest (phase 1 coarse)...", flush=True)
    coarse_axes = {
        "n_estimators": [200, 400],
        "max_depth": [None, 10, 20],
        "min_samples_leaf": [1, 5, 10],
    }
    coarse_grid = list(ParameterGrid(coarse_axes))
    phase1 = _run_grid("RF_phase1", lambda p: _build_rf(dataset, p), coarse_grid, X, y, folds)

    print("Tuning RandomForest (phase 2 fine)...", flush=True)
    fine_axes = {
        "n_estimators": [200, 400, 600],
        "max_depth": [None, 5, 10, 20, 30],
        "min_samples_leaf": [1, 3, 5, 10, 20],
        "max_features": ["sqrt", 0.3, 0.5],
    }
    fine_grid = _fine_around(phase1["best"]["params"], fine_axes)
    phase2 = _run_grid("RF_phase2", lambda p: _build_rf(dataset, p), fine_grid, X, y, folds)

    best = phase2["best"] if phase2["best"]["mean_pr_auc"] >= phase1["best"]["mean_pr_auc"] else phase1["best"]
    return TuningResult(
        model_name="random_forest",
        best_params=best["params"],
        best_mean_pr_auc=best["mean_pr_auc"],
        best_per_fold=best["per_fold_pr_auc"],
        phase_history=[phase1, phase2],
        elapsed_seconds=round(time.time() - t0, 2),
    )


# ---- Driver ----------------------------------------------------------------

def tune_all_base_learners(
    dataset: Any,
    X: pd.DataFrame,
    y: np.ndarray,
    dates: pd.Series,
    n_folds: int = 3,
    val_fraction: float = 0.15,
) -> dict[str, Any]:
    folds = temporal_expanding_folds(dates, n_folds=n_folds, val_fraction=val_fraction)
    print(f"Temporal folds: {len(folds)} (val_fraction={val_fraction})", flush=True)
    for i, (tr, va) in enumerate(folds):
        print(f"  fold {i}: train={len(tr)} val={len(va)} val_positives={int(y[va].sum())}", flush=True)

    results: dict[str, Any] = {}
    for tuner in (tune_logistic_regression, tune_lightgbm, tune_random_forest):
        res = tuner(dataset, X, y, folds)
        results[res.model_name] = {
            "best_params": res.best_params,
            "best_mean_pr_auc": res.best_mean_pr_auc,
            "best_per_fold_pr_auc": res.best_per_fold,
            "elapsed_seconds": res.elapsed_seconds,
            "phase_history": [
                {"label": p["label"], "top3": p["results"][:3]} for p in res.phase_history
            ],
        }
    return results
