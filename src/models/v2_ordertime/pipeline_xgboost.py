"""
XGBoost — v2 order-time backorder binary classifier.

Uses the same preprocessing stack as LR/LightGBM so comparisons stay fair.
If xgboost is unavailable, ``build_v2_xgboost_pipeline`` returns None.
"""

from __future__ import annotations

import os
from typing import Any

import numpy as np
from sklearn.pipeline import Pipeline

from .preprocessing import build_v2_column_preprocessor

try:
    from xgboost import XGBClassifier
except Exception:  # pragma: no cover - slim envs
    XGBClassifier = None


def xgboost_available() -> bool:
    return XGBClassifier is not None


# Presets for MODEL_XGBOOST_SWEEP=mild|medium|aggressive (PR-oriented imbalance tuning).
# scale_pos_mult multiplies the data-driven neg/pos ratio (capped below).
_XGB_SWEEP_PRESETS: dict[str, dict[str, Any]] = {
    "mild": {
        "n_estimators": 350,
        "learning_rate": 0.06,
        "max_depth": 4,
        "min_child_weight": 6,
        "subsample": 0.88,
        "colsample_bytree": 0.88,
        "reg_alpha": 1.5,
        "reg_lambda": 4.0,
        "gamma": 0.12,
        "max_delta_step": 0.0,
        "scale_pos_mult": 1.0,
    },
    "medium": {
        "n_estimators": 450,
        "learning_rate": 0.05,
        "max_depth": 5,
        "min_child_weight": 4,
        "subsample": 0.82,
        "colsample_bytree": 0.82,
        "reg_alpha": 1.0,
        "reg_lambda": 3.0,
        "gamma": 0.05,
        "max_delta_step": 0.0,
        "scale_pos_mult": 1.18,
    },
    "aggressive": {
        "n_estimators": 650,
        "learning_rate": 0.04,
        "max_depth": 6,
        "min_child_weight": 2,
        "subsample": 0.78,
        "colsample_bytree": 0.78,
        "reg_alpha": 0.4,
        "reg_lambda": 2.0,
        "gamma": 0.0,
        "max_delta_step": 1.0,
        "scale_pos_mult": 1.42,
    },
}


def build_v2_xgboost_pipeline(dataset: Any, y_train) -> Pipeline | None:
    """Build an unfitted sklearn + XGBoost Pipeline, or None if xgboost is unavailable."""
    if XGBClassifier is None:
        return None

    y = y_train.to_numpy()
    pos = int((y == 1).sum())
    neg = int((y == 0).sum())
    base_spw = float(neg / max(pos, 1))

    sweep = (os.environ.get("MODEL_XGBOOST_SWEEP") or "").strip().lower()
    if sweep in _XGB_SWEEP_PRESETS:
        preset = dict(_XGB_SWEEP_PRESETS[sweep])
    else:
        # No sweep env: original baseline hyperparameters.
        preset = {
            "n_estimators": 500,
            "learning_rate": 0.05,
            "max_depth": 5,
            "min_child_weight": 3,
            "subsample": 0.85,
            "colsample_bytree": 0.85,
            "reg_alpha": 1.0,
            "reg_lambda": 3.0,
            "gamma": 0.0,
            "max_delta_step": 0.0,
            "scale_pos_mult": 1.0,
        }

    spw_mult = float(preset.pop("scale_pos_mult", 1.0))
    scale_pos_weight = float(np.clip(base_spw * spw_mult, 1.0, 25.0))

    n_jobs = int(os.environ.get("MODEL_XGB_N_JOBS", "4"))
    tree_method = os.environ.get("MODEL_XGB_TREE_METHOD", "hist")
    eval_metric = os.environ.get("MODEL_XGB_EVAL_METRIC", "logloss")

    return Pipeline(
        steps=[
            ("preprocess", build_v2_column_preprocessor(dataset.numeric_features, dataset.categorical_features)),
            (
                "model",
                XGBClassifier(
                    n_estimators=int(preset["n_estimators"]),
                    learning_rate=float(preset["learning_rate"]),
                    max_depth=int(preset["max_depth"]),
                    min_child_weight=float(preset["min_child_weight"]),
                    subsample=float(preset["subsample"]),
                    colsample_bytree=float(preset["colsample_bytree"]),
                    reg_alpha=float(preset["reg_alpha"]),
                    reg_lambda=float(preset["reg_lambda"]),
                    gamma=float(preset["gamma"]),
                    max_delta_step=float(preset["max_delta_step"]),
                    objective="binary:logistic",
                    eval_metric=eval_metric,
                    scale_pos_weight=scale_pos_weight,
                    random_state=42,
                    n_jobs=n_jobs,
                    tree_method=tree_method,
                ),
            ),
        ]
    )
