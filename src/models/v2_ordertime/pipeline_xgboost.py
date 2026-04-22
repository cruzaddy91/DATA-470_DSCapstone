"""
XGBoost — v2 order-time backorder binary classifier.

Uses the same preprocessing stack as LR/LightGBM so comparisons stay fair.
If xgboost is unavailable, ``build_v2_xgboost_pipeline`` returns None.
"""

from __future__ import annotations

from typing import Any

from sklearn.pipeline import Pipeline

from .preprocessing import build_v2_column_preprocessor

try:
    from xgboost import XGBClassifier
except Exception:  # pragma: no cover - slim envs
    XGBClassifier = None


def xgboost_available() -> bool:
    return XGBClassifier is not None


def build_v2_xgboost_pipeline(dataset: Any, y_train) -> Pipeline | None:
    """Build an unfitted sklearn + XGBoost Pipeline, or None if xgboost is unavailable."""
    if XGBClassifier is None:
        return None

    y = y_train.to_numpy()
    pos = int((y == 1).sum())
    neg = int((y == 0).sum())
    scale_pos_weight = float(neg / max(pos, 1))

    return Pipeline(
        steps=[
            ("preprocess", build_v2_column_preprocessor(dataset.numeric_features, dataset.categorical_features)),
            (
                "model",
                XGBClassifier(
                    n_estimators=500,
                    learning_rate=0.05,
                    max_depth=5,
                    min_child_weight=3,
                    subsample=0.85,
                    colsample_bytree=0.85,
                    reg_alpha=1.0,
                    reg_lambda=3.0,
                    gamma=0.0,
                    objective="binary:logistic",
                    eval_metric="logloss",
                    scale_pos_weight=scale_pos_weight,
                    random_state=42,
                    n_jobs=4,
                    tree_method="hist",
                ),
            ),
        ]
    )
