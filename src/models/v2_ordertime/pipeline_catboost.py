"""
CatBoost — v2 order-time backorder binary classifier.

Uses the shared preprocessing stack for fair comparison with LR/LGBM/XGBoost.
If catboost is unavailable, ``build_v2_catboost_pipeline`` returns None.
"""

from __future__ import annotations

from typing import Any

from sklearn.pipeline import Pipeline

from .preprocessing import build_v2_column_preprocessor

try:
    from catboost import CatBoostClassifier
except Exception:  # pragma: no cover - slim envs
    CatBoostClassifier = None


def catboost_available() -> bool:
    return CatBoostClassifier is not None


def build_v2_catboost_pipeline(dataset: Any) -> Pipeline | None:
    """Build an unfitted sklearn + CatBoost Pipeline, or None if catboost is unavailable."""
    if CatBoostClassifier is None:
        return None

    return Pipeline(
        steps=[
            ("preprocess", build_v2_column_preprocessor(dataset.numeric_features, dataset.categorical_features)),
            (
                "model",
                CatBoostClassifier(
                    iterations=500,
                    learning_rate=0.05,
                    depth=6,
                    l2_leaf_reg=5.0,
                    loss_function="Logloss",
                    eval_metric="PRAUC",
                    auto_class_weights="Balanced",
                    random_seed=42,
                    verbose=False,
                ),
            ),
        ]
    )
