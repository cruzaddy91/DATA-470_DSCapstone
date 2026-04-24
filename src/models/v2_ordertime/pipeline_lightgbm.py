"""
LightGBM — v2 order-time backorder binary classifier (gradient boosted trees).

Tree model on the same preprocessed feature matrix as logistic regression.
If LightGBM is not installed, ``build_v2_lightgbm_pipeline`` returns None.
"""

from __future__ import annotations

from typing import Any

from sklearn.pipeline import Pipeline

from .preprocessing import build_v2_column_preprocessor
from .tuned_params import load_tuned_params

try:
    import lightgbm as lgb
except Exception:  # pragma: no cover - slim envs
    lgb = None


def lightgbm_available() -> bool:
    return lgb is not None


def build_v2_lightgbm_pipeline(dataset: Any) -> Pipeline | None:
    """
    Build an unfitted sklearn + LightGBM Pipeline, or None if LightGBM is unavailable.

    Parameters
    ----------
    dataset :
        PreparedDataset (or equivalent) with ``numeric_features`` and
        ``categorical_features``.
    """
    if lgb is None:
        return None

    tuned = load_tuned_params("lightgbm")
    defaults = dict(
        n_estimators=250,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=40,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=1.0,
        reg_lambda=3.0,
    )
    # Tuned params override matching defaults; untuned levers keep their hand-set values.
    defaults.update(tuned)
    return Pipeline(
        steps=[
            ("preprocess", build_v2_column_preprocessor(dataset.numeric_features, dataset.categorical_features)),
            (
                "model",
                lgb.LGBMClassifier(
                    **defaults,
                    class_weight="balanced",
                    random_state=42,
                    verbose=-1,
                ),
            ),
        ]
    )
