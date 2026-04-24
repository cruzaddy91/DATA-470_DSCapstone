"""
Logistic regression — v2 order-time backorder binary classifier.

Linear model + MaxAbsScaler after preprocessing. Interpretable baseline; keep this file
free of LightGBM or tree-specific logic.
"""

from __future__ import annotations

from typing import Any

from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MaxAbsScaler

from .preprocessing import build_v2_column_preprocessor
from .tuned_params import load_tuned_params


def build_v2_logistic_regression_pipeline(dataset: Any) -> Pipeline:
    """
    Build an unfitted sklearn Pipeline for order-time features.

    Parameters
    ----------
    dataset :
        PreparedDataset (or equivalent) with ``numeric_features`` and
        ``categorical_features``.
    """
    tuned = load_tuned_params("logistic_regression")
    return Pipeline(
        steps=[
            ("preprocess", build_v2_column_preprocessor(dataset.numeric_features, dataset.categorical_features)),
            ("scale", MaxAbsScaler()),
            (
                "model",
                LogisticRegression(
                    C=tuned.get("C", 1.0),
                    max_iter=2000,
                    class_weight="balanced",
                    solver="lbfgs",
                    random_state=42,
                ),
            ),
        ]
    )
