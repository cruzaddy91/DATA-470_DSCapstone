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


def build_v2_logistic_regression_pipeline(dataset: Any) -> Pipeline:
    """
    Build an unfitted sklearn Pipeline for order-time features.

    Parameters
    ----------
    dataset :
        PreparedDataset (or equivalent) with ``numeric_features`` and
        ``categorical_features``.
    """
    return Pipeline(
        steps=[
            ("preprocess", build_v2_column_preprocessor(dataset.numeric_features, dataset.categorical_features)),
            ("scale", MaxAbsScaler()),
            (
                "model",
                LogisticRegression(
                    max_iter=2000,
                    class_weight="balanced",
                    random_state=42,
                ),
            ),
        ]
    )
