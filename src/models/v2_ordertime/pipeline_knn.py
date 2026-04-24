"""
k-Nearest Neighbors — v2 order-time backorder binary classifier.

Instance-based, non-parametric. Fundamentally different family from linear
(LogisticRegression) and tree ensembles (LightGBM, RandomForest) — errors
decorrelate, which is the only reason to add it to a stack.

Distance-weighted voting is preferred under heavy class imbalance so that
closer training neighbors dominate the prediction.
"""

from __future__ import annotations

import os
from typing import Any

from sklearn.neighbors import KNeighborsClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from .preprocessing import build_v2_column_preprocessor


def build_v2_knn_pipeline(dataset: Any) -> Pipeline:
    return Pipeline(
        steps=[
            ("preprocess", build_v2_column_preprocessor(dataset.numeric_features, dataset.categorical_features)),
            ("scale", StandardScaler(with_mean=False)),
            (
                "model",
                KNeighborsClassifier(
                    n_neighbors=25,
                    weights="distance",
                    algorithm="auto",
                    n_jobs=int(os.environ.get("MODEL_N_JOBS", "4")),
                ),
            ),
        ]
    )
