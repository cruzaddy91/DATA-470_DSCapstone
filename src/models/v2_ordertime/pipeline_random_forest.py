"""
Random Forest — v2 order-time backorder binary classifier.

Bagged tree ensemble. Decorrelates with boosted trees (LightGBM/XGBoost/CatBoost)
because bagging reduces variance rather than bias — useful as a stack base learner
that makes different mistakes than GBDTs.
"""

from __future__ import annotations

import os
from typing import Any

from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline

from .preprocessing import build_v2_column_preprocessor


def build_v2_random_forest_pipeline(dataset: Any) -> Pipeline:
    return Pipeline(
        steps=[
            ("preprocess", build_v2_column_preprocessor(dataset.numeric_features, dataset.categorical_features)),
            (
                "model",
                RandomForestClassifier(
                    n_estimators=400,
                    max_depth=None,
                    min_samples_leaf=5,
                    max_features="sqrt",
                    class_weight="balanced_subsample",
                    n_jobs=int(os.environ.get("MODEL_N_JOBS", "4")),
                    random_state=42,
                ),
            ),
        ]
    )
