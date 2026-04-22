"""
Register v2 binary classifiers in one place (poster / comparison entry point).

This module is the intentional \"merge point\": separate LR and LightGBM live in
``pipeline_logistic`` and ``pipeline_lightgbm``; here we expose them together for
evaluation and artifact saving.
"""

from __future__ import annotations

from typing import Any

import os
import pandas as pd
from sklearn.pipeline import Pipeline

from .pipeline_catboost import build_v2_catboost_pipeline
from .pipeline_lightgbm import build_v2_lightgbm_pipeline
from .pipeline_logistic import build_v2_logistic_regression_pipeline
from .pipeline_xgboost import build_v2_xgboost_pipeline


def build_all_v2_binary_classifiers(dataset: Any, y_train: pd.Series) -> dict[str, Pipeline]:
    """
    Return unfitted pipelines keyed by model name.

    Parameters
    ----------
    dataset :
        PreparedDataset (or equivalent).
    y_train :
        Training labels for the current fold (reserved for future use, e.g. sample
        weights); kept for API compatibility with the previous ``_make_pipelines`` call.
    """
    models: dict[str, Pipeline] = {
        "logistic_regression": build_v2_logistic_regression_pipeline(dataset),
    }
    lgb_pipe = build_v2_lightgbm_pipeline(dataset)
    if lgb_pipe is not None:
        models["lightgbm"] = lgb_pipe
    # Keep the default path stable and honest around the best temporal iteration.
    # Extra learners stay available for explicit experiments only.
    if os.environ.get("MODEL_ENABLE_EXTRA_MODELS", "0") == "1":
        xgb_pipe = build_v2_xgboost_pipeline(dataset, y_train)
        if xgb_pipe is not None:
            models["xgboost"] = xgb_pipe
        cb_pipe = build_v2_catboost_pipeline(dataset)
        if cb_pipe is not None:
            models["catboost"] = cb_pipe
    return models
