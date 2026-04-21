"""
Register v2 binary classifiers in one place (poster / comparison entry point).

This module is the intentional \"merge point\": separate LR and LightGBM live in
``pipeline_logistic`` and ``pipeline_lightgbm``; here we expose them together for
evaluation and artifact saving.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from sklearn.pipeline import Pipeline

from .pipeline_lightgbm import build_v2_lightgbm_pipeline
from .pipeline_logistic import build_v2_logistic_regression_pipeline


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
    _ = y_train
    models: dict[str, Pipeline] = {
        "logistic_regression": build_v2_logistic_regression_pipeline(dataset),
    }
    lgb_pipe = build_v2_lightgbm_pipeline(dataset)
    if lgb_pipe is not None:
        models["lightgbm"] = lgb_pipe
    return models
