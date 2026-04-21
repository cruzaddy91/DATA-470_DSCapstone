"""
v2 order-time backorder modeling: separate pipelines + shared evaluation.

- ``pipeline_logistic`` / ``pipeline_lightgbm``: one model per module (linear vs tree).
- ``classifier_registry``: combine both for runs and artifacts.
- ``evaluation``: metrics and OOF thresholding (model-agnostic).
"""

from .classifier_registry import build_all_v2_binary_classifiers
from .evaluation import (
    compute_classification_metrics,
    evaluate_classifier_train_test_split,
    threshold_from_train_oof,
)
from .pipeline_lightgbm import build_v2_lightgbm_pipeline, lightgbm_available
from .pipeline_logistic import build_v2_logistic_regression_pipeline

__all__ = [
    "build_all_v2_binary_classifiers",
    "build_v2_lightgbm_pipeline",
    "build_v2_logistic_regression_pipeline",
    "compute_classification_metrics",
    "evaluate_classifier_train_test_split",
    "lightgbm_available",
    "threshold_from_train_oof",
]
