"""Model training, evaluation, and persistence."""

__all__ = ["run_overfit_evaluation", "build_all_v2_binary_classifiers"]


def run_overfit_evaluation(*args, **kwargs):
    from .backorder_modeling import run_overfit_evaluation as _run_overfit_evaluation

    return _run_overfit_evaluation(*args, **kwargs)


def build_all_v2_binary_classifiers(*args, **kwargs):
    """Merge point for LR + LightGBM v2 pipelines (see ``v2_ordertime.classifier_registry``)."""
    from .v2_ordertime import build_all_v2_binary_classifiers as _fn

    return _fn(*args, **kwargs)
