"""Shared metrics, OOF thresholding, and train/test evaluation for v2 classifiers."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from sklearn.base import clone


def compute_classification_metrics(
    y_true: pd.Series | np.ndarray,
    y_pred: np.ndarray,
    y_proba: np.ndarray,
) -> dict[str, float]:
    from sklearn.metrics import (
        accuracy_score,
        average_precision_score,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )

    y_true = np.asarray(y_true)
    if len(np.unique(y_true)) < 2:
        roc_auc = 0.0
        pr_auc = 0.0
    else:
        roc_auc = float(roc_auc_score(y_true, y_proba))
        pr_auc = float(average_precision_score(y_true, y_proba))

    return {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "roc_auc": roc_auc,
        "pr_auc": pr_auc,
    }


def select_f1_max_threshold(y_true: pd.Series | np.ndarray, y_proba: np.ndarray) -> float:
    """F1-maximizing threshold from (y, scores) on a calibration or OOF set."""
    from sklearn.metrics import precision_recall_curve

    y_true = np.asarray(y_true)
    if len(np.unique(y_true)) < 2:
        return 0.5

    precision, recall, thresholds = precision_recall_curve(y_true, y_proba)
    if len(thresholds) == 0:
        return 0.5

    f1_scores = (2 * precision[:-1] * recall[:-1]) / np.clip(precision[:-1] + recall[:-1], 1e-12, None)
    best_index = int(np.nanargmax(f1_scores))
    return float(thresholds[best_index])


def threshold_from_train_oof(
    pipeline: Any,
    X_train: pd.DataFrame,
    y_train: pd.Series,
) -> tuple[float, str, dict[str, float]]:
    """
    Decision threshold from stratified K-fold OOF positive-class probabilities on train,
    then callers fit a fresh clone on full X_train for test scoring.
    """
    from sklearn.model_selection import StratifiedKFold, train_test_split

    y_arr = y_train.to_numpy()
    n = len(y_arr)
    pos = int((y_arr == 1).sum())
    neg = int((y_arr == 0).sum())
    k = int(min(5, pos, neg))
    meta: dict[str, float] = {}

    if k >= 2:
        oof = np.zeros(n, dtype=np.float64)
        skf = StratifiedKFold(n_splits=k, shuffle=True, random_state=42)
        for train_rel, test_rel in skf.split(np.zeros(n), y_arr):
            pipe = clone(pipeline)
            pipe.fit(X_train.iloc[train_rel], y_train.iloc[train_rel])
            oof[test_rel] = pipe.predict_proba(X_train.iloc[test_rel])[:, 1]
        threshold = select_f1_max_threshold(y_arr, oof)
        meta["oof_folds"] = float(k)
        return threshold, "stratified_kfold_oof_train", meta

    idx = np.arange(n)
    try:
        tr, va = train_test_split(
            idx,
            test_size=0.25,
            stratify=y_arr if len(np.unique(y_arr)) >= 2 else None,
            random_state=42,
        )
    except ValueError:
        tr, va = train_test_split(idx, test_size=0.25, random_state=42)

    if len(va) == 0 or len(tr) == 0:
        return 0.5, "fallback_threshold_0.5_insufficient_train", meta

    pipe = clone(pipeline)
    pipe.fit(X_train.iloc[tr], y_train.iloc[tr])
    y_va = y_arr[va]
    p_va = pipe.predict_proba(X_train.iloc[va])[:, 1]
    threshold = select_f1_max_threshold(y_va, p_va)
    meta["holdout_val_rows"] = float(len(va))
    return threshold, "single_stratified_holdout_train", meta


def evaluate_classifier_train_test_split(
    dataset: Any,
    train_index: np.ndarray,
    test_index: np.ndarray,
    pipeline: Any,
) -> tuple[dict[str, Any], np.ndarray, np.ndarray, Any]:
    """
    OOF threshold on train, full-train refit, score test. ``dataset`` must expose
    ``features``, ``target`` (PreparedDataset-compatible).
    """
    X_train = dataset.features.iloc[train_index]
    y_train = dataset.target.iloc[train_index]
    X_test = dataset.features.iloc[test_index]
    y_test = dataset.target.iloc[test_index]

    threshold, strategy, thresh_meta = threshold_from_train_oof(pipeline, X_train, y_train)

    pipeline_final = clone(pipeline)
    pipeline_final.fit(X_train, y_train)
    y_proba = pipeline_final.predict_proba(X_test)[:, 1]
    y_pred = (y_proba >= threshold).astype(int)

    metrics = compute_classification_metrics(y_test, y_pred, y_proba)
    metrics["decision_threshold"] = threshold
    metrics["threshold_calibration_strategy"] = strategy
    metrics["threshold_calibration_train_rows"] = float(len(train_index))
    for key, value in thresh_meta.items():
        metrics[f"threshold_calibration_{key}"] = float(value)
    return metrics, y_pred, y_proba, pipeline_final
