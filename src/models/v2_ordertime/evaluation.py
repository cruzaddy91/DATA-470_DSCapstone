"""Shared metrics, OOF thresholding, and train/test evaluation for v2 classifiers."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from sklearn.base import clone
import os


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


def select_f1_max_threshold(
    y_true: pd.Series | np.ndarray,
    y_proba: np.ndarray,
    *,
    min_predicted_positives: int | None = None,
) -> float:
    """F1-maximizing threshold from (y, scores) with optional rare-positive guardrail."""
    from sklearn.metrics import precision_recall_curve

    y_true = np.asarray(y_true)
    if len(np.unique(y_true)) < 2:
        return 0.5

    precision, recall, thresholds = precision_recall_curve(y_true, y_proba)
    if len(thresholds) == 0:
        return 0.5

    f1_scores = (2 * precision[:-1] * recall[:-1]) / np.clip(precision[:-1] + recall[:-1], 1e-12, None)

    if min_predicted_positives is not None and min_predicted_positives > 0:
        positives = float((y_true == 1).sum())
        if positives > 0:
            # From PR definitions: predicted_positives = TP / precision = (recall*P) / precision
            pred_pos = (recall[:-1] * positives) / np.clip(precision[:-1], 1e-12, None)
            eligible = pred_pos >= float(min_predicted_positives)
            if np.any(eligible):
                masked = np.where(eligible, f1_scores, -np.inf)
                best_index = int(np.nanargmax(masked))
            else:
                best_index = int(np.nanargmax(f1_scores))
        else:
            best_index = int(np.nanargmax(f1_scores))
    else:
        best_index = int(np.nanargmax(f1_scores))
    return float(thresholds[best_index])


def select_threshold_with_precision_floor(
    y_true: pd.Series | np.ndarray,
    y_proba: np.ndarray,
    *,
    precision_floor: float = 0.30,
    min_predicted_positives: int | None = None,
) -> tuple[float, str]:
    """Choose threshold by max recall subject to precision floor; fallback to F1-max."""
    from sklearn.metrics import precision_recall_curve

    y_true = np.asarray(y_true)
    if len(np.unique(y_true)) < 2:
        return 0.5, "fallback_threshold_0.5_no_class_variation"

    precision, recall, thresholds = precision_recall_curve(y_true, y_proba)
    if len(thresholds) == 0:
        return 0.5, "fallback_threshold_0.5_empty_pr_curve"

    eligible = precision[:-1] >= float(precision_floor)
    if min_predicted_positives is not None and min_predicted_positives > 0:
        positives = float((y_true == 1).sum())
        if positives > 0:
            pred_pos = (recall[:-1] * positives) / np.clip(precision[:-1], 1e-12, None)
            eligible &= pred_pos >= float(min_predicted_positives)

    if np.any(eligible):
        # Among precision-feasible thresholds, maximize recall; tie-break with higher precision.
        recall_masked = np.where(eligible, recall[:-1], -np.inf)
        best_recall = np.nanmax(recall_masked)
        tie = eligible & np.isclose(recall[:-1], best_recall, atol=1e-12, rtol=0.0)
        if np.any(tie):
            precision_tie = np.where(tie, precision[:-1], -np.inf)
            best_index = int(np.nanargmax(precision_tie))
        else:
            best_index = int(np.nanargmax(recall_masked))
        return float(thresholds[best_index]), "recall_max_at_precision_floor"

    thr = select_f1_max_threshold(
        y_true,
        y_proba,
        min_predicted_positives=min_predicted_positives,
    )
    return thr, "fallback_f1_max_no_precision_floor_solution"


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
    precision_floor = float(os.environ.get("MODEL_PRECISION_FLOOR", "0.35"))
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
    guard_min_pred_pos = max(1, int(np.ceil((y_va == 1).sum() * 0.5)))
    threshold, objective = select_threshold_with_precision_floor(
        y_va,
        p_va,
        precision_floor=precision_floor,
        min_predicted_positives=guard_min_pred_pos,
    )
    meta["holdout_val_rows"] = float(len(va))
    meta["guard_min_predicted_positives"] = float(guard_min_pred_pos)
    meta["threshold_objective"] = 1.0
    meta["precision_floor"] = precision_floor
    meta["threshold_objective_label"] = 1.0 if objective == "recall_max_at_precision_floor" else 0.0
    return threshold, "single_stratified_holdout_train", meta


def threshold_from_train_temporal_tail(
    pipeline: Any,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    train_dates: pd.Series | np.ndarray,
    *,
    min_val_days: int = 28,
    min_val_positives: int = 10,
    min_calibration_positives: int = 25,
    val_date_share: float = 0.2,
    max_adaptive_window_days: int = 365,
) -> tuple[float, str, dict[str, float]]:
    """Calibrate threshold on a late-in-time validation tail within train (no future peeking)."""
    dates = pd.to_datetime(pd.Series(train_dates), errors="coerce")
    y_arr = y_train.to_numpy()
    precision_floor = float(os.environ.get("MODEL_PRECISION_FLOOR", "0.35"))
    meta: dict[str, float] = {}

    valid = dates.notna().to_numpy()
    if valid.sum() < 2:
        threshold, strategy, fallback_meta = threshold_from_train_oof(pipeline, X_train, y_train)
        fallback_meta["temporal_tail_fallback"] = 1.0
        return threshold, f"{strategy}_fallback_no_dates", fallback_meta

    unique_dates = np.sort(dates.loc[valid].drop_duplicates().to_numpy())
    max_date = pd.Timestamp(unique_dates[-1])

    def _split_at(cutoff: pd.Timestamp) -> tuple[np.ndarray, np.ndarray]:
        tr_mask = ((dates < cutoff) & dates.notna()).to_numpy()
        va_mask = (dates >= cutoff).fillna(False).to_numpy()
        return np.flatnonzero(tr_mask), np.flatnonzero(va_mask)

    chosen_cutoff: pd.Timestamp | None = None
    chosen_strategy = ""
    for raw_cutoff in unique_dates[::-1]:
        cutoff = pd.Timestamp(raw_cutoff)
        tr_idx, va_idx = _split_at(cutoff)
        if len(tr_idx) == 0 or len(va_idx) == 0:
            continue
        val_pos = int((y_arr[va_idx] == 1).sum())
        val_days = int((max_date - cutoff).days)
        if val_pos >= min_val_positives and val_days >= min_val_days:
            chosen_cutoff = cutoff
            chosen_strategy = "temporal_tail_min_pos_min_days"
            break

    if chosen_cutoff is None:
        for raw_cutoff in unique_dates[::-1]:
            cutoff = pd.Timestamp(raw_cutoff)
            tr_idx, va_idx = _split_at(cutoff)
            if len(tr_idx) == 0 or len(va_idx) == 0:
                continue
            val_pos = int((y_arr[va_idx] == 1).sum())
            if val_pos > 0:
                chosen_cutoff = cutoff
                chosen_strategy = "temporal_tail_any_pos"
                break

    if chosen_cutoff is None:
        default_position = max(1, int(np.floor(len(unique_dates) * (1 - val_date_share))))
        default_position = min(default_position, len(unique_dates) - 1)
        chosen_cutoff = pd.Timestamp(unique_dates[default_position])
        chosen_strategy = "temporal_tail_fallback_last_20pct_dates"

    tr_idx, va_idx = _split_at(chosen_cutoff)
    used_cutoff = chosen_cutoff
    used_strategy = chosen_strategy

    # Adaptive support: widen calibration window backward until enough positives are included.
    # This remains leak-safe (still inside train; never touches test rows).
    if len(va_idx) > 0:
        current_pos = int((y_arr[va_idx] == 1).sum())
        if current_pos < min_calibration_positives:
            for raw_cutoff in unique_dates[::-1]:
                candidate = pd.Timestamp(raw_cutoff)
                if candidate > chosen_cutoff:
                    continue
                tr_c, va_c = _split_at(candidate)
                if len(tr_c) == 0 or len(va_c) == 0:
                    continue
                val_days = int((max_date - candidate).days)
                if val_days > max_adaptive_window_days:
                    continue
                candidate_pos = int((y_arr[va_c] == 1).sum())
                if candidate_pos >= min_calibration_positives:
                    tr_idx, va_idx = tr_c, va_c
                    used_cutoff = candidate
                    used_strategy = f"{chosen_strategy}__adaptive_window_for_positive_support"
                    break

    if len(tr_idx) == 0 or len(va_idx) == 0:
        threshold, strategy, fallback_meta = threshold_from_train_oof(pipeline, X_train, y_train)
        fallback_meta["temporal_tail_fallback"] = 1.0
        return threshold, f"{strategy}_fallback_empty_split", fallback_meta

    pipe = clone(pipeline)
    pipe.fit(X_train.iloc[tr_idx], y_train.iloc[tr_idx])
    y_va = y_arr[va_idx]
    p_va = pipe.predict_proba(X_train.iloc[va_idx])[:, 1]
    val_pos_count = int((y_va == 1).sum())
    if val_pos_count < min_calibration_positives:
        meta["val_rows"] = float(len(va_idx))
        meta["val_positives"] = float(val_pos_count)
        meta["val_window_days"] = float(int((max_date - used_cutoff).days))
        meta["val_unique_dates"] = float(int(pd.Index(dates.iloc[va_idx]).nunique()))
        meta["val_cutoff_shift_days"] = float(int((chosen_cutoff - used_cutoff).days))
        meta["sparse_calibration_fallback_threshold"] = 0.5
        meta["minimum_calibration_positives"] = float(min_calibration_positives)
        return 0.5, "temporal_tail_fallback_0.5_sparse_positives", meta

    guard_min_pred_pos = max(1, int(np.ceil((y_va == 1).sum() * 0.5)))
    threshold, objective = select_threshold_with_precision_floor(
        y_va,
        p_va,
        precision_floor=precision_floor,
        min_predicted_positives=guard_min_pred_pos,
    )

    meta["val_rows"] = float(len(va_idx))
    meta["val_positives"] = float(int((y_va == 1).sum()))
    meta["guard_min_predicted_positives"] = float(guard_min_pred_pos)
    meta["precision_floor"] = precision_floor
    meta["threshold_objective_label"] = 1.0 if objective == "recall_max_at_precision_floor" else 0.0
    meta["val_window_days"] = float(int((max_date - used_cutoff).days))
    meta["val_unique_dates"] = float(int(pd.Index(dates.iloc[va_idx]).nunique()))
    meta["val_cutoff_shift_days"] = float(int((chosen_cutoff - used_cutoff).days))
    return threshold, f"{used_strategy}__{objective}", meta


def evaluate_classifier_train_test_split(
    dataset: Any,
    train_index: np.ndarray,
    test_index: np.ndarray,
    pipeline: Any,
    *,
    threshold_mode: str = "stratified_oof",
) -> tuple[dict[str, Any], np.ndarray, np.ndarray, Any]:
    """
    OOF threshold on train, full-train refit, score test. ``dataset`` must expose
    ``features``, ``target`` (PreparedDataset-compatible).
    """
    X_train = dataset.features.iloc[train_index]
    y_train = dataset.target.iloc[train_index]
    X_test = dataset.features.iloc[test_index]
    y_test = dataset.target.iloc[test_index]

    if threshold_mode == "temporal_tail":
        train_dates = dataset.meta.iloc[train_index]["order_date"]
        threshold, strategy, thresh_meta = threshold_from_train_temporal_tail(
            pipeline,
            X_train,
            y_train,
            train_dates,
        )
    else:
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
