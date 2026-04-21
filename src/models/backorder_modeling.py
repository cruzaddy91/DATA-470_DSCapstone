"""Official `v2` order-time backorder modeling and artifact generation."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sklearn.base import clone

from src.models.v2_ordertime.classifier_registry import build_all_v2_binary_classifiers
from src.models.v2_ordertime.evaluation import (
    compute_classification_metrics,
    evaluate_classifier_train_test_split,
    threshold_from_train_oof,
)

os.environ.setdefault("MPLBACKEND", "Agg")
os.environ.setdefault("PANDAS_NO_USE_PYARROW", "1")
# Do not stub pyarrow as None: pandas>=2.2 may call into pyarrow during CSV/DataFrame paths and crash.
sys.modules.setdefault("numexpr", None)
sys.modules.setdefault("bottleneck", None)
_DEFAULT_MPL_DIR = Path.cwd() / ".matplotlib"
_DEFAULT_MPL_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(_DEFAULT_MPL_DIR))

import joblib
import numpy as np
import pandas as pd

from src.features.build_targets import (
    DATE_COLUMN as ORDERTIME_DATE_COLUMN,
    DEMAND_INVENTORY_NUMERIC_FEATURES,
    SNAPSHOT_BACKORDER_MODELING_TABLE,
    SNAPSHOT_BACKORDER_TARGET_COLUMN,
    SNAPSHOT_ML_NUMERIC_FEATURES,
    SNAPSHOT_OSQ_COLUMN,
    SNAPSHOT_SI_COLUMN,
    SNAPSHOT_WIDE_NUMERIC_FEATURES,
    LEGACY_ORDER_TARGET_TABLE,
    ORDERTIME_CATEGORICAL_FEATURES,
    ORDERTIME_FORBIDDEN_COLUMNS,
    ORDERTIME_META_COLUMNS,
    ORDERTIME_MODELING_TABLE,
    ORDERTIME_NUMERIC_FEATURES,
    ORDER_GRAIN_COLUMNS,
    ORDER_HISTORY_WINDOW_WEEKS,
    DEMAND_ROLLING_WINDOW_WEEKS,
    TARGET_OBSERVED_COLUMN,
    TARGET_STATUS_COLUMN,
    TARGET_COLUMN as ORDERTIME_TARGET_COLUMN,
    V3_DEMAND_INVENTORY_MODELING_TABLE,
    validate_order_grain,
)


TARGET_COLUMN = ORDERTIME_TARGET_COLUMN
GROUP_COLUMN = "sales_document_number"
DATE_COLUMN = ORDERTIME_DATE_COLUMN

RAW_NUMERIC_FEATURES = ORDERTIME_NUMERIC_FEATURES
RAW_NUMERIC_FEATURES_V3 = ORDERTIME_NUMERIC_FEATURES + DEMAND_INVENTORY_NUMERIC_FEATURES
RAW_CATEGORICAL_FEATURES = ORDERTIME_CATEGORICAL_FEATURES
LEAKY_COLUMNS = ORDERTIME_FORBIDDEN_COLUMNS

LEGACY_FEATURE_COLUMNS = [
    "cumulative_order_quantity",
    "total_quantity_delivered",
    "net_value",
    "material_type",
    "item_category",
]

ARTIFACT_SUFFIX = "_v2_ordertime"
MODELING_TABLE_FILE = f"{ORDERTIME_MODELING_TABLE}.csv"
MODELING_V3_TABLE_FILE = f"{V3_DEMAND_INVENTORY_MODELING_TABLE}.csv"
ARTIFACT_SUFFIX_V3 = "_v3_demand_inventory_24wk"
SNAPSHOT_BACKORDER_MODELING_TABLE_FILE = f"{SNAPSHOT_BACKORDER_MODELING_TABLE}.csv"
LEGACY_TABLE_FILE = f"{LEGACY_ORDER_TARGET_TABLE}.csv"

MODEL_FILE_MAP = {
    "logistic_regression": f"backorder_logistic{ARTIFACT_SUFFIX}.joblib",
    "lightgbm": f"backorder_lightgbm{ARTIFACT_SUFFIX}.joblib",
}

CLASSIFICATION_METRICS_FILE = f"classification_metrics{ARTIFACT_SUFFIX}.json"
OVERFIT_RESULTS_FILE = f"overfit_eval_results{ARTIFACT_SUFFIX}.json"
AUC_DIAGNOSTICS_FILE = f"auc_diagnostics{ARTIFACT_SUFFIX}.json"
TEMPORAL_HOLDOUT_SCORES_FILE = f"temporal_holdout_test_scores{ARTIFACT_SUFFIX}.json"
REGRESSION_METRICS_FILE = f"regression_metrics{ARTIFACT_SUFFIX}.json"
TARGET_BALANCE_FIGURE_FILE = f"target_balance{ARTIFACT_SUFFIX}.png"
CONFUSION_FIGURE_FILE = f"classification_confusion_matrices{ARTIFACT_SUFFIX}.png"
FEATURE_IMPORTANCE_FIGURE_FILE = f"classification_feature_importance{ARTIFACT_SUFFIX}.png"
FEATURE_IMPORTANCE_TABLE_FILE = f"classification_feature_importance{ARTIFACT_SUFFIX}.csv"
MODEL_COMPARISON_TABLE_FILE = f"classification_model_comparison{ARTIFACT_SUFFIX}.csv"
DEMAND_FORECAST_TABLE_FILE = f"demand_forecast{ARTIFACT_SUFFIX}.csv"
EXCESS_INVENTORY_TABLE_FILE = f"excess_inventory{ARTIFACT_SUFFIX}.csv"

TEMPORAL_TEST_DATE_SHARE = 0.2
TEMPORAL_TEST_MIN_POSITIVES = 50
TEMPORAL_TEST_MIN_DAYS = 90

# Train/test both restricted to rows with order_date in (anchor - N weeks, anchor].
# Evaluation: temporal sub-split inside that window (earlier dates train, later dates test).
RECENT_WINDOW_WEEKS = 24
RECENT_WINDOW_MIN_TEST_POSITIVES = 10
RECENT_WINDOW_MIN_TEST_DAYS = 14

# Logistic regression on only outstanding_qty + saleable_inventory (label-defining quantities).
OSQ_SI_LOGISTIC_MODEL_NAME = "logistic_osq_si_only"


@dataclass
class PreparedDataset:
    """Prepared order-time modeling dataset and metadata."""

    features: pd.DataFrame
    target: pd.Series
    meta: pd.DataFrame
    numeric_features: list[str]
    categorical_features: list[str]
    missing_indicator_features: list[str]
    # Snapshot backorder label only: OSQ/SI aligned row-wise for rule + two-feature logistic (not in sklearn X).
    osq_si_label_inputs: pd.DataFrame | None = None


def _get_paths(project_root: str | Path | None = None) -> dict[str, Path]:
    root = Path(project_root) if project_root else Path(__file__).resolve().parents[2]
    return {
        "project_root": root,
        "processed": root / "data" / "processed",
        "models": root / "models",
        "figures": root / "output" / "figures",
        "tables": root / "output" / "tables",
    }


def _configure_runtime(paths: dict[str, Path]) -> None:
    matplotlib_dir = paths["project_root"] / ".matplotlib"
    matplotlib_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(matplotlib_dir))


def _get_plotting_modules():
    import matplotlib.pyplot as plt
    import seaborn as sns

    sns.set_theme(style="whitegrid", palette="deep", font_scale=1.0)
    return plt, sns


def _load_modeling_table(paths: dict[str, Path]) -> pd.DataFrame:
    order_path = paths["processed"] / MODELING_TABLE_FILE
    if not order_path.exists():
        raise FileNotFoundError(f"Missing processed order-time modeling table: {order_path}")
    return pd.read_csv(order_path, low_memory=False)


def _load_modeling_table_v3(paths: dict[str, Path]) -> pd.DataFrame:
    path = paths["processed"] / MODELING_V3_TABLE_FILE
    if not path.exists():
        raise FileNotFoundError(
            f"Missing v3 demand/inventory modeling table: {path}. "
            "Run: python -m src.features.build_targets (requires shipment_history.csv)."
        )
    return pd.read_csv(path, low_memory=False)


def _load_legacy_table(paths: dict[str, Path]) -> pd.DataFrame:
    order_path = paths["processed"] / LEGACY_TABLE_FILE
    if not order_path.exists():
        raise FileNotFoundError(f"Missing legacy snapshot table: {order_path}")
    return pd.read_csv(order_path, low_memory=False)


def _load_snapshot_backorder_modeling_table(paths: dict[str, Path]) -> pd.DataFrame:
    path = paths["processed"] / SNAPSHOT_BACKORDER_MODELING_TABLE_FILE
    if not path.exists():
        raise FileNotFoundError(
            f"Missing snapshot backorder modeling table: {path}. "
            "Run build targets after pipeline: python -m src.features.build_targets"
        )
    return pd.read_csv(path, low_memory=False)


def _clean_categorical_series(series: pd.Series) -> pd.Series:
    cleaned = series.copy().astype("object")
    cleaned = cleaned.map(lambda value: value.strip() if isinstance(value, str) else value)
    return cleaned.replace("", pd.NA)


def _assert_required_columns(df: pd.DataFrame, required_columns: list[str], label: str) -> None:
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise KeyError(f"{label} is missing required columns: {missing}")


def _assert_no_forbidden_columns(df: pd.DataFrame, forbidden_columns: list[str], label: str) -> None:
    present = [column for column in forbidden_columns if column in df.columns]
    if present:
        raise ValueError(f"{label} contains forbidden columns: {present}")


def _assert_no_non_finite(df: pd.DataFrame, numeric_columns: list[str], label: str) -> None:
    if not numeric_columns:
        return
    for column in numeric_columns:
        series = pd.to_numeric(df[column], errors="coerce")
        if np.isinf(series.to_numpy(dtype="float64", na_value=np.nan)).any():
            raise ValueError(f"{label} contains non-finite values in column: {column}")


def prepare_backorder_dataset(project_root: str | Path | None = None) -> PreparedDataset:
    """Build the official order-time feature matrix for backorder modeling."""
    paths = _get_paths(project_root)
    order = _load_modeling_table(paths).copy()

    validate_order_grain(order, ORDERTIME_MODELING_TABLE)
    _assert_required_columns(
        order,
        ORDER_GRAIN_COLUMNS
        + ORDERTIME_META_COLUMNS
        + RAW_NUMERIC_FEATURES
        + RAW_CATEGORICAL_FEATURES
        + [TARGET_COLUMN],
        ORDERTIME_MODELING_TABLE,
    )
    _assert_no_forbidden_columns(order, LEAKY_COLUMNS, ORDERTIME_MODELING_TABLE)

    for column in RAW_NUMERIC_FEATURES:
        order[column] = pd.to_numeric(order[column], errors="coerce")
    _assert_no_non_finite(order, RAW_NUMERIC_FEATURES, ORDERTIME_MODELING_TABLE)

    for column in RAW_CATEGORICAL_FEATURES:
        order[column] = _clean_categorical_series(order[column])

    target = pd.to_numeric(order[TARGET_COLUMN], errors="coerce")
    mask = target.notna()
    if not mask.any():
        raise ValueError("Order-time modeling dataset has no resolved targets after outcome filtering.")

    feature_columns = RAW_NUMERIC_FEATURES + RAW_CATEGORICAL_FEATURES
    features = order.loc[mask, feature_columns].copy()
    missing_indicator_features: list[str] = []

    for column in RAW_NUMERIC_FEATURES:
        if features[column].isna().any():
            indicator_column = f"missing__{column}"
            features[indicator_column] = features[column].isna().astype(int)
            missing_indicator_features.append(indicator_column)

    for column in RAW_CATEGORICAL_FEATURES:
        if features[column].isna().any():
            indicator_column = f"missing__{column}"
            features[indicator_column] = features[column].isna().astype(int)
            missing_indicator_features.append(indicator_column)

    numeric_features = RAW_NUMERIC_FEATURES + missing_indicator_features
    categorical_features = RAW_CATEGORICAL_FEATURES.copy()
    _assert_no_non_finite(features, numeric_features, "Prepared order-time feature matrix")

    meta_columns = ORDER_GRAIN_COLUMNS + [column for column in ORDERTIME_META_COLUMNS if column in order.columns]
    meta = order.loc[mask, meta_columns].copy()
    if DATE_COLUMN in meta.columns:
        meta[DATE_COLUMN] = pd.to_datetime(meta[DATE_COLUMN], errors="coerce")

    features = features.reset_index(drop=True)
    meta = meta.reset_index(drop=True)
    target = target.loc[mask].astype(int).reset_index(drop=True)

    return PreparedDataset(
        features=features,
        target=target,
        meta=meta,
        numeric_features=numeric_features,
        categorical_features=categorical_features,
        missing_indicator_features=missing_indicator_features,
        osq_si_label_inputs=None,
    )


def prepare_backorder_dataset_v3(project_root: str | Path | None = None) -> PreparedDataset:
    """
    v3 table: v2 order-time features + trailing shipped demand + WOC inventory; rows limited to the
    last ``ORDER_HISTORY_WINDOW_WEEKS`` of orders. Intended for stratified random / k-fold evaluation.
    """
    paths = _get_paths(project_root)
    order = _load_modeling_table_v3(paths).copy()

    validate_order_grain(order, V3_DEMAND_INVENTORY_MODELING_TABLE)
    _assert_required_columns(
        order,
        ORDER_GRAIN_COLUMNS
        + ORDERTIME_META_COLUMNS
        + RAW_NUMERIC_FEATURES_V3
        + RAW_CATEGORICAL_FEATURES
        + [TARGET_COLUMN],
        V3_DEMAND_INVENTORY_MODELING_TABLE,
    )

    for column in RAW_NUMERIC_FEATURES_V3:
        order[column] = pd.to_numeric(order[column], errors="coerce")
    _assert_no_non_finite(order, RAW_NUMERIC_FEATURES_V3, V3_DEMAND_INVENTORY_MODELING_TABLE)

    for column in RAW_CATEGORICAL_FEATURES:
        v = _clean_categorical_series(order[column])
        order[column] = v.map(lambda x: "Missing" if pd.isna(x) else str(x).strip())

    target = pd.to_numeric(order[TARGET_COLUMN], errors="coerce")
    mask = target.notna()
    if not mask.any():
        raise ValueError("v3 modeling dataset has no resolved targets after outcome filtering.")

    feature_columns = RAW_NUMERIC_FEATURES_V3 + RAW_CATEGORICAL_FEATURES
    features = order.loc[mask, feature_columns].copy()
    missing_indicator_features: list[str] = []

    for column in RAW_NUMERIC_FEATURES_V3:
        if features[column].isna().any():
            indicator_column = f"missing__{column}"
            features[indicator_column] = features[column].isna().astype(int)
            missing_indicator_features.append(indicator_column)

    for column in RAW_CATEGORICAL_FEATURES:
        if features[column].isna().any():
            indicator_column = f"missing__{column}"
            features[indicator_column] = features[column].isna().astype(int)
            missing_indicator_features.append(indicator_column)

    numeric_features = RAW_NUMERIC_FEATURES_V3 + missing_indicator_features
    categorical_features = RAW_CATEGORICAL_FEATURES.copy()
    _assert_no_non_finite(features, numeric_features, "Prepared v3 demand/inventory feature matrix")

    meta_columns = ORDER_GRAIN_COLUMNS + [column for column in ORDERTIME_META_COLUMNS if column in order.columns]
    meta = order.loc[mask, meta_columns].copy()
    if DATE_COLUMN in meta.columns:
        meta[DATE_COLUMN] = pd.to_datetime(meta[DATE_COLUMN], errors="coerce")

    features = features.reset_index(drop=True)
    meta = meta.reset_index(drop=True)
    target = target.loc[mask].astype(int).reset_index(drop=True)

    return PreparedDataset(
        features=features,
        target=target,
        meta=meta,
        numeric_features=numeric_features,
        categorical_features=categorical_features,
        missing_indicator_features=missing_indicator_features,
        osq_si_label_inputs=None,
    )


def prepare_snapshot_backorder_dataset(project_root: str | Path | None = None) -> PreparedDataset:
    """
    Snapshot backorder dataset: target = 1 iff max(0, outstanding_qty - saleable_inventory) > 0.

    Sklearn pipelines (LR/LGB) use order-time numerics and categoricals only — the same KPI contract
    as v2 — so models are not given the quantities that define the label. OSQ/SI are loaded into
    ``osq_si_label_inputs`` for the deterministic rule and the two-feature logistic baseline.
    """
    paths = _get_paths(project_root)
    order = _load_snapshot_backorder_modeling_table(paths).copy()

    validate_order_grain(order, SNAPSHOT_BACKORDER_MODELING_TABLE)
    _assert_required_columns(
        order,
        ORDER_GRAIN_COLUMNS
        + ORDERTIME_META_COLUMNS
        + SNAPSHOT_WIDE_NUMERIC_FEATURES
        + ORDERTIME_CATEGORICAL_FEATURES
        + [SNAPSHOT_BACKORDER_TARGET_COLUMN],
        SNAPSHOT_BACKORDER_MODELING_TABLE,
    )

    for column in SNAPSHOT_WIDE_NUMERIC_FEATURES:
        order[column] = pd.to_numeric(order[column], errors="coerce")
    _assert_no_non_finite(order, SNAPSHOT_WIDE_NUMERIC_FEATURES, SNAPSHOT_BACKORDER_MODELING_TABLE)

    for column in ORDERTIME_CATEGORICAL_FEATURES:
        order[column] = _clean_categorical_series(order[column])

    target = pd.to_numeric(order[SNAPSHOT_BACKORDER_TARGET_COLUMN], errors="coerce").fillna(0).astype(int)

    osq_si_label_inputs = order.loc[:, [SNAPSHOT_OSQ_COLUMN, SNAPSHOT_SI_COLUMN]].copy()

    feature_columns = SNAPSHOT_ML_NUMERIC_FEATURES + ORDERTIME_CATEGORICAL_FEATURES
    features = order.loc[:, feature_columns].copy()
    missing_indicator_features: list[str] = []

    for column in SNAPSHOT_ML_NUMERIC_FEATURES:
        if features[column].isna().any():
            indicator_column = f"missing__{column}"
            features[indicator_column] = features[column].isna().astype(int)
            missing_indicator_features.append(indicator_column)

    for column in ORDERTIME_CATEGORICAL_FEATURES:
        if features[column].isna().any():
            indicator_column = f"missing__{column}"
            features[indicator_column] = features[column].isna().astype(int)
            missing_indicator_features.append(indicator_column)

    numeric_features = SNAPSHOT_ML_NUMERIC_FEATURES + missing_indicator_features
    categorical_features = ORDERTIME_CATEGORICAL_FEATURES.copy()
    _assert_no_non_finite(features, numeric_features, "Prepared snapshot backorder feature matrix")

    meta_columns = ORDER_GRAIN_COLUMNS + [column for column in ORDERTIME_META_COLUMNS if column in order.columns]
    meta = order.loc[:, meta_columns].copy()
    if DATE_COLUMN in meta.columns:
        meta[DATE_COLUMN] = pd.to_datetime(meta[DATE_COLUMN], errors="coerce")

    features = features.reset_index(drop=True)
    meta = meta.reset_index(drop=True)
    target = target.reset_index(drop=True)
    osq_si_label_inputs = osq_si_label_inputs.reset_index(drop=True)

    return PreparedDataset(
        features=features,
        target=target,
        meta=meta,
        numeric_features=numeric_features,
        categorical_features=categorical_features,
        missing_indicator_features=missing_indicator_features,
        osq_si_label_inputs=osq_si_label_inputs,
    )


def _snapshot_backorder_rule_metrics(dataset: PreparedDataset, test_index: np.ndarray) -> dict[str, Any]:
    """Deterministic baseline: predict positive iff outstanding_qty > saleable_inventory (label identity)."""
    if dataset.osq_si_label_inputs is None:
        raise ValueError("Snapshot backorder rule requires osq_si_label_inputs on PreparedDataset.")
    snap = dataset.osq_si_label_inputs
    if SNAPSHOT_OSQ_COLUMN not in snap.columns or SNAPSHOT_SI_COLUMN not in snap.columns:
        raise KeyError("Snapshot backorder rule requires outstanding_qty and saleable_inventory columns.")

    osq = pd.to_numeric(
        snap[SNAPSHOT_OSQ_COLUMN].iloc[test_index],
        errors="coerce",
    ).fillna(0.0)
    si = pd.to_numeric(
        snap[SNAPSHOT_SI_COLUMN].iloc[test_index],
        errors="coerce",
    ).fillna(0.0)
    y_test = dataset.target.iloc[test_index].reset_index(drop=True)
    y_proba = (osq > si).astype(float).to_numpy()
    y_pred = y_proba.astype(int)
    metrics = compute_classification_metrics(y_test, y_pred, y_proba)
    metrics["decision_threshold"] = 0.5
    # Predictions reproduce the target construction; ranking metrics are not meaningful.
    metrics["roc_auc"] = None
    metrics["pr_auc"] = None
    metrics["metric_note"] = (
        "roc_auc and pr_auc omitted: rule matches snapshot target (shortfall = max(0, OSQ−SI))."
    )
    return metrics


def _evaluate_osq_si_logistic(
    dataset: PreparedDataset,
    train_index: np.ndarray,
    test_index: np.ndarray,
) -> dict[str, float]:
    """Train logistic regression on outstanding_qty and saleable_inventory only."""
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import MaxAbsScaler

    if dataset.osq_si_label_inputs is None:
        raise ValueError("OSQ/SI logistic requires osq_si_label_inputs on PreparedDataset.")
    cols = [SNAPSHOT_OSQ_COLUMN, SNAPSHOT_SI_COLUMN]
    snap = dataset.osq_si_label_inputs
    for column in cols:
        if column not in snap.columns:
            raise KeyError(f"OSQ/SI logistic requires column: {column}")

    X_train = snap.iloc[train_index][cols].apply(pd.to_numeric, errors="coerce")
    X_test = snap.iloc[test_index][cols].apply(pd.to_numeric, errors="coerce")
    y_train = dataset.target.iloc[train_index]
    y_test = dataset.target.iloc[test_index]

    template = Pipeline(
        steps=[
            ("impute", SimpleImputer(strategy="median")),
            ("scale", MaxAbsScaler()),
            (
                "model",
                LogisticRegression(max_iter=2000, class_weight="balanced", random_state=42),
            ),
        ]
    )
    threshold, strategy, thresh_meta = threshold_from_train_oof(template, X_train, y_train)
    pipeline_final = clone(template)
    pipeline_final.fit(X_train, y_train)
    y_proba = pipeline_final.predict_proba(X_test)[:, 1]
    y_pred = (y_proba >= threshold).astype(int)
    metrics = compute_classification_metrics(y_test, y_pred, y_proba)
    metrics["decision_threshold"] = threshold
    metrics["threshold_calibration_strategy"] = strategy
    metrics["threshold_calibration_train_rows"] = float(len(train_index))
    for key, value in thresh_meta.items():
        metrics[f"threshold_calibration_{key}"] = float(value)
    return metrics


def _merge_snapshot_backorder_models(
    dataset: PreparedDataset,
    train_index: np.ndarray,
    test_index: np.ndarray,
) -> dict[str, dict[str, Any]]:
    sklearn_metrics, _, _ = _evaluate_models(dataset, train_index, test_index)
    rule_metrics = _snapshot_backorder_rule_metrics(dataset, test_index)
    minimal_metrics = _evaluate_osq_si_logistic(dataset, train_index, test_index)
    merged = dict(sklearn_metrics)
    merged[OSQ_SI_LOGISTIC_MODEL_NAME] = minimal_metrics
    merged["rule_outstanding_gt_saleable"] = rule_metrics
    return merged


def _snapshot_backorder_metrics_addon(project_root: str | Path) -> dict[str, Any]:
    """JSON keys for snapshot backorder evaluation (neutral naming; same shortfall logic as ERP-style views)."""
    paths = _get_paths(project_root)
    try:
        snap_dataset = prepare_snapshot_backorder_dataset(paths["project_root"])
    except FileNotFoundError as exc:
        return {"snapshot_backorder_status": {"available": False, "detail": str(exc)}}

    try:
        temporal_train, temporal_test, temporal_split = _temporal_split_indices(snap_dataset)
        group_train, group_test = _group_split_indices(snap_dataset)
        recent_train, recent_test, recent_split = _recent_24_week_temporal_split_indices(snap_dataset)
    except Exception as exc:  # pragma: no cover - depends on live data shape
        return {"snapshot_backorder_status": {"available": False, "detail": str(exc)}}

    temporal_models = _merge_snapshot_backorder_models(snap_dataset, temporal_train, temporal_test)
    group_models = _merge_snapshot_backorder_models(snap_dataset, group_train, group_test)
    recent_models = _merge_snapshot_backorder_models(snap_dataset, recent_train, recent_test)

    return {
        "snapshot_backorder_status": {"available": True},
        "snapshot_backorder_dataset_summary": {
            "rows": int(len(snap_dataset.features)),
            "positive_rate": float(snap_dataset.target.mean()),
        },
        "snapshot_backorder_temporal_holdout": {
            "train_rows": int(len(temporal_train)),
            "test_rows": int(len(temporal_test)),
            "train_positives": int(snap_dataset.target.iloc[temporal_train].sum()),
            "test_positives": int(snap_dataset.target.iloc[temporal_test].sum()),
            "test_positive_rate": float(snap_dataset.target.iloc[temporal_test].mean()),
            **temporal_split,
            "models": temporal_models,
        },
        "snapshot_backorder_group_holdout": {
            "train_rows": int(len(group_train)),
            "test_rows": int(len(group_test)),
            "train_positives": int(snap_dataset.target.iloc[group_train].sum()),
            "test_positives": int(snap_dataset.target.iloc[group_test].sum()),
            "test_positive_rate": float(snap_dataset.target.iloc[group_test].mean()),
            "group_column": GROUP_COLUMN,
            "models": group_models,
        },
        "snapshot_backorder_recent_24_week_temporal_holdout": {
            "train_rows": int(len(recent_train)),
            "test_rows": int(len(recent_test)),
            "train_positives": int(snap_dataset.target.iloc[recent_train].sum()),
            "test_positives": int(snap_dataset.target.iloc[recent_test].sum()),
            "test_positive_rate": float(snap_dataset.target.iloc[recent_test].mean()),
            **recent_split,
            "models": recent_models,
        },
    }


def _evaluate_models(
    dataset: PreparedDataset,
    train_index: np.ndarray,
    test_index: np.ndarray,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any], dict[str, dict[str, np.ndarray]]]:
    """Run each registered v2 classifier (LR + LightGBM) through shared OOF thresholding."""
    y_train = dataset.target.iloc[train_index]

    metrics_by_model: dict[str, dict[str, Any]] = {}
    fitted_models: dict[str, Any] = {}
    predictions: dict[str, dict[str, np.ndarray]] = {}

    for name, pipeline in build_all_v2_binary_classifiers(dataset, y_train).items():
        metrics, y_pred, y_proba, pipeline_final = evaluate_classifier_train_test_split(
            dataset, train_index, test_index, pipeline
        )
        metrics_by_model[name] = metrics
        fitted_models[name] = pipeline_final
        predictions[name] = {"y_pred": y_pred, "y_proba": y_proba}

    return metrics_by_model, fitted_models, predictions


def _temporal_split_indices(dataset: PreparedDataset) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    if DATE_COLUMN not in dataset.meta.columns:
        raise ValueError("Temporal split requires order_date in the prepared dataset.")

    dates = pd.to_datetime(dataset.meta[DATE_COLUMN], errors="coerce")
    valid_mask = dates.notna().to_numpy()
    valid_indices = np.flatnonzero(valid_mask)
    if len(valid_indices) < 2:
        raise ValueError("Temporal split requires at least two dated rows.")

    unique_dates = np.sort(dates.loc[valid_mask].drop_duplicates().to_numpy())
    if len(unique_dates) < 2:
        raise ValueError("Temporal split requires at least two unique dates.")

    max_date = pd.Timestamp(unique_dates[-1])

    def _indices_for_cutoff(cutoff: pd.Timestamp) -> tuple[np.ndarray, np.ndarray]:
        test_mask = (dates >= cutoff).fillna(False).to_numpy()
        train_mask = ((dates < cutoff) & dates.notna()).to_numpy()
        return np.flatnonzero(train_mask), np.flatnonzero(test_mask)

    selected_cutoff: pd.Timestamp | None = None
    selected_strategy = ""

    for raw_cutoff in unique_dates[::-1]:
        cutoff = pd.Timestamp(raw_cutoff)
        train_index, test_index = _indices_for_cutoff(cutoff)
        if len(train_index) == 0 or len(test_index) == 0:
            continue
        test_positives = int(dataset.target.iloc[test_index].sum())
        window_days = int((max_date - cutoff).days)
        if test_positives >= TEMPORAL_TEST_MIN_POSITIVES and window_days >= TEMPORAL_TEST_MIN_DAYS:
            selected_cutoff = cutoff
            selected_strategy = "latest_cutoff_with_minimum_positive_support"
            break

    if selected_cutoff is None:
        for raw_cutoff in unique_dates[::-1]:
            cutoff = pd.Timestamp(raw_cutoff)
            train_index, test_index = _indices_for_cutoff(cutoff)
            if len(train_index) == 0 or len(test_index) == 0:
                continue
            test_positives = int(dataset.target.iloc[test_index].sum())
            window_days = int((max_date - cutoff).days)
            if test_positives > 0 and window_days >= TEMPORAL_TEST_MIN_DAYS:
                selected_cutoff = cutoff
                selected_strategy = "latest_cutoff_with_any_positive_support"
                break

    if selected_cutoff is None:
        default_position = max(1, int(np.floor(len(unique_dates) * (1 - TEMPORAL_TEST_DATE_SHARE))))
        default_position = min(default_position, len(unique_dates) - 1)
        selected_cutoff = pd.Timestamp(unique_dates[default_position])
        selected_strategy = "fallback_last_20pct_unique_dates"

    train_index, test_index = _indices_for_cutoff(selected_cutoff)
    split_metadata = {
        "split_date": selected_cutoff.strftime("%Y-%m-%d"),
        "strategy": selected_strategy,
        "minimum_test_positives": TEMPORAL_TEST_MIN_POSITIVES,
        "minimum_window_days": TEMPORAL_TEST_MIN_DAYS,
        "test_window_days": int((max_date - selected_cutoff).days),
        "test_unique_dates": int(pd.Index(dates.iloc[test_index]).nunique()),
    }
    return train_index, test_index, split_metadata


def _recent_24_week_temporal_split_indices(
    dataset: PreparedDataset,
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    """
    Restrict data to the most recent RECENT_WINDOW_WEEKS ending at max(order_date), then
    apply a calendar cutoff split inside that window (train strictly before cutoff, test on or after).

    This answers: "If we only trained on the last 24 weeks, how would forward-chronological
    holdout within that window look?" Distinct from global temporal_holdout, which trains on
    all history before a far-past cutoff.
    """
    if DATE_COLUMN not in dataset.meta.columns:
        raise ValueError("Recent-window split requires order_date in the prepared dataset.")

    dates = pd.to_datetime(dataset.meta[DATE_COLUMN], errors="coerce")
    valid_mask = dates.notna().to_numpy()
    if not valid_mask.any():
        raise ValueError("Recent-window split requires at least one valid order_date.")

    anchor = pd.Timestamp(dates[valid_mask].max())
    window_start = anchor - pd.Timedelta(weeks=RECENT_WINDOW_WEEKS)
    in_window = ((dates >= window_start) & (dates <= anchor)).to_numpy()
    window_indices = np.flatnonzero(in_window & valid_mask)
    if len(window_indices) < 2:
        raise ValueError("Recent-window split: fewer than two rows in the 24-week window.")

    window_dates = dates.iloc[window_indices]
    unique_dates = np.sort(np.unique(window_dates.dropna().to_numpy()))
    if len(unique_dates) < 2:
        raise ValueError("Recent-window split requires at least two unique order dates in the window.")

    def _split_at_cutoff(cutoff: pd.Timestamp) -> tuple[np.ndarray, np.ndarray]:
        train_mask = in_window & valid_mask & (dates < cutoff).to_numpy()
        test_mask = in_window & valid_mask & (dates >= cutoff).to_numpy()
        return np.flatnonzero(train_mask), np.flatnonzero(test_mask)

    selected_cutoff: pd.Timestamp | None = None
    selected_strategy = ""

    for raw_cutoff in unique_dates[::-1]:
        cutoff = pd.Timestamp(raw_cutoff)
        if cutoff <= window_start:
            continue
        train_index, test_index = _split_at_cutoff(cutoff)
        if len(train_index) == 0 or len(test_index) == 0:
            continue
        test_positives = int(dataset.target.iloc[test_index].sum())
        test_span_days = int((anchor - cutoff).days)
        if (
            test_positives >= RECENT_WINDOW_MIN_TEST_POSITIVES
            and test_span_days >= RECENT_WINDOW_MIN_TEST_DAYS
        ):
            selected_cutoff = cutoff
            selected_strategy = "recent_window_temporal_with_minimum_positive_support"
            break

    if selected_cutoff is None:
        for raw_cutoff in unique_dates[::-1]:
            cutoff = pd.Timestamp(raw_cutoff)
            if cutoff <= window_start:
                continue
            train_index, test_index = _split_at_cutoff(cutoff)
            if len(train_index) == 0 or len(test_index) == 0:
                continue
            test_positives = int(dataset.target.iloc[test_index].sum())
            test_span_days = int((anchor - cutoff).days)
            if test_positives > 0 and test_span_days >= RECENT_WINDOW_MIN_TEST_DAYS:
                selected_cutoff = cutoff
                selected_strategy = "recent_window_temporal_with_any_positive_support"
                break

    if selected_cutoff is None:
        default_position = max(1, int(np.floor(len(unique_dates) * 0.8)))
        default_position = min(default_position, len(unique_dates) - 1)
        selected_cutoff = pd.Timestamp(unique_dates[default_position])
        selected_strategy = "recent_window_temporal_fallback_20pct_unique_dates"

    train_index, test_index = _split_at_cutoff(selected_cutoff)

    split_metadata: dict[str, Any] = {
        "anchor_date": anchor.strftime("%Y-%m-%d"),
        "window_start_date": window_start.strftime("%Y-%m-%d"),
        "window_weeks": RECENT_WINDOW_WEEKS,
        "split_date": selected_cutoff.strftime("%Y-%m-%d"),
        "strategy": selected_strategy,
        "window_row_count": int(np.count_nonzero(in_window & valid_mask)),
        "minimum_test_positives": RECENT_WINDOW_MIN_TEST_POSITIVES,
        "minimum_test_window_days": RECENT_WINDOW_MIN_TEST_DAYS,
        "test_window_days": int((anchor - selected_cutoff).days),
        "test_unique_dates": int(pd.Index(dates.iloc[test_index]).nunique()),
    }
    return train_index, test_index, split_metadata


def _group_split_indices(dataset: PreparedDataset) -> tuple[np.ndarray, np.ndarray]:
    groups = dataset.meta.get(GROUP_COLUMN)
    if groups is None:
        groups = pd.Series(np.arange(len(dataset.features)), dtype=str)
    else:
        groups = groups.fillna("missing_group").astype(str)

    unique_groups = groups.drop_duplicates().to_numpy()
    if len(unique_groups) < 2:
        raise ValueError("Group split requires at least two unique sales documents.")

    rng = np.random.default_rng(42)
    shuffled_groups = unique_groups.copy()
    rng.shuffle(shuffled_groups)

    test_group_count = max(1, int(np.ceil(len(shuffled_groups) * 0.2)))
    test_group_count = min(test_group_count, len(shuffled_groups) - 1)
    test_groups = set(shuffled_groups[:test_group_count])
    test_mask = groups.isin(test_groups).to_numpy()
    test_index = np.flatnonzero(test_mask)
    train_index = np.flatnonzero(~test_mask)
    return train_index, test_index


def _stratified_random_split_indices(
    dataset: PreparedDataset,
    test_size: float = 0.2,
    random_state: int = 42,
) -> tuple[np.ndarray, np.ndarray]:
    from sklearn.model_selection import train_test_split

    idx = np.arange(len(dataset.target))
    y = dataset.target.to_numpy()
    train_idx, test_idx = train_test_split(
        idx,
        test_size=test_size,
        stratify=y,
        random_state=random_state,
        shuffle=True,
    )
    return train_idx, test_idx


def _stratified_kfold_metrics_by_model(
    dataset: PreparedDataset,
    n_splits: int = 5,
) -> dict[str, Any]:
    from sklearn.model_selection import StratifiedKFold

    y = dataset.target.to_numpy()
    pos = int((y == 1).sum())
    neg = int((y == 0).sum())
    k = int(min(n_splits, pos, neg))
    if k < 2:
        return {
            "available": False,
            "detail": f"Need at least 2 stratified folds; positives={pos}, negatives={neg}.",
        }

    skf = StratifiedKFold(n_splits=k, shuffle=True, random_state=42)
    model_names = list(build_all_v2_binary_classifiers(dataset, dataset.target).keys())
    models_out: dict[str, dict[str, float]] = {}
    for name in model_names:
        f1_scores: list[float] = []
        roc_scores: list[float] = []
        pr_scores: list[float] = []
        for train_index, test_index in skf.split(np.zeros(len(y)), y):
            pipeline = build_all_v2_binary_classifiers(dataset, dataset.target.iloc[train_index])[name]
            metrics, _, _, _ = evaluate_classifier_train_test_split(
                dataset, train_index, test_index, pipeline
            )
            f1_scores.append(metrics["f1"])
            roc_scores.append(metrics["roc_auc"])
            pr_scores.append(metrics["pr_auc"])
        models_out[name] = {
            "folds": float(k),
            "f1_mean": float(np.mean(f1_scores)),
            "f1_std": float(np.std(f1_scores)),
            "roc_auc_mean": float(np.mean(roc_scores)),
            "roc_auc_std": float(np.std(roc_scores)),
            "pr_auc_mean": float(np.mean(pr_scores)),
            "pr_auc_std": float(np.std(pr_scores)),
        }
    return {"available": True, "n_splits": k, "models": models_out}


def _group_kfold_summary(dataset: PreparedDataset, model_name: str = "xgboost") -> dict[str, float]:
    from sklearn.model_selection import GroupKFold

    groups = dataset.meta.get(GROUP_COLUMN)
    if groups is None:
        raise ValueError("Group CV requires sales_document_number in the dataset.")

    groups = groups.fillna("missing_group").astype(str)
    unique_groups = groups.nunique()
    n_splits = min(5, unique_groups)
    if n_splits < 2:
        raise ValueError("Group CV requires at least two unique groups.")
    if model_name not in build_all_v2_binary_classifiers(dataset, dataset.target).keys():
        model_name = "lightgbm"

    gkf = GroupKFold(n_splits=n_splits)
    f1_scores: list[float] = []
    roc_auc_scores: list[float] = []
    pr_auc_scores: list[float] = []

    for train_index, test_index in gkf.split(dataset.features, dataset.target, groups=groups):
        pipeline = build_all_v2_binary_classifiers(dataset, dataset.target.iloc[train_index])[model_name]
        metrics, _, _, _ = evaluate_classifier_train_test_split(
            dataset, train_index, test_index, pipeline
        )
        f1_scores.append(metrics["f1"])
        roc_auc_scores.append(metrics["roc_auc"])
        pr_auc_scores.append(metrics["pr_auc"])

    return {
        "model": model_name,
        "folds": n_splits,
        "f1_mean": float(np.mean(f1_scores)),
        "f1_std": float(np.std(f1_scores)),
        "roc_auc_mean": float(np.mean(roc_auc_scores)),
        "roc_auc_std": float(np.std(roc_auc_scores)),
        "pr_auc_mean": float(np.mean(pr_auc_scores)),
        "pr_auc_std": float(np.std(pr_auc_scores)),
    }


def _feature_signature(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    if not columns:
        return pd.Series(["no_features"] * len(df), index=df.index, dtype="object")
    normalized = df[columns].copy()
    for column in columns:
        if pd.api.types.is_numeric_dtype(normalized[column]):
            normalized[column] = normalized[column].fillna(-999999)
        else:
            normalized[column] = normalized[column].fillna("Missing").astype(str)
    return normalized.astype(str).agg("||".join, axis=1)


def generate_diagnostics(dataset: PreparedDataset, project_root: str | Path | None = None) -> dict[str, Any]:
    paths = _get_paths(project_root)
    modeling = _load_modeling_table(paths)
    validate_order_grain(modeling, ORDERTIME_MODELING_TABLE)

    legacy = _load_legacy_table(paths)
    legacy_columns = [column for column in LEGACY_FEATURE_COLUMNS if column in legacy.columns]
    safe_columns = RAW_NUMERIC_FEATURES + RAW_CATEGORICAL_FEATURES + dataset.missing_indicator_features

    legacy_signatures = _feature_signature(legacy, legacy_columns) if legacy_columns else pd.Series(dtype="object")
    safe_signatures = _feature_signature(dataset.features, safe_columns)

    grain_duplicates = int(modeling.duplicated(subset=ORDER_GRAIN_COLUMNS).sum())
    duplicate_sales_documents = int(modeling.duplicated(subset=[GROUP_COLUMN]).sum())
    duplicate_doc_items = int(modeling.duplicated(subset=[GROUP_COLUMN, "item_number"]).sum())
    business_signature_columns = [
        column
        for column in ["material_number", "customer_number", DATE_COLUMN, "cumulative_order_quantity"]
        if column in modeling.columns
    ]
    business_signature_duplicates = (
        int(modeling.duplicated(subset=business_signature_columns).sum()) if business_signature_columns else 0
    )

    if {"cumulative_order_quantity", "total_quantity_delivered", TARGET_COLUMN}.issubset(legacy.columns):
        for column in ["cumulative_order_quantity", "total_quantity_delivered"]:
            legacy[column] = pd.to_numeric(legacy[column], errors="coerce")
        rule_pred = (
            (legacy["cumulative_order_quantity"].fillna(0) > legacy["total_quantity_delivered"].fillna(0))
            .astype(int)
            .to_numpy()
        )
        score = (
            legacy["cumulative_order_quantity"].fillna(0) - legacy["total_quantity_delivered"].fillna(0)
        ).to_numpy()
        y_legacy = pd.to_numeric(legacy[TARGET_COLUMN], errors="coerce").fillna(0).astype(int)
        legacy_snapshot_rule = compute_classification_metrics(y_legacy, rule_pred, score)
    else:
        legacy_snapshot_rule = compute_classification_metrics(
            dataset.target, np.zeros(len(dataset.target), dtype=int), np.zeros(len(dataset.target))
        )

    diagnostics = {
        "dataset_contract": {
            "modeling_table": MODELING_TABLE_FILE,
            "order_grain": ORDER_GRAIN_COLUMNS,
            "raw_numeric_features": RAW_NUMERIC_FEATURES,
            "raw_categorical_features": RAW_CATEGORICAL_FEATURES,
            "missing_indicator_features": dataset.missing_indicator_features,
            "excluded_leaky_columns": LEAKY_COLUMNS,
        },
        "target_summary": {
            "source_rows": int(len(modeling)),
            "labeled_rows": int(len(dataset.target)),
            "unlabeled_rows": int(len(modeling) - len(dataset.target)),
            "label_coverage": float(len(dataset.target) / len(modeling)) if len(modeling) else 0.0,
            "positive_rate": float(dataset.target.mean()),
        },
        "target_stability": _target_stability_summary(modeling),
        "grain_integrity": {
            "exact_order_grain_duplicates": grain_duplicates,
            "duplicate_sales_document_rows": duplicate_sales_documents,
            "duplicate_sales_document_item_rows": duplicate_doc_items,
            "duplicate_business_signature_rows": business_signature_duplicates,
        },
        "feature_duplication": {
            "legacy_feature_columns": legacy_columns,
            "legacy_duplicate_share": float(legacy_signatures.duplicated().mean()) if len(legacy_signatures) else 0.0,
            "safe_feature_columns": safe_columns,
            "safe_duplicate_share": float(safe_signatures.duplicated().mean()) if len(safe_signatures) else 0.0,
        },
        "legacy_snapshot_rule": legacy_snapshot_rule,
    }

    output_path = paths["models"] / AUC_DIAGNOSTICS_FILE
    output_path.write_text(json.dumps(diagnostics, indent=2))
    return diagnostics


def _target_stability_summary(modeling: pd.DataFrame) -> dict[str, Any]:
    if DATE_COLUMN not in modeling.columns or TARGET_COLUMN not in modeling.columns:
        return {}

    dated = modeling.copy()
    dated[DATE_COLUMN] = pd.to_datetime(dated[DATE_COLUMN], errors="coerce")
    dated[TARGET_COLUMN] = pd.to_numeric(dated[TARGET_COLUMN], errors="coerce")
    if TARGET_OBSERVED_COLUMN in dated.columns:
        dated[TARGET_OBSERVED_COLUMN] = pd.to_numeric(dated[TARGET_OBSERVED_COLUMN], errors="coerce").fillna(0).astype(int)
    else:
        dated[TARGET_OBSERVED_COLUMN] = dated[TARGET_COLUMN].notna().astype(int)
    if TARGET_STATUS_COLUMN in dated.columns:
        dated[TARGET_STATUS_COLUMN] = dated[TARGET_STATUS_COLUMN].fillna("unresolved").astype(str)
    else:
        dated[TARGET_STATUS_COLUMN] = np.where(dated[TARGET_COLUMN].notna(), "resolved", "unresolved")
    dated = dated.dropna(subset=[DATE_COLUMN])
    if dated.empty:
        return {}

    max_date = dated[DATE_COLUMN].max()
    windows: dict[str, Any] = {}
    for days in [90, 180]:
        cutoff = max_date - pd.Timedelta(days=days)
        subset = dated.loc[dated[DATE_COLUMN] >= cutoff]
        labeled_subset = subset.loc[subset[TARGET_COLUMN].notna()]
        windows[f"last_{days}_days"] = {
            "rows": int(len(subset)),
            "labeled_rows": int(len(labeled_subset)),
            "label_coverage": float(len(labeled_subset) / len(subset)) if len(subset) else 0.0,
            "positives": int(labeled_subset[TARGET_COLUMN].sum()) if len(labeled_subset) else 0,
            "positive_rate": float(labeled_subset[TARGET_COLUMN].mean()) if len(labeled_subset) else 0.0,
        }

    monthly = (
        dated.assign(order_month=dated[DATE_COLUMN].dt.to_period("M"))
        .groupby("order_month")
        .agg(
            rows=(TARGET_COLUMN, "size"),
            labeled_rows=(TARGET_OBSERVED_COLUMN, "sum"),
            positives=(TARGET_COLUMN, lambda series: int(pd.to_numeric(series, errors="coerce").fillna(0).sum())),
        )
        .reset_index()
        .tail(24)
    )
    monthly["order_month"] = monthly["order_month"].astype(str)
    monthly["positive_rate"] = np.where(
        monthly["labeled_rows"] > 0,
        monthly["positives"] / monthly["labeled_rows"],
        0.0,
    )
    monthly["label_coverage"] = np.where(monthly["rows"] > 0, monthly["labeled_rows"] / monthly["rows"], 0.0)
    monthly_records = [
        {
            "order_month": row["order_month"],
            "rows": int(row["rows"]),
            "labeled_rows": int(row["labeled_rows"]),
            "label_coverage": float(row["label_coverage"]),
            "positives": int(row["positives"]),
            "positive_rate": float(row["positive_rate"]),
        }
        for _, row in monthly.iterrows()
    ]
    return {
        "latest_date": max_date.strftime("%Y-%m-%d"),
        "recent_windows": windows,
        "monthly_tail": monthly_records,
    }


def _save_temporal_holdout_scores(
    y_true: pd.Series,
    temporal_metrics: dict[str, dict[str, Any]],
    temporal_predictions: dict[str, dict[str, np.ndarray]],
    output_path: Path,
) -> None:
    """Persist test labels and per-model scores for ROC/PR/poster plots (no retrain)."""
    baseline_positive_rate = float(y_true.mean())
    payload: dict[str, Any] = {
        "evaluation_split": "temporal_holdout",
        "test_rows": int(len(y_true)),
        "y_true": [int(x) for x in y_true.tolist()],
        "baseline_positive_rate": baseline_positive_rate,
        "models": {},
    }
    for name, pred in temporal_predictions.items():
        m = temporal_metrics.get(name, {})
        y_proba = np.asarray(pred["y_proba"], dtype=float).ravel()
        y_pred = np.asarray(pred["y_pred"]).ravel()
        payload["models"][name] = {
            "y_proba": [float(x) for x in y_proba],
            "y_pred": [int(x) for x in y_pred],
            "decision_threshold": m.get("decision_threshold"),
            "roc_auc": m.get("roc_auc"),
            "pr_auc": m.get("pr_auc"),
            "f1": m.get("f1"),
            "precision": m.get("precision"),
            "recall": m.get("recall"),
        }
    output_path.write_text(json.dumps(payload, indent=2))


def _plot_confusion_matrices(
    y_true: pd.Series,
    temporal_predictions: dict[str, dict[str, np.ndarray]],
    output_path: Path,
) -> None:
    from sklearn.metrics import confusion_matrix

    plt, sns = _get_plotting_modules()
    n_models = len(temporal_predictions)
    if n_models == 0:
        return
    fig, axes = plt.subplots(1, n_models, figsize=(4.5 * n_models + 1, 4), squeeze=False)
    for ax, (name, predictions) in zip(axes[0], temporal_predictions.items()):
        cm = confusion_matrix(y_true, predictions["y_pred"])
        sns.heatmap(
            cm,
            annot=True,
            fmt="d",
            cmap="Blues",
            ax=ax,
            xticklabels=["No", "Yes"],
            yticklabels=["No", "Yes"],
        )
        ax.set_title(name.replace("_", " ").title())
        ax.set_xlabel("Predicted")
        ax.set_ylabel("Actual")
    plt.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def _feature_importance_frame(model_pipeline: Pipeline) -> pd.DataFrame:
    preprocessor = model_pipeline.named_steps["preprocess"]
    model = model_pipeline.named_steps["model"]
    feature_names = preprocessor.get_feature_names_out()

    if hasattr(model, "feature_importances_"):
        importance = model.feature_importances_
    elif hasattr(model, "coef_"):
        importance = np.abs(model.coef_).ravel()
    else:
        importance = np.zeros(len(feature_names))

    frame = pd.DataFrame({"feature": feature_names, "importance": importance})
    frame = frame.sort_values("importance", ascending=False).head(20).reset_index(drop=True)
    return frame


def _plot_feature_importance(importance_frame: pd.DataFrame, output_path: Path) -> None:
    plt, sns = _get_plotting_modules()
    fig, ax = plt.subplots(figsize=(9, 6))
    sns.barplot(data=importance_frame, x="importance", y="feature", ax=ax, orient="h")
    ax.set_title("Leakage-Safe Order-Time Feature Importance")
    ax.set_xlabel("Importance")
    ax.set_ylabel("Feature")
    plt.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def _save_target_balance(paths: dict[str, Path]) -> None:
    plt, _ = _get_plotting_modules()
    order = _load_modeling_table(paths)
    inv_path = paths["processed"] / "master_inventory_material_with_targets.csv"

    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    backorder_counts = order[TARGET_COLUMN].value_counts(dropna=False).sort_index()
    axes[0].bar(
        ["No", "Yes", "Unresolved"],
        [
            backorder_counts.get(0, 0),
            backorder_counts.get(1, 0),
            int(order[TARGET_COLUMN].isna().sum()),
        ],
        color=["#2ecc71", "#e74c3c", "#95a5a6"],
    )
    axes[0].set_title("Backorder (order-time v2)")
    axes[0].set_ylabel("Count")

    if inv_path.exists():
        inv = pd.read_csv(inv_path, low_memory=False)
        if "target_overstock_risk" in inv.columns:
            overstock_counts = inv["target_overstock_risk"].value_counts().sort_index()
            axes[1].bar(
                ["No (no overstock)", "Yes (overstock)"],
                [overstock_counts.get(0, 0), overstock_counts.get(1, 0)],
                color=["#2ecc71", "#e74c3c"],
            )
            axes[1].set_title("Overstock (material/plant)")
            axes[1].set_ylabel("Count")
        else:
            axes[1].axis("off")
    else:
        axes[1].axis("off")

    plt.tight_layout()
    figure_path = paths["figures"] / TARGET_BALANCE_FIGURE_FILE
    figure_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(figure_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def _save_model_artifacts(dataset: PreparedDataset, paths: dict[str, Path]) -> dict[str, Pipeline]:
    full_pipelines = build_all_v2_binary_classifiers(dataset, dataset.target)
    fitted_full_models: dict[str, Pipeline] = {}
    for name, pipeline in full_pipelines.items():
        pipeline.fit(dataset.features, dataset.target)
        joblib.dump(pipeline, paths["models"] / MODEL_FILE_MAP[name])
        fitted_full_models[name] = pipeline
    return fitted_full_models


def _build_model_comparison_table(results: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    split_order = [
        "temporal_holdout",
        "group_holdout",
        "recent_24_week_temporal_holdout",
    ]
    for split_name in split_order:
        split_block = results.get(split_name) or {}
        models = split_block.get("models") or {}
        if not models:
            continue
        for model_name, metrics in models.items():
            if not isinstance(metrics, dict) or "accuracy" not in metrics:
                continue
            row = {"split": split_name, "model": model_name}
            row.update(metrics)
            rows.append(row)
    return pd.DataFrame(rows).sort_values(["split", "f1", "pr_auc"], ascending=[True, False, False])


def save_regression_side_outputs(project_root: str | Path | None = None) -> dict[str, Any]:
    """Preserve demand/excess outputs as read-only side artifacts for the report."""
    paths = _get_paths(project_root)
    shipment_path = paths["processed"] / "shipment_history.csv"
    woc_path = paths["processed"] / "master_woc.csv"

    regression_metrics: dict[str, Any] = {
        "demand_forecast": {"materials_with_forecast": 0},
        "excess_inventory": {"materials_with_excess": 0, "total_excess_units": 0.0},
    }

    if shipment_path.exists():
        shipment = pd.read_csv(shipment_path, low_memory=False)
        shipment["shipment_week"] = pd.to_datetime(shipment["shipment_week"], errors="coerce")
        shipment["quantity_shipped"] = pd.to_numeric(shipment["quantity_shipped"], errors="coerce").fillna(0)
        shipment = shipment.dropna(subset=["shipment_week"])
        demand_agg = (
            shipment.groupby(["client_id", "material_number", "plant_code", "shipment_week"], dropna=False)[
                "quantity_shipped"
            ]
            .sum()
            .reset_index()
            .sort_values("shipment_week")
        )
        demand_agg["roll_mean"] = (
            demand_agg.groupby(["client_id", "material_number", "plant_code"])["quantity_shipped"]
            .transform(lambda series: series.rolling(12, min_periods=1).mean())
        )
        forecast_df = (
            demand_agg.sort_values("shipment_week")
            .groupby(["client_id", "material_number", "plant_code"], as_index=False)
            .tail(1)[["client_id", "material_number", "plant_code", "roll_mean", "shipment_week"]]
            .rename(columns={"roll_mean": "demand_forecast", "shipment_week": "forecast_as_of"})
        )
        forecast_df.to_csv(paths["tables"] / DEMAND_FORECAST_TABLE_FILE, index=False)
        regression_metrics["demand_forecast"] = {
            "materials_with_forecast": int(forecast_df["material_number"].nunique()),
            "forecast_window_weeks": 12,
        }

    if woc_path.exists():
        woc = pd.read_csv(woc_path, low_memory=False)
        woc["awd"] = pd.to_numeric(woc["awd"], errors="coerce").fillna(0)
        woc["saleable_inventory"] = pd.to_numeric(woc["saleable_inventory"], errors="coerce").fillna(0)
        woc["ideal_inventory"] = woc["awd"] * 8
        woc["excess_units"] = (woc["saleable_inventory"] - woc["ideal_inventory"]).clip(lower=0)
        woc["excess_units"] = np.where(woc["awd"] <= 0, np.nan, woc["excess_units"])
        excess_summary = woc[
            ["client_id", "material_number", "plant_code", "saleable_inventory", "awd", "woc", "excess_units"]
        ].dropna(subset=["excess_units"])
        excess_summary.to_csv(paths["tables"] / EXCESS_INVENTORY_TABLE_FILE, index=False)
        regression_metrics["excess_inventory"] = {
            "materials_with_excess": int((excess_summary["excess_units"] > 0).sum()),
            "total_excess_units": float(excess_summary["excess_units"].sum()),
            "target_woc_weeks": 8,
        }

    (paths["models"] / REGRESSION_METRICS_FILE).write_text(json.dumps(regression_metrics, indent=2))
    return regression_metrics


def run_overfit_evaluation(project_root: str | Path | None = None) -> dict[str, Any]:
    paths = _get_paths(project_root)
    _configure_runtime(paths)
    for output_dir in [paths["models"], paths["figures"], paths["tables"]]:
        output_dir.mkdir(parents=True, exist_ok=True)

    dataset = prepare_backorder_dataset(paths["project_root"])

    temporal_train, temporal_test, temporal_split = _temporal_split_indices(dataset)
    temporal_metrics, temporal_models, temporal_predictions = _evaluate_models(dataset, temporal_train, temporal_test)

    group_train, group_test = _group_split_indices(dataset)
    group_metrics, _, _ = _evaluate_models(dataset, group_train, group_test)

    recent_train, recent_test, recent_split = _recent_24_week_temporal_split_indices(dataset)
    recent_metrics, _, _ = _evaluate_models(dataset, recent_train, recent_test)

    diagnostics = generate_diagnostics(dataset, paths["project_root"])

    temporal_train_positives = int(dataset.target.iloc[temporal_train].sum())
    temporal_test_positives = int(dataset.target.iloc[temporal_test].sum())
    group_train_positives = int(dataset.target.iloc[group_train].sum())
    group_test_positives = int(dataset.target.iloc[group_test].sum())
    recent_train_positives = int(dataset.target.iloc[recent_train].sum())
    recent_test_positives = int(dataset.target.iloc[recent_test].sum())

    results: dict[str, Any] = {
        "dataset_summary": {
            "rows": int(len(dataset.features)),
            "positive_rate": float(dataset.target.mean()),
        },
        "dataset_contract": diagnostics["dataset_contract"],
        "temporal_holdout": {
            "train_rows": int(len(temporal_train)),
            "test_rows": int(len(temporal_test)),
            "train_positives": temporal_train_positives,
            "test_positives": temporal_test_positives,
            "test_positive_rate": float(dataset.target.iloc[temporal_test].mean()),
            **temporal_split,
            "models": temporal_metrics,
        },
        "group_holdout": {
            "train_rows": int(len(group_train)),
            "test_rows": int(len(group_test)),
            "train_positives": group_train_positives,
            "test_positives": group_test_positives,
            "test_positive_rate": float(dataset.target.iloc[group_test].mean()),
            "group_column": GROUP_COLUMN,
            "models": group_metrics,
        },
        "recent_24_week_temporal_holdout": {
            "train_rows": int(len(recent_train)),
            "test_rows": int(len(recent_test)),
            "train_positives": recent_train_positives,
            "test_positives": recent_test_positives,
            "test_positive_rate": float(dataset.target.iloc[recent_test].mean()),
            **recent_split,
            "models": recent_metrics,
        },
        "diagnostics": diagnostics,
    }

    best_model_name = max(
        temporal_metrics,
        key=lambda name: (temporal_metrics[name]["f1"], temporal_metrics[name]["pr_auc"]),
    )
    selection_note = "Primary model selected from the temporal holdout; grouped metrics are secondary diagnostics."
    if temporal_test_positives < TEMPORAL_TEST_MIN_POSITIVES:
        selection_note += " Temporal holdout is sparse, so treat threshold-based metrics cautiously."

    results["selected_model"] = {
        "name": best_model_name,
        "selection_split": "temporal_holdout",
        "selection_basis": selection_note,
        "metrics": temporal_metrics[best_model_name],
    }

    results_path = paths["models"] / OVERFIT_RESULTS_FILE
    results_path.write_text(json.dumps(results, indent=2))

    y_temporal_test = dataset.target.iloc[temporal_test]
    _plot_confusion_matrices(
        y_temporal_test,
        temporal_predictions,
        paths["figures"] / CONFUSION_FIGURE_FILE,
    )
    _save_temporal_holdout_scores(
        y_temporal_test,
        temporal_metrics,
        temporal_predictions,
        paths["models"] / TEMPORAL_HOLDOUT_SCORES_FILE,
    )
    from .poster_figures_v2 import generate_temporal_holdout_poster_figures

    generate_temporal_holdout_poster_figures(paths["project_root"])

    full_models = _save_model_artifacts(dataset, paths)
    selected_pipeline = full_models[best_model_name]
    importance_frame = _feature_importance_frame(selected_pipeline)
    importance_frame.to_csv(paths["tables"] / FEATURE_IMPORTANCE_TABLE_FILE, index=False)
    _plot_feature_importance(importance_frame, paths["figures"] / FEATURE_IMPORTANCE_FIGURE_FILE)

    comparison_table = _build_model_comparison_table(results)
    comparison_table.to_csv(paths["tables"] / MODEL_COMPARISON_TABLE_FILE, index=False)

    classification_metrics = {
        "dataset_summary": results["dataset_summary"],
        "dataset_contract": results["dataset_contract"],
        "selected_model": results["selected_model"],
        "temporal_holdout": results["temporal_holdout"],
        "group_holdout": results["group_holdout"],
        "recent_24_week_temporal_holdout": results["recent_24_week_temporal_holdout"],
        "diagnostics": results["diagnostics"],
    }
    classification_metrics.update(_snapshot_backorder_metrics_addon(paths["project_root"]))
    (paths["models"] / CLASSIFICATION_METRICS_FILE).write_text(json.dumps(classification_metrics, indent=2))

    _save_target_balance(paths)
    save_regression_side_outputs(paths["project_root"])
    return results


def main() -> dict[str, Any]:
    return run_overfit_evaluation()


if __name__ == "__main__":
    main()
