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
    build_training_sample_weights,
    compute_classification_metrics,
    evaluate_classifier_train_test_split,
    extend_sample_weight_after_smote,
    fit_pipeline_maybe_weighted,
    maybe_smote_resample_training,
    select_threshold_with_precision_floor,
    threshold_from_train_temporal_tail,
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
try:
    from westminster_poster_palette import BIRCH, COPPER, FLINT, NIGHT, SKY, THISTLE
    from westminster_poster_palette import brand_confusion_heatmap_cmap
except Exception:  # pragma: no cover - fallback only
    NIGHT = "#211551"
    COPPER = "#9D581F"
    FLINT = "#101820"
    BIRCH = "#F1F1DE"
    THISTLE = "#8252C7"
    SKY = "#00B5E2"
    def brand_confusion_heatmap_cmap():
        return "Blues"

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
    "xgboost": f"backorder_xgboost{ARTIFACT_SUFFIX}.joblib",
    "catboost": f"backorder_catboost{ARTIFACT_SUFFIX}.joblib",
    "random_forest": f"backorder_random_forest{ARTIFACT_SUFFIX}.joblib",
    "knn": f"backorder_knn{ARTIFACT_SUFFIX}.joblib",
    "soft_vote_lr_lightgbm": f"backorder_soft_vote_lr_lightgbm{ARTIFACT_SUFFIX}.joblib",
    "oof_calibrated_stack": f"backorder_oof_calibrated_stack{ARTIFACT_SUFFIX}.joblib",
}

CLASSIFICATION_METRICS_FILE = f"classification_metrics{ARTIFACT_SUFFIX}.json"
OVERFIT_RESULTS_FILE = f"overfit_eval_results{ARTIFACT_SUFFIX}.json"
AUC_DIAGNOSTICS_FILE = f"auc_diagnostics{ARTIFACT_SUFFIX}.json"
TEMPORAL_HOLDOUT_SCORES_FILE = f"temporal_holdout_test_scores{ARTIFACT_SUFFIX}.json"
RECENT_HOLDOUT_SCORES_FILE = f"recent_24_week_test_scores{ARTIFACT_SUFFIX}.json"
REGRESSION_METRICS_FILE = f"regression_metrics{ARTIFACT_SUFFIX}.json"
TARGET_BALANCE_FIGURE_FILE = f"target_balance{ARTIFACT_SUFFIX}.png"
CONFUSION_FIGURE_FILE = f"classification_confusion_matrices{ARTIFACT_SUFFIX}.png"
FEATURE_IMPORTANCE_FIGURE_FILE = f"classification_feature_importance{ARTIFACT_SUFFIX}.png"
FEATURE_IMPORTANCE_TABLE_FILE = f"classification_feature_importance{ARTIFACT_SUFFIX}.csv"
MODEL_COMPARISON_TABLE_FILE = f"classification_model_comparison{ARTIFACT_SUFFIX}.csv"
DEMAND_FORECAST_TABLE_FILE = f"demand_forecast{ARTIFACT_SUFFIX}.csv"
EXCESS_INVENTORY_TABLE_FILE = f"excess_inventory{ARTIFACT_SUFFIX}.csv"
EVIDENCE_SCATTER_FILE = f"evidence_precision_recall_scatter{ARTIFACT_SUFFIX}.png"
EVIDENCE_CI_ERRORBAR_FILE = f"evidence_ci_errorbars{ARTIFACT_SUFFIX}.png"
EVIDENCE_PVALUE_HIST_FILE = f"evidence_pvalue_bootstrap_hist{ARTIFACT_SUFFIX}.png"
EVIDENCE_PR_GAIN_FILE = f"evidence_pr_gain{ARTIFACT_SUFFIX}.png"
EVIDENCE_DECISION_CURVE_FILE = f"evidence_decision_curve{ARTIFACT_SUFFIX}.png"
EVIDENCE_DET_FILE = f"evidence_det_curve{ARTIFACT_SUFFIX}.png"
EVIDENCE_BRIER_FILE = f"evidence_brier_decomposition{ARTIFACT_SUFFIX}.png"
EVIDENCE_CALIBRATION_FILE = f"evidence_calibration_ci{ARTIFACT_SUFFIX}.png"
EVIDENCE_LIFT_GAINS_FILE = f"evidence_lift_gains{ARTIFACT_SUFFIX}.png"
EVIDENCE_KS_FILE = f"evidence_ks_curve{ARTIFACT_SUFFIX}.png"
EVIDENCE_PERM_IMPORTANCE_CI_FILE = f"evidence_permutation_importance_ci{ARTIFACT_SUFFIX}.png"
EVIDENCE_PDP_ICE_FILE = f"evidence_pdp_ice_top3{ARTIFACT_SUFFIX}.png"
EVIDENCE_DRIFT_PERF_FILE = f"evidence_drift_performance_overlay{ARTIFACT_SUFFIX}.png"
EVIDENCE_MODEL_HEATMAP_LIVE_FILE = f"evidence_model_comparison_heatmap_live{ARTIFACT_SUFFIX}.png"
EVIDENCE_TEMPORAL_SNAPSHOT_LIVE_FILE = f"evidence_temporal_snapshot_live{ARTIFACT_SUFFIX}.png"

TEMPORAL_TEST_DATE_SHARE = 0.2
TEMPORAL_TEST_MIN_POSITIVES = 50
TEMPORAL_TEST_MIN_DAYS = 90

# Train/test both restricted to rows with order_date in (anchor - N weeks, anchor].
# Evaluation: temporal sub-split inside that window (earlier dates train, later dates test).
RECENT_WINDOW_WEEKS = 24
RECENT_WINDOW_MIN_TEST_POSITIVES = 10
RECENT_WINDOW_MIN_TEST_DAYS = 14
RECENT_WINDOW_TARGET_MIN_POSITIVES = 40
RECENT_WINDOW_MAX_EXPANSION_DAYS = 270
RECENT_WINDOW_EXPANSION_STEP_WEEKS = 4

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


@dataclass
class SoftVoteBinaryEnsemble:
    """Simple average-probability ensemble over fitted binary estimators."""

    estimators: list[Any]

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        if not self.estimators:
            raise ValueError("SoftVoteBinaryEnsemble requires at least one estimator.")
        probs = [np.asarray(est.predict_proba(X), dtype=float)[:, 1] for est in self.estimators]
        avg = np.mean(np.vstack(probs), axis=0)
        return np.column_stack([1.0 - avg, avg])

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return (self.predict_proba(X)[:, 1] >= 0.5).astype(int)


@dataclass
class OOFCalibratedStackEnsemble:
    """Stack base model probabilities into a calibrated logistic meta-model."""

    estimators: list[tuple[str, Any]]
    meta_model: Any

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        if not self.estimators:
            raise ValueError("OOFCalibratedStackEnsemble requires at least one estimator.")
        base_probs = []
        for _, est in self.estimators:
            p = np.asarray(est.predict_proba(X), dtype=float)[:, 1]
            base_probs.append(p)
        Z = np.column_stack(base_probs)
        p_meta = np.asarray(self.meta_model.predict_proba(Z), dtype=float)[:, 1]
        return np.column_stack([1.0 - p_meta, p_meta])

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return (self.predict_proba(X)[:, 1] >= 0.5).astype(int)


def _stack_oof_probability_matrix(
    dataset: PreparedDataset,
    train_index: np.ndarray,
    base_names: list[str],
    base_templates: dict[str, Any],
) -> tuple[np.ndarray, np.ndarray, int]:
    """
    Forward-time OOF positive-class probabilities for stack base learners.

    Uses sklearn ``TimeSeriesSplit`` over date-sorted training rows: each fold
    trains on strictly-prior data and predicts on the next chronological block.
    Random ``StratifiedKFold`` would leak future rows into earlier-fold training
    because the meta-LR would see base probabilities informed by dates the
    stack could not have known about at inference time.

    Returns
    -------
    oof :
        (n_train, n_bases) probability matrix. Rows in the earliest seed block
        (never used as a test fold) contain NaN — meta-LR must drop them via
        the returned mask.
    mask :
        Boolean array of length n_train; True for rows that received an OOF
        prediction across all base learners.
    n_splits :
        Number of temporal folds actually used.
    """
    if not base_names:
        return np.zeros((0, 0), dtype=float), np.zeros(0, dtype=bool), 0
    X_train = dataset.features.iloc[train_index]
    y_train_arr = dataset.target.iloc[train_index].to_numpy()
    train_dates = pd.to_datetime(dataset.meta.iloc[train_index][DATE_COLUMN], errors="coerce")

    min_count = int(np.bincount(y_train_arr.astype(int), minlength=2).min()) if len(y_train_arr) else 0
    if min_count < 2:
        return (
            np.zeros((len(X_train), len(base_names)), dtype=float),
            np.zeros(len(X_train), dtype=bool),
            0,
        )
    n_splits = max(2, min(5, min_count))

    oof = np.full((len(X_train), len(base_names)), np.nan, dtype=float)
    order = np.argsort(train_dates.to_numpy(), kind="stable")
    from sklearn.model_selection import TimeSeriesSplit

    tss = TimeSeriesSplit(n_splits=n_splits)
    for tr_ord_idx, va_ord_idx in tss.split(order):
        tr_idx = order[tr_ord_idx]
        va_idx = order[va_ord_idx]
        y_tr = y_train_arr[tr_idx]
        # Skip folds whose training block is single-class — class_weight="balanced"
        # cannot calibrate without both classes present.
        if len(np.unique(y_tr)) < 2:
            continue
        X_tr_raw = X_train.iloc[tr_idx]
        fold_weight = build_training_sample_weights(
            y_tr,
            train_dates=train_dates.iloc[tr_idx],
        )
        for j, model_name in enumerate(base_names):
            pipe = clone(base_templates[model_name])
            fit_pipeline_maybe_weighted(pipe, X_tr_raw, y_tr, fold_weight)
            oof[va_idx, j] = pipe.predict_proba(X_train.iloc[va_idx])[:, 1]

    mask = ~np.isnan(oof).any(axis=1)
    return oof, mask, n_splits


def _stack_oof_pr_aucs_per_model(base_names: list[str], oof: np.ndarray, y_arr: np.ndarray) -> dict[str, float]:
    from sklearn.metrics import average_precision_score

    if not base_names or oof.shape[1] != len(base_names):
        return {}
    if len(np.unique(y_arr)) < 2:
        return {name: 0.0 for name in base_names}
    out: dict[str, float] = {}
    # TimeSeriesSplit leaves an initial seed block without OOF predictions; drop those rows.
    row_mask = ~np.isnan(oof).any(axis=1)
    if not row_mask.any():
        return {name: 0.0 for name in base_names}
    y_masked = y_arr[row_mask]
    if len(np.unique(y_masked)) < 2:
        return {name: 0.0 for name in base_names}
    for j, name in enumerate(base_names):
        out[name] = float(average_precision_score(y_masked, oof[row_mask, j]))
    return out


def _prune_stack_base_names(
    base_names: list[str],
    oof: np.ndarray,
    y_arr: np.ndarray,
) -> tuple[list[str], dict[str, float], dict[str, float]]:
    """
    Drop weak stack bases using train OOF PR-AUC only.

    If too few pass the floor, keep the top two by OOF PR-AUC so the stack can still run.
    """
    pr_aucs = _stack_oof_pr_aucs_per_model(base_names, oof, y_arr)
    meta: dict[str, float] = {
        "stack_pruning_enabled": 1.0 if os.environ.get("MODEL_STACK_ENABLE_PRUNING", "1") == "1" else 0.0,
        "stack_prune_min_oof_pr_auc": float(os.environ.get("MODEL_STACK_MIN_OOF_PR_AUC", "0.06")),
        "stack_pruning_fallback_top2": 0.0,
    }
    for name in base_names:
        meta[f"stack_oof_pr_auc__{name}"] = float(pr_aucs.get(name, 0.0))

    enable = os.environ.get("MODEL_STACK_ENABLE_PRUNING", "1") == "1"
    min_pr = float(os.environ.get("MODEL_STACK_MIN_OOF_PR_AUC", "0.06"))
    if not enable or not base_names:
        return list(base_names), pr_aucs, meta

    kept = [n for n in base_names if pr_aucs.get(n, 0.0) >= min_pr]
    if len(kept) >= 2:
        return kept, pr_aucs, meta

    ranked = sorted(base_names, key=lambda n: pr_aucs.get(n, 0.0), reverse=True)
    fallback = ranked[: min(2, len(ranked))]
    meta["stack_pruning_fallback_top2"] = 1.0
    return fallback, pr_aucs, meta


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


def _add_lagged_risk_rate_features(
    features: pd.DataFrame,
    meta: pd.DataFrame,
    target: pd.Series,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Add leakage-safe historical positive-rate features using strictly prior rows.

    Rates are built by sorting rows by ``order_date`` (then stable row order) and using
    cumulative counts shifted by one row within each key. This uses only past outcomes for
    each row and remains valid for temporal evaluation.
    """
    if DATE_COLUMN not in meta.columns:
        return features, []

    work = features.copy()
    work["_order_date_tmp"] = pd.to_datetime(meta[DATE_COLUMN], errors="coerce").fillna(pd.Timestamp.min)
    work["_orig_idx_tmp"] = np.arange(len(work), dtype=int)
    work["_y_tmp"] = target.to_numpy().astype(int)
    # IDs are metadata columns (not leakage targets) and are known at order time.
    work["_material_number_tmp"] = meta.get("material_number", pd.Series(index=meta.index, dtype="object")).fillna("Missing").astype(str)
    work["_customer_number_tmp"] = meta.get("customer_number", pd.Series(index=meta.index, dtype="object")).fillna("Missing").astype(str)
    work["_client_id_tmp"] = meta.get("client_id", pd.Series(index=meta.index, dtype="object")).fillna("Missing").astype(str)

    work = work.sort_values(["_order_date_tmp", "_orig_idx_tmp"]).reset_index(drop=True)

    created: list[str] = []
    key_specs: list[tuple[list[str], str]] = [
        (["_material_number_tmp"], "hist_rate_material"),
        (["_customer_number_tmp"], "hist_rate_customer"),
        (["_client_id_tmp", "_material_number_tmp"], "hist_rate_client_material"),
    ]

    for keys, rate_name in key_specs:
        grp = work.groupby(keys, sort=False)["_y_tmp"]
        prior_count = grp.cumcount()
        prior_pos = grp.cumsum() - work["_y_tmp"]
        # Conservative prior for unseen groups: global positive rate.
        global_prior = float(work["_y_tmp"].mean()) if len(work) else 0.0
        rate = prior_pos / prior_count.replace(0, np.nan)
        work[rate_name] = rate.fillna(global_prior).clip(lower=0.0, upper=1.0)
        cnt_name = f"{rate_name}_count"
        work[cnt_name] = np.log1p(prior_count.astype(float))
        created.extend([rate_name, cnt_name])

    work = work.sort_values("_orig_idx_tmp").reset_index(drop=True)
    drop_cols = [
        "_order_date_tmp",
        "_orig_idx_tmp",
        "_y_tmp",
        "_material_number_tmp",
        "_customer_number_tmp",
        "_client_id_tmp",
    ]
    work = work.drop(columns=drop_cols, errors="ignore")
    return work, created


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

    # Leakage-safe derived risk signals at order time:
    # - confirmation_gap_qty: requested minus confirmed quantity at order creation.
    # - confirmation_fill_ratio: confirmed/requested (clipped to [0, 1.5], neutral 1.0 when missing/zero).
    if {"cumulative_order_quantity", "cumulative_confirmed_quantity"}.issubset(order.columns):
        oq = pd.to_numeric(order["cumulative_order_quantity"], errors="coerce")
        cq = pd.to_numeric(order["cumulative_confirmed_quantity"], errors="coerce")
        order["confirmation_gap_qty"] = (oq - cq).clip(lower=0)
        denom = oq.where(oq > 0)
        ratio = (cq / denom).replace([np.inf, -np.inf], np.nan)
        order["confirmation_fill_ratio"] = ratio.fillna(1.0).clip(lower=0.0, upper=1.5)
    else:
        order["confirmation_gap_qty"] = 0.0
        order["confirmation_fill_ratio"] = 1.0

    derived_numeric_features = ["confirmation_gap_qty", "confirmation_fill_ratio"]

    # Optional signal expansion tuned for rare-positive recall.
    # Uses only values known at order creation (no future outcomes).
    enable_signal_expansion = os.environ.get("MODEL_ENABLE_SIGNAL_EXPANSION", "0").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    if enable_signal_expansion:
        oq = pd.to_numeric(order.get("cumulative_order_quantity"), errors="coerce")
        lead = pd.to_numeric(order.get("requested_lead_time_days"), errors="coerce")
        weekday = pd.to_numeric(order.get("order_weekday"), errors="coerce")
        month = pd.to_numeric(order.get("order_month"), errors="coerce")
        quarter = pd.to_numeric(order.get("order_quarter"), errors="coerce")

        denom = oq.where(oq > 0)
        order["confirmation_gap_ratio"] = (
            pd.to_numeric(order["confirmation_gap_qty"], errors="coerce") / denom
        ).replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(lower=0.0, upper=2.0)
        order["lead_time_gap_interaction"] = np.log1p(order["confirmation_gap_qty"].clip(lower=0)) * np.log1p(
            lead.clip(lower=0).fillna(0.0)
        )
        order["low_fill_high_lead_flag"] = (
            (pd.to_numeric(order["confirmation_fill_ratio"], errors="coerce").fillna(1.0) < 0.85)
            & (lead.fillna(0.0) >= 14.0)
        ).astype(int)
        order["zero_confirmed_qty_flag"] = (pd.to_numeric(order.get("cumulative_confirmed_quantity"), errors="coerce").fillna(0.0) <= 0.0).astype(int)

        # Cyclical encodings help linear models represent seasonality without ordinal distortion.
        order["order_weekday_sin"] = np.sin(2.0 * np.pi * weekday.fillna(0.0) / 7.0)
        order["order_weekday_cos"] = np.cos(2.0 * np.pi * weekday.fillna(0.0) / 7.0)
        # order_month in dataset is month-of-year [1..12]; shift to zero-based cycle.
        order["order_month_sin"] = np.sin(2.0 * np.pi * (month.fillna(1.0) - 1.0) / 12.0)
        order["order_month_cos"] = np.cos(2.0 * np.pi * (month.fillna(1.0) - 1.0) / 12.0)
        # quarter in dataset is [1..4].
        order["order_quarter_sin"] = np.sin(2.0 * np.pi * (quarter.fillna(1.0) - 1.0) / 4.0)
        order["order_quarter_cos"] = np.cos(2.0 * np.pi * (quarter.fillna(1.0) - 1.0) / 4.0)

        derived_numeric_features.extend(
            [
                "confirmation_gap_ratio",
                "lead_time_gap_interaction",
                "low_fill_high_lead_flag",
                "zero_confirmed_qty_flag",
                "order_weekday_sin",
                "order_weekday_cos",
                "order_month_sin",
                "order_month_cos",
                "order_quarter_sin",
                "order_quarter_cos",
            ]
        )

    # Drift-focused recent-signal pack (order-time safe), enabled by default.
    # Targets features called out in temporal audit drift diagnostics.
    enable_drift_signal_pack = os.environ.get("MODEL_ENABLE_DRIFT_SIGNAL_PACK", "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    if enable_drift_signal_pack:
        lead = pd.to_numeric(order.get("requested_lead_time_days"), errors="coerce").fillna(0.0).clip(lower=0.0)
        gap = pd.to_numeric(order.get("confirmation_gap_qty"), errors="coerce").fillna(0.0).clip(lower=0.0)
        net = pd.to_numeric(order.get("net_value"), errors="coerce").fillna(0.0).clip(lower=0.0)
        conf = pd.to_numeric(order.get("cumulative_confirmed_quantity"), errors="coerce").fillna(0.0).clip(lower=0.0)

        # Captures non-linear risk when lead time stretches, while damping outliers.
        order["lead_time_log1p"] = np.log1p(lead)
        # Stress score combines delay and unconfirmed demand size.
        order["lead_gap_stress"] = np.log1p(lead) * np.log1p(gap)
        # Value-at-risk proxy where larger orders with low confirmation are more fragile.
        order["net_unconfirmed_stress"] = np.log1p(net) * np.log1p(gap)
        # Confirmation inefficiency relative to requested lead time.
        order["confirmation_velocity_inverse"] = gap / (1.0 + lead)
        # Fraction of value with no confirmed quantity signal.
        order["zero_confirmed_value_risk"] = ((conf <= 0.0).astype(float) * np.log1p(net))

        derived_numeric_features.extend(
            [
                "lead_time_log1p",
                "lead_gap_stress",
                "net_unconfirmed_stress",
                "confirmation_velocity_inverse",
                "zero_confirmed_value_risk",
            ]
        )
    for column in derived_numeric_features:
        order[column] = pd.to_numeric(order[column], errors="coerce")
    _assert_no_non_finite(order, RAW_NUMERIC_FEATURES, ORDERTIME_MODELING_TABLE)

    for column in RAW_CATEGORICAL_FEATURES:
        order[column] = _clean_categorical_series(order[column])

    target = pd.to_numeric(order[TARGET_COLUMN], errors="coerce")
    mask = target.notna()
    if TARGET_OBSERVED_COLUMN in order.columns:
        observed = pd.to_numeric(order[TARGET_OBSERVED_COLUMN], errors="coerce").fillna(0).astype(int)
        mask &= observed.eq(1)
    if not mask.any():
        raise ValueError("Order-time modeling dataset has no resolved targets after outcome filtering.")

    feature_columns = RAW_NUMERIC_FEATURES + derived_numeric_features + RAW_CATEGORICAL_FEATURES
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

    numeric_features = RAW_NUMERIC_FEATURES + derived_numeric_features + missing_indicator_features
    categorical_features = RAW_CATEGORICAL_FEATURES.copy()
    _assert_no_non_finite(features, numeric_features, "Prepared order-time feature matrix")

    meta_columns = ORDER_GRAIN_COLUMNS + [column for column in ORDERTIME_META_COLUMNS if column in order.columns]
    meta = order.loc[mask, meta_columns].copy()
    if DATE_COLUMN in meta.columns:
        meta[DATE_COLUMN] = pd.to_datetime(meta[DATE_COLUMN], errors="coerce")
    features, history_numeric_features = _add_lagged_risk_rate_features(features, meta, target.loc[mask])

    features = features.reset_index(drop=True)
    meta = meta.reset_index(drop=True)
    target = target.loc[mask].astype(int).reset_index(drop=True)

    numeric_features = numeric_features + history_numeric_features
    _assert_no_non_finite(features, numeric_features, "Prepared order-time feature matrix (with history features)")

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
    *,
    threshold_mode: str = "stratified_oof",
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
    if threshold_mode == "temporal_tail":
        train_dates = dataset.meta.iloc[train_index][DATE_COLUMN]
        threshold, strategy, thresh_meta = threshold_from_train_temporal_tail(
            template,
            X_train,
            y_train,
            train_dates,
            categorical_features=[],
        )
    else:
        threshold, strategy, thresh_meta = threshold_from_train_oof(
            template, X_train, y_train, categorical_features=[]
        )
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
    *,
    threshold_mode: str = "stratified_oof",
) -> dict[str, dict[str, Any]]:
    sklearn_metrics, _, _ = _evaluate_models(
        dataset,
        train_index,
        test_index,
        threshold_mode=threshold_mode,
    )
    rule_metrics = _snapshot_backorder_rule_metrics(dataset, test_index)
    minimal_metrics = _evaluate_osq_si_logistic(
        dataset,
        train_index,
        test_index,
        threshold_mode=threshold_mode,
    )
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

    temporal_models = _merge_snapshot_backorder_models(
        snap_dataset,
        temporal_train,
        temporal_test,
        threshold_mode="temporal_tail",
    )
    group_models = _merge_snapshot_backorder_models(snap_dataset, group_train, group_test)
    recent_models = _merge_snapshot_backorder_models(
        snap_dataset,
        recent_train,
        recent_test,
        threshold_mode="temporal_tail",
    )

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
    *,
    threshold_mode: str = "stratified_oof",
) -> tuple[dict[str, dict[str, Any]], dict[str, Any], dict[str, dict[str, np.ndarray]]]:
    """Run each registered v2 classifier with configurable threshold calibration."""
    y_train = dataset.target.iloc[train_index]

    metrics_by_model: dict[str, dict[str, Any]] = {}
    fitted_models: dict[str, Any] = {}
    predictions: dict[str, dict[str, np.ndarray]] = {}

    for name, pipeline in build_all_v2_binary_classifiers(dataset, y_train).items():
        metrics, y_pred, y_proba, pipeline_final = evaluate_classifier_train_test_split(
            dataset,
            train_index,
            test_index,
            pipeline,
            threshold_mode=threshold_mode,
        )
        metrics_by_model[name] = metrics
        fitted_models[name] = pipeline_final
        predictions[name] = {"y_pred": y_pred, "y_proba": y_proba}

    # Diagnostic soft-voting blend: LR + LightGBM (same split, train-side threshold calibration only).
    if "logistic_regression" in predictions and "lightgbm" in predictions:
        p_lr = np.asarray(predictions["logistic_regression"]["y_proba"], dtype=float)
        p_lgb = np.asarray(predictions["lightgbm"]["y_proba"], dtype=float)
        blend_weight_lr = 0.5
        p_blend = 0.5 * p_lr + 0.5 * p_lgb
        y_test = dataset.target.iloc[test_index].to_numpy()

        threshold = 0.5
        strategy = "fixed_0.5_soft_vote"
        meta: dict[str, Any] = {}

        if threshold_mode == "temporal_tail":
            X_train = dataset.features.iloc[train_index]
            y_train_arr = dataset.target.iloc[train_index].to_numpy()
            dates = pd.to_datetime(dataset.meta.iloc[train_index][DATE_COLUMN], errors="coerce")
            valid = dates.notna().to_numpy()
            if valid.sum() >= 2:
                unique_dates = np.sort(dates.loc[valid].drop_duplicates().to_numpy())
                max_date = pd.Timestamp(unique_dates[-1])

                def _split_at(cutoff: pd.Timestamp) -> tuple[np.ndarray, np.ndarray]:
                    tr_mask = ((dates < cutoff) & dates.notna()).to_numpy()
                    va_mask = (dates >= cutoff).fillna(False).to_numpy()
                    return np.flatnonzero(tr_mask), np.flatnonzero(va_mask)

                chosen_cutoff: pd.Timestamp | None = None
                for raw in unique_dates[::-1]:
                    cutoff = pd.Timestamp(raw)
                    tr_idx, va_idx = _split_at(cutoff)
                    if len(tr_idx) == 0 or len(va_idx) == 0:
                        continue
                    val_pos = int((y_train_arr[va_idx] == 1).sum())
                    val_days = int((max_date - cutoff).days)
                    if val_pos >= 10 and val_days >= 28:
                        chosen_cutoff = cutoff
                        break
                if chosen_cutoff is None:
                    default_pos = max(1, int(np.floor(len(unique_dates) * 0.8)))
                    default_pos = min(default_pos, len(unique_dates) - 1)
                    chosen_cutoff = pd.Timestamp(unique_dates[default_pos])

                tr_idx, va_idx = _split_at(chosen_cutoff)
                used_cutoff = chosen_cutoff
                # adaptive widen for min positive support
                if int((y_train_arr[va_idx] == 1).sum()) < 25:
                    for raw in unique_dates[::-1]:
                        candidate = pd.Timestamp(raw)
                        if candidate > chosen_cutoff:
                            continue
                        tr_c, va_c = _split_at(candidate)
                        if len(tr_c) == 0 or len(va_c) == 0:
                            continue
                        if int((max_date - candidate).days) > 365:
                            continue
                        if int((y_train_arr[va_c] == 1).sum()) >= 25:
                            tr_idx, va_idx = tr_c, va_c
                            used_cutoff = candidate
                            break

                if (
                    len(tr_idx) > 0
                    and len(va_idx) > 0
                    and pd.Series(y_train_arr[tr_idx]).nunique() >= 2
                ):
                    from copy import deepcopy

                    base_models = build_all_v2_binary_classifiers(dataset, dataset.target.iloc[train_index])
                    lr_pipe = deepcopy(base_models["logistic_regression"])
                    lgb_pipe = deepcopy(base_models["lightgbm"])
                    X_sv = X_train.iloc[tr_idx]
                    y_sv = y_train_arr[tr_idx]
                    sample_weight = build_training_sample_weights(
                        y_sv,
                        train_dates=dates.iloc[tr_idx],
                    )
                    n_sv = len(X_sv)
                    X_sv, y_sv = maybe_smote_resample_training(
                        X_sv, y_sv, list(dataset.categorical_features)
                    )
                    sample_weight = extend_sample_weight_after_smote(sample_weight, n_sv, len(X_sv))
                    fit_pipeline_maybe_weighted(lr_pipe, X_sv, y_sv, sample_weight)
                    fit_pipeline_maybe_weighted(lgb_pipe, X_sv, y_sv, sample_weight)
                    p_va_lr = lr_pipe.predict_proba(X_train.iloc[va_idx])[:, 1]
                    p_va_lgb = lgb_pipe.predict_proba(X_train.iloc[va_idx])[:, 1]
                    precision_floor = float(os.environ.get("MODEL_PRECISION_FLOOR", "0.35"))
                    guard = max(1, int(np.ceil((y_train_arr[va_idx] == 1).sum() * 0.5)))
                    p_va = 0.5 * p_va_lr + 0.5 * p_va_lgb
                    threshold, objective = select_threshold_with_precision_floor(
                        y_train_arr[va_idx],
                        p_va,
                        precision_floor=precision_floor,
                        min_predicted_positives=guard,
                    )
                    blend_weight_lr = 0.5
                    p_blend = blend_weight_lr * p_lr + (1.0 - blend_weight_lr) * p_lgb
                    strategy = (
                        "temporal_tail_min_pos_min_days__adaptive_window_for_positive_support__"
                        f"{objective}__soft_vote_lr_lightgbm"
                    )
                    meta = {
                        "threshold_calibration_val_rows": float(len(va_idx)),
                        "threshold_calibration_val_positives": float(int((y_train_arr[va_idx] == 1).sum())),
                        "threshold_calibration_val_window_days": float(int((max_date - used_cutoff).days)),
                        "threshold_calibration_precision_floor": float(precision_floor),
                        "threshold_calibration_guard_min_predicted_positives": float(guard),
                        "blend_weight_lr": float(blend_weight_lr),
                        "blend_weight_lightgbm": float(1.0 - blend_weight_lr),
                    }
                elif len(tr_idx) > 0 and len(va_idx) > 0:
                    threshold = float(os.environ.get("MODEL_SOFT_VOTE_SINGLE_CLASS_FALLBACK_THRESHOLD", "0.35"))
                    strategy = "fixed_soft_vote__fallback_single_class_temporal_tail_train"
                    meta = {
                        "threshold_calibration_single_class_train_fallback": 1.0,
                        "threshold_calibration_single_class_fallback_threshold": float(threshold),
                    }

        y_pred = (p_blend >= threshold).astype(int)
        blend_metrics = compute_classification_metrics(y_test, y_pred, p_blend)
        blend_metrics["smote_enabled"] = 1.0 if os.environ.get("MODEL_ENABLE_SMOTE", "0") == "1" else 0.0
        blend_metrics["decision_threshold"] = float(threshold)
        blend_metrics["threshold_calibration_strategy"] = strategy
        blend_metrics["threshold_calibration_train_rows"] = float(len(train_index))
        blend_metrics["blend_weight_lr"] = float(blend_weight_lr)
        blend_metrics["blend_weight_lightgbm"] = float(1.0 - blend_weight_lr)
        blend_metrics.update(meta)
        metrics_by_model["soft_vote_lr_lightgbm"] = blend_metrics
        predictions["soft_vote_lr_lightgbm"] = {"y_pred": y_pred, "y_proba": p_blend}

    # OOF-calibrated stacking over available base models (strictly train-only OOF for meta fit).
    # Convention: cover distinct model families. LR (linear) + LightGBM (boosted trees) +
    # RandomForest (bagged trees) + kNN (instance-based). XGBoost/CatBoost are excluded — they
    # are highly correlated with LightGBM and do not add independent signal to the meta model.
    stack_raw = [name for name in ["logistic_regression", "lightgbm", "random_forest", "knn"] if name in metrics_by_model]
    if len(stack_raw) >= 2:
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import Pipeline as SkPipeline
        from sklearn.preprocessing import StandardScaler

        X_train = dataset.features.iloc[train_index]
        y_train_arr = dataset.target.iloc[train_index].to_numpy()
        X_test = dataset.features.iloc[test_index]
        y_test = dataset.target.iloc[test_index].to_numpy()
        train_dates = pd.to_datetime(dataset.meta.iloc[train_index][DATE_COLUMN], errors="coerce")

        base_templates = build_all_v2_binary_classifiers(dataset, dataset.target.iloc[train_index])
        oof_full, oof_mask, n_splits = _stack_oof_probability_matrix(dataset, train_index, stack_raw, base_templates)
        if n_splits >= 2 and oof_mask.any():
            stack_candidates, _, prune_meta = _prune_stack_base_names(stack_raw, oof_full, y_train_arr)
            col_ix = [stack_raw.index(n) for n in stack_candidates]
            oof = oof_full[:, col_ix]

            # Meta-LR trains only on rows that received OOF predictions from every base learner.
            # The earliest seed block (never used as a TimeSeriesSplit test fold) is dropped.
            meta_model = SkPipeline(
                steps=[
                    ("scale", StandardScaler()),
                    ("model", LogisticRegression(max_iter=2000, class_weight="balanced", random_state=42)),
                ]
            )
            meta_model.fit(oof[oof_mask], y_train_arr[oof_mask])

            # Fit base models on full train for test-time stack inference. SMOTE removed from
            # this path: OOF fold fits use raw training data, so the final refit must match —
            # otherwise the meta-LR learns a mapping from OOF probabilities that the test-time
            # stack cannot reproduce. Class imbalance is handled at each base learner via
            # class_weight / scale_pos_weight.
            z_test_cols = []
            fitted_estimators: list[tuple[str, Any]] = []
            full_weight = build_training_sample_weights(y_train_arr, train_dates=train_dates)
            for model_name in stack_candidates:
                pipe_full = clone(base_templates[model_name])
                fit_pipeline_maybe_weighted(pipe_full, X_train, y_train_arr, full_weight)
                fitted_estimators.append((model_name, pipe_full))
                z_test_cols.append(pipe_full.predict_proba(X_test)[:, 1])
            z_test = np.column_stack(z_test_cols)
            p_stack = meta_model.predict_proba(z_test)[:, 1]

            # Threshold is picked on train-only OOF-meta probabilities — never on y_test.
            # Using y_test here was a calibration-layer leakage bug that flattered the
            # stack's temporal holdout metrics.
            p_stack_oof = meta_model.predict_proba(oof[oof_mask])[:, 1]
            y_train_masked = y_train_arr[oof_mask]
            precision_floor = float(os.environ.get("MODEL_PRECISION_FLOOR", "0.35"))
            guard = max(1, int(np.ceil((y_train_masked == 1).sum() * 0.5)))
            threshold, objective = select_threshold_with_precision_floor(
                y_train_masked,
                p_stack_oof,
                precision_floor=precision_floor,
                min_predicted_positives=guard,
            )
            y_pred = (p_stack >= threshold).astype(int)
            stack_metrics = compute_classification_metrics(y_test, y_pred, p_stack)
            stack_metrics["smote_enabled"] = 0.0
            stack_metrics["decision_threshold"] = float(threshold)
            stack_metrics["threshold_calibration_strategy"] = f"oof_calibrated_stack__train_oof__{objective}"
            stack_metrics["threshold_calibration_train_rows"] = float(len(train_index))
            stack_metrics["threshold_calibration_oof_folds"] = float(n_splits)
            stack_metrics["stack_base_models_pre_prune"] = ",".join(stack_raw)
            stack_metrics["stack_base_models"] = ",".join(stack_candidates)
            stack_metrics.update(prune_meta)
            stack_metrics["cost_sensitive_weighting"] = (
                1.0 if os.environ.get("MODEL_ENABLE_COST_SENSITIVE_WEIGHTS", "0") == "1" else 0.0
            )
            stack_metrics["focal_proxy_weighting"] = (
                1.0 if os.environ.get("MODEL_ENABLE_FOCAL_WEIGHTING", "0") == "1" else 0.0
            )
            metrics_by_model["oof_calibrated_stack"] = stack_metrics
            predictions["oof_calibrated_stack"] = {"y_pred": y_pred, "y_proba": p_stack}
            fitted_models["oof_calibrated_stack"] = OOFCalibratedStackEnsemble(
                estimators=fitted_estimators,
                meta_model=meta_model,
            )

    return metrics_by_model, fitted_models, predictions


def _select_model_from_temporal_train(
    dataset: PreparedDataset,
    temporal_train_index: np.ndarray,
) -> tuple[str, dict[str, Any]]:
    """
    Pick model architecture using only temporal-train rows via an inner temporal split.

    Prevents selecting LR vs LightGBM on the final temporal holdout test set.
    """
    train_only_dataset = PreparedDataset(
        features=dataset.features.iloc[temporal_train_index].reset_index(drop=True),
        target=dataset.target.iloc[temporal_train_index].reset_index(drop=True),
        meta=dataset.meta.iloc[temporal_train_index].reset_index(drop=True),
        numeric_features=dataset.numeric_features,
        categorical_features=dataset.categorical_features,
        missing_indicator_features=dataset.missing_indicator_features,
        osq_si_label_inputs=None
        if dataset.osq_si_label_inputs is None
        else dataset.osq_si_label_inputs.iloc[temporal_train_index].reset_index(drop=True),
    )
    inner_train, inner_val, inner_split = _temporal_split_indices(train_only_dataset)
    inner_metrics, _, _ = _evaluate_models(
        train_only_dataset,
        inner_train,
        inner_val,
        threshold_mode="temporal_tail",
    )
    best_model_name = max(
        inner_metrics,
        key=lambda name: (
            inner_metrics[name].get("f1", 0.0),
            inner_metrics[name].get("pr_auc", 0.0),
            inner_metrics[name].get("recall", 0.0),
            inner_metrics[name].get("precision", 0.0),
        ),
    )
    return best_model_name, {"inner_temporal_split": inner_split, "inner_metrics": inner_metrics}


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
    target_min_pos = int(os.environ.get("MODEL_MIN_RECENT_HOLDOUT_POSITIVES", str(RECENT_WINDOW_TARGET_MIN_POSITIVES)))
    max_expansion_days = int(
        os.environ.get("MODEL_RECENT_HOLDOUT_MAX_EXPANSION_DAYS", str(RECENT_WINDOW_MAX_EXPANSION_DAYS))
    )
    step_weeks = int(os.environ.get("MODEL_RECENT_HOLDOUT_EXPANSION_STEP_WEEKS", str(RECENT_WINDOW_EXPANSION_STEP_WEEKS)))
    min_train_rows = int(os.environ.get("MODEL_MIN_RECENT_TRAIN_ROWS", "500"))
    min_train_days = int(os.environ.get("MODEL_MIN_RECENT_TRAIN_DAYS", "28"))

    best_result: tuple[np.ndarray, np.ndarray, pd.Timestamp, pd.Timestamp, str] | None = None
    support_pass = False
    expanded = False
    window_weeks = RECENT_WINDOW_WEEKS
    max_weeks = max(RECENT_WINDOW_WEEKS, int(np.ceil(max_expansion_days / 7.0)))

    while window_weeks <= max_weeks:
        window_start = anchor - pd.Timedelta(weeks=window_weeks)
        in_window = ((dates >= window_start) & (dates <= anchor)).to_numpy()
        window_indices = np.flatnonzero(in_window & valid_mask)
        if len(window_indices) < 2:
            window_weeks += step_weeks
            expanded = True
            continue

        window_dates = dates.iloc[window_indices]
        unique_dates = np.sort(np.unique(window_dates.dropna().to_numpy()))
        if len(unique_dates) < 2:
            window_weeks += step_weeks
            expanded = True
            continue

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
            train_days = int((cutoff - window_start).days)
            if len(train_index) < min_train_rows or train_days < min_train_days:
                continue
            test_positives = int(dataset.target.iloc[test_index].sum())
            test_span_days = int((anchor - cutoff).days)
            if test_positives >= target_min_pos and test_span_days >= RECENT_WINDOW_MIN_TEST_DAYS:
                selected_cutoff = cutoff
                selected_strategy = "recent_window_temporal_with_target_positive_support"
                support_pass = True
                break

        if selected_cutoff is None:
            for raw_cutoff in unique_dates[::-1]:
                cutoff = pd.Timestamp(raw_cutoff)
                if cutoff <= window_start:
                    continue
                train_index, test_index = _split_at_cutoff(cutoff)
                if len(train_index) == 0 or len(test_index) == 0:
                    continue
                train_days = int((cutoff - window_start).days)
                if len(train_index) < min_train_rows or train_days < min_train_days:
                    continue
                test_positives = int(dataset.target.iloc[test_index].sum())
                test_span_days = int((anchor - cutoff).days)
                if test_positives >= RECENT_WINDOW_MIN_TEST_POSITIVES and test_span_days >= RECENT_WINDOW_MIN_TEST_DAYS:
                    selected_cutoff = cutoff
                    selected_strategy = "recent_window_temporal_with_minimum_positive_support"
                    break

        if selected_cutoff is None:
            default_position = max(1, int(np.floor(len(unique_dates) * 0.8)))
            default_position = min(default_position, len(unique_dates) - 1)
            selected_cutoff = pd.Timestamp(unique_dates[default_position])
            selected_strategy = "recent_window_temporal_fallback_20pct_unique_dates"

        train_index, test_index = _split_at_cutoff(selected_cutoff)
        best_result = (train_index, test_index, selected_cutoff, window_start, selected_strategy)
        if support_pass:
            break
        window_weeks += step_weeks
        expanded = True

    if best_result is None:
        raise ValueError("Recent-window split could not produce a valid temporal split.")
    train_index, test_index, selected_cutoff, window_start, selected_strategy = best_result

    split_metadata: dict[str, Any] = {
        "anchor_date": anchor.strftime("%Y-%m-%d"),
        "window_start_date": window_start.strftime("%Y-%m-%d"),
        "window_weeks": int((anchor - window_start).days // 7),
        "split_date": selected_cutoff.strftime("%Y-%m-%d"),
        "strategy": selected_strategy,
        "window_expanded": bool(expanded),
        "target_min_test_positives": target_min_pos,
        "target_positive_support_passed": bool(support_pass),
        "window_row_count": int(len(train_index) + len(test_index)),
        "minimum_test_positives": RECENT_WINDOW_MIN_TEST_POSITIVES,
        "minimum_test_window_days": RECENT_WINDOW_MIN_TEST_DAYS,
        "minimum_train_rows": int(min_train_rows),
        "minimum_train_days": int(min_train_days),
        "test_window_days": int((anchor - selected_cutoff).days),
        "test_unique_dates": int(pd.Index(dates.iloc[test_index]).nunique()),
    }
    return train_index, test_index, split_metadata


def _bootstrap_metric_cis(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    *,
    n_boot: int = 500,
    alpha: float = 0.95,
) -> dict[str, dict[str, float]]:
    from sklearn.metrics import f1_score, precision_score, recall_score

    rng = np.random.default_rng(42)
    y_true = np.asarray(y_true).astype(int)
    y_pred = np.asarray(y_pred).astype(int)
    n = len(y_true)
    if n == 0:
        return {}
    prec_vals: list[float] = []
    rec_vals: list[float] = []
    f1_vals: list[float] = []
    for _ in range(n_boot):
        idx = rng.integers(0, n, size=n)
        yt = y_true[idx]
        yp = y_pred[idx]
        prec_vals.append(float(precision_score(yt, yp, zero_division=0)))
        rec_vals.append(float(recall_score(yt, yp, zero_division=0)))
        f1_vals.append(float(f1_score(yt, yp, zero_division=0)))
    lo = (1.0 - alpha) / 2.0
    hi = 1.0 - lo

    def _ci(vals: list[float]) -> dict[str, float]:
        arr = np.asarray(vals, dtype=float)
        return {"low": float(np.quantile(arr, lo)), "high": float(np.quantile(arr, hi))}

    return {"precision": _ci(prec_vals), "recall": _ci(rec_vals), "f1": _ci(f1_vals)}


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


def _enforce_label_maturity_gate(
    diagnostics: dict[str, Any],
    *,
    strict: bool = True,
) -> dict[str, Any]:
    """
    Guardrail against unstable temporal labels.

    Uses target-stability windows produced by diagnostics and enforces minimum recent
    label coverage and positive support thresholds. Strict mode raises, warn mode logs only.
    """
    stability = diagnostics.get("target_stability") or {}
    windows = stability.get("recent_windows") or {}
    w180 = windows.get("last_180_days") or {}
    w90 = windows.get("last_90_days") or {}

    cov180 = float(w180.get("label_coverage", 0.0))
    pos180 = int(w180.get("positives", 0))
    cov90 = float(w90.get("label_coverage", 0.0))
    pos90 = int(w90.get("positives", 0))

    profile = os.environ.get("MODEL_LABEL_MATURITY_PROFILE", "balanced").strip().lower()
    profile_thresholds: dict[str, dict[str, float | int]] = {
        "conservative": {
            "min_cov_180": 0.60,
            "min_pos_180": 60,
            "min_cov_90": 0.40,
            "min_pos_90": 25,
        },
        "balanced": {
            "min_cov_180": 0.50,
            "min_pos_180": 40,
            "min_cov_90": 0.30,
            "min_pos_90": 15,
        },
        "exploratory": {
            "min_cov_180": 0.35,
            "min_pos_180": 25,
            "min_cov_90": 0.20,
            "min_pos_90": 8,
        },
    }
    if profile not in profile_thresholds:
        print(f"[WARN] Unknown MODEL_LABEL_MATURITY_PROFILE='{profile}', using 'balanced'")
        profile = "balanced"
    selected = profile_thresholds[profile]
    min_cov_180 = float(os.environ.get("MODEL_MIN_LABEL_COVERAGE_180D", str(selected["min_cov_180"])))
    min_pos_180 = int(os.environ.get("MODEL_MIN_POSITIVES_180D", str(selected["min_pos_180"])))
    min_cov_90 = float(os.environ.get("MODEL_MIN_LABEL_COVERAGE_90D", str(selected["min_cov_90"])))
    min_pos_90 = int(os.environ.get("MODEL_MIN_POSITIVES_90D", str(selected["min_pos_90"])))

    failures: list[str] = []
    if cov180 < min_cov_180:
        failures.append(f"last_180_days label_coverage {cov180:.3f} < {min_cov_180:.3f}")
    if pos180 < min_pos_180:
        failures.append(f"last_180_days positives {pos180} < {min_pos_180}")
    if cov90 < min_cov_90:
        failures.append(f"last_90_days label_coverage {cov90:.3f} < {min_cov_90:.3f}")
    if pos90 < min_pos_90:
        failures.append(f"last_90_days positives {pos90} < {min_pos_90}")

    gate_payload = {
        "strict_mode": bool(strict),
        "profile": profile,
        "thresholds": {
            "min_cov_180": min_cov_180,
            "min_pos_180": min_pos_180,
            "min_cov_90": min_cov_90,
            "min_pos_90": min_pos_90,
        },
        "observed": {
            "cov180": cov180,
            "pos180": pos180,
            "cov90": cov90,
            "pos90": pos90,
        },
        "passed": len(failures) == 0,
        "failures": failures,
    }

    if failures:
        msg = "Label maturity gate failed: " + "; ".join(failures)
        if strict:
            raise RuntimeError(msg)
        print(f"[WARN] {msg}")
    return gate_payload


def _save_split_scores(
    y_true: pd.Series,
    split_metrics: dict[str, dict[str, Any]],
    split_predictions: dict[str, dict[str, np.ndarray]],
    split_name: str,
    output_path: Path,
) -> None:
    """Persist test labels and per-model scores for ROC/PR/poster plots (no retrain)."""
    baseline_positive_rate = float(y_true.mean())
    payload: dict[str, Any] = {
        "evaluation_split": split_name,
        "test_rows": int(len(y_true)),
        "y_true": [int(x) for x in y_true.tolist()],
        "baseline_positive_rate": baseline_positive_rate,
        "models": {},
    }
    for name, pred in split_predictions.items():
        m = split_metrics.get(name, {})
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


def _build_split_baselines(
    y_true: pd.Series | np.ndarray,
    *,
    train_positive_rate: float,
) -> dict[str, dict[str, float]]:
    """Transparent non-model baselines for imbalanced binary classification."""
    y = np.asarray(y_true).astype(int)
    n = len(y)
    if n == 0:
        return {}

    # Majority baseline: predict no backorder for all rows.
    always_negative_pred = np.zeros(n, dtype=int)
    always_negative_proba = np.zeros(n, dtype=float)
    always_negative_metrics = compute_classification_metrics(y, always_negative_pred, always_negative_proba)
    always_negative_metrics["decision_threshold"] = 0.5
    always_negative_metrics["baseline_note"] = "Predicts negative for every row."

    # Random baseline: Bernoulli predictions at train prevalence, no feature signal.
    rng = np.random.default_rng(42)
    p = float(np.clip(train_positive_rate, 0.0, 1.0))
    random_pred = (rng.random(n) < p).astype(int)
    random_proba = np.full(n, p, dtype=float)
    random_metrics = compute_classification_metrics(y, random_pred, random_proba)
    random_metrics["decision_threshold"] = 0.5
    random_metrics["baseline_note"] = "Random Bernoulli predictions at train positive rate."
    random_metrics["train_positive_rate"] = p

    return {
        "always_negative": always_negative_metrics,
        "prevalence_random": random_metrics,
    }


def _build_model_vs_baseline_lift(
    model_metrics: dict[str, dict[str, Any]],
    baselines: dict[str, dict[str, float]],
    *,
    baseline_name: str = "prevalence_random",
) -> dict[str, dict[str, float]]:
    """Compute per-model metric lift against a named baseline."""
    baseline = baselines.get(baseline_name, {})
    if not baseline:
        return {}
    tracked = ("accuracy", "precision", "recall", "f1", "roc_auc", "pr_auc")
    lifts: dict[str, dict[str, float]] = {}
    for model_name, metrics in model_metrics.items():
        row: dict[str, float] = {"baseline": baseline_name}
        for metric in tracked:
            m_val = metrics.get(metric)
            b_val = baseline.get(metric)
            if isinstance(m_val, (int, float)) and isinstance(b_val, (int, float)):
                row[f"delta_{metric}"] = float(m_val - b_val)
        lifts[model_name] = row
    return lifts


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
            cmap=brand_confusion_heatmap_cmap(),
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


def _feature_importance_frame(model_pipeline: Any) -> pd.DataFrame:
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


def _plot_feature_importance(
    importance_frame: pd.DataFrame, output_path: Path, *, title_suffix: str = ""
) -> None:
    plt, sns = _get_plotting_modules()
    fig, ax = plt.subplots(figsize=(9, 6))
    sns.barplot(data=importance_frame, x="importance", y="feature", ax=ax, orient="h", color=NIGHT)
    base = "Leakage-Safe Order-Time Feature Importance"
    ax.set_title(f"{base}{title_suffix}")
    ax.set_xlabel("Importance")
    ax.set_ylabel("Feature")
    plt.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def _plot_evidence_bundle(
    results: dict[str, Any],
    temporal_models: dict[str, Any],
    temporal_predictions: dict[str, dict[str, np.ndarray]],
    recent_predictions: dict[str, dict[str, np.ndarray]],
    dataset: PreparedDataset,
    temporal_test_index: np.ndarray,
    recent_test_index: np.ndarray,
    paths: dict[str, Path],
) -> None:
    """High-signal validation visuals for dashboard evidence."""
    plt, sns = _get_plotting_modules()
    from sklearn.metrics import (
        det_curve,
        precision_recall_curve,
        roc_curve,
        brier_score_loss,
        f1_score,
    )
    from sklearn.inspection import permutation_importance, PartialDependenceDisplay
    brand = {
        "night": NIGHT,
        "copper": COPPER,
        "flint": FLINT,
        "birch": BIRCH,
        "thistle": THISTLE,
        "sky": SKY,
    }

    # 1) Scatter: precision vs recall, sized by PR-AUC, faceted by split.
    rows: list[dict[str, Any]] = []
    for split_name in ["temporal_holdout", "group_holdout", "recent_24_week_temporal_holdout"]:
        models = (results.get(split_name) or {}).get("models") or {}
        for model_name, m in models.items():
            rows.append(
                {
                    "split": split_name,
                    "model": model_name,
                    "precision": float(m.get("precision", 0.0)),
                    "recall": float(m.get("recall", 0.0)),
                    "f1": float(m.get("f1", 0.0)),
                    "pr_auc": float(m.get("pr_auc", 0.0)),
                }
            )
    if rows:
        df = pd.DataFrame(rows)
        fig, axes = plt.subplots(1, 3, figsize=(16, 4.8), sharex=True, sharey=True)
        split_order = ["temporal_holdout", "group_holdout", "recent_24_week_temporal_holdout"]
        title_map = {
            "temporal_holdout": "Temporal (Primary)",
            "group_holdout": "Group (Diagnostic)",
            "recent_24_week_temporal_holdout": "Recent 24-Week",
        }
        for ax, split_name in zip(axes, split_order):
            sub = df[df["split"] == split_name]
            if len(sub):
                base_palette = {
                    "logistic_regression": brand["night"],
                    "lightgbm": brand["copper"],
                    "soft_vote_lr_lightgbm": brand["thistle"],
                    "xgboost": brand["sky"],
                    "catboost": brand["flint"],
                    "oof_calibrated_stack": brand["night"],
                }
                extras = sorted(m for m in sub["model"].unique() if m not in base_palette)
                extra_colors = [
                    brand["birch"],
                    brand["sky"],
                    brand["thistle"],
                    brand["copper"],
                    brand["night"],
                    brand["flint"],
                ]
                palette = {
                    **base_palette,
                    **{m: extra_colors[i % len(extra_colors)] for i, m in enumerate(extras)},
                }
                sns.scatterplot(
                    data=sub,
                    x="recall",
                    y="precision",
                    size="pr_auc",
                    hue="model",
                    sizes=(80, 320),
                    palette=palette,
                    hue_order=sorted(sub["model"].unique(), key=lambda n: (str(n),)),
                    ax=ax,
                )
            ax.set_title(title_map.get(split_name, split_name))
            ax.set_xlabel("Recall")
            ax.set_ylabel("Precision")
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
            ax.grid(alpha=0.2)
        plt.tight_layout()
        fig.savefig(paths["figures"] / EVIDENCE_SCATTER_FILE, dpi=150, bbox_inches="tight")
        plt.close(fig)
        # Live model-comparison heatmap (current run only)
        piv = df.pivot(index="model", columns="split", values="f1").fillna(0.0)
        fig, ax = plt.subplots(figsize=(8.5, 4.8))
        sns.heatmap(piv, annot=True, fmt=".3f", cmap=brand_confusion_heatmap_cmap(), ax=ax, cbar=True)
        ax.set_title("Live Model Comparison Heatmap (F1)")
        ax.set_xlabel("Split")
        ax.set_ylabel("Model")
        plt.tight_layout()
        fig.savefig(paths["figures"] / EVIDENCE_MODEL_HEATMAP_LIVE_FILE, dpi=150, bbox_inches="tight")
        plt.close(fig)

    # 2) Error bars: 95% CI for precision/recall/F1 on temporal + recent.
    ci = results.get("confidence_intervals") or {}
    for split_key, out_name, title in [
        ("temporal_primary", f"tmp_{EVIDENCE_CI_ERRORBAR_FILE}", "Temporal Primary 95% CIs"),
        ("recent_24_week", f"rcn_{EVIDENCE_CI_ERRORBAR_FILE}", "Recent 24-Week 95% CIs"),
    ]:
        block = ci.get(split_key) or {}
        if not block:
            continue
        metric_rows: list[dict[str, Any]] = []
        split_models = (results.get("temporal_holdout") if split_key == "temporal_primary" else results.get("recent_24_week_temporal_holdout"))
        split_models = (split_models or {}).get("models") or {}
        for model_name, metrics in block.items():
            point_src = split_models.get(model_name, {})
            for metric in ["precision", "recall", "f1"]:
                ci_m = metrics.get(metric, {})
                point = float(point_src.get(metric, 0.0))
                lo = float(ci_m.get("low", point))
                hi = float(ci_m.get("high", point))
                metric_rows.append(
                    {
                        "model": model_name,
                        "metric": metric,
                        "point": point,
                        "low": lo,
                        "high": hi,
                    }
                )
        if metric_rows:
            d = pd.DataFrame(metric_rows)
            fig, ax = plt.subplots(figsize=(11, 5))
            y_positions = np.arange(len(d))
            colors = {"precision": "#4caf50", "recall": "#2196f3", "f1": "#ff9800"}
            for i, row in d.reset_index(drop=True).iterrows():
                ax.errorbar(
                    x=row["point"],
                    y=i,
                    xerr=[[max(0.0, row["point"] - row["low"])], [max(0.0, row["high"] - row["point"])]],
                    fmt="o",
                    color=colors.get(row["metric"], "#9e9e9e"),
                    ecolor=colors.get(row["metric"], "#9e9e9e"),
                    capsize=3,
                )
            ax.set_yticks(y_positions)
            ax.set_yticklabels([f"{r.model} | {r.metric}" for r in d.itertuples()])
            ax.set_xlim(0, 1)
            ax.set_xlabel("Metric value")
            ax.set_title(title)
            ax.grid(axis="x", alpha=0.25)
            plt.tight_layout()
            fig.savefig(paths["figures"] / out_name, dpi=150, bbox_inches="tight")
            plt.close(fig)

    # Merge temporal/recent CI panels into one stacked figure for dashboard simplicity.
    tmp_ci = paths["figures"] / f"tmp_{EVIDENCE_CI_ERRORBAR_FILE}"
    rcn_ci = paths["figures"] / f"rcn_{EVIDENCE_CI_ERRORBAR_FILE}"
    if tmp_ci.exists() or rcn_ci.exists():
        fig, axes = plt.subplots(2, 1, figsize=(12, 10))
        for ax, pth, ttl in [
            (axes[0], tmp_ci, "Temporal Primary 95% CIs"),
            (axes[1], rcn_ci, "Recent 24-Week 95% CIs"),
        ]:
            ax.axis("off")
            if pth.exists():
                img = plt.imread(pth)
                ax.imshow(img)
                ax.set_title(ttl)
            else:
                ax.text(0.5, 0.5, f"{ttl}: no data", ha="center", va="center")
        plt.tight_layout()
        fig.savefig(paths["figures"] / EVIDENCE_CI_ERRORBAR_FILE, dpi=150, bbox_inches="tight")
        plt.close(fig)
    for pth in [tmp_ci, rcn_ci]:
        if pth.exists():
            pth.unlink()

    # 3) Bootstrap significance-style plot: F1 lift distribution vs prevalence-random on temporal.
    y_true = dataset.target.iloc[temporal_test_index].to_numpy()
    model_name = results.get("selected_model", {}).get("name", "logistic_regression")
    if model_name in temporal_predictions and len(y_true):
        y_pred_model = np.asarray(temporal_predictions[model_name]["y_pred"]).astype(int)
        y_proba_model = np.asarray(temporal_predictions[model_name]["y_proba"]).astype(float)
        p_random = float(results.get("temporal_holdout", {}).get("train_positives", 0)) / max(
            1.0, float(results.get("temporal_holdout", {}).get("train_rows", 1))
        )
        rng = np.random.default_rng(42)
        y_pred_rand = (rng.random(len(y_true)) < p_random).astype(int)
        n_boot = int(os.environ.get("MODEL_BOOTSTRAP_SAMPLES", "500"))
        lifts: list[float] = []
        for _ in range(n_boot):
            idx = rng.integers(0, len(y_true), size=len(y_true))
            lifts.append(
                float(f1_score(y_true[idx], y_pred_model[idx], zero_division=0))
                - float(f1_score(y_true[idx], y_pred_rand[idx], zero_division=0))
            )
        p_value = float(np.mean(np.asarray(lifts) <= 0.0))
        fig, ax = plt.subplots(figsize=(9, 4.8))
        sns.histplot(lifts, bins=30, kde=True, ax=ax, color="#64b5f6")
        sns.histplot(lifts, bins=30, kde=True, ax=ax, color=brand["sky"])
        ax.axvline(0.0, color=brand["copper"], linestyle="--", linewidth=1.5)
        ax.set_title(f"Bootstrap F1 Lift vs Prevalence-Random (p~{p_value:.4f})")
        ax.set_xlabel("F1 lift (model - prevalence_random)")
        ax.set_ylabel("Bootstrap count")
        ax.grid(alpha=0.2)
        plt.tight_layout()
        fig.savefig(paths["figures"] / EVIDENCE_PVALUE_HIST_FILE, dpi=150, bbox_inches="tight")
        plt.close(fig)

        # 4) PR-Gain style transform (relative improvement over prevalence).
        prevalence = float(np.mean(y_true))
        prec, rec, _ = precision_recall_curve(y_true, y_proba_model)
        with np.errstate(divide="ignore", invalid="ignore"):
            pr_gain = (prec - prevalence) / np.clip(1.0 - prevalence, 1e-12, None)
        pr_gain = np.nan_to_num(pr_gain, nan=0.0, posinf=0.0, neginf=0.0)
        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.plot(rec, pr_gain, color=brand["sky"], linewidth=2)
        ax.axhline(0.0, color=brand["copper"], linestyle="--", linewidth=1)
        ax.set_xlim(0, 1)
        ax.set_xlabel("Recall")
        ax.set_ylabel("PR-Gain")
        ax.set_title("PR-Gain Curve (Selected Model, Temporal)")
        ax.grid(alpha=0.2)
        plt.tight_layout()
        fig.savefig(paths["figures"] / EVIDENCE_PR_GAIN_FILE, dpi=150, bbox_inches="tight")
        plt.close(fig)

        # 5) Decision curve analysis (net benefit vs threshold).
        thresholds = np.linspace(0.05, 0.95, 19)
        nb_model = []
        nb_all = []
        n = len(y_true)
        for pt in thresholds:
            yhat = (y_proba_model >= pt).astype(int)
            tp = float(np.sum((yhat == 1) & (y_true == 1)))
            fp = float(np.sum((yhat == 1) & (y_true == 0)))
            w = pt / max(1e-12, (1.0 - pt))
            nb_model.append((tp / n) - (fp / n) * w)
            nb_all.append(prevalence - (1.0 - prevalence) * w)
        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.plot(thresholds, nb_model, label="Model", color=brand["night"], linewidth=2)
        ax.plot(thresholds, nb_all, label="Treat All", color=brand["copper"], linewidth=1.5)
        ax.plot(thresholds, np.zeros_like(thresholds), label="Treat None", color=brand["flint"], linewidth=1.5)
        ax.set_xlabel("Threshold probability")
        ax.set_ylabel("Net benefit")
        ax.set_title("Decision Curve Analysis (Temporal)")
        ax.legend(loc="best", fontsize=8)
        ax.grid(alpha=0.2)
        plt.tight_layout()
        fig.savefig(paths["figures"] / EVIDENCE_DECISION_CURVE_FILE, dpi=150, bbox_inches="tight")
        plt.close(fig)

        # 6) DET curve.
        fpr, fnr, _ = det_curve(y_true, y_proba_model)
        fig, ax = plt.subplots(figsize=(7.5, 4.5))
        ax.plot(fpr, fnr, color=brand["thistle"], linewidth=2)
        ax.set_xlabel("False Positive Rate")
        ax.set_ylabel("False Negative Rate")
        ax.set_title("DET Curve (Temporal)")
        ax.grid(alpha=0.2)
        plt.tight_layout()
        fig.savefig(paths["figures"] / EVIDENCE_DET_FILE, dpi=150, bbox_inches="tight")
        plt.close(fig)

        # 7) Brier decomposition-like panel (overall + by deciles).
        bins = pd.qcut(pd.Series(y_proba_model), q=min(10, max(2, len(np.unique(y_proba_model)))), duplicates="drop")
        calib = (
            pd.DataFrame({"y": y_true, "p": y_proba_model, "bin": bins})
            .groupby("bin")
            .agg(obs=("y", "mean"), pred=("p", "mean"), n=("y", "size"))
            .reset_index(drop=True)
        )
        brier = float(brier_score_loss(y_true, y_proba_model))
        fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
        axes[0].bar(np.arange(len(calib)), calib["obs"], alpha=0.7, label="Observed")
        axes[0].plot(np.arange(len(calib)), calib["pred"], color=brand["night"], marker="o", label="Predicted")
        axes[0].set_title("Calibration by Probability Decile")
        axes[0].set_xlabel("Decile index")
        axes[0].set_ylabel("Rate")
        axes[0].legend(fontsize=8)
        axes[0].grid(alpha=0.2)
        axes[1].bar(["Brier Score"], [brier], color=brand["sky"])
        axes[1].set_ylim(0, max(0.01, brier * 1.6))
        axes[1].set_title("Brier Score")
        axes[1].grid(axis="y", alpha=0.2)
        plt.tight_layout()
        fig.savefig(paths["figures"] / EVIDENCE_BRIER_FILE, dpi=150, bbox_inches="tight")
        plt.close(fig)

        # 8) Calibration plot with simple binomial CI bands.
        fig, ax = plt.subplots(figsize=(7.5, 4.5))
        pred = calib["pred"].to_numpy()
        obs = calib["obs"].to_numpy()
        n_bin = calib["n"].to_numpy(dtype=float)
        se = np.sqrt(np.clip(obs * (1.0 - obs) / np.clip(n_bin, 1.0, None), 0.0, None))
        lo = np.clip(obs - 1.96 * se, 0, 1)
        hi = np.clip(obs + 1.96 * se, 0, 1)
        ax.plot([0, 1], [0, 1], linestyle="--", color=brand["flint"], linewidth=1)
        ax.errorbar(pred, obs, yerr=[obs - lo, hi - obs], fmt="o", color=brand["sky"], ecolor=brand["night"], capsize=3)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_xlabel("Predicted probability")
        ax.set_ylabel("Observed backorder rate")
        ax.set_title("Calibration with 95% Bin CI (Temporal)")
        ax.grid(alpha=0.2)
        plt.tight_layout()
        fig.savefig(paths["figures"] / EVIDENCE_CALIBRATION_FILE, dpi=150, bbox_inches="tight")
        plt.close(fig)

        # 9) Lift / cumulative gains.
        order = np.argsort(-y_proba_model)
        y_sorted = y_true[order]
        cum_pos = np.cumsum(y_sorted)
        x = np.arange(1, len(y_sorted) + 1) / len(y_sorted)
        gains = cum_pos / max(1, int(np.sum(y_true)))
        random_line = x
        lift = np.divide(gains, x, out=np.zeros_like(gains), where=x > 0)
        fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
        axes[0].plot(x, gains, color=brand["night"], label="Model")
        axes[0].plot(x, random_line, color=brand["flint"], linestyle="--", label="Random")
        axes[0].set_title("Cumulative Gains (Temporal)")
        axes[0].set_xlabel("Population fraction")
        axes[0].set_ylabel("Captured positives fraction")
        axes[0].legend(fontsize=8)
        axes[0].grid(alpha=0.2)
        axes[1].plot(x, lift, color=brand["sky"])
        axes[1].axhline(1.0, color=brand["copper"], linestyle="--")
        axes[1].set_title("Lift Curve (Temporal)")
        axes[1].set_xlabel("Population fraction")
        axes[1].set_ylabel("Lift")
        axes[1].grid(alpha=0.2)
        plt.tight_layout()
        fig.savefig(paths["figures"] / EVIDENCE_LIFT_GAINS_FILE, dpi=150, bbox_inches="tight")
        plt.close(fig)

        # 10) KS curve.
        fpr_roc, tpr_roc, thr = roc_curve(y_true, y_proba_model)
        ks = tpr_roc - fpr_roc
        ks_idx = int(np.argmax(ks))
        fig, ax = plt.subplots(figsize=(7.5, 4.5))
        ax.plot(thr, tpr_roc, label="TPR", color=brand["night"])
        ax.plot(thr, fpr_roc, label="FPR", color=brand["copper"])
        ax.axvline(thr[ks_idx], color=brand["thistle"], linestyle="--", label=f"KS max={ks[ks_idx]:.3f}")
        ax.set_xlabel("Threshold")
        ax.set_ylabel("Rate")
        ax.set_title("KS Curve (Temporal)")
        ax.legend(fontsize=8)
        ax.grid(alpha=0.2)
        plt.tight_layout()
        fig.savefig(paths["figures"] / EVIDENCE_KS_FILE, dpi=150, bbox_inches="tight")
        plt.close(fig)

        # 11) Permutation importance with bootstrap-like repeats.
        try:
            selected_name = results.get("selected_model", {}).get("name", "logistic_regression")
            pipe = temporal_models.get(selected_name)
            if isinstance(pipe, SoftVoteBinaryEnsemble):
                pipe = temporal_models.get("lightgbm") or temporal_models.get("logistic_regression")
            if pipe is not None:
                X_eval = dataset.features.iloc[temporal_test_index]
                y_eval = dataset.target.iloc[temporal_test_index]
                pi = permutation_importance(
                    pipe,
                    X_eval,
                    y_eval,
                    scoring="average_precision",
                    n_repeats=8,
                    random_state=42,
                    n_jobs=1,
                )
                importances = pd.DataFrame(
                    {
                        "feature": np.asarray(dataset.features.columns),
                        "mean": pi.importances_mean,
                        "std": pi.importances_std,
                    }
                ).sort_values("mean", ascending=False).head(15)
                fig, ax = plt.subplots(figsize=(9, 5.5))
                ax.barh(
                    importances["feature"][::-1],
                    importances["mean"][::-1],
                    xerr=1.96 * importances["std"][::-1],
                    color=brand["thistle"],
                    alpha=0.85,
                )
                ax.set_title("Permutation Importance (AP) with ~95% error bars")
                ax.set_xlabel("Mean importance")
                ax.grid(axis="x", alpha=0.2)
                plt.tight_layout()
                fig.savefig(paths["figures"] / EVIDENCE_PERM_IMPORTANCE_CI_FILE, dpi=150, bbox_inches="tight")
                plt.close(fig)
        except Exception:
            pass

        # 12) PDP/ICE for top 3 numeric features (best-effort).
        try:
            selected_name = results.get("selected_model", {}).get("name", "logistic_regression")
            pipe = temporal_models.get(selected_name)
            if isinstance(pipe, SoftVoteBinaryEnsemble):
                pipe = temporal_models.get("lightgbm") or temporal_models.get("logistic_regression")
            if pipe is not None:
                top3 = dataset.numeric_features[:3] if len(dataset.numeric_features) >= 3 else dataset.numeric_features
                if top3:
                    X_eval = dataset.features.iloc[temporal_test_index]
                    fig, ax = plt.subplots(len(top3), 1, figsize=(8, 3.2 * len(top3)))
                    if len(top3) == 1:
                        ax = [ax]
                    for i, feat in enumerate(top3):
                        PartialDependenceDisplay.from_estimator(
                            pipe,
                            X_eval,
                            [feat],
                            kind="both",
                            ax=ax[i],
                        )
                        ax[i].set_title(f"PDP + ICE: {feat}")
                    plt.tight_layout()
                    fig.savefig(paths["figures"] / EVIDENCE_PDP_ICE_FILE, dpi=150, bbox_inches="tight")
                    plt.close(fig)
        except Exception:
            pass

        # 13) Drift-performance overlay over monthly timeline.
        try:
            stability = (results.get("diagnostics") or {}).get("target_stability", {})
            monthly = stability.get("monthly_tail", [])
            if monthly:
                dfm = pd.DataFrame(monthly)
                dfm["order_month"] = pd.to_datetime(dfm["order_month"], errors="coerce")
                dfm = dfm.dropna(subset=["order_month"]).sort_values("order_month")
                # Approx monthly F1 from temporal selected-model predictions.
                meta_test = dataset.meta.iloc[temporal_test_index].copy()
                meta_test["order_month"] = pd.to_datetime(meta_test[DATE_COLUMN], errors="coerce").dt.to_period("M").dt.to_timestamp()
                pred = np.asarray(temporal_predictions[model_name]["y_pred"]).astype(int)
                truth = dataset.target.iloc[temporal_test_index].to_numpy()
                perf_rows: list[dict[str, Any]] = []
                for mth, g in meta_test.groupby("order_month"):
                    idx = g.index.to_numpy()
                    # map absolute indices in temporal_test_index
                    rel = np.nonzero(np.isin(temporal_test_index, idx))[0]
                    if len(rel) == 0:
                        continue
                    perf_rows.append({"order_month": mth, "f1": float(f1_score(truth[rel], pred[rel], zero_division=0))})
                dperf = pd.DataFrame(perf_rows)
                fig, ax1 = plt.subplots(figsize=(10, 4.8))
                ax1.plot(dfm["order_month"], dfm["label_coverage"], color=brand["sky"], label="Label coverage")
                ax1.plot(dfm["order_month"], dfm["positive_rate"], color=brand["thistle"], label="Positive rate")
                ax1.set_ylabel("Coverage / Positive rate")
                ax1.set_xlabel("Month")
                ax1.grid(alpha=0.2)
                ax2 = ax1.twinx()
                if len(dperf):
                    ax2.plot(dperf["order_month"], dperf["f1"], color=brand["copper"], label="Monthly F1 (temporal test)")
                ax2.set_ylabel("F1")
                lines1, labels1 = ax1.get_legend_handles_labels()
                lines2, labels2 = ax2.get_legend_handles_labels()
                ax1.legend(lines1 + lines2, labels1 + labels2, loc="best", fontsize=8)
                ax1.set_title("Drift / Label Stability vs Performance Overlay")
                plt.tight_layout()
                fig.savefig(paths["figures"] / EVIDENCE_DRIFT_PERF_FILE, dpi=150, bbox_inches="tight")
                plt.close(fig)
        except Exception:
            pass

        # 14) Live temporal snapshot card chart (selected model).
        try:
            m = results.get("temporal_holdout", {}).get("models", {}).get(model_name, {})
            vals = [float(m.get("precision", 0.0)), float(m.get("recall", 0.0)), float(m.get("f1", 0.0)), float(m.get("pr_auc", 0.0))]
            labels = ["Precision", "Recall", "F1", "PR-AUC"]
            fig, ax = plt.subplots(figsize=(8, 4.5))
            bars = ax.bar(labels, vals, color=[brand["night"], brand["copper"], brand["thistle"], brand["sky"]])
            ax.set_ylim(0, 1)
            ax.set_title(f"Live Temporal Snapshot ({model_name})")
            ax.set_ylabel("Score")
            ax.grid(axis="y", alpha=0.2)
            for b, v in zip(bars, vals):
                ax.text(b.get_x() + b.get_width() / 2, v + 0.015, f"{v:.3f}", ha="center", va="bottom", fontsize=9)
            plt.tight_layout()
            fig.savefig(paths["figures"] / EVIDENCE_TEMPORAL_SNAPSHOT_LIVE_FILE, dpi=150, bbox_inches="tight")
            plt.close(fig)
        except Exception:
            pass


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
        color=[NIGHT, COPPER, THISTLE],
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
                color=[BIRCH, SKY],
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


def _save_model_artifacts(dataset: PreparedDataset, paths: dict[str, Path]) -> dict[str, Any]:
    full_pipelines = build_all_v2_binary_classifiers(dataset, dataset.target)
    fitted_full_models: dict[str, Any] = {}
    dates = pd.to_datetime(dataset.meta.get(DATE_COLUMN), errors="coerce")
    sample_weight = build_training_sample_weights(dataset.target, train_dates=dates)
    n0 = len(dataset.features)
    X_fit, y_fit = maybe_smote_resample_training(
        dataset.features,
        dataset.target.to_numpy(),
        list(dataset.categorical_features),
    )
    sample_weight = extend_sample_weight_after_smote(sample_weight, n0, len(X_fit))
    for name, pipeline in full_pipelines.items():
        fit_pipeline_maybe_weighted(pipeline, X_fit, y_fit, sample_weight)
        joblib.dump(pipeline, paths["models"] / MODEL_FILE_MAP[name])
        fitted_full_models[name] = pipeline
    if "logistic_regression" in fitted_full_models and "lightgbm" in fitted_full_models:
        ensemble = SoftVoteBinaryEnsemble(
            estimators=[fitted_full_models["logistic_regression"], fitted_full_models["lightgbm"]]
        )
        joblib.dump(ensemble, paths["models"] / MODEL_FILE_MAP["soft_vote_lr_lightgbm"])
        fitted_full_models["soft_vote_lr_lightgbm"] = ensemble
    stack_base_names = [
        name for name in ["logistic_regression", "lightgbm", "random_forest", "knn"] if name in fitted_full_models
    ]
    if len(stack_base_names) >= 2:
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import Pipeline as SkPipeline
        from sklearn.preprocessing import StandardScaler

        full_idx = np.arange(len(dataset.target), dtype=int)
        base_templates = build_all_v2_binary_classifiers(dataset, dataset.target)
        oof_full, _oof_mask_overfit, n_splits = _stack_oof_probability_matrix(
            dataset, full_idx, stack_base_names, base_templates
        )
        y_train = dataset.target.to_numpy().astype(int)
        if n_splits < 2:
            pruned_stack_names = list(stack_base_names)
        else:
            pruned_stack_names, _, _ = _prune_stack_base_names(stack_base_names, oof_full, y_train)

        z_train = np.column_stack(
            [
                np.asarray(fitted_full_models[name].predict_proba(dataset.features), dtype=float)[:, 1]
                for name in pruned_stack_names
            ]
        )
        meta = SkPipeline(
            steps=[
                ("scale", StandardScaler()),
                ("model", LogisticRegression(max_iter=2000, class_weight="balanced", random_state=42)),
            ]
        )
        meta.fit(z_train, y_train)
        stack_ensemble = OOFCalibratedStackEnsemble(
            estimators=[(name, fitted_full_models[name]) for name in pruned_stack_names],
            meta_model=meta,
        )
        joblib.dump(stack_ensemble, paths["models"] / MODEL_FILE_MAP["oof_calibrated_stack"])
        fitted_full_models["oof_calibrated_stack"] = stack_ensemble
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
    temporal_metrics, temporal_models, temporal_predictions = _evaluate_models(
        dataset,
        temporal_train,
        temporal_test,
        threshold_mode="temporal_tail",
    )

    group_train, group_test = _group_split_indices(dataset)
    group_metrics, _, _ = _evaluate_models(dataset, group_train, group_test)

    recent_train, recent_test, recent_split = _recent_24_week_temporal_split_indices(dataset)
    recent_metrics, _, recent_predictions = _evaluate_models(
        dataset,
        recent_train,
        recent_test,
        threshold_mode="temporal_tail",
    )

    diagnostics = generate_diagnostics(dataset, paths["project_root"])
    strict_env = os.environ.get("MODEL_LABEL_MATURITY_GATE_STRICT")
    if strict_env is None:
        gate_strict = bool(os.environ.get("CI"))
    else:
        gate_strict = strict_env == "1"
    gate_result = _enforce_label_maturity_gate(diagnostics, strict=gate_strict)
    diagnostics["label_maturity_gate"] = gate_result

    temporal_train_positives = int(dataset.target.iloc[temporal_train].sum())
    temporal_test_positives = int(dataset.target.iloc[temporal_test].sum())
    group_train_positives = int(dataset.target.iloc[group_train].sum())
    group_test_positives = int(dataset.target.iloc[group_test].sum())
    recent_train_positives = int(dataset.target.iloc[recent_train].sum())
    recent_test_positives = int(dataset.target.iloc[recent_test].sum())
    temporal_baselines = _build_split_baselines(
        dataset.target.iloc[temporal_test],
        train_positive_rate=float(dataset.target.iloc[temporal_train].mean()),
    )
    group_baselines = _build_split_baselines(
        dataset.target.iloc[group_test],
        train_positive_rate=float(dataset.target.iloc[group_train].mean()),
    )
    recent_baselines = _build_split_baselines(
        dataset.target.iloc[recent_test],
        train_positive_rate=float(dataset.target.iloc[recent_train].mean()),
    )

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
            "baselines": temporal_baselines,
            "model_vs_baseline_lift": _build_model_vs_baseline_lift(temporal_metrics, temporal_baselines),
            "models": temporal_metrics,
        },
        "group_holdout": {
            "train_rows": int(len(group_train)),
            "test_rows": int(len(group_test)),
            "train_positives": group_train_positives,
            "test_positives": group_test_positives,
            "test_positive_rate": float(dataset.target.iloc[group_test].mean()),
            "group_column": GROUP_COLUMN,
            "baselines": group_baselines,
            "model_vs_baseline_lift": _build_model_vs_baseline_lift(group_metrics, group_baselines),
            "models": group_metrics,
        },
        "recent_24_week_temporal_holdout": {
            "train_rows": int(len(recent_train)),
            "test_rows": int(len(recent_test)),
            "train_positives": recent_train_positives,
            "test_positives": recent_test_positives,
            "test_positive_rate": float(dataset.target.iloc[recent_test].mean()),
            **recent_split,
            "baselines": recent_baselines,
            "model_vs_baseline_lift": _build_model_vs_baseline_lift(recent_metrics, recent_baselines),
            "models": recent_metrics,
        },
        "diagnostics": diagnostics,
    }

    best_model_name, selection_detail = _select_model_from_temporal_train(dataset, temporal_train)
    selection_note = (
        "Primary model selected from inner temporal split on temporal-train rows only; "
        "temporal holdout test remains final evaluation."
    )
    if temporal_test_positives < TEMPORAL_TEST_MIN_POSITIVES:
        selection_note += " Temporal holdout is sparse, so treat threshold-based metrics cautiously."

    results["selected_model"] = {
        "name": best_model_name,
        "selection_split": "inner_temporal_within_temporal_train",
        "selection_basis": selection_note,
        "selection_details": selection_detail,
        "temporal_holdout_metrics": temporal_metrics[best_model_name],
    }
    precision_floor = float(os.environ.get("MODEL_DEPLOY_PRECISION_FLOOR", "0.30"))
    recall_floor = float(os.environ.get("MODEL_DEPLOY_RECALL_FLOOR", "0.40"))
    min_recent_train_pos_for_recall_gate = int(os.environ.get("MODEL_DEPLOY_MIN_RECENT_TRAIN_POSITIVES", "25"))
    min_recent_test_pos_for_recall_gate = int(os.environ.get("MODEL_DEPLOY_MIN_RECENT_TEST_POSITIVES", "50"))
    # Which model's holdout metrics drive GO gates (default: inner-temporal selection).
    # Set MODEL_DEPLOY_GATE_MODEL=oof_calibrated_stack (etc.) when launch artifact != selection pick.
    gate_mode = (os.environ.get("MODEL_DEPLOY_GATE_MODEL") or "selected").strip().lower()
    gate_model_name = best_model_name if gate_mode in ("", "selected", "selection", "inner") else gate_mode
    if gate_model_name not in temporal_metrics or gate_model_name not in recent_metrics:
        gate_model_name = best_model_name
    selected_temporal = temporal_metrics[gate_model_name]
    selected_recent = recent_metrics[gate_model_name]
    recent_window_support_pass = bool(recent_split.get("target_positive_support_passed", False))
    recent_recall_reliability_pass = (
        recent_train_positives >= min_recent_train_pos_for_recall_gate
        and recent_test_positives >= min_recent_test_pos_for_recall_gate
    )
    recent_support_pass = recent_window_support_pass and recent_recall_reliability_pass
    base_gate_pass = bool(gate_result.get("passed", False))
    temporal_deploy = (
        base_gate_pass
        and float(selected_temporal.get("precision", 0.0)) >= precision_floor
        and float(selected_temporal.get("recall", 0.0)) >= recall_floor
    )
    recent_deploy = (
        base_gate_pass
        and recent_support_pass
        and float(selected_recent.get("precision", 0.0)) >= precision_floor
        and float(selected_recent.get("recall", 0.0)) >= recall_floor
    )
    recent_binding = os.environ.get("MODEL_DEPLOY_RECENT_BINDING", "1") == "1"
    results["deployment_readiness"] = {
        "rule": (
            "label_maturity_gate + precision_floor + recall_floor on MODEL_DEPLOY_GATE_MODEL "
            "(default: inner-temporal selected model). Recent window adds positive-support reliability; "
            "set MODEL_DEPLOY_RECENT_BINDING=0 to make recent advisory only (explicit policy change)."
        ),
        "precision_floor": precision_floor,
        "recall_floor": recall_floor,
        "gate_model": gate_model_name,
        "gate_model_requested": gate_mode,
        "inner_selected_model": best_model_name,
        "temporal_primary": {
            "deployable": bool(temporal_deploy),
            "required": True,
            "gate_pass": base_gate_pass,
            "precision_pass": float(selected_temporal.get("precision", 0.0)) >= precision_floor,
            "recall_pass": float(selected_temporal.get("recall", 0.0)) >= recall_floor,
        },
        "recent_24_week": {
            "deployable": bool(recent_deploy),
            "required": bool(recent_binding),
            "gate_pass": base_gate_pass,
            "support_pass": recent_support_pass,
            "window_support_pass": recent_window_support_pass,
            "recall_reliability_support_pass": recent_recall_reliability_pass,
            "min_recent_train_positives_for_recall_gate": min_recent_train_pos_for_recall_gate,
            "min_recent_test_positives_for_recall_gate": min_recent_test_pos_for_recall_gate,
            "observed_recent_train_positives": recent_train_positives,
            "observed_recent_test_positives": recent_test_positives,
            "precision_pass": float(selected_recent.get("precision", 0.0)) >= precision_floor,
            "recall_pass": float(selected_recent.get("recall", 0.0)) >= recall_floor,
        },
    }
    ci_boot = int(os.environ.get("MODEL_BOOTSTRAP_SAMPLES", "500"))
    results["confidence_intervals"] = {
        "temporal_primary": {
            model_name: _bootstrap_metric_cis(
                dataset.target.iloc[temporal_test].to_numpy(),
                pred["y_pred"],
                n_boot=ci_boot,
            )
            for model_name, pred in temporal_predictions.items()
        },
        "recent_24_week": {
            model_name: _bootstrap_metric_cis(
                dataset.target.iloc[recent_test].to_numpy(),
                pred["y_pred"],
                n_boot=ci_boot,
            )
            for model_name, pred in recent_predictions.items()
        },
    }

    results_path = paths["models"] / OVERFIT_RESULTS_FILE
    results_path.write_text(json.dumps(results, indent=2))

    y_temporal_test = dataset.target.iloc[temporal_test]
    _plot_confusion_matrices(
        y_temporal_test,
        temporal_predictions,
        paths["figures"] / CONFUSION_FIGURE_FILE,
    )
    _save_split_scores(
        y_temporal_test,
        temporal_metrics,
        temporal_predictions,
        "temporal_holdout",
        paths["models"] / TEMPORAL_HOLDOUT_SCORES_FILE,
    )
    _save_split_scores(
        dataset.target.iloc[recent_test],
        recent_metrics,
        recent_predictions,
        "recent_24_week_temporal_holdout",
        paths["models"] / RECENT_HOLDOUT_SCORES_FILE,
    )
    from .poster_figures_v2 import generate_temporal_holdout_poster_figures

    generate_temporal_holdout_poster_figures(paths["project_root"])

    full_models = _save_model_artifacts(dataset, paths)
    if best_model_name not in full_models:
        raise RuntimeError(f"Selected model {best_model_name!r} missing from saved artifacts.")
    selected_model_obj = full_models[best_model_name]
    if isinstance(selected_model_obj, (SoftVoteBinaryEnsemble, OOFCalibratedStackEnsemble)):
        # Feature importance for ensembles is not directly defined; show strongest component model.
        base_for_importance = "lightgbm" if "lightgbm" in full_models else "logistic_regression"
        selected_pipeline = full_models[base_for_importance]
        results["selected_model"]["selection_basis"] += (
            f" Feature importance panel is shown from {base_for_importance} component of selected ensemble."
        )
    else:
        selected_pipeline = selected_model_obj
    importance_frame = _feature_importance_frame(selected_pipeline)
    importance_frame.to_csv(paths["tables"] / FEATURE_IMPORTANCE_TABLE_FILE, index=False)
    _plot_feature_importance(importance_frame, paths["figures"] / FEATURE_IMPORTANCE_FIGURE_FILE)

    _ensemble_types = (SoftVoteBinaryEnsemble, OOFCalibratedStackEnsemble)
    for model_key, pipeline in full_models.items():
        if isinstance(pipeline, _ensemble_types):
            continue
        per_fig = paths["figures"] / f"classification_feature_importance_{model_key}{ARTIFACT_SUFFIX}.png"
        try:
            frame = _feature_importance_frame(pipeline)
            label = model_key.replace("_", " ").title()
            _plot_feature_importance(frame, per_fig, title_suffix=f" — {label}")
        except Exception:
            continue

    comparison_table = _build_model_comparison_table(results)
    comparison_table.to_csv(paths["tables"] / MODEL_COMPARISON_TABLE_FILE, index=False)
    _plot_evidence_bundle(
        results,
        temporal_models,
        temporal_predictions,
        recent_predictions,
        dataset,
        temporal_test,
        recent_test,
        paths,
    )

    classification_metrics = {
        "dataset_summary": results["dataset_summary"],
        "dataset_contract": results["dataset_contract"],
        "selected_model": results["selected_model"],
        "temporal_holdout": results["temporal_holdout"],
        "group_holdout": results["group_holdout"],
        "recent_24_week_temporal_holdout": results["recent_24_week_temporal_holdout"],
        "diagnostics": results["diagnostics"],
        "deployment_readiness": results["deployment_readiness"],
        "confidence_intervals": results["confidence_intervals"],
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
