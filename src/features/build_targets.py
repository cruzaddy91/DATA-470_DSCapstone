"""
Build the official `v2` order-time backorder dataset.

Primary output:
  - master_order_fulfillment_modeling_v2_ordertime.csv

Additional side outputs remain in the module for historical comparison, but
`v2` is the only official modeling contract used by the current project.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional, Union

os.environ.setdefault("PANDAS_NO_USE_PYARROW", "1")
# Do not stub pyarrow as None (pandas>=2.2 compatibility).
sys.modules.setdefault("numexpr", None)
sys.modules.setdefault("bottleneck", None)

import numpy as np
import pandas as pd


LEGACY_ORDER_TARGET_TABLE = "master_order_fulfillment_with_targets"
ORDERTIME_MODELING_TABLE = "master_order_fulfillment_modeling_v2_ordertime"
# v3: last-N-week order window + trailing shipment demand + WOC inventory (see module docstring).
V3_DEMAND_INVENTORY_MODELING_TABLE = "master_order_fulfillment_modeling_v3_demand_inventory_24wk"
DEMAND_ROLLING_WINDOW_WEEKS = 24
ORDER_HISTORY_WINDOW_WEEKS = 24  # keep rows with order_date in (anchor − 24w, anchor]
DEMAND_INVENTORY_NUMERIC_FEATURES = [
    "demand_shipped_24wk_sum",
    "demand_shipped_24wk_active_weeks",
    "inventory_saleable_si",
    "inventory_awd",
    "inventory_woc",
]
# Snapshot backorder shortfall: max(0, outstanding_qty - saleable_inventory) at order line grain.
# Layered stg/int/mart exports mirror common ERP backorder reporting (no employer-specific naming).
SNAPSHOT_BACKORDER_MODELING_TABLE = "master_order_fulfillment_modeling_snapshot_backorder"
SNAPSHOT_BACKORDER_STG_TABLE = "stg_snapshot_backorder_line"
SNAPSHOT_BACKORDER_INT_TABLE = "int_snapshot_backorder_derived"
INVENTORY_TARGET_TABLE = "master_inventory_material_with_targets"

ORDER_GRAIN_COLUMNS = ["client_id", "sales_document_number", "item_number"]
TARGET_COLUMN = "target_backorder_risk"
DATE_COLUMN = "order_date"
TARGET_OBSERVED_COLUMN = "target_backorder_observed"
TARGET_STATUS_COLUMN = "target_backorder_label_status"
ORDERTIME_TARGET_GRACE_DAYS = 30

# The official v2 order-time contract only keeps fields known at order creation.
ORDERTIME_NUMERIC_FEATURES = [
    "cumulative_order_quantity",
    "cumulative_confirmed_quantity",
    "net_value",
    "requested_lead_time_days",
    "order_month",
    "order_weekday",
    "order_quarter",
]

SNAPSHOT_BACKORDER_TARGET_COLUMN = "target_snapshot_backorder_risk"
SNAPSHOT_OSQ_COLUMN = "outstanding_qty"
SNAPSHOT_SI_COLUMN = "saleable_inventory"
SNAPSHOT_BACKORDER_UNITS_COLUMN = "snapshot_backorder_units"
# Parity flag: 1 iff outstanding_qty > saleable_inventory (aligns with target when quantities are nonnegative).
SNAPSHOT_RULE_FLAG_COLUMN = "rule_outstanding_gt_saleable"
SNAPSHOT_WIDE_NUMERIC_FEATURES = ORDERTIME_NUMERIC_FEATURES + [
    SNAPSHOT_OSQ_COLUMN,
    SNAPSHOT_SI_COLUMN,
]

# Sklearn models on snapshot-backorder rows use order-time KPIs only. OSQ/SI define the label
# and remain in the CSV for audit, deterministic baselines, and the two-feature logistic.
SNAPSHOT_ML_NUMERIC_FEATURES = list(ORDERTIME_NUMERIC_FEATURES)

ORDERTIME_CATEGORICAL_FEATURES = [
    "client_id",
    "item_category",
    "sales_organization",
    "division",
    "plant_code",
    "country_code",
]
ORDERTIME_META_COLUMNS = [
    "material_number",
    "customer_number",
    DATE_COLUMN,
    "requested_delivery_date",
    "requested_delivery_date_schedule",
]

# Explicitly forbidden from the v2 modeling dataset.
ORDERTIME_FORBIDDEN_COLUMNS = [
    "material_description",
    "material_description_text",
    "material_type",
    "product_hierarchy",
    "customer_name",
    "distribution_channel",
    "storage_location",
    "total_requested_quantity",
    "total_confirmed_quantity",
    "total_delivery_quantity",
    "total_quantity_delivered",
    "total_billed_quantity",
    "total_billed_value",
    "order_header_net_value",
    "effective_shipment_date",
    "outstanding_qty",
    "is_open",
    "saleable_inventory",
    "backorder_units",
    "backorder_amount",
    "backorder_aging_days",
    "backorder_aging_bucket",
    "delivery_status",
    "billing_status",
    "goods_movement_status",
]


def _get_paths(project_root: Optional[Union[str, Path]] = None) -> dict[str, Path]:
    """Resolve paths for processed data."""
    if project_root is None:
        project_root = Path(__file__).resolve().parents[2]
    project_root = Path(project_root)
    return {"processed": project_root / "data" / "processed"}


def _normalize_grain_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    for column in ORDER_GRAIN_COLUMNS:
        if column not in normalized.columns:
            raise KeyError(f"Missing required order grain column: {column}")
        normalized[column] = normalized[column].astype(str).str.strip()
    return normalized


def validate_order_grain(df: pd.DataFrame, label: str) -> None:
    """Fail fast when order-level tables violate the agreed grain."""
    duplicates = df.duplicated(subset=ORDER_GRAIN_COLUMNS, keep=False)
    if duplicates.any():
        duplicate_rows = int(duplicates.sum())
        raise ValueError(
            f"{label} violates the order grain {ORDER_GRAIN_COLUMNS}. "
            f"Found {duplicate_rows} duplicate rows."
        )


def _ensure_columns(df: pd.DataFrame, columns: list[str], fill_value: object) -> pd.DataFrame:
    ensured = df.copy()
    for column in columns:
        if column not in ensured.columns:
            ensured[column] = fill_value
    return ensured


def _assert_required_columns(df: pd.DataFrame, required_columns: list[str], label: str) -> None:
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise KeyError(f"{label} is missing required columns: {missing}")


def _clean_string_series(series: pd.Series) -> pd.Series:
    cleaned = series.copy().astype("object")
    cleaned = cleaned.map(lambda value: value.strip() if isinstance(value, str) else value)
    return cleaned.replace("", pd.NA)


def _derive_requested_lead_time_days(df: pd.DataFrame) -> pd.Series:
    order_date = pd.to_datetime(df.get(DATE_COLUMN), errors="coerce")
    requested_date = pd.to_datetime(df.get("requested_delivery_date"), errors="coerce")
    schedule_date = pd.to_datetime(df.get("requested_delivery_date_schedule"), errors="coerce")
    effective_requested = schedule_date.fillna(requested_date)
    lead_time = (effective_requested - order_date).dt.days
    return lead_time.where(lead_time.isna(), lead_time.clip(lower=0))


def _validate_no_non_finite(df: pd.DataFrame, numeric_columns: list[str], label: str) -> None:
    if not numeric_columns:
        return
    numeric_frame = df[numeric_columns].apply(pd.to_numeric, errors="coerce")
    non_finite_mask = np.isinf(numeric_frame.to_numpy(dtype="float64", na_value=np.nan))
    if non_finite_mask.any():
        bad_columns = [
            column for column in numeric_columns if np.isinf(numeric_frame[column].to_numpy(dtype="float64", na_value=np.nan)).any()
        ]
        raise ValueError(f"{label} contains non-finite values in columns: {bad_columns}")


def build_order_targets(master_order_brd: pd.DataFrame) -> pd.DataFrame:
    """
    Add the legacy snapshot backorder target to the BRD table.

    This remains available for comparison, but is not the official trainable dataset.
    """
    df = _normalize_grain_columns(master_order_brd)
    validate_order_grain(df, "master_order_fulfillment_brd")
    df[TARGET_COLUMN] = (pd.to_numeric(df.get("backorder_units", 0), errors="coerce").fillna(0) > 0).astype(int)
    return df


def _derive_ordertime_target(master_order_brd: pd.DataFrame) -> pd.DataFrame:
    """
    Build the official order-time target from observed fulfillment outcomes.

    Rows are labeled only when the requested date is sufficiently old and the
    fulfillment outcome is observable. Ambiguous rows remain unlabeled so they
    do not pollute training or temporal evaluation.
    """
    brd = _normalize_grain_columns(master_order_brd)
    validate_order_grain(brd, "master_order_fulfillment_brd")

    brd[DATE_COLUMN] = pd.to_datetime(brd.get(DATE_COLUMN, pd.Series(index=brd.index, dtype="object")), errors="coerce")
    requested_date = pd.to_datetime(
        brd.get("requested_delivery_date", pd.Series(index=brd.index, dtype="object")),
        errors="coerce",
    )
    scheduled_date = pd.to_datetime(
        brd.get("requested_delivery_date_schedule", pd.Series(index=brd.index, dtype="object")),
        errors="coerce",
    )
    effective_requested = scheduled_date.fillna(requested_date).fillna(brd[DATE_COLUMN])

    latest_order_date = brd[DATE_COLUMN].max()
    if pd.isna(latest_order_date):
        raise ValueError("Unable to derive order-time target because order_date is entirely missing.")

    observation_cutoff = latest_order_date - pd.Timedelta(days=ORDERTIME_TARGET_GRACE_DAYS)
    outcome_mature = effective_requested.notna() & (effective_requested <= observation_cutoff)

    order_qty = pd.to_numeric(brd.get("cumulative_order_quantity"), errors="coerce")
    delivered_qty = pd.to_numeric(brd.get("total_quantity_delivered"), errors="coerce")
    delivery_status = brd.get("delivery_status", pd.Series(index=brd.index, dtype="object")).fillna("").astype(str)

    delivered_short = delivered_qty.notna() & order_qty.notna() & (delivered_qty < order_qty)
    delivered_complete = delivered_qty.notna() & order_qty.notna() & (delivered_qty >= order_qty)
    status_complete = delivery_status.eq("C")

    positive = outcome_mature & delivered_short
    negative = outcome_mature & ~positive & (delivered_complete | status_complete)
    observed = positive | negative

    target = pd.Series(pd.NA, index=brd.index, dtype="Int64")
    target.loc[positive] = 1
    target.loc[negative] = 0

    status = pd.Series("unresolved", index=brd.index, dtype="object")
    status.loc[~outcome_mature] = "pending_window"
    status.loc[outcome_mature & ~observed] = "missing_outcome"
    status.loc[negative] = "negative"
    status.loc[positive] = "positive"

    return pd.DataFrame(
        {
            **{column: brd[column] for column in ORDER_GRAIN_COLUMNS},
            TARGET_COLUMN: target,
            TARGET_OBSERVED_COLUMN: observed.astype(int),
            TARGET_STATUS_COLUMN: status,
        }
    )


def build_ordertime_modeling_dataset(
    master_order: pd.DataFrame,
    master_order_brd: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build the official v2 leakage-safe order-time modeling dataset.

    The label comes from the snapshot outcome table, but only order-time fields are
    carried forward into the trainable dataset.
    """
    order = _normalize_grain_columns(master_order)
    brd = build_order_targets(master_order_brd)

    validate_order_grain(order, "master_order_fulfillment")
    validate_order_grain(brd, LEGACY_ORDER_TARGET_TABLE)

    order = _ensure_columns(order, ORDERTIME_META_COLUMNS + ORDERTIME_NUMERIC_FEATURES + ORDERTIME_CATEGORICAL_FEATURES, pd.NA)
    for forbidden in ORDERTIME_FORBIDDEN_COLUMNS:
        if forbidden not in order.columns:
            continue

    target_frame = _derive_ordertime_target(master_order_brd)
    merged = order.merge(target_frame, on=ORDER_GRAIN_COLUMNS, how="left", validate="one_to_one")
    if len(merged) != len(order):
        raise ValueError("Order-time modeling merge changed the row count.")
    if merged[[TARGET_COLUMN, TARGET_OBSERVED_COLUMN, TARGET_STATUS_COLUMN]].isna().all(axis=None):
        raise ValueError("Order-time modeling dataset failed to derive any target observations.")

    merged[DATE_COLUMN] = pd.to_datetime(merged[DATE_COLUMN], errors="coerce")
    merged["requested_delivery_date"] = pd.to_datetime(merged["requested_delivery_date"], errors="coerce")
    merged["requested_delivery_date_schedule"] = pd.to_datetime(
        merged["requested_delivery_date_schedule"],
        errors="coerce",
    )
    merged["requested_lead_time_days"] = _derive_requested_lead_time_days(merged)
    merged["order_month"] = merged[DATE_COLUMN].dt.month
    merged["order_weekday"] = merged[DATE_COLUMN].dt.weekday
    merged["order_quarter"] = merged[DATE_COLUMN].dt.quarter

    for column in ORDERTIME_NUMERIC_FEATURES:
        merged[column] = pd.to_numeric(merged[column], errors="coerce")
    for column in ORDERTIME_CATEGORICAL_FEATURES:
        merged[column] = _clean_string_series(merged[column])

    output_columns = list(
        dict.fromkeys(
            ORDER_GRAIN_COLUMNS
            + ORDERTIME_META_COLUMNS
            + ORDERTIME_NUMERIC_FEATURES
            + ORDERTIME_CATEGORICAL_FEATURES
            + [TARGET_COLUMN, TARGET_OBSERVED_COLUMN, TARGET_STATUS_COLUMN]
        )
    )
    dataset = merged[output_columns].copy()
    validate_order_grain(dataset, ORDERTIME_MODELING_TABLE)
    _validate_no_non_finite(dataset, ORDERTIME_NUMERIC_FEATURES, ORDERTIME_MODELING_TABLE)

    unexpected_forbidden = [column for column in ORDERTIME_FORBIDDEN_COLUMNS if column in dataset.columns]
    if unexpected_forbidden:
        raise ValueError(
            f"{ORDERTIME_MODELING_TABLE} leaked forbidden columns into the trainable dataset: {unexpected_forbidden}"
        )

    dataset[TARGET_COLUMN] = pd.to_numeric(dataset[TARGET_COLUMN], errors="coerce").astype("Int64")
    dataset[TARGET_OBSERVED_COLUMN] = pd.to_numeric(dataset[TARGET_OBSERVED_COLUMN], errors="raise").astype(int)
    dataset[TARGET_STATUS_COLUMN] = _clean_string_series(dataset[TARGET_STATUS_COLUMN]).fillna("unresolved")
    return dataset


def build_ordertime_demand_inventory_24wk_dataset(
    ordertime_modeling: pd.DataFrame,
    shipment_history: pd.DataFrame,
    master_woc: pd.DataFrame,
) -> pd.DataFrame:
    """
    Extend the v2 order-time modeling table with demand and inventory context, then restrict rows
    to orders in the last ``ORDER_HISTORY_WINDOW_WEEKS`` (by max labeled order_date).

    Demand: sum of ``quantity_shipped`` from ``shipment_history`` for the same client / material /
    plant where the shipment week ends **strictly before** the order date and the week falls in the
    trailing ``DEMAND_ROLLING_WINDOW_WEEKS`` window (no future shipments).

    Inventory: left join ``master_woc`` saleable_inventory, awd, woc (point-in-time WOC snapshot;
    not order-dated — document in reporting).
    """
    base = ordertime_modeling.copy()
    validate_order_grain(base, ORDERTIME_MODELING_TABLE)

    for column in ["client_id", "material_number", "plant_code"]:
        base[column] = base[column].astype(str)

    ship = shipment_history.copy()
    _assert_required_columns(
        ship,
        ["client_id", "material_number", "plant_code", "shipment_week", "quantity_shipped"],
        "shipment_history",
    )
    ship["client_id"] = ship["client_id"].astype(str)
    ship["material_number"] = ship["material_number"].astype(str)
    ship["plant_code"] = ship["plant_code"].astype(str)
    ship["shipment_week"] = pd.to_datetime(ship["shipment_week"], errors="coerce")
    ship["quantity_shipped"] = pd.to_numeric(ship["quantity_shipped"], errors="coerce").fillna(0.0)
    ship_agg = (
        ship.groupby(["client_id", "material_number", "plant_code", "shipment_week"], as_index=False)[
            "quantity_shipped"
        ]
        .sum()
    )
    ship_agg["week_end"] = ship_agg["shipment_week"] + pd.Timedelta(days=6)

    base["_row_id"] = np.arange(len(base), dtype=np.int64)
    base[DATE_COLUMN] = pd.to_datetime(base[DATE_COLUMN], errors="coerce")
    base["demand_window_start"] = base[DATE_COLUMN] - pd.Timedelta(weeks=DEMAND_ROLLING_WINDOW_WEEKS)

    keys = ["client_id", "material_number", "plant_code"]
    merged = base[["_row_id"] + keys + [DATE_COLUMN, "demand_window_start"]].merge(
        ship_agg,
        on=keys,
        how="left",
    )
    eligible = (
        merged["week_end"].notna()
        & (merged["week_end"] < merged[DATE_COLUMN])
        & (merged["shipment_week"] >= merged["demand_window_start"])
    )
    merged["_qty_eligible"] = np.where(eligible, merged["quantity_shipped"].fillna(0.0), 0.0)
    demand_sum = merged.groupby("_row_id", sort=False)["_qty_eligible"].sum()
    demand_weeks = merged.loc[eligible].groupby("_row_id", sort=False)["shipment_week"].nunique()

    base["demand_shipped_24wk_sum"] = base["_row_id"].map(demand_sum).fillna(0.0)
    base["demand_shipped_24wk_active_weeks"] = (
        base["_row_id"].map(demand_weeks).fillna(0).astype(int)
    )

    woc = master_woc.copy()
    woc["client_id"] = woc["client_id"].astype(str)
    woc["material_number"] = woc["material_number"].astype(str)
    woc["plant_code"] = woc["plant_code"].astype(str)
    woc_keep = woc[keys + ["saleable_inventory", "awd", "woc"]].copy()
    woc_keep = woc_keep.rename(
        columns={
            "saleable_inventory": "inventory_saleable_si",
            "awd": "inventory_awd",
            "woc": "inventory_woc",
        }
    )
    for column in ["inventory_saleable_si", "inventory_awd", "inventory_woc"]:
        woc_keep[column] = pd.to_numeric(woc_keep[column], errors="coerce")
    woc_keep = woc_keep.drop_duplicates(subset=keys, keep="first")

    base = base.merge(woc_keep, on=keys, how="left")
    base["inventory_saleable_si"] = base["inventory_saleable_si"].fillna(0.0)
    base["inventory_awd"] = base["inventory_awd"].fillna(0.0)
    base["inventory_woc"] = base["inventory_woc"].fillna(0.0)

    labeled_dates = base.loc[base[TARGET_COLUMN].notna(), DATE_COLUMN]
    if labeled_dates.empty:
        raise ValueError("v3 demand/inventory build requires labeled rows (resolved target).")
    anchor = labeled_dates.max()
    cutoff = anchor - pd.Timedelta(weeks=ORDER_HISTORY_WINDOW_WEEKS)
    base = base.loc[base[DATE_COLUMN] >= cutoff].copy()

    base = base.drop(columns=["_row_id", "demand_window_start"], errors="ignore")

    extra_numeric = DEMAND_INVENTORY_NUMERIC_FEATURES
    output_columns = list(dict.fromkeys(list(ordertime_modeling.columns) + extra_numeric))
    missing = [column for column in output_columns if column not in base.columns]
    if missing:
        raise KeyError(f"v3 demand/inventory build missing expected columns: {missing}")
    dataset = base[output_columns].copy()
    validate_order_grain(dataset, V3_DEMAND_INVENTORY_MODELING_TABLE)
    all_numeric = ORDERTIME_NUMERIC_FEATURES + extra_numeric
    _validate_no_non_finite(dataset, all_numeric, V3_DEMAND_INVENTORY_MODELING_TABLE)

    unexpected_forbidden = [column for column in ORDERTIME_FORBIDDEN_COLUMNS if column in dataset.columns]
    if unexpected_forbidden:
        raise ValueError(
            f"{V3_DEMAND_INVENTORY_MODELING_TABLE} leaked unexpected forbidden columns: {unexpected_forbidden}"
        )

    dataset[TARGET_COLUMN] = pd.to_numeric(dataset[TARGET_COLUMN], errors="coerce").astype("Int64")
    dataset[TARGET_OBSERVED_COLUMN] = pd.to_numeric(dataset[TARGET_OBSERVED_COLUMN], errors="raise").astype(int)
    dataset[TARGET_STATUS_COLUMN] = _clean_string_series(dataset[TARGET_STATUS_COLUMN]).fillna("unresolved")
    return dataset


def build_snapshot_backorder_bundle(
    master_order: pd.DataFrame,
    master_order_brd: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """
    Build snapshot backorder tables in one pass: staging (OSQ + saleable inputs), intermediate
    (shortfall units + rule flag), and mart (wide modeling table with order-time KPIs).

    Shortfall matches standard ERP-style backorder math:
    snapshot_backorder_units = max(0, outstanding_qty - saleable_inventory);
    target_snapshot_backorder_risk = 1 iff shortfall > 0.
    """
    order = _normalize_grain_columns(master_order)
    brd = _normalize_grain_columns(master_order_brd)

    validate_order_grain(order, "master_order_fulfillment")
    validate_order_grain(brd, "master_order_fulfillment_brd")

    order = _ensure_columns(
        order,
        ORDERTIME_META_COLUMNS + ORDERTIME_NUMERIC_FEATURES + ORDERTIME_CATEGORICAL_FEATURES,
        pd.NA,
    )

    brd_required = ORDER_GRAIN_COLUMNS + [SNAPSHOT_OSQ_COLUMN, SNAPSHOT_SI_COLUMN]
    _assert_required_columns(brd, brd_required, "master_order_fulfillment_brd")

    snap = brd[brd_required].copy()
    merged = order.merge(snap, on=ORDER_GRAIN_COLUMNS, how="left", validate="one_to_one")
    if len(merged) != len(order):
        raise ValueError("Snapshot backorder merge changed the row count.")

    merged[DATE_COLUMN] = pd.to_datetime(merged[DATE_COLUMN], errors="coerce")
    merged["requested_delivery_date"] = pd.to_datetime(merged["requested_delivery_date"], errors="coerce")
    merged["requested_delivery_date_schedule"] = pd.to_datetime(
        merged["requested_delivery_date_schedule"],
        errors="coerce",
    )
    merged["requested_lead_time_days"] = _derive_requested_lead_time_days(merged)
    merged["order_month"] = merged[DATE_COLUMN].dt.month
    merged["order_weekday"] = merged[DATE_COLUMN].dt.weekday
    merged["order_quarter"] = merged[DATE_COLUMN].dt.quarter

    osq = pd.to_numeric(merged[SNAPSHOT_OSQ_COLUMN], errors="coerce").fillna(0.0)
    si = pd.to_numeric(merged[SNAPSHOT_SI_COLUMN], errors="coerce").fillna(0.0)
    shortfall = (osq - si).clip(lower=0.0)
    merged[SNAPSHOT_OSQ_COLUMN] = osq
    merged[SNAPSHOT_SI_COLUMN] = si
    merged[SNAPSHOT_BACKORDER_UNITS_COLUMN] = shortfall
    merged[SNAPSHOT_BACKORDER_TARGET_COLUMN] = (shortfall > 0).astype(int)

    for column in ORDERTIME_NUMERIC_FEATURES:
        merged[column] = pd.to_numeric(merged[column], errors="coerce")

    for column in ORDERTIME_CATEGORICAL_FEATURES:
        merged[column] = _clean_string_series(merged[column])

    stg_columns = list(
        dict.fromkeys(
            ORDER_GRAIN_COLUMNS
            + ["material_number", DATE_COLUMN]
            + [SNAPSHOT_OSQ_COLUMN, SNAPSHOT_SI_COLUMN]
        )
    )
    stg = merged[stg_columns].copy()
    validate_order_grain(stg, SNAPSHOT_BACKORDER_STG_TABLE)
    _validate_no_non_finite(
        stg,
        [SNAPSHOT_OSQ_COLUMN, SNAPSHOT_SI_COLUMN],
        SNAPSHOT_BACKORDER_STG_TABLE,
    )

    int_columns = list(
        dict.fromkeys(
            ORDER_GRAIN_COLUMNS
            + [
                SNAPSHOT_BACKORDER_UNITS_COLUMN,
                SNAPSHOT_BACKORDER_TARGET_COLUMN,
                SNAPSHOT_RULE_FLAG_COLUMN,
            ]
        )
    )
    int_df = merged[
        ORDER_GRAIN_COLUMNS + [SNAPSHOT_BACKORDER_UNITS_COLUMN, SNAPSHOT_BACKORDER_TARGET_COLUMN]
    ].copy()
    int_df[SNAPSHOT_RULE_FLAG_COLUMN] = (osq > si).astype(int)
    int_df = int_df[int_columns].copy()
    validate_order_grain(int_df, SNAPSHOT_BACKORDER_INT_TABLE)

    output_columns = list(
        dict.fromkeys(
            ORDER_GRAIN_COLUMNS
            + ORDERTIME_META_COLUMNS
            + SNAPSHOT_WIDE_NUMERIC_FEATURES
            + ORDERTIME_CATEGORICAL_FEATURES
            + [SNAPSHOT_BACKORDER_TARGET_COLUMN, SNAPSHOT_BACKORDER_UNITS_COLUMN]
        )
    )
    mart = merged[output_columns].copy()
    validate_order_grain(mart, SNAPSHOT_BACKORDER_MODELING_TABLE)
    _validate_no_non_finite(mart, SNAPSHOT_WIDE_NUMERIC_FEATURES, SNAPSHOT_BACKORDER_MODELING_TABLE)

    return {
        SNAPSHOT_BACKORDER_STG_TABLE: stg,
        SNAPSHOT_BACKORDER_INT_TABLE: int_df,
        SNAPSHOT_BACKORDER_MODELING_TABLE: mart,
    }


def build_snapshot_backorder_modeling_dataset(
    master_order: pd.DataFrame,
    master_order_brd: pd.DataFrame,
) -> pd.DataFrame:
    """
    Mart / wide modeling table: order-time KPIs + OSQ/SI + snapshot backorder target and shortfall units.

    For layered stg/int tables, use ``build_snapshot_backorder_bundle`` or run the full targets build.
    """
    return build_snapshot_backorder_bundle(master_order, master_order_brd)[SNAPSHOT_BACKORDER_MODELING_TABLE]


def build_inventory_targets(
    master_inventory: pd.DataFrame,
    master_woc: pd.DataFrame,
    overstock_woc_threshold_weeks: float = 26.0,
) -> pd.DataFrame:
    """
    Add overstock target to master inventory.
    target_overstock_risk: 1 if WOC > threshold (excess inventory), else 0.
    Material/plant grain; merged from master_woc.
    """
    inv = master_inventory.copy()
    inv["client_id"] = inv["client_id"].astype(str)
    inv["material_number"] = inv["material_number"].astype(str)
    inv["plant_code"] = inv["plant_code"].fillna("").astype(str)

    woc = master_woc.copy()
    woc["client_id"] = woc["client_id"].astype(str)
    woc["material_number"] = woc["material_number"].astype(str)
    woc["plant_code"] = woc["plant_code"].fillna("").astype(str)

    merged = inv.merge(
        woc[["client_id", "material_number", "plant_code", "woc", "awd"]],
        on=["client_id", "material_number", "plant_code"],
        how="left",
    )

    merged["target_overstock_risk"] = np.where(
        (merged["woc"].notna())
        & (pd.to_numeric(merged["awd"], errors="coerce").fillna(0) > 0)
        & (merged["woc"] > overstock_woc_threshold_weeks),
        1,
        0,
    ).astype(int)

    return merged


def build_all_targets(
    project_root: Optional[Union[str, Path]] = None,
    overstock_woc_threshold_weeks: float = 26.0,
    save: bool = True,
) -> dict[str, pd.DataFrame]:
    """
    Load processed tables, build legacy snapshot targets plus the official v2 dataset.

    Returns a dict of {table_name: DataFrame}.
    """
    paths = _get_paths(project_root)
    processed = paths["processed"]

    order_path = processed / "master_order_fulfillment.csv"
    order_brd_path = processed / "master_order_fulfillment_brd.csv"
    woc_path = processed / "master_woc.csv"
    inv_path = processed / "master_inventory_material.csv"

    if not order_path.exists():
        raise FileNotFoundError("master_order_fulfillment.csv not found. Run build_master_tables first.")
    if not order_brd_path.exists():
        raise FileNotFoundError(
            "master_order_fulfillment_brd.csv not found. Run build_brd_metrics first: "
            "python -m src.data.run_pipeline"
        )
    if not woc_path.exists():
        raise FileNotFoundError("master_woc.csv not found. Run build_brd_metrics first.")
    if not inv_path.exists():
        raise FileNotFoundError("master_inventory_material.csv not found. Run build_master_tables first.")

    master_order = pd.read_csv(order_path, low_memory=False)
    order_brd = pd.read_csv(order_brd_path, low_memory=False)
    master_woc = pd.read_csv(woc_path, low_memory=False)
    master_inv = pd.read_csv(inv_path, low_memory=False)

    legacy_order_with_targets = build_order_targets(order_brd)
    ordertime_modeling = build_ordertime_modeling_dataset(master_order, order_brd)
    snapshot_backorder_bundle = build_snapshot_backorder_bundle(master_order, order_brd)
    inv_with_targets = build_inventory_targets(
        master_inv,
        master_woc,
        overstock_woc_threshold_weeks=overstock_woc_threshold_weeks,
    )

    result = {
        LEGACY_ORDER_TARGET_TABLE: legacy_order_with_targets,
        ORDERTIME_MODELING_TABLE: ordertime_modeling,
        INVENTORY_TARGET_TABLE: inv_with_targets,
        **snapshot_backorder_bundle,
    }

    shipment_path = processed / "shipment_history.csv"
    if shipment_path.exists():
        shipment = pd.read_csv(shipment_path, low_memory=False)
        v3_modeling = build_ordertime_demand_inventory_24wk_dataset(ordertime_modeling, shipment, master_woc)
        result[V3_DEMAND_INVENTORY_MODELING_TABLE] = v3_modeling
    else:
        print(
            f"Skipped {V3_DEMAND_INVENTORY_MODELING_TABLE}: {shipment_path} not found "
            "(BRD phase must produce shipment_history)."
        )

    if save:
        processed.mkdir(parents=True, exist_ok=True)
        for name, df in result.items():
            out_path = processed / f"{name}.csv"
            df.to_csv(out_path, index=False)
            print(f"Saved {name}: {len(df):,} rows -> {out_path}")

    return result


if __name__ == "__main__":
    build_all_targets()
