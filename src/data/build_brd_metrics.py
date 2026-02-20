"""
Build BRD-aligned metrics from core master tables and raw delivery data.

Produces:
  - master_order_fulfillment_brd - Order fulfillment + BRD metrics (outstanding, backorder, aging)
  - shipment_history - Material x week shipments for AWD
  - master_woc - Weeks of coverage by material/plant

Requires: master_order_fulfillment, master_inventory_material in data/processed/
          delivery_item, delivery_header in data/clean/main/
"""

from pathlib import Path
from typing import Optional, Union

import numpy as np
import pandas as pd


def _get_paths(project_root: Optional[Union[str, Path]] = None) -> dict:
    """Resolve paths for processed and clean data."""
    if project_root is None:
        project_root = Path(__file__).resolve().parents[2]
    project_root = Path(project_root)
    return {
        "clean_main": project_root / "data" / "clean" / "main",
        "processed": project_root / "data" / "processed",
    }


def load_core_tables(paths: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load master_order_fulfillment and master_inventory_material from processed/."""
    order_path = paths["processed"] / "master_order_fulfillment.csv"
    inv_path = paths["processed"] / "master_inventory_material.csv"
    if not order_path.exists() or not inv_path.exists():
        raise FileNotFoundError(
            "Core tables not found. Run build_master_tables first: "
            "python -m src.data.build_master_tables"
        )
    master_order = pd.read_csv(order_path, low_memory=False)
    master_inv = pd.read_csv(inv_path, low_memory=False)
    return master_order, master_inv


def load_delivery_tables(paths: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load delivery_item and delivery_header from clean/main/."""
    di_path = paths["clean_main"] / "delivery_item.csv"
    dh_path = paths["clean_main"] / "delivery_header.csv"
    if not di_path.exists() or not dh_path.exists():
        return pd.DataFrame(), pd.DataFrame()
    return pd.read_csv(di_path, low_memory=False), pd.read_csv(dh_path, low_memory=False)


def build_master_order_fulfillment_brd(
    master_order: pd.DataFrame,
    master_inv: pd.DataFrame,
    reference_date: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """
    Add BRD metrics to master order fulfillment.
    Shipment date rule: requested_delivery_date_schedule -> requested_delivery_date -> order_date.
    """
    df = master_order.copy()
    if reference_date is None:
        reference_date = pd.Timestamp.now().normalize()

    # effective_shipment_date (BRD: Requested -> Planned -> Order)
    date_cols = ["requested_delivery_date_schedule", "requested_delivery_date", "order_date"]
    for c in date_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    df["effective_shipment_date"] = (
        df["requested_delivery_date_schedule"]
        .fillna(df["requested_delivery_date"])
        .fillna(df["order_date"])
    )

    # outstanding_qty
    ord_qty = pd.to_numeric(df["cumulative_order_quantity"], errors="coerce").fillna(0)
    del_qty = pd.to_numeric(df["total_quantity_delivered"], errors="coerce").fillna(0)
    df["outstanding_qty"] = (ord_qty - del_qty).clip(lower=0)

    # is_open (BRD: Open/Released with Outstanding > 0)
    df["is_open"] = df["outstanding_qty"] > 0

    # saleable_inventory: aggregate unrestricted_stock by (client_id, material_number)
    if "unrestricted_stock" in master_inv.columns:
        si_agg = (
            master_inv.groupby(["client_id", "material_number"], dropna=False)["unrestricted_stock"]
            .sum()
            .reset_index()
            .rename(columns={"unrestricted_stock": "saleable_inventory"})
        )
        si_agg["client_id"] = si_agg["client_id"].astype(str)
        si_agg["material_number"] = si_agg["material_number"].astype(str)
        df["client_id"] = df["client_id"].astype(str)
        df["material_number"] = df["material_number"].astype(str)
        df = df.merge(si_agg, on=["client_id", "material_number"], how="left")
        df["saleable_inventory"] = pd.to_numeric(df["saleable_inventory"], errors="coerce").fillna(0)
    else:
        df["saleable_inventory"] = 0.0

    # backorder_units = max(0, outstanding_qty - SI) for open lines
    df["backorder_units"] = np.where(
        df["is_open"],
        (df["outstanding_qty"] - df["saleable_inventory"]).clip(lower=0),
        0.0,
    )

    # backorder_amount = backorder_units * unit_price (net_value / order_qty)
    unit_price = np.where(
        ord_qty > 0,
        pd.to_numeric(df["net_value"], errors="coerce").fillna(0) / ord_qty,
        0,
    )
    df["backorder_amount"] = df["backorder_units"] * unit_price

    # backorder_aging_days, backorder_aging_bucket
    ref_ts = pd.Timestamp(reference_date)
    df["effective_shipment_date"] = pd.to_datetime(df["effective_shipment_date"], errors="coerce")
    eff_dt = df["effective_shipment_date"].dt.tz_localize(None, ambiguous="NaT")
    df["backorder_aging_days"] = np.where(
        df["is_open"] & df["effective_shipment_date"].notna(),
        (ref_ts - eff_dt).dt.days,
        np.nan,
    )

    days = df["backorder_aging_days"]
    df["backorder_aging_bucket"] = np.select(
        [days.isna() | (days < 0), days <= 7, days <= 14, days <= 30],
        ["", "0-7", "8-14", "15-30"],
        default="31+",
    )

    return df


def build_shipment_history(delivery_item: pd.DataFrame, delivery_header: pd.DataFrame) -> pd.DataFrame:
    """
    Material x week shipment quantities for AWD calculation.
    Grain: client_id, material_number, plant_code, shipment_week.
    """
    if len(delivery_item) == 0 or len(delivery_header) == 0:
        return pd.DataFrame(
            columns=["client_id", "material_number", "plant_code", "shipment_week", "quantity_shipped"]
        )
    if "goods_issue_date" not in delivery_header.columns or "quantity_delivered" not in delivery_item.columns:
        return pd.DataFrame(
            columns=["client_id", "material_number", "plant_code", "shipment_week", "quantity_shipped"]
        )

    dh = delivery_header[["client_id", "sales_document_number", "goods_issue_date"]].copy()
    dh["client_id"] = dh["client_id"].astype(str)
    di = delivery_item[
        ["client_id", "sales_document_number", "material_number", "plant_code", "quantity_delivered"]
    ].copy()
    di["client_id"] = di["client_id"].astype(str)

    merged = di.merge(dh, on=["client_id", "sales_document_number"], how="left")
    merged["goods_issue_date"] = pd.to_datetime(merged["goods_issue_date"], errors="coerce")
    merged = merged.dropna(subset=["goods_issue_date", "quantity_delivered"])
    merged["quantity_delivered"] = pd.to_numeric(merged["quantity_delivered"], errors="coerce").fillna(0)
    merged = merged[merged["quantity_delivered"] > 0]

    merged["shipment_week"] = merged["goods_issue_date"].dt.to_period("W").dt.start_time.dt.strftime("%Y-%m-%d")

    agg = (
        merged.groupby(
            ["client_id", "material_number", "plant_code", "shipment_week"],
            dropna=False,
        )["quantity_delivered"]
        .sum()
        .reset_index()
        .rename(columns={"quantity_delivered": "quantity_shipped"})
    )
    agg["plant_code"] = agg["plant_code"].fillna("").astype(str)
    return agg


def build_master_woc(
    master_inv: pd.DataFrame,
    shipment_history: pd.DataFrame,
    master_order_brd: pd.DataFrame,
    woc_weeks: int = 24,
    woc_low_threshold_weeks: float = 2.0,
) -> pd.DataFrame:
    """
    Weeks of Coverage by material (and plant when available).
    WOC = NetAvailableInventory / AWD.
    NetAvailable = SI - Open Sales Order Qty.
    AWD = rolling woc_weeks of shipments.
    """
    if "unrestricted_stock" not in master_inv.columns:
        return pd.DataFrame()

    inv = master_inv.copy()
    inv["client_id"] = inv["client_id"].astype(str)
    inv["plant_code"] = inv["plant_code"].fillna("").astype(str)
    si = (
        inv.groupby(["client_id", "material_number", "plant_code"], dropna=False)["unrestricted_stock"]
        .sum()
        .reset_index()
        .rename(columns={"unrestricted_stock": "saleable_inventory"})
    )

    ord_brd = master_order_brd[master_order_brd["is_open"]].copy()
    ord_brd["client_id"] = ord_brd["client_id"].astype(str)
    ord_brd["plant_code"] = ord_brd["plant_code"].fillna("").astype(str)
    open_qty = (
        ord_brd.groupby(["client_id", "material_number", "plant_code"], dropna=False)["outstanding_qty"]
        .sum()
        .reset_index()
        .rename(columns={"outstanding_qty": "open_order_qty"})
    )

    woc = si.merge(open_qty, on=["client_id", "material_number", "plant_code"], how="left")
    woc["open_order_qty"] = woc["open_order_qty"].fillna(0)
    woc["net_available"] = (woc["saleable_inventory"] - woc["open_order_qty"]).clip(lower=0)

    if len(shipment_history) == 0:
        woc["awd"] = np.nan
        woc["woc"] = np.nan
        woc["woc_low_flag"] = False
        return woc

    sh = shipment_history.copy()
    sh["week_dt"] = pd.to_datetime(sh["shipment_week"], errors="coerce")
    sh = sh.dropna(subset=["week_dt"])
    if len(sh) == 0:
        woc["awd"] = np.nan
        woc["woc"] = np.nan
        woc["woc_low_flag"] = False
        return woc

    max_week = sh["week_dt"].max()
    cutoff = max_week - pd.Timedelta(weeks=woc_weeks)
    sh_recent = sh[sh["week_dt"] >= cutoff]
    awd_agg = (
        sh_recent.groupby(["client_id", "material_number", "plant_code"], dropna=False)["quantity_shipped"]
        .sum()
        .reset_index()
    )
    awd_agg["awd"] = awd_agg["quantity_shipped"] / woc_weeks
    awd_agg = awd_agg[["client_id", "material_number", "plant_code", "awd"]]

    woc = woc.merge(awd_agg, on=["client_id", "material_number", "plant_code"], how="left")
    woc["woc"] = np.where(woc["awd"] > 0, woc["net_available"] / woc["awd"], np.nan)
    woc["woc_low_flag"] = (woc["woc"].notna()) & (woc["woc"] <= woc_low_threshold_weeks)

    return woc


def build_all_brd_metrics(
    project_root: Optional[Union[str, Path]] = None,
    save: bool = True,
) -> dict:
    """
    Load core tables and delivery data, build BRD metrics, optionally save.
    Returns dict of {table_name: DataFrame}.
    """
    paths = _get_paths(project_root)

    master_order, master_inv = load_core_tables(paths)
    delivery_item, delivery_header = load_delivery_tables(paths)

    master_order_brd = build_master_order_fulfillment_brd(master_order, master_inv)
    shipment_history = build_shipment_history(delivery_item, delivery_header)
    master_woc = build_master_woc(master_inv, shipment_history, master_order_brd)

    result = {
        "master_order_fulfillment_brd": master_order_brd,
        "shipment_history": shipment_history,
        "master_woc": master_woc,
    }

    if save:
        paths["processed"].mkdir(parents=True, exist_ok=True)
        for name, df in result.items():
            out_path = paths["processed"] / f"{name}.csv"
            df.to_csv(out_path, index=False)
            print(f"Saved {name}: {len(df):,} rows -> {out_path}")

    return result
