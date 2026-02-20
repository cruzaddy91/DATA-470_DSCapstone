"""
Build master aggregated tables from clean SAP CSVs.

Reduces 20+ tables to 3 core master tables:
  1. master_order_fulfillment - Order-to-cash flow (SO + delivery + billing + material + customer)
  2. master_inventory_material - Inventory by material/plant (stock + material + plant)
  3. master_purchase - Purchase orders (PO + vendor + material)

BRD metrics (master_order_fulfillment_brd, shipment_history, master_woc) are built separately
via build_brd_metrics.py.
"""

import os
from pathlib import Path
from typing import Optional, Union

import numpy as np
import pandas as pd


def _get_paths(project_root: Optional[Union[str, Path]] = None) -> dict:
    """Resolve paths for clean and processed data."""
    if project_root is None:
        project_root = Path(__file__).resolve().parents[2]
    project_root = Path(project_root)
    return {
        "clean_main": project_root / "data" / "clean" / "main",
        "clean_supporting": project_root / "data" / "clean" / "supporting",
        "processed": project_root / "data" / "processed",
    }


def load_all_tables(paths: dict) -> dict[str, pd.DataFrame]:
    """Load all clean CSVs into a dict of DataFrames."""
    tables = {}
    for name in [
        "sales_order_header",
        "sales_order_item",
        "sales_order_schedule_line",
        "delivery_header",
        "delivery_item",
        "billing_document_header",
        "billing_document_item",
        "sales_document_status_item",
        "material_master",
        "material_description",
        "material_stock",
        "customer_master",
        "vendor_master",
        "purchase_order_header",
        "purchase_order_item",
        "purchase_order_schedule",
        "plant",
        "company_code",
    ]:
        folder = "clean_supporting" if name in ["plant", "company_code"] else "clean_main"
        p = paths[folder] / f"{name}.csv"
        if p.exists():
            tables[name] = pd.read_csv(p, low_memory=False)
    return tables


def _normalize_merge_keys(df: pd.DataFrame, keys: list) -> pd.DataFrame:
    """Convert merge keys to string for consistent joins."""
    out = df.copy()
    for k in keys:
        if k in out.columns:
            out[k] = out[k].fillna("").astype(str).str.strip()
    return out


def _canon_doc_series(s: pd.Series) -> pd.Series:
    """Canonical document number for cross-table matching (handles 4500000051 -> 51). Vectorized."""
    n = pd.to_numeric(s, errors="coerce")
    out = (n.fillna(0).astype(np.int64) % 100000000).astype(str)
    return out.where(~n.isna(), "")


def _canon_item_series(s: pd.Series) -> pd.Series:
    """Canonical item number for cross-table matching. Vectorized."""
    n = pd.to_numeric(s, errors="coerce")
    out = n.fillna(0).astype(np.int64).astype(str)
    return out.where(~n.isna(), "")


def build_master_order_fulfillment(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Build master order fulfillment table.
    Grain: one row per sales order item.
    Joins: SO header, delivery (aggregated), billing (aggregated), material, customer, status.
    """
    merge_keys = ["client_id", "sales_document_number", "item_number"]
    so_item = _normalize_merge_keys(tables["sales_order_item"].copy(), merge_keys)
    so_item["_doc"] = _canon_doc_series(so_item["sales_document_number"])
    so_item["_item"] = _canon_item_series(so_item["item_number"])
    so_header = _normalize_merge_keys(tables["sales_order_header"].copy(), ["client_id", "sales_document_number"])
    delivery_item = tables["delivery_item"].copy()
    billing_item = tables["billing_document_item"].copy()
    material_master = tables["material_master"].copy()
    material_desc = tables["material_description"].copy()
    customer = tables["customer_master"].copy()
    status_item = tables["sales_document_status_item"].copy()
    schedule = tables["sales_order_schedule_line"].copy()

    # --- Aggregations ---
    # Delivery: aggregate by reference to SO. Use canonical doc/item keys for cross-table matching
    # (handles format differences such as 4500000051 vs 51).
    del_cols = ["reference_document_number", "reference_item_number", "client_id"]
    if all(c in delivery_item.columns for c in del_cols):
        del_df = delivery_item.copy()
        del_df["client_id"] = del_df["client_id"].astype(str)
        del_df["_doc"] = _canon_doc_series(del_df["reference_document_number"])
        del_df["_item"] = _canon_item_series(del_df["reference_item_number"])
        del_df = del_df[del_df["_doc"] != ""]
        del_agg = (
            del_df.groupby(["client_id", "_doc", "_item"], dropna=False)
            .agg(
                total_delivery_quantity=("delivery_quantity", "sum"),
                total_quantity_delivered=("quantity_delivered", "sum"),
            )
            .reset_index()
        )
    else:
        del_agg = pd.DataFrame(
            columns=["client_id", "_doc", "_item", "total_delivery_quantity", "total_quantity_delivered"]
        )

    # Billing: aggregate by order reference (aubel = order doc, aupos = order item). Use canonical keys.
    if "aubel" in billing_item.columns and "aupos" in billing_item.columns:
        bill_df = billing_item.copy()
        bill_df["client_id"] = bill_df["client_id"].astype(str)
        bill_df["_doc"] = _canon_doc_series(bill_df["aubel"])
        bill_df["_item"] = _canon_item_series(bill_df["aupos"])
        bill_df = bill_df[bill_df["_doc"] != ""]
        bill_agg = (
            bill_df.groupby(["client_id", "_doc", "_item"], dropna=False)
            .agg(
                total_billed_quantity=("billed_quantity", "sum"),
                total_billed_value=("net_value", "sum"),
            )
            .reset_index()
        )
    else:
        bill_agg = pd.DataFrame(
            columns=["client_id", "_doc", "_item", "total_billed_quantity", "total_billed_value"]
        )

    # Schedule: take first requested date and sum quantities per SO item
    sched_cols = ["client_id", "sales_document_number", "item_number"]
    if all(c in schedule.columns for c in sched_cols):
        sched_norm = _normalize_merge_keys(schedule, sched_cols)
        sched_agg = (
            sched_norm.groupby(sched_cols, dropna=False)
            .agg(
                requested_delivery_date_schedule=("requested_delivery_date_schedule", "first"),
                total_requested_quantity=("requested_quantity", "sum"),
                total_confirmed_quantity=("confirmed_quantity", "sum"),
            )
            .reset_index()
        )
    else:
        sched_agg = pd.DataFrame(
            columns=sched_cols + ["requested_delivery_date_schedule", "total_requested_quantity", "total_confirmed_quantity"]
        )
    if len(sched_agg) > 0:
        sched_agg = _normalize_merge_keys(sched_agg, sched_cols)

    # --- Joins ---
    # SO item + SO header
    so_item = so_item.merge(
        so_header[
            [
                "client_id",
                "sales_document_number",
                "customer_number",
                "order_date",
                "requested_delivery_date",
                "net_value",
                "currency_code",
                "sales_organization",
                "distribution_channel",
                "division",
            ]
        ].rename(columns={"net_value": "order_header_net_value"}),
        on=["client_id", "sales_document_number"],
        how="left",
    )

    # + delivery agg (on canonical keys)
    so_item = so_item.merge(del_agg, on=["client_id", "_doc", "_item"], how="left")

    # + billing agg (on canonical keys)
    so_item = so_item.merge(bill_agg, on=["client_id", "_doc", "_item"], how="left")

    # drop canonical helper columns
    so_item = so_item.drop(columns=["_doc", "_item"], errors="ignore")

    # + schedule agg
    so_item = so_item.merge(sched_agg, on=["client_id", "sales_document_number", "item_number"], how="left")

    # + material master (key attributes only)
    mat_cols = ["client_id", "material_number", "material_type", "material_group", "base_unit_of_measure"]
    mat_cols = [c for c in mat_cols if c in material_master.columns]
    if mat_cols:
        mat_sub = _normalize_merge_keys(
            material_master[mat_cols].drop_duplicates(subset=["client_id", "material_number"]),
            ["client_id", "material_number"],
        )
        so_item = so_item.merge(mat_sub, on=["client_id", "material_number"], how="left")

    # + material description (first language)
    if "material_number" in material_desc.columns:
        mat_desc_first = (
            material_desc.groupby(["client_id", "material_number"])
            .first()
            .reset_index()[["client_id", "material_number", "material_description_text"]]
        )
        mat_desc_first = _normalize_merge_keys(mat_desc_first, ["client_id", "material_number"])
        so_item = so_item.merge(
            mat_desc_first,
            on=["client_id", "material_number"],
            how="left",
        )

    # + customer
    cust_cols = ["client_id", "customer_number", "country_code", "name_line_1"]
    cust_cols = [c for c in cust_cols if c in customer.columns]
    if cust_cols:
        cust_sub = (
            customer[cust_cols]
            .rename(columns={"name_line_1": "customer_name"})
            .drop_duplicates(subset=["client_id", "customer_number"])
        )
        cust_sub = _normalize_merge_keys(cust_sub, ["client_id", "customer_number"])
        so_item = so_item.merge(
            cust_sub,
            on=["client_id", "customer_number"],
            how="left",
        )

    # + status
    status_cols = [
        "client_id",
        "sales_document_number",
        "item_number",
        "delivery_status",
        "billing_status",
        "goods_movement_status",
    ]
    status_cols = [c for c in status_cols if c in status_item.columns]
    if status_cols:
        status_sub = (
            status_item[status_cols]
            .drop_duplicates(subset=["client_id", "sales_document_number", "item_number"])
        )
        status_sub = _normalize_merge_keys(
            status_sub,
            ["client_id", "sales_document_number", "item_number"],
        )
        so_item = so_item.merge(
            status_sub,
            on=["client_id", "sales_document_number", "item_number"],
            how="left",
        )

    # --- Select and order columns ---
    keep = [
        "client_id",
        "sales_document_number",
        "item_number",
        "material_number",
        "material_description",
        "material_description_text",
        "material_type",
        "material_group",
        "product_hierarchy",
        "item_category",
        "customer_number",
        "customer_name",
        "country_code",
        "order_date",
        "requested_delivery_date",
        "requested_delivery_date_schedule",
        "cumulative_order_quantity",
        "cumulative_confirmed_quantity",
        "total_requested_quantity",
        "total_confirmed_quantity",
        "total_delivery_quantity",
        "total_quantity_delivered",
        "total_billed_quantity",
        "total_billed_value",
        "net_value",
        "order_header_net_value",
        "currency_code",
        "base_unit_of_measure",
        "sales_unit",
        "sales_organization",
        "distribution_channel",
        "division",
        "plant_code",
        "storage_location",
        "delivery_status",
        "billing_status",
        "goods_movement_status",
    ]
    keep = [c for c in keep if c in so_item.columns]
    out = so_item[keep].copy()

    # Ensure one row per sales order item (grain)
    out = out.drop_duplicates(
        subset=["client_id", "sales_document_number", "item_number"],
        keep="first",
    )

    return out


def build_master_inventory_material(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Build master inventory/material table.
    Grain: one row per material + plant (+ storage_location if needed).
    Joins: material_stock + material_master + material_description + plant.
    """
    stock = tables["material_stock"].copy()
    material = tables["material_master"].copy()
    desc = tables["material_description"].copy()
    plant = tables["plant"].copy()

    # Material master: key attributes
    mat_cols = [
        "client_id",
        "material_number",
        "material_type",
        "material_group",
        "base_unit_of_measure",
    ]
    mat_cols = [c for c in mat_cols if c in material.columns]
    stock = stock.merge(
        material[mat_cols].drop_duplicates(subset=["client_id", "material_number"]),
        on=["client_id", "material_number"],
        how="left",
    )

    # Material description
    if "material_number" in desc.columns:
        desc_first = (
            desc.groupby(["client_id", "material_number"])
            .first()
            .reset_index()[["client_id", "material_number", "material_description_text"]]
        )
        stock = stock.merge(desc_first, on=["client_id", "material_number"], how="left")

    # Plant
    plant_cols = ["client_id", "plant_code", "name_line_1", "country_code"]
    plant_cols = [c for c in plant_cols if c in plant.columns]
    if plant_cols:
        stock = stock.merge(
            plant[plant_cols].rename(columns={"name_line_1": "plant_name"}),
            on=["client_id", "plant_code"],
            how="left",
        )

    keep = [
        "client_id",
        "material_number",
        "material_description_text",
        "material_type",
        "material_group",
        "plant_code",
        "plant_name",
        "storage_location",
        "unrestricted_stock",
        "returns",
        "stock_in_quality_inspection",
        "restricted_use_stock",
        "blocked_stock",
        "base_unit_of_measure",
        "country_code",
    ]
    keep = [c for c in keep if c in stock.columns]
    return stock[keep].copy()


def build_master_purchase(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Build master purchase table.
    Grain: one row per purchase order item.
    Joins: PO item + PO header + vendor + material.
    """
    po_item = tables["purchase_order_item"].copy()
    po_header = tables["purchase_order_header"].copy()
    vendor = tables["vendor_master"].copy()
    material = tables["material_master"].copy()

    # PO header
    header_cols = [
        "client_id",
        "purchase_order_number",
        "vendor_number",
        "purchasing_document_date",
        "currency_code",
    ]
    header_cols = [c for c in header_cols if c in po_header.columns]
    po_item = po_item.merge(
        po_header[header_cols],
        on=["client_id", "purchase_order_number"],
        how="left",
        suffixes=("", "_header"),
    )

    # Vendor
    vendor_cols = ["client_id", "vendor_number", "name_line_1", "country_code"]
    vendor_cols = [c for c in vendor_cols if c in vendor.columns]
    if vendor_cols:
        po_item = po_item.merge(
            vendor[vendor_cols].rename(columns={"name_line_1": "vendor_name"}),
            on=["client_id", "vendor_number"],
            how="left",
        )

    # Material
    mat_cols = ["client_id", "material_number", "material_type", "material_group", "base_unit_of_measure"]
    mat_cols = [c for c in mat_cols if c in material.columns]
    if mat_cols:
        po_item = po_item.merge(
            material[mat_cols].drop_duplicates(subset=["client_id", "material_number"]),
            on=["client_id", "material_number"],
            how="left",
        )

    keep = [
        "client_id",
        "purchase_order_number",
        "purchase_order_item_number",
        "material_number",
        "material_type",
        "material_group",
        "vendor_number",
        "vendor_name",
        "country_code",
        "quantity",
        "base_unit_of_measure",
        "net_price",
        "net_value",
        "purchasing_document_date",
        "currency_code",
        "company_code",
        "plant_code",
    ]
    keep = [c for c in keep if c in po_item.columns]
    return po_item[keep].copy()


def build_all_master_tables(
    project_root: Optional[Union[str, Path]] = None,
    save: bool = True,
) -> dict:
    """
    Load clean CSVs, build all master tables, optionally save to data/processed/.
    Returns dict of {table_name: DataFrame}.
    """
    paths = _get_paths(project_root)
    tables = load_all_tables(paths)

    master_order = build_master_order_fulfillment(tables)
    master_inv = build_master_inventory_material(tables)
    master_po = build_master_purchase(tables)

    result = {
        "master_order_fulfillment": master_order,
        "master_inventory_material": master_inv,
        "master_purchase": master_po,
    }

    if save:
        paths["processed"].mkdir(parents=True, exist_ok=True)
        for name, df in result.items():
            out_path = paths["processed"] / f"{name}.csv"
            df.to_csv(out_path, index=False)
            print(f"Saved {name}: {len(df):,} rows -> {out_path}")

    return result
