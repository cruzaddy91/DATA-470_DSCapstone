#!/usr/bin/env python3
"""Replace ``notebooks/01_eda_targets.ipynb``: load master tables, EDA, discovery figures.

Writes under ``output/figures/`` and ``output/tables/`` (same paths as the notebook).
Safe to run headless (no ``display()`` / no ``plt.show()``).
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402
def _project_root() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.dirname(here)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=None, help="Project root (default: parent of scripts/)")
    args = parser.parse_args()
    root = args.root or _project_root()
    processed = os.path.join(root, "data", "processed")
    db_path = os.path.join(processed, "master_tables.db")
    reports_fig = os.path.join(root, "output", "figures")
    reports_tables = os.path.join(root, "output", "tables")
    os.makedirs(reports_fig, exist_ok=True)
    os.makedirs(reports_tables, exist_ok=True)

    try:
        plt.style.use("seaborn-v0_8-whitegrid")
    except OSError:
        plt.style.use("seaborn-whitegrid")
    sns.set_theme(style="whitegrid", palette="husl", font_scale=1.1)
    plt.rcParams["figure.facecolor"] = "white"

    order = pd.read_csv(os.path.join(processed, "master_order_fulfillment_brd.csv"), low_memory=False)
    inventory = pd.read_csv(os.path.join(processed, "master_inventory_material.csv"), low_memory=False)
    purchase = pd.read_csv(os.path.join(processed, "master_purchase.csv"), low_memory=False)
    shipment_history = pd.read_csv(os.path.join(processed, "shipment_history.csv"), low_memory=False)
    woc = pd.read_csv(os.path.join(processed, "master_woc.csv"), low_memory=False)

    conn = sqlite3.connect(db_path)
    order.to_sql("order_fulfillment", conn, if_exists="replace", index=False)
    inventory.to_sql("inventory_material", conn, if_exists="replace", index=False)
    purchase.to_sql("purchase", conn, if_exists="replace", index=False)
    shipment_history.to_sql("shipment_history", conn, if_exists="replace", index=False)
    woc.to_sql("master_woc", conn, if_exists="replace", index=False)
    conn.close()
    print("Loaded into DataFrames and SQLite:", db_path)
    print("master_order_fulfillment_brd:", order.shape)
    print("master_inventory_material:", inventory.shape)
    print("master_purchase:", purchase.shape)
    print("shipment_history:", shipment_history.shape)
    print("master_woc:", woc.shape)

    if "is_open" in order.columns:
        pct_open = order["is_open"].mean() * 100
        print(f"Open orders (outstanding_qty > 0): {pct_open:.1f}%")
    if "backorder_units" in order.columns:
        n_backorder = (order["backorder_units"] > 0).sum()
        print(f"Rows with backorder_units > 0: {n_backorder:,} ({n_backorder / len(order) * 100:.1f}%)")

    conn = sqlite3.connect(db_path)
    sample = pd.read_sql_query("SELECT * FROM order_fulfillment LIMIT 5", conn)
    conn.close()
    print("Sample via SQL:\n", sample.to_string(), sep="")

    def missing_summary(df: pd.DataFrame, name: str) -> pd.DataFrame:
        m = df.isnull().sum()
        m = m[m > 0].sort_values(ascending=False)
        return pd.DataFrame({"count": m, "pct": (m / len(df) * 100).round(1)})

    missing_order = missing_summary(order, "order")
    missing_inv = missing_summary(inventory, "inventory")
    missing_purch = missing_summary(purchase, "purchase")

    print("Order fulfillment: columns with missing values:\n", missing_order.head(15).to_string())
    print("\nInventory: columns with missing values:\n", missing_inv.head(10).to_string())
    print("\nPurchase: columns with missing values:\n", missing_purch.head(10).to_string())

    missing_order.to_csv(os.path.join(reports_tables, "discovery_missing_order.csv"))
    missing_inv.to_csv(os.path.join(reports_tables, "discovery_missing_inventory.csv"))
    missing_purch.to_csv(os.path.join(reports_tables, "discovery_missing_purchase.csv"))

    fig, axes = plt.subplots(1, 3, figsize=(14, 5))
    for ax, (m_df, title) in zip(
        axes,
        [
            (missing_order.head(12), "Order Fulfillment"),
            (missing_inv.head(10), "Inventory"),
            (missing_purch.head(10), "Purchase"),
        ],
    ):
        if len(m_df) > 0:
            sns.barplot(data=m_df.reset_index(), x="pct", y="index", hue="index", palette="Blues_r", legend=False, ax=ax)
            ax.set_xlabel("Missing %")
            ax.set_ylabel("")
            ax.set_title(title)
        else:
            ax.text(0.5, 0.5, "No missing values", ha="center", va="center", transform=ax.transAxes)
    plt.tight_layout()
    plt.savefig(os.path.join(reports_fig, "discovery_missing_values.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)

    print("Order fulfillment: dtypes:\n", order.dtypes.to_string())
    num_order = order.select_dtypes(include=[np.number]).columns.tolist()
    if num_order:
        print("\nNumeric columns (order) describe:\n", order[num_order].describe().to_string())

    order["cumulative_order_quantity"] = pd.to_numeric(order["cumulative_order_quantity"], errors="coerce")
    order["total_delivery_quantity"] = pd.to_numeric(order["total_delivery_quantity"], errors="coerce")
    order["order_date"] = pd.to_datetime(order["order_date"], errors="coerce")

    neg_qty_order = (order["cumulative_order_quantity"] < 0).sum()
    neg_delivery = (order["total_delivery_quantity"] < 0).sum()
    future_dates = (order["order_date"] > pd.Timestamp.today()).sum()
    print(
        f"Order: negative order qty: {neg_qty_order}, negative delivery qty: {neg_delivery}, "
        f"future order dates: {future_dates}"
    )

    inventory["unrestricted_stock"] = pd.to_numeric(inventory["unrestricted_stock"], errors="coerce")
    neg_stock = (inventory["unrestricted_stock"] < 0).sum()
    print(f"Inventory: negative stock: {neg_stock}")

    order_key = ["client_id", "sales_document_number", "item_number"]
    inv_key = ["client_id", "material_number", "plant_code", "storage_location"]
    purch_key = ["client_id", "purchase_order_number", "purchase_order_item_number"]

    dup_order = order.duplicated(subset=order_key).sum()
    dup_inv = inventory.duplicated(subset=inv_key).sum()
    dup_purch = purchase.duplicated(subset=purch_key).sum()

    print(f"Order: {dup_order:,} duplicate rows (expected unique: client + sales_doc + item)")
    print(f"Inventory: {dup_inv:,} duplicate rows (expected unique: client + material + plant + storage)")
    print(f"Purchase: {dup_purch:,} duplicate rows (expected unique: client + PO + PO item)")

    dup_data = pd.DataFrame({"table": ["Order", "Inventory", "Purchase"], "duplicates": [dup_order, dup_inv, dup_purch]})
    fig, ax = plt.subplots(figsize=(6, 4))
    sns.barplot(data=dup_data, x="table", y="duplicates", hue="table", palette="Oranges_r", legend=False, ax=ax)
    ax.set_ylabel("Number of duplicate rows")
    ax.set_title("Duplicate rows by table (key-based)")
    plt.tight_layout()
    plt.savefig(os.path.join(reports_fig, "discovery_duplicates.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)

    qty_cols = ["cumulative_order_quantity", "total_delivery_quantity", "net_value"]
    qty_cols = [c for c in qty_cols if c in order.columns]
    order_numeric = order[qty_cols].apply(pd.to_numeric, errors="coerce")

    percentiles = order_numeric.quantile([0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99])
    print("Order fulfillment: percentiles for key numeric columns:\n", percentiles.to_string())

    plot_df = order_numeric.copy()
    for c in plot_df.columns:
        p99 = plot_df[c].quantile(0.99)
        plot_df[c] = plot_df[c].clip(upper=p99)

    fig, axes = plt.subplots(1, 3, figsize=(12, 5))
    for i, col in enumerate(plot_df.columns):
        data = plot_df[col].dropna()
        if len(data) > 0:
            axes[i].boxplot(data, vert=True)
        axes[i].set_ylabel("")
        axes[i].set_title(col.replace("_", " ").title())
    plt.suptitle("Order fulfillment: numeric distributions (capped at 99th %ile)", y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(reports_fig, "discovery_outliers_boxplot.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)

    order_mats = set(order["material_number"].dropna().astype(str).unique())
    inv_mats = set(inventory["material_number"].dropna().astype(str).unique())
    in_both = order_mats & inv_mats
    order_only = order_mats - inv_mats

    print(f"Materials in order: {len(order_mats):,}")
    print(f"Materials in inventory: {len(inv_mats):,}")
    print(f"Materials in both: {len(in_both):,}")
    print(f"Materials in order but NOT inventory: {len(order_only):,}")

    venn_data = pd.DataFrame(
        {"Overlap": [len(in_both), len(order_only), len(inv_mats - order_mats)], "Label": ["In both", "Order only", "Inventory only"]}
    )
    fig, ax = plt.subplots(figsize=(6, 4))
    sns.barplot(data=venn_data, x="Label", y="Overlap", hue="Label", palette="Set2", legend=False, ax=ax)
    ax.set_ylabel("Count")
    ax.set_title("Material overlap: Order vs Inventory")
    plt.tight_layout()
    plt.savefig(os.path.join(reports_fig, "discovery_cross_table_consistency.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)

    print("EDA discovery figures written to:", reports_fig)
    return 0


if __name__ == "__main__":
    sys.exit(main())
