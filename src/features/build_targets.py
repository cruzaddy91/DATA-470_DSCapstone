"""
Build ML-ready targets from BRD metrics.

Produces:
  - master_order_fulfillment_with_targets.csv - Order-level target: target_backorder_risk (binary)
  - master_inventory_material_with_targets.csv - Material/plant-level target: target_overstock_risk (binary)

Requires: master_order_fulfillment_brd, master_woc, master_inventory_material in data/processed/
"""

from pathlib import Path
from typing import Optional, Union

import numpy as np
import pandas as pd


def _get_paths(project_root: Optional[Union[str, Path]] = None) -> dict:
    """Resolve paths for processed data."""
    if project_root is None:
        project_root = Path(__file__).resolve().parents[2]
    project_root = Path(project_root)
    return {"processed": project_root / "data" / "processed"}


def build_order_targets(master_order_brd: pd.DataFrame) -> pd.DataFrame:
    """
    Add backorder target to order fulfillment BRD.
    target_backorder_risk: 1 if backorder_units > 0, else 0.
    """
    df = master_order_brd.copy()
    df["target_backorder_risk"] = (pd.to_numeric(df.get("backorder_units", 0), errors="coerce").fillna(0) > 0).astype(int)
    return df


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

    # Overstock: high WOC when we have demand (awd > 0)
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
) -> dict:
    """
    Load BRD outputs, build targets, optionally save.
    Returns dict of {table_name: DataFrame}.
    """
    paths = _get_paths(project_root)
    processed = paths["processed"]

    order_brd_path = processed / "master_order_fulfillment_brd.csv"
    woc_path = processed / "master_woc.csv"
    inv_path = processed / "master_inventory_material.csv"

    if not order_brd_path.exists():
        raise FileNotFoundError(
            "master_order_fulfillment_brd.csv not found. Run build_brd_metrics first: "
            "python -m src.data.run_pipeline"
        )
    if not woc_path.exists():
        raise FileNotFoundError("master_woc.csv not found. Run build_brd_metrics first.")
    if not inv_path.exists():
        raise FileNotFoundError("master_inventory_material.csv not found. Run build_master_tables first.")

    order_brd = pd.read_csv(order_brd_path, low_memory=False)
    master_woc = pd.read_csv(woc_path, low_memory=False)
    master_inv = pd.read_csv(inv_path, low_memory=False)

    order_with_targets = build_order_targets(order_brd)
    inv_with_targets = build_inventory_targets(
        master_inv, master_woc, overstock_woc_threshold_weeks=overstock_woc_threshold_weeks
    )

    result = {
        "master_order_fulfillment_with_targets": order_with_targets,
        "master_inventory_material_with_targets": inv_with_targets,
    }

    if save:
        processed.mkdir(parents=True, exist_ok=True)
        for name, df in result.items():
            out_path = processed / f"{name}.csv"
            df.to_csv(out_path, index=False)
            print(f"Saved {name}: {len(df):,} rows -> {out_path}")

    return result


if __name__ == "__main__":
    build_all_targets()
