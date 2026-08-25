#!/usr/bin/env python3
"""
Build temporal-safe rolling features for v2 order-time modeling.

For each order, computes per-cohort aggregates using ONLY data strictly
before the order's date. This is the cheapest way to inject real signal
into a model whose current feature set is just transactional attributes.

New features added:
  plant_backorder_rate_90d          plant's backorder rate in last 90 days
  material_backorder_rate_90d       material's rate in last 90 days
  customer_backorder_rate_90d       customer's rate in last 90 days
  plant_order_volume_30d            plant's order count in last 30 days
  material_order_volume_30d         material's order count in last 30 days
  customer_order_volume_30d         customer's order count in last 30 days
  plant_confirmation_rate_90d       avg confirmed_qty / ordered_qty at plant, last 90d

Temporal safety:
  Every rolling window ENDS at `order_date - 1 day`. Orders on the exact
  same date are excluded from each other's windows. Prevents leakage.

Output:
  data/processed/master_order_fulfillment_modeling_v2_ordertime_rollup.csv
  (same shape + 7 new numeric columns)
"""

from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
IN_PATH = PROJECT_ROOT / "data" / "processed" / "master_order_fulfillment_modeling_v2_ordertime.csv"
OUT_PATH = PROJECT_ROOT / "data" / "processed" / "master_order_fulfillment_modeling_v2_ordertime_rollup.csv"


def _rolling_cohort_past(
    df: pd.DataFrame,
    cohort_col: str,
    value_col: str,
    window_days: int,
    agg: str,
) -> pd.Series:
    """
    For each row, return agg(value_col) over rows in the same cohort whose
    order_date falls in [current - window_days, current - 1 day].

    Implementation: process each cohort independently — sort by date, build
    a cumulative-sum view, compute window_days-bounded aggregate with a
    1-day end-shift to exclude same-day orders. Returns a Series aligned
    to df's original integer index.
    """
    out = pd.Series(index=df.index, dtype=float)
    # Group positional indices by cohort key.
    for cohort_key, group_idx in df.groupby(cohort_col, observed=True).indices.items():
        sub = df.loc[group_idx, ["order_date", value_col]].copy()
        sub = sub.sort_values("order_date")
        # Build a DatetimeIndex; same-day rows handled by end-shift below.
        s = pd.Series(sub[value_col].to_numpy(), index=pd.DatetimeIndex(sub["order_date"]))
        # Rolling over time with window_days; closed='left' excludes the
        # current timestamp and everything after from the window. Any rows
        # at the same timestamp are then excluded from each other's window.
        if agg == "mean":
            rolled = s.rolling(f"{window_days}D", closed="left").mean()
        elif agg == "sum":
            rolled = s.rolling(f"{window_days}D", closed="left").sum()
        else:
            raise ValueError(agg)
        # Map back to original positional indices (preserving sub's sort order).
        out.iloc[sub.index] = rolled.to_numpy()
    return out


def _rolling_cohort_mean_past(df, cohort_col, value_col, window_days, new_col):
    s = _rolling_cohort_past(df, cohort_col, value_col, window_days, "mean")
    s.name = new_col
    return s


def _rolling_cohort_count_past(df, cohort_col, window_days, new_col):
    work = df.copy()
    work["_one"] = 1.0
    s = _rolling_cohort_past(work, cohort_col, "_one", window_days, "sum")
    s.name = new_col
    return s


def main() -> int:
    t0 = time.time()
    print(f"Loading {IN_PATH}...", flush=True)
    df = pd.read_csv(IN_PATH, low_memory=False)
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    before = len(df)
    df = df.dropna(subset=["order_date"]).reset_index(drop=True)
    print(f"  rows: {before} -> {len(df)} (dropped rows w/ NaT order_date)", flush=True)

    # Ensure numeric types.
    for c in ["target_backorder_risk", "cumulative_order_quantity", "cumulative_confirmed_quantity"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["target_backorder_risk"] = df["target_backorder_risk"].fillna(0).astype(int)

    df["_confirmation_ratio"] = np.where(
        df["cumulative_order_quantity"].fillna(0) > 0,
        df["cumulative_confirmed_quantity"].fillna(0) / df["cumulative_order_quantity"].replace(0, np.nan),
        np.nan,
    )

    # Build each feature. _rolling_cohort_mean_past returns the mean of the
    # value column over the past window, grouped by cohort. For
    # target_backorder_risk that's the rolling backorder rate; for
    # _confirmation_ratio that's the average confirmation rate.
    for cohort_col, win, prefix in [
        ("plant_code", 90, "plant"),
        ("material_number", 90, "material"),
        ("customer_number", 90, "customer"),
    ]:
        print(f"  rolling backorder_rate_90d by {cohort_col}...", flush=True)
        s = _rolling_cohort_mean_past(
            df, cohort_col, "target_backorder_risk", win, f"{prefix}_backorder_rate_90d"
        )
        df[f"{prefix}_backorder_rate_90d"] = s.astype(float)

    for cohort_col, win, prefix in [
        ("plant_code", 30, "plant"),
        ("material_number", 30, "material"),
        ("customer_number", 30, "customer"),
    ]:
        print(f"  rolling order_volume_30d by {cohort_col}...", flush=True)
        s = _rolling_cohort_count_past(df, cohort_col, win, f"{prefix}_order_volume_30d")
        df[f"{prefix}_order_volume_30d"] = s.fillna(0).astype(float)

    print("  rolling confirmation_rate_90d by plant_code...", flush=True)
    s = _rolling_cohort_mean_past(
        df, "plant_code", "_confirmation_ratio", 90, "plant_confirmation_rate_90d"
    )
    df["plant_confirmation_rate_90d"] = s.astype(float)

    df = df.drop(columns=["_confirmation_ratio"], errors="ignore")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False)
    elapsed = time.time() - t0
    print(f"\nWrote {OUT_PATH} ({len(df)} rows, {df.shape[1]} cols) in {elapsed:.1f}s", flush=True)
    print("\nNew feature coverage (non-null %):")
    for c in [
        "plant_backorder_rate_90d",
        "material_backorder_rate_90d",
        "customer_backorder_rate_90d",
        "plant_order_volume_30d",
        "material_order_volume_30d",
        "customer_order_volume_30d",
        "plant_confirmation_rate_90d",
    ]:
        if c in df.columns:
            pct = 100.0 * df[c].notna().mean()
            print(f"  {c:36s}  {pct:5.1f}%  mean={df[c].mean():.4f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
