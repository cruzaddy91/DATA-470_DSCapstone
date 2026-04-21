#!/usr/bin/env python3
"""Generate the versioned v2 order-time target balance figure."""

import os
import sys

import matplotlib.pyplot as plt
import pandas as pd

from src.models.backorder_modeling import TARGET_BALANCE_FIGURE_FILE


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from poster_matplotlib_style import apply_poster_matplotlib_style
PROCESSED = os.path.join(PROJECT_ROOT, "data", "processed")
FIGURES_DIR = os.path.join(PROJECT_ROOT, "output", "figures")
os.makedirs(FIGURES_DIR, exist_ok=True)

apply_poster_matplotlib_style()

order = pd.read_csv(
    os.path.join(PROCESSED, "master_order_fulfillment_modeling_v2_ordertime.csv"),
    low_memory=False,
)

fig, axes = plt.subplots(1, 2, figsize=(10, 4))

bo_counts = order["target_backorder_risk"].value_counts(dropna=False).sort_index()
bo_labels = ["No", "Yes", "Unresolved"]
colors_bo = ["#2ecc71", "#e74c3c", "#95a5a6"]
bo_values = [bo_counts.get(0, 0), bo_counts.get(1, 0), int(order["target_backorder_risk"].isna().sum())]
axes[0].bar(bo_labels, bo_values, color=colors_bo)
axes[0].set_title("Backorder (order-time v2)")
axes[0].set_ylabel("Count")
axes[0].set_xlabel("target_backorder_risk")
for i, cnt in enumerate(bo_values):
    pct = cnt / len(order) * 100
    axes[0].text(i, cnt + 500, f"{pct:.1f}%", ha="center", fontsize=10)

inv_path = os.path.join(PROCESSED, "master_inventory_material_with_targets.csv")
if os.path.exists(inv_path):
    inv = pd.read_csv(inv_path, low_memory=False)
    if "target_overstock_risk" in inv.columns:
        ov_counts = inv["target_overstock_risk"].value_counts().sort_index()
        ov_labels = ["No (no overstock)", "Yes (overstock)"]
        colors_ov = ["#2ecc71", "#e74c3c"]
        axes[1].bar(ov_labels, [ov_counts.get(0, 0), ov_counts.get(1, 0)], color=colors_ov)
        axes[1].set_title("Overstock (material/plant)")
        axes[1].set_ylabel("Count")
        axes[1].set_xlabel("target_overstock_risk")
        for i, cnt in enumerate([ov_counts.get(0, 0), ov_counts.get(1, 0)]):
            pct = cnt / len(inv) * 100
            axes[1].text(i, cnt + 500, f"{pct:.1f}%", ha="center", fontsize=10)
    else:
        axes[1].axis("off")
else:
    axes[1].axis("off")

plt.tight_layout()
out_path = os.path.join(FIGURES_DIR, TARGET_BALANCE_FIGURE_FILE)
plt.savefig(out_path, dpi=150, bbox_inches="tight")
plt.close()
print(f"Saved {TARGET_BALANCE_FIGURE_FILE} to output/figures/")
