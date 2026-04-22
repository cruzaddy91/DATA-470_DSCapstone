#!/usr/bin/env python3
"""Generate the versioned v2 order-time target balance figure.

Output is sized for ``ppt/media/image2.png`` on the fact poster (default 1773×694 px) so the
overlay does not heavily downscale bar labels.
"""

import os
import sys

import matplotlib.pyplot as plt
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.models.backorder_modeling import TARGET_BALANCE_FIGURE_FILE
from poster_matplotlib_style import apply_poster_matplotlib_style
from westminster_poster_palette import BAR_CLASS_NO, BAR_CLASS_OTHER, BAR_CLASS_YES, FLINT

PROCESSED = os.path.join(PROJECT_ROOT, "data", "processed")
FIGURES_DIR = os.path.join(PROJECT_ROOT, "output", "figures")
os.makedirs(FIGURES_DIR, exist_ok=True)

apply_poster_matplotlib_style()

_px_w = int(os.environ.get("POSTER_TARGET_BALANCE_EMBED_W", "1773"))
_px_h = int(os.environ.get("POSTER_TARGET_BALANCE_EMBED_H", "694"))
_dpi = int(os.environ.get("POSTER_TARGET_BALANCE_DPI", "200"))

order = pd.read_csv(
    os.path.join(PROCESSED, "master_order_fulfillment_modeling_v2_ordertime.csv"),
    low_memory=False,
)

fig, axes = plt.subplots(
    1,
    2,
    figsize=(_px_w / _dpi, _px_h / _dpi),
    facecolor="white",
)
fig.subplots_adjust(left=0.08, right=0.98, top=0.88, bottom=0.14, wspace=0.22)

bo_counts = order["target_backorder_risk"].value_counts(dropna=False).sort_index()
bo_labels = ["No", "Yes", "Unresolved"]
colors_bo = [BAR_CLASS_NO, BAR_CLASS_YES, BAR_CLASS_OTHER]
bo_values = [bo_counts.get(0, 0), bo_counts.get(1, 0), int(order["target_backorder_risk"].isna().sum())]
axes[0].bar(bo_labels, bo_values, color=colors_bo, edgecolor=FLINT, linewidth=0.5)
axes[0].set_title("Backorder (order-time v2)", fontsize=13, fontweight="bold", pad=6)
axes[0].set_ylabel("Count", fontsize=11, fontweight="bold")
axes[0].set_xlabel("target_backorder_risk", fontsize=11, fontweight="bold")
axes[0].tick_params(axis="both", labelsize=11)
for i, cnt in enumerate(bo_values):
    pct = cnt / len(order) * 100
    axes[0].text(i, cnt + 500, f"{pct:.1f}%", ha="center", fontsize=10, fontweight="bold")

inv_path = os.path.join(PROCESSED, "master_inventory_material_with_targets.csv")
if os.path.exists(inv_path):
    inv = pd.read_csv(inv_path, low_memory=False)
    if "target_overstock_risk" in inv.columns:
        ov_counts = inv["target_overstock_risk"].value_counts().sort_index()
        ov_labels = ["No (no overstock)", "Yes (overstock)"]
        colors_ov = [BAR_CLASS_NO, BAR_CLASS_YES]
        axes[1].bar(ov_labels, [ov_counts.get(0, 0), ov_counts.get(1, 0)], color=colors_ov, edgecolor=FLINT, linewidth=0.5)
        axes[1].set_title("Overstock (material/plant)", fontsize=13, fontweight="bold", pad=6)
        axes[1].set_ylabel("Count", fontsize=11, fontweight="bold")
        axes[1].set_xlabel("target_overstock_risk", fontsize=11, fontweight="bold")
        axes[1].tick_params(axis="both", labelsize=10)
        for i, cnt in enumerate([ov_counts.get(0, 0), ov_counts.get(1, 0)]):
            pct = cnt / len(inv) * 100
            axes[1].text(i, cnt + 500, f"{pct:.1f}%", ha="center", fontsize=10, fontweight="bold")
    else:
        axes[1].axis("off")
else:
    axes[1].axis("off")

out_path = os.path.join(FIGURES_DIR, TARGET_BALANCE_FIGURE_FILE)
plt.savefig(out_path, dpi=_dpi, bbox_inches=None, facecolor="white", pad_inches=0)
plt.close()
print(f"Saved {TARGET_BALANCE_FIGURE_FILE} to output/figures/")
