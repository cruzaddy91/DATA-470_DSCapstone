"""
Westminster University (Salt Lake City) — single poster palette for figures.

Brand colors (institutional marketing): Night, Copper, Birch, Thistle, Sky, Flint.
Use these for all capstone poster rasters so charts read as one theme.

Reference: https://westminsteru.edu/about/office-of-marketing-communication-and-events/brand-visual-identity.html
(Verify hex values if the university updates brand standards.)
"""

from __future__ import annotations

# ── Core brand ───────────────────────────────────────────────────────────────
NIGHT = "#211551"  # primary purple
COPPER = "#9D581F"  # accent
SNOW = "#FFFFFF"
FLINT = "#101820"  # near-black text
BIRCH = "#F1F1DE"  # warm light background
THISTLE = "#9063CD"  # secondary purple
SKY = "#00B5E2"  # accent (use sparingly vs. Night/Copper)

# ── Model / series (discriminable + on-brand) ─────────────────────────────────
COLOR_LOGISTIC = NIGHT  # baseline = primary brand
COLOR_LIGHTGBM = COPPER  # benchmark = copper
COLOR_BASELINE = "#6B7280"  # neutral reference lines

# ── Categorical bars (3 levels: negative / positive / other) ─────────────────
BAR_CLASS_NO = SKY
BAR_CLASS_YES = COPPER
BAR_CLASS_OTHER = THISTLE

# ── Binary inventory panel ────────────────────────────────────────────────────
BAR_INV_NO = BIRCH
BAR_INV_YES = NIGHT

# ── Histograms (score distributions) ────────────────────────────────────────
HIST_MAJORITY = SKY
HIST_MINORITY = COPPER

# ── Snapshot / cards (light tints) ──────────────────────────────────────────
TINT_HEADER = "#E8E4EF"  # Night tint
TINT_CARD_A = "#EDE5F7"  # Thistle tint
TINT_CARD_B = "#E0F4FA"  # Sky tint
TINT_CARD_C = "#F5EBE0"  # Copper tint
TINT_CARD_D = BIRCH
EDGE_SUBTLE = "#C4B8D4"


def brand_metric_heatmap_cmap():
    """Sequential colormap for metric heatmaps (Birch → purple); stops before pure Night for legible annotations."""
    from matplotlib.colors import LinearSegmentedColormap

    colors = ["#F5F4EA", "#DDD4EE", "#A67FC4", "#6E4A8C", "#4A2C6E"]
    return LinearSegmentedColormap.from_list("westminster_metrics", colors, N=256)


def brand_confusion_heatmap_cmap():
    """Same family as metric heatmaps for confusion matrices (0–1 row-normalized)."""
    return brand_metric_heatmap_cmap()
