"""
Westminster University (Salt Lake City) — single poster palette for figures.

Brand colors (institutional marketing): Night, Copper, Birch, Thistle, Sky, Flint, Snow.
Use these for all capstone poster rasters so charts read as one theme.

**Pairing rule:** Sky (#00B5E2) and Copper (#9D581F) are not used as co-dominant fills in the
same figure (they read harsh side-by-side). Each color appears at least once across the
template figure set (bars, lines, heatmap ramp, axes, Mermaid exports).

Reference: https://westminsteru.edu/about/office-of-marketing-communication-and-events/brand-visual-identity.html
(Verify hex values if the university updates brand standards.)
"""

from __future__ import annotations

# ── Core brand ───────────────────────────────────────────────────────────────
# Exact digital values from Westminster University brand visual identity (Apr 2026).
NIGHT = "#211551"  # primary purple — PMS 274 C
COPPER = "#9D581F"  # — PMS 10134 Metallic
SNOW = "#FFFFFF"
FLINT = "#101820"  # PMS Black 6 C
BIRCH = "#F1F1DE"  # PMS 9064 C
THISTLE = "#8252C7"  # PMS 265 C (secondary purple)
SKY = "#00B5E2"  # PMS 306 C

# ── Model / series (discriminable + on-brand) ─────────────────────────────────
COLOR_LOGISTIC = NIGHT  # baseline = primary brand
COLOR_LIGHTGBM = COPPER  # benchmark = copper
COLOR_BASELINE = FLINT  # dashed reference (random / prevalence) — brand dark, not gray

# ── Categorical bars — target balance 3-class (No / Yes / Unresolved) ───────
# Night + Copper + Thistle: Sky reserved for other charts to avoid Sky+Copper clash.
BAR_CLASS_NO = NIGHT
BAR_CLASS_YES = COPPER
BAR_CLASS_OTHER = THISTLE

# ── Binary inventory panel (distinct from 3-class target; Birch + Sky, no Copper) ─
BAR_INV_NO = BIRCH
BAR_INV_YES = SKY

# ── Histograms (score distributions): Sky (majority) + Night (minority) — no Copper + Sky
HIST_MAJORITY = SKY
HIST_MINORITY = NIGHT

# ── Snapshot / cards (light tints) ──────────────────────────────────────────
TINT_HEADER = "#E8E4EF"  # Night tint
TINT_CARD_A = "#EDE5F7"  # Thistle tint
TINT_CARD_B = "#E0F4FA"  # Sky tint
TINT_CARD_C = "#F5EBE0"  # Copper tint
TINT_CARD_D = BIRCH
EDGE_SUBTLE = "#C4B8D4"


def brand_metric_heatmap_cmap():
    """Sequential colormap: Birch → Sky wash → Thistle → Night (every cool brand hue in the ramp)."""
    from matplotlib.colors import LinearSegmentedColormap

    # Sky enters as TINT_CARD_B wash (institutional cyan) before purples — no Copper in ramp.
    colors = [
        BIRCH,
        TINT_CARD_B,
        "#D8C8EF",
        "#B596DC",
        THISTLE,
        "#6E4A9E",
        "#3D2A66",
    ]
    return LinearSegmentedColormap.from_list("westminster_metrics", colors, N=256)


def brand_confusion_heatmap_cmap():
    """Same family as metric heatmaps for confusion matrices (0–1 row-normalized)."""
    return brand_metric_heatmap_cmap()
