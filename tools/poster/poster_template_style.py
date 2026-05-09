"""Distinct Seaborn-forward styling for spec-driven poster PNGs (canonical YAML → template render).

PowerPoint body text remains Times New Roman; figures use this template so they are visibly
different from the legacy ``poster_matplotlib_style`` charts while still print-safe.
"""

from __future__ import annotations

TEMPLATE_ID = "westminster_brand_v1"

# Sans-serif for chart labels (high legibility at poster size)
_TEMPLATE_FONT = "DejaVu Sans"


def apply_poster_template_style() -> None:
    """Set matplotlib + seaborn theme for template-rendered figures. Call once per process."""
    import matplotlib.pyplot as plt
    import seaborn as sns

    from westminster_poster_palette import BIRCH, COPPER, FLINT, NIGHT, SKY, THISTLE

    sns.set_theme(
        style="ticks",
        context="talk",
        font=_TEMPLATE_FONT,
        palette=[NIGHT, COPPER, THISTLE, SKY],
    )
    plt.rcParams.update(
        {
            "font.family": "sans-serif",
            "font.sans-serif": [_TEMPLATE_FONT, "Helvetica", "Arial", "DejaVu Sans"],
            "font.size": 11,
            "axes.titlesize": 14,
            "axes.labelsize": 12,
            "xtick.labelsize": 11,
            "ytick.labelsize": 11,
            "legend.fontsize": 10,
            "figure.titlesize": 15,
            "figure.facecolor": "white",
            "axes.facecolor": BIRCH,
            "axes.edgecolor": NIGHT,
            "axes.labelcolor": FLINT,
            "text.color": FLINT,
            "axes.grid": True,
            "grid.alpha": 0.35,
            "grid.color": "#D1C9E0",
        }
    )


def heatmap_cmap():
    """Westminster sequential scale for metric / confusion heatmaps."""
    from westminster_poster_palette import brand_metric_heatmap_cmap

    return brand_metric_heatmap_cmap()
