"""Shared Matplotlib rcParams for capstone poster PNGs (match PowerPoint body font)."""

POSTER_FONT = "Times New Roman"


def apply_poster_matplotlib_style() -> None:
    """Use Times New Roman (serif) for all plot text; call once before creating figures."""
    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": [POSTER_FONT, "DejaVu Serif", "Bitstream Vera Serif", "Computer Modern Roman"],
            "font.size": 12,
            "axes.labelsize": 14,
            "axes.titlesize": 15,
            "xtick.labelsize": 12,
            "ytick.labelsize": 12,
            "legend.fontsize": 11,
            "figure.titlesize": 16,
        }
    )
