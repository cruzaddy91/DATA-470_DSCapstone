"""Shared Matplotlib rcParams for capstone poster PNGs and PDF report figures."""

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
            # STIX is Times-like for math labels in axis titles / annotations.
            "mathtext.fontset": "stix",
        }
    )


def apply_report_matplotlib_style() -> None:
    """Serif text sized for PDF report figures (Quarto body uses Times New Roman).

    Call after ``seaborn.set_theme`` so it overrides seaborn's default sans-serif.
    """
    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": [POSTER_FONT, "Times", "Nimbus Roman", "DejaVu Serif", "Bitstream Vera Serif"],
            "font.size": 10,
            "axes.labelsize": 10,
            "axes.titlesize": 11,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "legend.fontsize": 9,
            "figure.titlesize": 12,
            "mathtext.fontset": "stix",
        }
    )
