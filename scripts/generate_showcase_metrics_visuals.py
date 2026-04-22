import os
import sys
from pathlib import Path

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import pandas as pd
import seaborn as sns


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from poster_matplotlib_style import POSTER_FONT, apply_poster_matplotlib_style
from westminster_poster_palette import (
    BIRCH,
    EDGE_SUBTLE,
    FLINT,
    NIGHT,
    SNOW,
    TINT_CARD_A,
    TINT_CARD_B,
    TINT_CARD_C,
    TINT_CARD_D,
    TINT_HEADER,
    brand_metric_heatmap_cmap,
)


def _annot_text_color(value: float, vmin: float, vmax: float, cmap_obj) -> str:
    """Match poster_visual_templates: light cells get Flint, dark cells Snow (Westminster legibility)."""
    if vmax <= vmin:
        return FLINT
    t = (float(value) - vmin) / (vmax - vmin)
    t = max(0.0, min(1.0, t))
    rgba = cmap_obj(t)
    rgb = mcolors.to_rgb(rgba)
    lum = 0.2126 * rgb[0] + 0.7152 * rgb[1] + 0.0722 * rgb[2]
    return SNOW if lum < 0.48 else FLINT

TABLE_PATH = REPO_ROOT / "output" / "tables" / "classification_model_comparison_v2_ordertime.csv"
FIGURES_DIR = REPO_ROOT / "output" / "figures"
HEATMAP_PATH = FIGURES_DIR / "showcase_model_comparison_heatmap.png"
SNAPSHOT_PATH = FIGURES_DIR / "showcase_temporal_snapshot.png"

# Native size of ``ppt/media/image3.png`` on ``DS_Capstone_Poster_FINAL_SCRIPT_COPY.pptx`` (fact overlay
# letterboxes to this box; generating at this resolution avoids heavy downscale blur).
def _poster_heatmap_embed_px() -> tuple[int, int]:
    w = int(os.environ.get("POSTER_HEATMAP_EMBED_W", "2821"))
    h = int(os.environ.get("POSTER_HEATMAP_EMBED_H", "1353"))
    return w, h


MODEL_LABELS = {
    "logistic_regression": "Logistic",
    "random_forest": "Random Forest",
    "xgboost": "XGBoost",
    "lightgbm": "LightGBM",
}

MODEL_ORDER = ["logistic_regression", "random_forest", "xgboost", "lightgbm"]
# Poster hero heatmap: only models discussed in narrative & diagrams (easier to read than 4×3 grids)
POSTER_MODEL_ORDER = ["logistic_regression", "lightgbm"]
SPLIT_ORDER = ["temporal_holdout", "group_holdout", "recent_24_week_temporal_holdout"]
# Poster heatmap: only the two stable splits (recent 24-wk has ~14 positives — F1/PR look “broken”)
POSTER_SPLIT_ORDER = ["temporal_holdout", "group_holdout"]
METRICS = [
    (
        "roc_auc",
        "ROC-AUC",
        "Threshold-free ranking; stays interpretable under imbalance",
    ),
    (
        "pr_auc",
        "PR-AUC",
        "Stress-tests on rare positives (often << grouped split)",
    ),
]

# Multi-line column headers: what each split means (poster is read at a glance)
SPLIT_LABELS = {
    "temporal_holdout": "Temporal\n(primary)",
    "group_holdout": "Grouped\n(diagnostic)",
    "recent_24_week_temporal_holdout": "Recent 24 wk\n(small test n)",
}


def _load_comparison_frame() -> pd.DataFrame:
    df = pd.read_csv(TABLE_PATH)
    df = df[df["split"].isin(SPLIT_ORDER) & df["model"].isin(MODEL_ORDER)].copy()
    df["split_label"] = pd.Categorical(df["split"].map(SPLIT_LABELS), categories=[SPLIT_LABELS[s] for s in SPLIT_ORDER], ordered=True)
    df["model_label"] = pd.Categorical(df["model"].map(MODEL_LABELS), categories=[MODEL_LABELS[m] for m in MODEL_ORDER], ordered=True)
    return df.sort_values(["model_label", "split_label"]).reset_index(drop=True)


def _add_best_outlines(ax, metric_frame: pd.DataFrame) -> None:
    for col_idx, col_name in enumerate(metric_frame.columns):
        best_row = metric_frame[col_name].astype(float).idxmax()
        row_idx = metric_frame.index.get_loc(best_row)
        rect = patches.Rectangle(
            (col_idx, row_idx),
            1,
            1,
            fill=False,
            edgecolor=NIGHT,
            linewidth=2.0,
        )
        ax.add_patch(rect)


def save_model_comparison_heatmap() -> Path:
    apply_poster_matplotlib_style()
    sns.set_theme(style="whitegrid", font=POSTER_FONT)
    df = _load_comparison_frame()
    df = df[df["model"].isin(POSTER_MODEL_ORDER)].copy()
    df["model_label"] = pd.Categorical(
        df["model"].map(MODEL_LABELS),
        categories=[MODEL_LABELS[m] for m in POSTER_MODEL_ORDER],
        ordered=True,
    )

    px_w, px_h = _poster_heatmap_embed_px()
    dpi = int(os.environ.get("POSTER_HEATMAP_DPI", "200"))
    fig_w_in, fig_h_in = px_w / dpi, px_h / dpi
    fig, axes = plt.subplots(1, 2, figsize=(fig_w_in, fig_h_in), facecolor="white")
    fig.subplots_adjust(left=0.07, right=0.99, top=0.86, bottom=0.20, wspace=0.30)
    fig.suptitle(
        "Logistic vs. LightGBM — ranking metrics under stress splits (not F1 at a fixed threshold)",
        fontsize=15,
        fontweight="bold",
        y=0.98,
        color=FLINT,
    )

    brand_cmap = brand_metric_heatmap_cmap()
    for ax, (metric_key, metric_title, metric_blurb) in zip(axes, METRICS):
        piv = df.pivot(index="model_label", columns="split_label", values=metric_key).astype(float)
        row_order = [MODEL_LABELS[m] for m in POSTER_MODEL_ORDER if MODEL_LABELS[m] in piv.index]
        col_order = [SPLIT_LABELS[s] for s in POSTER_SPLIT_ORDER if SPLIT_LABELS[s] in piv.columns]
        if not row_order or not col_order:
            raise ValueError("comparison table missing expected model or split labels")
        metric_frame = piv.loc[row_order, col_order]
        if metric_key == "roc_auc":
            vmin_m, vmax_m = 0.5, 1.0
            cbar_label = "ROC-AUC (0.5 = random, 1 = perfect)"
        else:
            arr = metric_frame.to_numpy().astype(float)
            vmax_m = max(0.55, float(arr.max()), 0.05)
            vmin_m = 0.0
            cbar_label = "PR-AUC (higher = better ranking of positives)"
        sns.heatmap(
            metric_frame,
            ax=ax,
            annot=False,
            cmap=brand_cmap,
            vmin=vmin_m,
            vmax=vmax_m,
            linewidths=1.8,
            linecolor=SNOW,
            cbar=True,
            cbar_kws={
                "shrink": 0.78,
                "pad": 0.04,
                "label": cbar_label,
            },
        )
        for i in range(len(metric_frame.index)):
            for j in range(len(metric_frame.columns)):
                val = float(metric_frame.iloc[i, j])
                tc = _annot_text_color(val, vmin_m, vmax_m, brand_cmap)
                ax.text(
                    j + 0.5,
                    i + 0.5,
                    f"{val:.2f}",
                    ha="center",
                    va="center",
                    fontsize=17,
                    fontweight="bold",
                    color=tc,
                )
        _add_best_outlines(ax, metric_frame)
        ax.set_title(
            f"{metric_title}\n{metric_blurb}", fontsize=12, fontweight="bold", pad=8, color=NIGHT
        )
        ax.set_xlabel("Holdout type (how the test set was formed)", fontsize=11, fontweight="bold", labelpad=6)
        ax.tick_params(axis="x", labelrotation=0, labelsize=11)
        ax.tick_params(axis="y", labelsize=12)
        ax.set_yticklabels(ax.get_yticklabels(), rotation=0)

    axes[0].set_ylabel("Model", fontsize=11, fontweight="bold", labelpad=6)
    axes[1].set_ylabel("")

    # Colorbar tick/label size (seaborn does not pass through all text props on all versions).
    _main = {axes[0], axes[1]}
    for a in fig.axes:
        if a in _main:
            continue
        a.tick_params(labelsize=9, length=3, width=0.9)
        yl = a.get_ylabel()
        if yl:
            a.set_ylabel(yl, fontsize=9, fontweight="bold")

    fig.text(
        0.5,
        0.125,
        "Scope: order–time v2 table scores. Extension: staged T–k pre–outcome model—see poster.",
        ha="center",
        va="top",
        fontsize=9,
        color=FLINT,
    )
    fig.text(
        0.5,
        0.015,
        (
            "Compare the same two estimators on temporal (strict) vs. grouped (easier) holdouts. "
            "Temporal ≈ train on past periods, test on later orders (~0.9% positives). "
            "Grouped = by sales document. Recent 24-week omitted (~14 positives — unstable). "
            "If logistic matches or leads on temporal PR-AUC, boosting has not earned complexity here. "
            "Night (brand) outline = best in column."
        ),
        ha="center",
        va="bottom",
        fontsize=9,
        color=FLINT,
    )
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(HEATMAP_PATH, dpi=dpi, bbox_inches=None, facecolor="white", pad_inches=0)
    plt.close(fig)
    return HEATMAP_PATH


def save_temporal_snapshot() -> Path:
    apply_poster_matplotlib_style()
    df = _load_comparison_frame()
    row = df[df["split"] == "temporal_holdout"].set_index("model").loc["logistic_regression"]

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.axis("off")
    fig.patch.set_facecolor("white")

    header_box = patches.FancyBboxPatch(
        (0.04, 0.82),
        0.92,
        0.12,
        boxstyle="round,pad=0.012,rounding_size=0.02",
        linewidth=0,
        facecolor=TINT_HEADER,
        transform=ax.transAxes,
    )
    ax.add_patch(header_box)
    ax.text(
        0.5,
        0.88,
        "Selected Production Snapshot",
        ha="center",
        va="center",
        fontsize=24,
        fontweight="bold",
        color=FLINT,
        transform=ax.transAxes,
    )
    ax.text(
        0.5,
        0.84,
        "Strict temporal holdout on leakage-safe order-time features",
        ha="center",
        va="center",
        fontsize=13,
        color=FLINT,
        transform=ax.transAxes,
    )

    card_specs = [
        (0.06, 0.54, 0.4, 0.2, TINT_CARD_A, "Selected Model", "Logistic Regression"),
        (0.54, 0.54, 0.4, 0.2, TINT_CARD_C, "Decision Threshold", f"{row['decision_threshold']:.3f}"),
        (0.06, 0.28, 0.27, 0.18, TINT_CARD_D, "F1", f"{row['f1']:.2f}"),
        (0.365, 0.28, 0.27, 0.18, TINT_CARD_B, "PR-AUC", f"{row['pr_auc']:.2f}"),
        (0.67, 0.28, 0.27, 0.18, BIRCH, "ROC-AUC", f"{row['roc_auc']:.2f}"),
    ]

    for x, y, w, h, color, label, value in card_specs:
        rect = patches.FancyBboxPatch(
            (x, y),
            w,
            h,
            boxstyle="round,pad=0.012,rounding_size=0.02",
            linewidth=1.5,
            edgecolor=EDGE_SUBTLE,
            facecolor=color,
            transform=ax.transAxes,
        )
        ax.add_patch(rect)
        ax.text(
            x + 0.03,
            y + h - 0.05,
            label,
            ha="left",
            va="top",
            fontsize=13,
            fontweight="bold",
            color=FLINT,
            transform=ax.transAxes,
        )
        ax.text(
            x + w / 2,
            y + 0.045,
            value,
            ha="center",
            va="bottom",
            fontsize=24,
            fontweight="bold",
            color=NIGHT,
            transform=ax.transAxes,
        )

    notes_box = patches.FancyBboxPatch(
        (0.06, 0.05),
        0.88,
        0.16,
        boxstyle="round,pad=0.012,rounding_size=0.02",
        linewidth=1.2,
        edgecolor=EDGE_SUBTLE,
        facecolor=SNOW,
        transform=ax.transAxes,
    )
    ax.add_patch(notes_box)
    notes = [
        "Test set: 6,521 rows with 58 positives (0.89%).",
        f"Precision {row['precision']:.2f} and recall {row['recall']:.2f}.",
        "Grouped split is stronger but easier because train/test overlap in calendar time.",
    ]
    for idx, note in enumerate(notes):
        ax.text(
            0.09,
            0.17 - (idx * 0.045),
            note,
            ha="left",
            va="center",
            fontsize=13,
            color=FLINT,
            transform=ax.transAxes,
        )

    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(SNAPSHOT_PATH, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return SNAPSHOT_PATH


if __name__ == "__main__":
    print(save_model_comparison_heatmap())
    print(save_temporal_snapshot())
