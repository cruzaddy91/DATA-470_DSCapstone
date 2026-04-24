"""
Render poster PNGs from ``canonical_poster_visual_spec.yaml`` + on-disk JSON arrays.

Uses ``poster_template_style`` (Seaborn/mako) — visually distinct from legacy
``poster_matplotlib_style`` figures. Filenames match ``build_poster.py`` expectations.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.metrics import average_precision_score, confusion_matrix, precision_recall_curve, roc_auc_score, roc_curve

from poster_template_style import apply_poster_template_style, heatmap_cmap
from westminster_poster_palette import (
    BAR_CLASS_NO,
    BAR_CLASS_OTHER,
    BAR_CLASS_YES,
    BAR_INV_NO,
    BAR_INV_YES,
    COLOR_BASELINE,
    COLOR_CATBOOST,
    COLOR_LIGHTGBM,
    COLOR_LOGISTIC,
    COLOR_XGBOOST,
    EDGE_SUBTLE,
    FLINT,
    SNOW,
    HIST_MAJORITY,
    HIST_MINORITY,
    NIGHT,
    SKY,
    THISTLE,
    TINT_CARD_A,
    TINT_CARD_B,
    TINT_CARD_C,
    TINT_CARD_D,
    TINT_HEADER,
    brand_confusion_heatmap_cmap,
)

def _annot_text_color(val: float, vmin: float, vmax: float, cmap_obj) -> str:
    """White text on dark cells, dark text on light cells (WCAG-style luminance)."""
    if vmax <= vmin:
        return FLINT
    t = (float(val) - vmin) / (vmax - vmin)
    t = max(0.0, min(1.0, t))
    rgba = cmap_obj(t)
    rgb = mcolors.to_rgb(rgba)
    lum = 0.2126 * rgb[0] + 0.7152 * rgb[1] + 0.0722 * rgb[2]
    return SNOW if lum < 0.48 else FLINT


FIG_FILES = {
    "target_balance": "target_balance_v2_ordertime.png",
    "heatmap": "showcase_model_comparison_heatmap.png",
    "snapshot": "showcase_temporal_snapshot.png",
    "roc": "roc_curves_temporal_v2_ordertime.png",
    "pr": "pr_curves_temporal_v2_ordertime.png",
    "drift": "temporal_positive_rate_drift_v2_ordertime.png",
    "scores": "score_distribution_temporal_v2_ordertime.png",
    "confusion": "classification_confusion_matrices_v2_ordertime.png",
}


def _figures_dir(root: Path) -> Path:
    d = root / "output" / "figures"
    d.mkdir(parents=True, exist_ok=True)
    return d


def render_target_balance(spec: dict, root: Path) -> Path | None:
    tb = spec.get("target_balance") or {}
    counts = tb.get("counts") or {}
    labels = tb.get("bar_labels") or ["No", "Yes", "Unresolved"]
    vals = [
        counts.get("no_backorder", 0),
        counts.get("backorder", 0),
        counts.get("unresolved_na", 0),
    ]
    apply_poster_template_style()
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    colors_bo = [BAR_CLASS_NO, BAR_CLASS_YES, BAR_CLASS_OTHER]
    axes[0].bar(labels, vals, color=colors_bo, edgecolor=NIGHT, linewidth=0.8)
    axes[0].set_title("Backorder (order-time v2) [template]")
    axes[0].set_ylabel("Count")
    axes[0].set_xlabel("target_backorder_risk")
    total = max(tb.get("order_lines_total") or sum(vals), 1)
    for i, cnt in enumerate(vals):
        axes[0].text(i, cnt + max(total * 0.002, 1), f"{cnt / total * 100:.1f}%", ha="center", fontsize=10)

    ospec = spec.get("overstock_optional")
    if ospec and ospec.get("counts"):
        ov = ospec["counts"]
        olabels = ospec.get("bar_labels") or ["No", "Yes"]
        ovals = [ov.get("no_overstock", 0), ov.get("overstock", 0)]
        colors_ov = [BAR_INV_NO, BAR_INV_YES]
        axes[1].bar(olabels, ovals, color=colors_ov, edgecolor=NIGHT, linewidth=0.8)
        axes[1].set_title("Overstock (material/plant) [template]")
        axes[1].set_ylabel("Count")
        axes[1].set_xlabel("target_overstock_risk")
        inv_n = max(ospec.get("inventory_rows_total") or sum(ovals), 1)
        for i, cnt in enumerate(ovals):
            axes[1].text(i, cnt + max(inv_n * 0.002, 1), f"{cnt / inv_n * 100:.1f}%", ha="center", fontsize=10)
    else:
        axes[1].axis("off")

    fig.tight_layout()
    out = _figures_dir(root) / FIG_FILES["target_balance"]
    fig.savefig(out, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out


def render_model_comparison_heatmap(spec: dict, root: Path) -> Path | None:
    h = spec.get("model_comparison_heatmap") or {}
    vals = h.get("values") or {}
    models_order = h.get("models_order") or []
    splits_order = h.get("splits_order") or []
    model_labels = h.get("model_labels") or {}
    split_labels = h.get("split_labels") or {}
    metrics_cfg = [
        ("roc_auc", "ROC-AUC", "Threshold-free ranking; stays interpretable under imbalance", 0.5, 1.0),
        ("pr_auc", "PR-AUC", "Stress-tests on rare positives (often << grouped split)", None, None),
    ]

    apply_poster_template_style()
    cmap = heatmap_cmap()

    fig, axes = plt.subplots(1, 2, figsize=(18, 7.2))
    fig.suptitle(
        "Logistic vs. XGBoost vs. CatBoost — ranking metrics under stress splits (template render)",
        fontsize=20,
        fontweight="bold",
        y=0.99,
    )

    for ax, (metric_key, metric_title, metric_blurb, vmin_fix, vmax_fix) in zip(axes, metrics_cfg):
        block = vals.get(metric_key) or {}
        row_labels = [model_labels[m] for m in models_order if m in model_labels]
        col_labels = [split_labels[s] for s in splits_order if s in split_labels]
        if not row_labels or not col_labels:
            continue
        data = np.full((len(row_labels), len(col_labels)), np.nan, dtype=float)
        for i, m in enumerate(models_order):
            if m not in model_labels:
                continue
            for j, s in enumerate(splits_order):
                if m in block and s in block.get(m, {}):
                    data[i, j] = float(block[m][s])
        metric_frame = pd.DataFrame(data, index=row_labels, columns=col_labels)

        if metric_key == "roc_auc":
            vmin_m, vmax_m = vmin_fix, vmax_fix
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
            cmap=cmap,
            vmin=vmin_m,
            vmax=vmax_m,
            linewidths=2.5,
            linecolor=SNOW,
            cbar=True,
            cbar_kws={"shrink": 0.78, "pad": 0.04, "label": cbar_label},
        )
        nrows, ncols = metric_frame.shape
        for i in range(nrows):
            for j in range(ncols):
                val = float(metric_frame.iloc[i, j])
                if np.isnan(val):
                    continue
                tc = _annot_text_color(val, vmin_m, vmax_m, cmap)
                ax.text(
                    j + 0.5,
                    i + 0.5,
                    f"{val:.2f}",
                    ha="center",
                    va="center",
                    fontsize=22,
                    fontweight="bold",
                    color=tc,
                )
        for col_idx, col_name in enumerate(metric_frame.columns):
            best_row = metric_frame[col_name].astype(float).idxmax()
            row_idx = metric_frame.index.get_loc(best_row)
            rect = patches.Rectangle(
                (col_idx, row_idx),
                1,
                1,
                fill=False,
                edgecolor=NIGHT,
                linewidth=3,
            )
            ax.add_patch(rect)
        ax.set_title(f"{metric_title}\n{metric_blurb}", fontsize=16, fontweight="bold", pad=14, color=NIGHT)
        ax.set_xlabel("Holdout type (how the test set was formed)", fontsize=15, fontweight="bold", labelpad=8)
        ax.tick_params(axis="x", labelrotation=0, labelsize=15)
        ax.tick_params(axis="y", labelsize=16)

    axes[0].set_ylabel("Model", fontsize=15, fontweight="bold", labelpad=8)
    axes[1].set_ylabel("")
    _main = {axes[0], axes[1]}
    for a in fig.axes:
        if a in _main:
            continue
        a.tick_params(labelsize=13, length=4, width=1.0)
        yl = a.get_ylabel()
        if yl:
            a.set_ylabel(yl, fontsize=13, fontweight="bold")
    fig.text(
        0.5,
        0.10,
        "Scope: order–time v2 table scores. Extension: staged T–k pre–outcome model—see poster.",
        ha="center",
        va="top",
        fontsize=11.5,
        color=FLINT,
    )
    fig.text(
        0.5,
        0.02,
        "Values from canonical_poster_visual_spec.yaml (classification_model_comparison_v2_ordertime.csv). "
        "Night outline = best in column. Poster trio: LR, XGBoost, CatBoost.",
        ha="center",
        va="bottom",
        fontsize=12,
        color=FLINT,
    )
    plt.tight_layout(rect=(0, 0.11, 1, 0.88))
    out = _figures_dir(root) / FIG_FILES["heatmap"]
    fig.savefig(out, dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out


def _load_temporal_json(root: Path, spec: dict) -> dict[str, Any]:
    rel = (spec.get("truth_paths") or {}).get("temporal_holdout_scores_json")
    if not rel:
        raise FileNotFoundError("spec.truth_paths.temporal_holdout_scores_json missing")
    path = root / rel
    if not path.is_file():
        raise FileNotFoundError(path)
    return json.loads(path.read_text(encoding="utf-8"))


def render_temporal_figures(spec: dict, root: Path) -> dict[str, Path]:
    payload = _load_temporal_json(root, spec)
    y_true = np.asarray(payload["y_true"], dtype=int)
    baseline_rate = float(payload.get("baseline_positive_rate", y_true.mean()))
    models = payload["models"]
    apply_poster_template_style()
    out: dict[str, Path] = {}
    fig_dir = _figures_dir(root)

    # ROC
    fig, ax = plt.subplots(figsize=(7, 6))
    ax.plot(
        [0, 1],
        [0, 1],
        linestyle="--",
        color=COLOR_BASELINE,
        linewidth=1.5,
        alpha=0.45,
        label="Random classifier",
    )
    for name, color, label_short in (
        ("logistic_regression", COLOR_LOGISTIC, "Logistic"),
        ("xgboost", COLOR_XGBOOST, "XGBoost"),
        ("catboost", COLOR_CATBOOST, "CatBoost"),
        ("lightgbm", COLOR_LIGHTGBM, "LightGBM"),
    ):
        if name not in models:
            continue
        proba = np.asarray(models[name]["y_proba"], dtype=float)
        fpr, tpr, _ = roc_curve(y_true, proba)
        roc_auc = float(models[name].get("roc_auc", roc_auc_score(y_true, proba)))
        ax.plot(fpr, tpr, color=color, linewidth=2.5, label=f"{label_short} (AUC={roc_auc:.2f})")
    ax.set_xlabel("False positive rate")
    ax.set_ylabel("True positive rate")
    ax.set_title("ROC — temporal holdout (template)")
    ax.legend(loc="lower right", frameon=True)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_aspect("equal")
    fig.tight_layout()
    p = fig_dir / FIG_FILES["roc"]
    fig.savefig(p, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    out["roc"] = p

    # PR
    fig, ax = plt.subplots(figsize=(7, 6))
    ax.axhline(
        baseline_rate,
        color=COLOR_BASELINE,
        linestyle="--",
        linewidth=1.5,
        alpha=0.45,
        label=f"Baseline positive rate ({baseline_rate * 100:.2f}%)",
    )
    for name, color, label_short in (
        ("logistic_regression", COLOR_LOGISTIC, "Logistic"),
        ("xgboost", COLOR_XGBOOST, "XGBoost"),
        ("catboost", COLOR_CATBOOST, "CatBoost"),
        ("lightgbm", COLOR_LIGHTGBM, "LightGBM"),
    ):
        if name not in models:
            continue
        proba = np.asarray(models[name]["y_proba"], dtype=float)
        prec, rec, _ = precision_recall_curve(y_true, proba)
        pr_auc = float(models[name].get("pr_auc", average_precision_score(y_true, proba)))
        ax.plot(rec, prec, color=color, linewidth=2.5, label=f"{label_short} (PR-AUC={pr_auc:.2f})")
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_title("Precision–recall — temporal holdout (template)")
    ax.legend(loc="upper right", frameon=True)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.tight_layout()
    p = fig_dir / FIG_FILES["pr"]
    fig.savefig(p, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    out["pr"] = p

    # Drift
    rel_diag = (spec.get("truth_paths") or {}).get("auc_diagnostics_json")
    if rel_diag:
        diag_path = root / rel_diag
        if diag_path.is_file():
            diag = json.loads(diag_path.read_text(encoding="utf-8"))
            monthly = (diag.get("target_stability") or {}).get("monthly_tail") or []
            if monthly:
                months = [row["order_month"] for row in monthly]
                rates = [float(row["positive_rate"]) * 100.0 for row in monthly]
                fig, ax = plt.subplots(figsize=(9, 4.5))
                x = range(len(months))
                ax.plot(
                    x,
                    rates,
                    color=THISTLE,
                    linewidth=2.4,
                    marker="o",
                    markersize=5,
                    markerfacecolor=SKY,
                    markeredgecolor=NIGHT,
                    markeredgewidth=0.9,
                )
                ax.set_xticks(list(x))
                ax.set_xticklabels(months, rotation=45, ha="right")
                ax.set_ylabel("Positive rate (%)")
                ax.set_xlabel("Order month")
                ax.set_title("Temporal drift — positive rate by month (template)")
                ax.axhline(
                    baseline_rate * 100.0,
                    color=COLOR_BASELINE,
                    linestyle="--",
                    linewidth=1.5,
                    alpha=0.45,
                    label="Temporal test mean",
                )
                ax.legend()
                sns.despine()
                fig.tight_layout()
                p = fig_dir / FIG_FILES["drift"]
                fig.savefig(p, dpi=200, bbox_inches="tight", facecolor="white")
                plt.close(fig)
                out["drift"] = p

    # Score distributions
    model_items: list[tuple[str, str]] = []
    for label_short, key in (("Logistic", "logistic_regression"), ("LightGBM", "lightgbm")):
        if key in models:
            model_items.append((key, label_short))
    if model_items:
        n_models = len(model_items)
        fig, axes = plt.subplots(n_models, 1, figsize=(8, 3.5 * n_models), squeeze=False)
        for ax, (name, label_short) in zip(axes.flat, model_items):
            proba = np.asarray(models[name]["y_proba"], dtype=float)
            thr = float(models[name].get("decision_threshold", 0.5))
            ax.hist(proba[y_true == 0], bins=40, alpha=0.72, color=HIST_MAJORITY, label="No backorder", density=True, edgecolor=NIGHT, linewidth=0.3)
            ax.hist(proba[y_true == 1], bins=40, alpha=0.72, color=HIST_MINORITY, label="Backorder", density=True, edgecolor=NIGHT, linewidth=0.3)
            ax.axvline(thr, color=NIGHT, linestyle="--", linewidth=2, label=f"Threshold={thr:.3f}")
            ax.set_xlabel("Predicted P(backorder)")
            ax.set_ylabel("Density")
            ax.set_title(f"{label_short} — score distribution (template)")
            ax.legend(fontsize=8)
        fig.tight_layout()
        p = fig_dir / FIG_FILES["scores"]
        fig.savefig(p, dpi=200, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        out["scores"] = p

    # Confusion matrices (row-normalized), side by side
    cmap_cm = brand_confusion_heatmap_cmap()
    fig, axes = plt.subplots(1, 2, figsize=(15, 5.8))
    for ax, (name, label_short) in zip(axes, (("logistic_regression", "Logistic"), ("lightgbm", "LightGBM"))):
        if name not in models:
            ax.axis("off")
            continue
        y_pred = np.asarray(models[name]["y_pred"], dtype=int)
        cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
        with np.errstate(divide="ignore", invalid="ignore"):
            cm_rn = cm.astype(float) / cm.sum(axis=1, keepdims=True)
            cm_rn = np.nan_to_num(cm_rn)
        sns.heatmap(
            cm_rn,
            annot=False,
            cmap=cmap_cm,
            vmin=0,
            vmax=1,
            ax=ax,
            xticklabels=["Pred 0", "Pred 1"],
            yticklabels=["Act 0", "Act 1"],
            linewidths=2.0,
            linecolor=SNOW,
        )
        for i in range(cm_rn.shape[0]):
            for j in range(cm_rn.shape[1]):
                val = float(cm_rn[i, j])
                tc = _annot_text_color(val, 0.0, 1.0, cmap_cm)
                ax.text(
                    j + 0.5,
                    i + 0.5,
                    f"{val:.2f}",
                    ha="center",
                    va="center",
                    fontsize=22,
                    fontweight="bold",
                    color=tc,
                )
        ax.set_title(f"{label_short} (row-normalized)", fontsize=16, fontweight="bold", pad=10, color=NIGHT)
        ax.tick_params(axis="both", labelsize=15)
    plt.suptitle("Confusion matrices — temporal holdout (template)", y=1.01, fontsize=16, fontweight="bold")
    fig.tight_layout()
    p = fig_dir / FIG_FILES["confusion"]
    fig.savefig(p, dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    out["confusion"] = p

    return out


def render_temporal_snapshot_card(spec: dict, root: Path) -> Path | None:
    """Snapshot card using comparison CSV path from spec (logistic temporal row)."""
    rel = (spec.get("truth_paths") or {}).get("classification_model_comparison_csv")
    if not rel:
        return None
    csv_path = root / rel
    if not csv_path.is_file():
        return None
    df = pd.read_csv(csv_path)
    row = df[df["split"] == "temporal_holdout"].set_index("model").loc["logistic_regression"]

    apply_poster_template_style()
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
    ax.text(0.5, 0.88, "Selected Production Snapshot", ha="center", va="center", fontsize=24, fontweight="bold", transform=ax.transAxes)
    ax.text(0.5, 0.84, "Strict temporal holdout (template card)", ha="center", va="center", fontsize=13, transform=ax.transAxes)

    card_specs = [
        (0.06, 0.54, 0.4, 0.2, TINT_CARD_A, "Selected Model", "Logistic Regression"),
        (0.54, 0.54, 0.4, 0.2, TINT_CARD_B, "Decision Threshold", f"{row['decision_threshold']:.3f}"),
        (0.06, 0.28, 0.27, 0.18, TINT_CARD_C, "F1", f"{row['f1']:.2f}"),
        (0.365, 0.28, 0.27, 0.18, TINT_CARD_D, "PR-AUC", f"{row['pr_auc']:.2f}"),
        (0.67, 0.28, 0.27, 0.18, TINT_HEADER, "ROC-AUC", f"{row['roc_auc']:.2f}"),
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
        ax.text(x + 0.03, y + h - 0.05, label, ha="left", va="top", fontsize=13, fontweight="bold", color=FLINT, transform=ax.transAxes)
        ax.text(x + w / 2, y + 0.045, value, ha="center", va="bottom", fontsize=24, fontweight="bold", color=NIGHT, transform=ax.transAxes)

    meta = spec.get("temporal_holdout_meta") or {}
    n_test = meta.get("n_test", "")
    n_pos = meta.get("n_positive", "")
    notes = [
        f"Test rows (from scores JSON): {n_test}; positives: {n_pos}.",
        f"Precision {row['precision']:.2f} and recall {row['recall']:.2f}.",
        "Numbers from canonical spec / CSV — not generated by diffusion.",
    ]
    for idx, note in enumerate(notes):
        ax.text(0.09, 0.17 - (idx * 0.045), note, ha="left", va="center", fontsize=13, color=FLINT, transform=ax.transAxes)

    out = _figures_dir(root) / FIG_FILES["snapshot"]
    fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out


def render_all_from_spec(spec: dict, root: Path) -> dict[str, Path | None]:
    """Render every template figure; return map of logical name -> path."""
    root = Path(root).resolve()
    results: dict[str, Path | None] = {}
    results["target_balance"] = render_target_balance(spec, root)
    results["heatmap"] = render_model_comparison_heatmap(spec, root)
    results.update({f"temporal_{k}": v for k, v in render_temporal_figures(spec, root).items()})
    results["snapshot"] = render_temporal_snapshot_card(spec, root)
    return results
