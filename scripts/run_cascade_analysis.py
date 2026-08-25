#!/usr/bin/env python3
"""
Two-stage cascade analysis — post-hoc, no retraining.

Textbook pattern for high-stakes rare-event classification:
  Stage 1 (high-recall filter): score every order; keep the top K% by
  probability. Aim is to catch ~95% of true positives in that top slice.
  Stage 2 (precision filter): among Stage 1's survivors, apply a stricter
  rule using a different model's probability. Only rows that clear both
  bars are flagged.

The cascade narrows the population to where signal concentrates, then
pushes precision in that narrower population. If the tail of the
probability distribution has real structure, this beats any single
threshold. If not, cascade adds complexity without payoff — and we
should discard it.

This script reads temporal_holdout_test_scores and evaluates several
cascade configurations at multiple top-K% cuts. Pure analysis — no
model retraining.

Outputs:
  output/tables/cascade_analysis.md
  output/figures/cascade_frontier.png
  output/tables/cascade_analysis.json
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.metrics import precision_recall_curve

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

SCORE_PATH = PROJECT_ROOT / "models" / "temporal_holdout_test_scores_v2_ordertime.json"
FIGURES_DIR = PROJECT_ROOT / "output" / "figures"
TABLES_DIR = PROJECT_ROOT / "output" / "tables"

# Stage 1 is the "filter" (low-threshold, high-recall) — we want the
# model that ranks positives into the top slice most aggressively.
# Stage 2 is the "verifier" — a different model that scores the survivors.
# Using DIFFERENT models at the two stages is what makes a cascade work;
# same-model cascades collapse to the single-model curve.
CASCADE_CONFIGS = [
    {"stage1": "logistic_regression", "stage2": "oof_calibrated_stack",
     "label": "LR filter → Stack verifier"},
    {"stage1": "logistic_regression", "stage2": "lightgbm",
     "label": "LR filter → LightGBM verifier"},
    {"stage1": "soft_vote_4_bases", "stage2": "logistic_regression",
     "label": "Vote filter → LR verifier"},
    {"stage1": "oof_calibrated_stack_all_bases", "stage2": "logistic_regression",
     "label": "Stack-all filter → LR verifier"},
]
TOP_K_PCTS = [0.05, 0.10, 0.15, 0.20, 0.30]


def _f1(p: float, r: float) -> float:
    return (2 * p * r / (p + r)) if (p + r) > 0 else 0.0


def _single_stage_f1_max(y_true: np.ndarray, y_proba: np.ndarray) -> dict[str, float]:
    precision, recall, thresholds = precision_recall_curve(y_true, y_proba)
    p = precision[:-1]
    r = recall[:-1]
    f1 = np.where((p + r) > 0, 2 * p * r / (p + r), 0.0)
    i = int(np.argmax(f1))
    return {
        "threshold": float(thresholds[i]),
        "precision": float(p[i]),
        "recall": float(r[i]),
        "f1": float(f1[i]),
    }


def _cascade_point(
    y_true: np.ndarray,
    p1: np.ndarray,
    p2: np.ndarray,
    top_k_pct: float,
) -> dict[str, Any]:
    """Stage 1: keep top_k_pct% by p1 probability.
    Stage 2: among survivors, sweep p2 threshold for F1-max."""
    n = len(y_true)
    k = max(1, int(round(top_k_pct * n)))
    # Stage 1 survivors: top-k by p1.
    order_desc = np.argsort(-p1)
    keep = order_desc[:k]
    mask = np.zeros(n, dtype=bool)
    mask[keep] = True
    y_survive = y_true[mask]
    p2_survive = p2[mask]
    total_positives = int(y_true.sum())

    # Stage 1 stats: recall captured by the top-k slice.
    stage1_tp = int(y_survive.sum())
    stage1_recall = stage1_tp / total_positives if total_positives else 0.0
    stage1_precision = stage1_tp / k if k else 0.0

    # Stage 2: sweep thresholds on the survivors, pick F1-max.
    if len(np.unique(y_survive)) < 2:
        return {
            "top_k_pct": top_k_pct,
            "k_survivors": int(k),
            "stage1_recall_of_positives_kept": float(stage1_recall),
            "stage1_precision_in_kept": float(stage1_precision),
            "stage2_f1_max": None,
            "cascade_precision": 0.0,
            "cascade_recall": 0.0,
            "cascade_f1": 0.0,
            "cascade_threshold_p2": float("nan"),
            "notes": "Stage 1 survivors are single-class; Stage 2 cannot separate.",
        }
    precision, recall_s, thresholds_s = precision_recall_curve(y_survive, p2_survive)
    p = precision[:-1]
    r_local = recall_s[:-1]
    # Translate "recall on survivors" back to "recall on full population":
    # cascade_recall = (fraction of survivors flagged positive) * stage1_tp / total_positives.
    # Equivalently: TP at this threshold / total_positives.
    # TP_at_thr = r_local * stage1_tp
    tp_at_thr = r_local * stage1_tp
    r_global = tp_at_thr / total_positives if total_positives else r_local * 0.0
    f1_global = np.where((p + r_global) > 0, 2 * p * r_global / (p + r_global), 0.0)
    i = int(np.argmax(f1_global))
    return {
        "top_k_pct": top_k_pct,
        "k_survivors": int(k),
        "stage1_recall_of_positives_kept": float(stage1_recall),
        "stage1_precision_in_kept": float(stage1_precision),
        "stage2_f1_max": float(np.max(np.where((p + r_local) > 0, 2 * p * r_local / (p + r_local), 0.0))),
        "cascade_precision": float(p[i]),
        "cascade_recall": float(r_global[i]),
        "cascade_f1": float(f1_global[i]),
        "cascade_threshold_p2": float(thresholds_s[i]),
    }


def main() -> int:
    payload = json.loads(SCORE_PATH.read_text(encoding="utf-8"))
    y_true = np.asarray(payload.get("y_true", []), dtype=int)
    if y_true.size == 0:
        print("No y_true in score file.", file=sys.stderr)
        return 1
    models = payload.get("models", {})

    # Single-stage F1-max baselines for every model we'll cascade from.
    baselines: dict[str, dict[str, float]] = {}
    for name in set(c["stage1"] for c in CASCADE_CONFIGS) | set(c["stage2"] for c in CASCADE_CONFIGS):
        entry = models.get(name)
        if not entry:
            continue
        y_proba = np.asarray(entry.get("y_proba", []), dtype=float)
        if y_proba.size != y_true.size:
            continue
        baselines[name] = _single_stage_f1_max(y_true, y_proba)

    report: dict[str, Any] = {
        "n_test": int(len(y_true)),
        "n_positive": int(y_true.sum()),
        "baseline_positive_rate": float(y_true.mean()),
        "single_stage_baselines": baselines,
        "cascades": [],
    }
    for cfg in CASCADE_CONFIGS:
        e1 = models.get(cfg["stage1"])
        e2 = models.get(cfg["stage2"])
        if not e1 or not e2:
            continue
        p1 = np.asarray(e1.get("y_proba", []), dtype=float)
        p2 = np.asarray(e2.get("y_proba", []), dtype=float)
        if p1.size != y_true.size or p2.size != y_true.size:
            continue
        cascade_entry = {
            "label": cfg["label"],
            "stage1": cfg["stage1"],
            "stage2": cfg["stage2"],
            "stage1_baseline_f1": baselines.get(cfg["stage1"], {}).get("f1"),
            "stage2_baseline_f1": baselines.get(cfg["stage2"], {}).get("f1"),
            "points": [_cascade_point(y_true, p1, p2, k) for k in TOP_K_PCTS],
        }
        report["cascades"].append(cascade_entry)

    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    # Markdown summary.
    lines = ["# Two-Stage Cascade Analysis — temporal holdout", ""]
    lines.append(f"- n_test = {report['n_test']}, n_positive = {report['n_positive']}, base_rate = {report['baseline_positive_rate']:.4f}")
    lines.append("")
    lines.append("Single-stage F1-max baselines (for reference):")
    for name, b in baselines.items():
        lines.append(f"- **{name}**: F1 = {b['f1']:.3f} (P {b['precision']:.3f}, R {b['recall']:.3f}) at thr {b['threshold']:.3f}")
    lines.append("")
    lines.append("Cascade: Stage 1 keeps top-K% by Stage 1 probability; Stage 2 picks F1-max threshold on the survivors. `cascade_recall` is computed against the full test set (not just survivors).")
    lines.append("")
    for cas in report["cascades"]:
        lines.append(f"## {cas['label']}")
        lines.append(f"- Stage 1 baseline F1: {cas['stage1_baseline_f1']:.3f} · Stage 2 baseline F1: {cas['stage2_baseline_f1']:.3f}")
        lines.append("")
        lines.append("| Top-K% | Survivors | Stage 1 recall | Stage 1 precision | Cascade P | Cascade R | Cascade F1 |")
        lines.append("|---|---|---|---|---|---|---|")
        for pt in cas["points"]:
            lines.append(
                f"| {pt['top_k_pct']*100:.0f}% | {pt['k_survivors']} | "
                f"{pt['stage1_recall_of_positives_kept']:.3f} | "
                f"{pt['stage1_precision_in_kept']:.3f} | "
                f"{pt['cascade_precision']:.3f} | "
                f"{pt['cascade_recall']:.3f} | "
                f"**{pt['cascade_f1']:.3f}** |"
            )
        lines.append("")

    (TABLES_DIR / "cascade_analysis.md").write_text("\n".join(lines), encoding="utf-8")
    (TABLES_DIR / "cascade_analysis.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(f"Wrote {TABLES_DIR / 'cascade_analysis.md'}")
    print(f"Wrote {TABLES_DIR / 'cascade_analysis.json'}")

    # Figure: cascade F1 vs top-K%, one line per cascade config, with single-stage baselines as horizontal rules.
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(9, 6))
    for cas in report["cascades"]:
        ks = [pt["top_k_pct"] * 100 for pt in cas["points"]]
        f1s = [pt["cascade_f1"] for pt in cas["points"]]
        ax.plot(ks, f1s, marker="o", linewidth=2, label=cas["label"])
    # Baseline rules.
    for name, b in baselines.items():
        ax.axhline(b["f1"], linestyle="--", linewidth=1, alpha=0.5)
    ax.set_xlabel("Top-K% kept after Stage 1")
    ax.set_ylabel("Cascade F1 (on full test set)")
    ax.set_title("Two-stage cascade — F1 vs top-K%")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    out = FIGURES_DIR / "cascade_frontier.png"
    fig.savefig(out, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Wrote {out}")

    # Console summary.
    print()
    print("Best cascade point per config:")
    for cas in report["cascades"]:
        best = max(cas["points"], key=lambda pt: pt["cascade_f1"] or 0.0)
        stage1_f1 = cas["stage1_baseline_f1"] or 0.0
        stage2_f1 = cas["stage2_baseline_f1"] or 0.0
        baseline_max = max(stage1_f1, stage2_f1)
        delta = best["cascade_f1"] - baseline_max
        print(
            f"  {cas['label']:45s}  best F1={best['cascade_f1']:.3f}  "
            f"(at top-{best['top_k_pct']*100:.0f}%)  vs best single F1={baseline_max:.3f}  "
            f"Δ={delta:+.3f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
