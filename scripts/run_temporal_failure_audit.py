#!/usr/bin/env python3
"""Run a focused temporal failure audit for backorder modeling.

Outputs:
  - models/temporal_audit_label_maturity.json
  - models/temporal_audit_drift.json
  - models/temporal_audit_cohort_errors.json
  - models/temporal_audit_report.md
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
import sys

import numpy as np
import pandas as pd
from sklearn.metrics import f1_score, precision_score, recall_score

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.models import backorder_modeling as bm


def _paths(root: Path) -> dict[str, Path]:
    return {
        "root": root,
        "models": root / "models",
        "processed": root / "data" / "processed",
        "scores": root / "models" / "temporal_holdout_test_scores_v2_ordertime.json",
        "results": root / "models" / "overfit_eval_results_v2_ordertime.json",
        "label_maturity": root / "models" / "temporal_audit_label_maturity.json",
        "drift": root / "models" / "temporal_audit_drift.json",
        "cohorts": root / "models" / "temporal_audit_cohort_errors.json",
        "report": root / "models" / "temporal_audit_report.md",
    }


def _json_dump(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def _monthly_label_maturity(order_df: pd.DataFrame) -> pd.DataFrame:
    df = order_df.copy()
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["order_month"] = df["order_date"].dt.to_period("M").astype(str)
    y = pd.to_numeric(df["target_backorder_risk"], errors="coerce")
    obs = pd.to_numeric(df.get("target_backorder_observed", 0), errors="coerce").fillna(0).astype(int)
    grp = (
        df.assign(y=y, observed=obs)
        .groupby("order_month", dropna=False)
        .agg(
            rows=("target_backorder_risk", "size"),
            labeled_rows=("y", lambda s: int(s.notna().sum())),
            observed_rows=("observed", "sum"),
            positives=("y", lambda s: int(pd.to_numeric(s, errors="coerce").fillna(0).sum())),
            pending_window=("target_backorder_label_status", lambda s: int((s == "pending_window").sum())),
            missing_outcome=("target_backorder_label_status", lambda s: int((s == "missing_outcome").sum())),
        )
        .reset_index()
    )
    grp["label_coverage"] = np.where(grp["rows"] > 0, grp["labeled_rows"] / grp["rows"], 0.0)
    grp["positive_rate_labeled"] = np.where(grp["labeled_rows"] > 0, grp["positives"] / grp["labeled_rows"], 0.0)
    return grp


def _psi_numeric(train: pd.Series, test: pd.Series, bins: int = 10) -> float:
    t = pd.to_numeric(train, errors="coerce").dropna()
    v = pd.to_numeric(test, errors="coerce").dropna()
    if t.empty or v.empty:
        return 0.0
    qs = np.linspace(0, 1, bins + 1)
    edges = np.unique(np.quantile(t, qs))
    if len(edges) < 3:
        return 0.0
    t_hist, _ = np.histogram(t, bins=edges)
    v_hist, _ = np.histogram(v, bins=edges)
    t_pct = np.clip(t_hist / max(t_hist.sum(), 1), 1e-6, 1.0)
    v_pct = np.clip(v_hist / max(v_hist.sum(), 1), 1e-6, 1.0)
    return float(np.sum((t_pct - v_pct) * np.log(t_pct / v_pct)))


def _ks_numeric(train: pd.Series, test: pd.Series) -> float:
    t = np.sort(pd.to_numeric(train, errors="coerce").dropna().to_numpy())
    v = np.sort(pd.to_numeric(test, errors="coerce").dropna().to_numpy())
    if len(t) == 0 or len(v) == 0:
        return 0.0
    all_vals = np.unique(np.concatenate([t, v]))
    t_cdf = np.searchsorted(t, all_vals, side="right") / len(t)
    v_cdf = np.searchsorted(v, all_vals, side="right") / len(v)
    return float(np.max(np.abs(t_cdf - v_cdf)))


def _build_drift_audit(dataset: bm.PreparedDataset, train_idx: np.ndarray, test_idx: np.ndarray) -> dict[str, Any]:
    X_train = dataset.features.iloc[train_idx]
    X_test = dataset.features.iloc[test_idx]
    rows: list[dict[str, Any]] = []
    for col in dataset.numeric_features:
        if col not in X_train.columns or col not in X_test.columns:
            continue
        psi = _psi_numeric(X_train[col], X_test[col], bins=10)
        ks = _ks_numeric(X_train[col], X_test[col])
        rows.append(
            {
                "feature": col,
                "psi": psi,
                "ks": ks,
                "train_mean": float(pd.to_numeric(X_train[col], errors="coerce").mean()),
                "test_mean": float(pd.to_numeric(X_test[col], errors="coerce").mean()),
            }
        )
    rows = sorted(rows, key=lambda r: r["psi"], reverse=True)
    return {
        "n_numeric_features": len(rows),
        "high_psi_features": [r for r in rows if r["psi"] >= 0.2][:20],
        "all_features_sorted": rows[:50],
    }


def _choose_model_for_errors(scores_payload: dict[str, Any], results_payload: dict[str, Any]) -> str:
    models = scores_payload.get("models", {})
    if "soft_vote_lr_lightgbm" in models:
        return "soft_vote_lr_lightgbm"
    sel = (((results_payload.get("selected_model") or {}).get("name")) or "").strip()
    if sel in models:
        return sel
    return next(iter(models.keys()))


def _build_cohort_error_audit(
    dataset: bm.PreparedDataset,
    test_idx: np.ndarray,
    scores_payload: dict[str, Any],
    model_name: str,
) -> dict[str, Any]:
    y_true = np.asarray(scores_payload["y_true"], dtype=int)
    y_pred = np.asarray(scores_payload["models"][model_name]["y_pred"], dtype=int)
    y_proba = np.asarray(scores_payload["models"][model_name]["y_proba"], dtype=float)

    meta = dataset.meta.iloc[test_idx].reset_index(drop=True).copy()
    # Pull through common cohort IDs that may be in feature columns but not meta.
    test_features = dataset.features.iloc[test_idx].reset_index(drop=True)
    for col in ["plant_code", "client_id", "material_number", "customer_number"]:
        if col not in meta.columns and col in test_features.columns:
            meta[col] = test_features[col]
    if len(meta) != len(y_true):
        n = min(len(meta), len(y_true))
        meta = meta.iloc[:n].copy()
        y_true = y_true[:n]
        y_pred = y_pred[:n]
        y_proba = y_proba[:n]

    meta["y_true"] = y_true
    meta["y_pred"] = y_pred
    meta["y_proba"] = y_proba
    meta["fn"] = ((meta["y_true"] == 1) & (meta["y_pred"] == 0)).astype(int)
    meta["tp"] = ((meta["y_true"] == 1) & (meta["y_pred"] == 1)).astype(int)
    meta["fp"] = ((meta["y_true"] == 0) & (meta["y_pred"] == 1)).astype(int)
    meta["order_month"] = pd.to_datetime(meta["order_date"], errors="coerce").dt.to_period("M").astype(str)

    def agg_col(col: str, top_n: int = 20) -> list[dict[str, Any]]:
        if col not in meta.columns:
            return []
        g = (
            meta.groupby(col, dropna=False)
            .agg(rows=("y_true", "size"), positives=("y_true", "sum"), tp=("tp", "sum"), fn=("fn", "sum"), fp=("fp", "sum"))
            .reset_index()
        )
        g["recall"] = np.where(g["positives"] > 0, g["tp"] / g["positives"], 0.0)
        g["fn_rate"] = np.where(g["positives"] > 0, g["fn"] / g["positives"], 0.0)
        g = g.sort_values(["fn", "positives"], ascending=[False, False]).head(top_n)
        out: list[dict[str, Any]] = []
        for _, r in g.iterrows():
            out.append(
                {
                    col: str(r[col]),
                    "rows": int(r["rows"]),
                    "positives": int(r["positives"]),
                    "tp": int(r["tp"]),
                    "fn": int(r["fn"]),
                    "fp": int(r["fp"]),
                    "recall": float(r["recall"]),
                    "fn_rate": float(r["fn_rate"]),
                }
            )
        return out

    return {
        "model_for_error_audit": model_name,
        "test_rows": int(len(meta)),
        "positives": int(meta["y_true"].sum()),
        "precision": float(precision_score(meta["y_true"], meta["y_pred"], zero_division=0)),
        "recall": float(recall_score(meta["y_true"], meta["y_pred"], zero_division=0)),
        "f1": float(f1_score(meta["y_true"], meta["y_pred"], zero_division=0)),
        "top_fn_by_month": agg_col("order_month", 12),
        "top_fn_by_client": agg_col("client_id", 15),
        "top_fn_by_material": agg_col("material_number", 15),
        "top_fn_by_plant": agg_col("plant_code", 10),
    }


def _bootstrap_ci(y_true: np.ndarray, y_pred: np.ndarray, y_proba: np.ndarray, n_boot: int = 400) -> dict[str, Any]:
    from sklearn.metrics import average_precision_score, roc_auc_score

    rng = np.random.default_rng(42)
    n = len(y_true)
    stats = {"precision": [], "recall": [], "f1": [], "pr_auc": [], "roc_auc": []}
    for _ in range(n_boot):
        idx = rng.integers(0, n, size=n)
        yt = y_true[idx]
        yp = y_pred[idx]
        ys = y_proba[idx]
        if len(np.unique(yt)) < 2:
            continue
        stats["precision"].append(float(precision_score(yt, yp, zero_division=0)))
        stats["recall"].append(float(recall_score(yt, yp, zero_division=0)))
        stats["f1"].append(float(f1_score(yt, yp, zero_division=0)))
        stats["pr_auc"].append(float(average_precision_score(yt, ys)))
        stats["roc_auc"].append(float(roc_auc_score(yt, ys)))

    out: dict[str, Any] = {}
    for k, vals in stats.items():
        if not vals:
            out[k] = {"n": 0, "mean": None, "p05": None, "p50": None, "p95": None}
            continue
        arr = np.asarray(vals, dtype=float)
        out[k] = {
            "n": int(len(arr)),
            "mean": float(arr.mean()),
            "p05": float(np.quantile(arr, 0.05)),
            "p50": float(np.quantile(arr, 0.50)),
            "p95": float(np.quantile(arr, 0.95)),
        }
    return out


def _write_report(
    path: Path,
    label_payload: dict[str, Any],
    drift_payload: dict[str, Any],
    cohort_payload: dict[str, Any],
    ci_payload: dict[str, Any],
) -> None:
    top_shift = drift_payload.get("high_psi_features", [])[:5]
    top_month_fn = cohort_payload.get("top_fn_by_month", [])[:5]
    lines = [
        "# Temporal Failure Audit",
        "",
        "## Summary",
        f"- Label coverage tail (last 180 days): {label_payload['recent_windows']['last_180_days']['label_coverage']:.3f}",
        f"- Positive rate tail (last 180 days, labeled only): {label_payload['recent_windows']['last_180_days']['positive_rate']:.4f}",
        f"- Error audit model: `{cohort_payload['model_for_error_audit']}`",
        "",
        "## Bootstrap CI (temporal holdout)",
    ]
    for k, v in ci_payload.items():
        if v["n"] == 0:
            lines.append(f"- {k}: unavailable")
        else:
            lines.append(f"- {k}: mean={v['mean']:.4f}, p05={v['p05']:.4f}, p95={v['p95']:.4f}")
    lines.extend(
        [
            "",
            "## Top Drifted Features (PSI >= 0.2)",
        ]
    )
    for item in top_shift:
        lines.append(f"- {item['feature']}: psi={item['psi']:.3f}, ks={item['ks']:.3f}")
    lines.extend(["", "## Months With Most False Negatives"])
    for item in top_month_fn:
        lines.append(
            f"- {item['order_month']}: fn={item['fn']}, positives={item['positives']}, recall={item['recall']:.3f}"
        )
    lines.extend(["", "## Artifacts", "- `temporal_audit_label_maturity.json`", "- `temporal_audit_drift.json`", "- `temporal_audit_cohort_errors.json`"])
    path.write_text("\n".join(lines) + "\n")


def main() -> int:
    root = REPO_ROOT
    p = _paths(root)
    if not p["scores"].is_file() or not p["results"].is_file():
        raise FileNotFoundError(
            "Missing modeling outputs. Run `python scripts/run_modeling.py` first."
        )

    order = pd.read_csv(p["processed"] / "master_order_fulfillment_modeling_v2_ordertime.csv", low_memory=False)
    monthly = _monthly_label_maturity(order)
    label_payload = {
        "latest_months": monthly.tail(18).to_dict(orient="records"),
        "recent_windows": {
            "last_90_days": {
                "label_coverage": float(
                    monthly.tail(3)["labeled_rows"].sum() / max(monthly.tail(3)["rows"].sum(), 1)
                ),
                "positive_rate": float(
                    monthly.tail(3)["positives"].sum() / max(monthly.tail(3)["labeled_rows"].sum(), 1)
                ),
            },
            "last_180_days": {
                "label_coverage": float(
                    monthly.tail(6)["labeled_rows"].sum() / max(monthly.tail(6)["rows"].sum(), 1)
                ),
                "positive_rate": float(
                    monthly.tail(6)["positives"].sum() / max(monthly.tail(6)["labeled_rows"].sum(), 1)
                ),
            },
        },
    }
    _json_dump(p["label_maturity"], label_payload)

    dataset = bm.prepare_backorder_dataset(root)
    train_idx, test_idx, split_meta = bm._temporal_split_indices(dataset)
    drift_payload = {"split_meta": split_meta, **_build_drift_audit(dataset, train_idx, test_idx)}
    _json_dump(p["drift"], drift_payload)

    scores_payload = json.loads(p["scores"].read_text())
    results_payload = json.loads(p["results"].read_text())
    model_name = _choose_model_for_errors(scores_payload, results_payload)
    cohort_payload = _build_cohort_error_audit(dataset, test_idx, scores_payload, model_name)
    _json_dump(p["cohorts"], cohort_payload)

    y_true = np.asarray(scores_payload["y_true"], dtype=int)
    y_pred = np.asarray(scores_payload["models"][model_name]["y_pred"], dtype=int)
    y_proba = np.asarray(scores_payload["models"][model_name]["y_proba"], dtype=float)
    ci_payload = _bootstrap_ci(y_true, y_pred, y_proba, n_boot=400)

    _write_report(p["report"], label_payload, drift_payload, cohort_payload, ci_payload)
    print(f"Wrote: {p['label_maturity']}")
    print(f"Wrote: {p['drift']}")
    print(f"Wrote: {p['cohorts']}")
    print(f"Wrote: {p['report']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
