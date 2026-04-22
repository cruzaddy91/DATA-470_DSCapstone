#!/usr/bin/env python3
"""Build a lightweight model-health dashboard from latest artifacts only."""

from __future__ import annotations

import base64
import json
from datetime import datetime
from pathlib import Path
from typing import Any

# Stable cockpit ordering (then any extra model keys alphabetically).
_COCKPIT_MODEL_ORDER: tuple[str, ...] = (
    "logistic_regression",
    "lightgbm",
    "xgboost",
    "catboost",
    "soft_vote_lr_lightgbm",
    "oof_calibrated_stack",
)


def _cockpit_rank(name: str) -> int:
    try:
        return _COCKPIT_MODEL_ORDER.index(name)
    except ValueError:
        return len(_COCKPIT_MODEL_ORDER)


def _ordered_models(models: dict[str, Any]) -> dict[str, Any]:
    """Return same model metrics in a stable display order (all keys retained)."""
    if not models:
        return {}
    ordered: dict[str, Any] = {}
    for key in _COCKPIT_MODEL_ORDER:
        if key in models:
            ordered[key] = models[key]
    for key in sorted(models.keys()):
        if key not in ordered:
            ordered[key] = models[key]
    return ordered


def _all_cockpit_model_names(metrics: dict[str, Any]) -> list[str]:
    """Union of model names across splits and CI blocks, cockpit-sorted."""
    names: set[str] = set()
    for split_key in ("temporal_holdout", "group_holdout", "recent_24_week_temporal_holdout"):
        block = metrics.get(split_key) or {}
        m = block.get("models") or {}
        if isinstance(m, dict):
            names |= set(m.keys())
    ci_root = metrics.get("confidence_intervals") or {}
    for ck in ("temporal_primary", "recent_24_week"):
        c = ci_root.get(ck) or {}
        if isinstance(c, dict):
            names |= set(c.keys())
    return sorted(names, key=lambda n: (_cockpit_rank(n), n))


def _fmt(x: Any, digits: int = 4) -> str:
    if isinstance(x, (int, float)):
        return f"{x:.{digits}f}"
    return "-"


def _heat_color(norm: float) -> str:
    n = max(0.0, min(1.0, float(norm)))
    hue = 120.0 * n
    return f"hsla({hue:.1f}, 75%, 30%, 0.55)"


def _metric_td(value: Any, *, norm: float | None = None) -> str:
    text = _fmt(value)
    if norm is None or not isinstance(value, (int, float)):
        return f"<td>{text}</td>"
    return f"<td class='metric-cell' style='background:{_heat_color(norm)}'>{text}</td>"


def _models_table_rows(models: dict[str, Any]) -> str:
    rows: list[str] = []
    for name, m in _ordered_models(models).items():
        rows.append(
            "<tr>"
            f"<td>{name}</td>"
            f"{_metric_td(m.get('precision'), norm=float(m.get('precision', 0.0)))}"
            f"{_metric_td(m.get('recall'), norm=float(m.get('recall', 0.0)))}"
            f"{_metric_td(m.get('f1'), norm=float(m.get('f1', 0.0)))}"
            f"{_metric_td(m.get('roc_auc'), norm=float(m.get('roc_auc', 0.0)))}"
            f"{_metric_td(m.get('pr_auc'), norm=float(m.get('pr_auc', 0.0)))}"
            "</tr>"
        )
    return "\n".join(rows) if rows else "<tr><td colspan='6'>No model rows found.</td></tr>"


def _baseline_rows(baselines: dict[str, Any]) -> str:
    rows: list[str] = []
    for name, m in baselines.items():
        rows.append(
            "<tr>"
            f"<td>{name}</td>"
            f"{_metric_td(m.get('accuracy'), norm=float(m.get('accuracy', 0.0)))}"
            f"{_metric_td(m.get('f1'), norm=float(m.get('f1', 0.0)))}"
            f"{_metric_td(m.get('pr_auc'), norm=float(m.get('pr_auc', 0.0)))}"
            f"{_metric_td(m.get('recall'), norm=float(m.get('recall', 0.0)))}"
            f"{_metric_td(m.get('precision'), norm=float(m.get('precision', 0.0)))}"
            "</tr>"
        )
    return "\n".join(rows) if rows else "<tr><td colspan='6'>No baseline rows found.</td></tr>"


def _lift_rows(lift: dict[str, Any]) -> str:
    rows: list[str] = []
    deltas = {
        key: [
            float(v.get(key))
            for v in lift.values()
            if isinstance(v.get(key), (int, float))
        ]
        for key in ("delta_f1", "delta_pr_auc", "delta_recall", "delta_precision")
    }
    max_abs = {k: (max((abs(x) for x in vals), default=1.0) or 1.0) for k, vals in deltas.items()}

    def _delta_norm(metric: str, value: Any) -> float | None:
        if not isinstance(value, (int, float)):
            return None
        m = max_abs.get(metric, 1.0)
        return max(0.0, min(1.0, 0.5 + 0.5 * (float(value) / m)))

    for name, m in _ordered_models(lift).items():
        rows.append(
            "<tr>"
            f"<td>{name}</td>"
            f"<td>{m.get('baseline', '-')}</td>"
            f"{_metric_td(m.get('delta_f1'), norm=_delta_norm('delta_f1', m.get('delta_f1')))}"
            f"{_metric_td(m.get('delta_pr_auc'), norm=_delta_norm('delta_pr_auc', m.get('delta_pr_auc')))}"
            f"{_metric_td(m.get('delta_recall'), norm=_delta_norm('delta_recall', m.get('delta_recall')))}"
            f"{_metric_td(m.get('delta_precision'), norm=_delta_norm('delta_precision', m.get('delta_precision')))}"
            "</tr>"
        )
    return "\n".join(rows) if rows else "<tr><td colspan='6'>No lift rows found.</td></tr>"


def _kpi_chip(label: str, value: Any, *, norm: float | None = None) -> str:
    text = _fmt(value)
    style = ""
    if norm is not None and isinstance(value, (int, float)):
        style = f" style='background:{_heat_color(norm)}'"
    return f"<div class='kpi-chip'{style}><div class='kpi-label'>{label}</div><div class='kpi-value'>{text}</div></div>"


def _gauge_chip(
    label: str,
    value: Any,
    *,
    norm: float | None = None,
    min_value: float = 0.0,
    max_value: float = 1.0,
) -> str:
    if not isinstance(value, (int, float)):
        return _kpi_chip(label, value, norm=norm)
    v = float(value)
    lo = float(min_value)
    hi = float(max_value)
    span = max(hi - lo, 1e-9)
    pct = max(0.0, min(1.0, (v - lo) / span))
    display = _fmt(v)
    color = _heat_color(norm if norm is not None else pct)
    angle = -90.0 + (pct * 180.0)
    return (
        "<div class='gauge-chip'>"
        f"<div class='gauge-label'>{label}</div>"
        f"<div class='gauge' style='--pct:{pct:.6f}; --needle-angle:{angle:.4f}deg; --gauge-color:{color}'>"
        "<div class='gauge-ticks'>"
        "<span class='tick t0'></span>"
        "<span class='tick t25'></span>"
        "<span class='tick t50'></span>"
        "<span class='tick t75'></span>"
        "<span class='tick t100'></span>"
        "</div>"
        "<div class='gauge-needle'></div>"
        "<div class='gauge-hub'></div>"
        f"<div class='gauge-value'>{display}</div>"
        "</div>"
        f"<div class='gauge-scale'><span>{_fmt(lo, 2)}</span><span>{_fmt(hi, 2)}</span></div>"
        "</div>"
    )


def _models_dash(models: dict[str, Any], section_id: str) -> str:
    if not models:
        return "<div class='dash-empty'>No model rows found.</div>"
    cards: list[str] = []
    for name, m in _ordered_models(models).items():
        f1 = float(m.get("f1", 0.0))
        pr_auc = float(m.get("pr_auc", 0.0))
        cards.append(
            f"<div class='dash-card sortable' data-f1='{f1:.8f}' data-pr_auc='{pr_auc:.8f}'>"
            f"<div class='dash-title'>{name}</div>"
            f"{_gauge_chip('Precision', m.get('precision'), norm=float(m.get('precision', 0.0)))}"
            f"{_gauge_chip('Recall', m.get('recall'), norm=float(m.get('recall', 0.0)))}"
            f"{_gauge_chip('F1', m.get('f1'), norm=float(m.get('f1', 0.0)))}"
            f"{_gauge_chip('ROC-AUC', m.get('roc_auc'), norm=float(m.get('roc_auc', 0.0)))}"
            f"{_gauge_chip('PR-AUC', m.get('pr_auc'), norm=float(m.get('pr_auc', 0.0)))}"
            "</div>"
        )
    return (
        f"<div class='dash-sort-row'>"
        f"<span>Sort cards:</span>"
        f"<button class='sort-btn' type='button' data-target='{section_id}' data-metric='f1'>F1</button>"
        f"<button class='sort-btn' type='button' data-target='{section_id}' data-metric='pr_auc'>PR-AUC</button>"
        f"</div>"
        f"<div id='{section_id}' class='dash-grid sortable-grid'>"
        + "".join(cards)
        + "</div>"
    )


def _baselines_dash(baselines: dict[str, Any]) -> str:
    if not baselines:
        return "<div class='dash-empty'>No baseline rows found.</div>"
    cards: list[str] = []
    for name, m in baselines.items():
        cards.append(
            "<div class='dash-card'>"
            f"<div class='dash-title'>{name}</div>"
            f"{_gauge_chip('Accuracy', m.get('accuracy'), norm=float(m.get('accuracy', 0.0)))}"
            f"{_gauge_chip('F1', m.get('f1'), norm=float(m.get('f1', 0.0)))}"
            f"{_gauge_chip('PR-AUC', m.get('pr_auc'), norm=float(m.get('pr_auc', 0.0)))}"
            f"{_gauge_chip('Recall', m.get('recall'), norm=float(m.get('recall', 0.0)))}"
            f"{_gauge_chip('Precision', m.get('precision'), norm=float(m.get('precision', 0.0)))}"
            "</div>"
        )
    return "<div class='dash-grid'>" + "".join(cards) + "</div>"


def _lift_dash(lift: dict[str, Any]) -> str:
    if not lift:
        return "<div class='dash-empty'>No lift rows found.</div>"
    deltas = {
        key: [
            float(v.get(key))
            for v in lift.values()
            if isinstance(v.get(key), (int, float))
        ]
        for key in ("delta_f1", "delta_pr_auc", "delta_recall", "delta_precision")
    }
    max_abs = {k: (max((abs(x) for x in vals), default=1.0) or 1.0) for k, vals in deltas.items()}

    def _dn(metric: str, val: Any) -> float | None:
        if not isinstance(val, (int, float)):
            return None
        m = max_abs.get(metric, 1.0)
        return max(0.0, min(1.0, 0.5 + 0.5 * (float(val) / m)))

    cards: list[str] = []
    for name, m in _ordered_models(lift).items():
        cards.append(
            "<div class='dash-card'>"
            f"<div class='dash-title'>{name}</div>"
            f"<div class='dash-sub'>Baseline: {m.get('baseline', '-')}</div>"
            f"{_gauge_chip('Delta F1', m.get('delta_f1'), norm=_dn('delta_f1', m.get('delta_f1')), min_value=-1.0, max_value=1.0)}"
            f"{_gauge_chip('Delta PR-AUC', m.get('delta_pr_auc'), norm=_dn('delta_pr_auc', m.get('delta_pr_auc')), min_value=-1.0, max_value=1.0)}"
            f"{_gauge_chip('Delta Recall', m.get('delta_recall'), norm=_dn('delta_recall', m.get('delta_recall')), min_value=-1.0, max_value=1.0)}"
            f"{_gauge_chip('Delta Precision', m.get('delta_precision'), norm=_dn('delta_precision', m.get('delta_precision')), min_value=-1.0, max_value=1.0)}"
            "</div>"
        )
    return "<div class='dash-grid'>" + "".join(cards) + "</div>"


def _pick_selected_name(models: dict[str, Any], preferred: str | None) -> str | None:
    ordered = _ordered_models(models)
    if preferred and preferred in ordered:
        return preferred
    for k in ordered:
        return k
    return None


def _instrument_model_panel(models: dict[str, Any], preferred: str | None) -> str:
    if not models:
        return "<div class='dash-empty'>No model rows found.</div>"
    selected_name = _pick_selected_name(models, preferred)
    if not selected_name:
        return "<div class='dash-empty'>No model rows found.</div>"
    m = models.get(selected_name, {})
    gauges = (
        f"{_gauge_chip('Precision', m.get('precision'), norm=float(m.get('precision', 0.0)))}"
        f"{_gauge_chip('Recall', m.get('recall'), norm=float(m.get('recall', 0.0)))}"
        f"{_gauge_chip('F1', m.get('f1'), norm=float(m.get('f1', 0.0)))}"
        f"{_gauge_chip('PR-AUC', m.get('pr_auc'), norm=float(m.get('pr_auc', 0.0)))}"
    )
    rails = []
    for name, vals in _ordered_models(models).items():
        f1 = float(vals.get("f1", 0.0))
        width = max(2.0, min(100.0, f1 * 100.0))
        rails.append(
            "<div class='rail-row'>"
            f"<span class='rail-label'>{name}</span>"
            f"<div class='rail-track'><div class='rail-fill' style='width:{width:.2f}%'></div></div>"
            f"<span class='rail-val'>{_fmt(f1)}</span>"
            "</div>"
        )
    return (
        "<div class='instrument-grid'>"
        "<div class='instrument-card'>"
        f"<div class='dash-title'>Primary Dials ({selected_name})</div>"
        f"{gauges}"
        "</div>"
        "<div class='instrument-card instrument-card--rails'>"
        "<div class='dash-title'>F1 Leaderboard Rails</div>"
        "<div class='rails-body'>"
        + "".join(rails)
        + "</div>"
        + "</div>"
        "</div>"
    )


def _instrument_lift_panel(lift: dict[str, Any], preferred: str | None) -> str:
    if not lift:
        return "<div class='dash-empty'>No lift rows found.</div>"
    selected_name = _pick_selected_name(lift, preferred)
    if not selected_name:
        return "<div class='dash-empty'>No lift rows found.</div>"
    m = lift.get(selected_name, {})
    return (
        "<div class='instrument-grid'>"
        "<div class='instrument-card'>"
        f"<div class='dash-title'>Delta Dials ({selected_name})</div>"
        f"{_gauge_chip('Delta F1', m.get('delta_f1'), norm=0.5 + 0.5 * float(m.get('delta_f1', 0.0)), min_value=-1.0, max_value=1.0)}"
        f"{_gauge_chip('Delta PR-AUC', m.get('delta_pr_auc'), norm=0.5 + 0.5 * float(m.get('delta_pr_auc', 0.0)), min_value=-1.0, max_value=1.0)}"
        f"{_gauge_chip('Delta Recall', m.get('delta_recall'), norm=0.5 + 0.5 * float(m.get('delta_recall', 0.0)), min_value=-1.0, max_value=1.0)}"
        f"{_gauge_chip('Delta Precision', m.get('delta_precision'), norm=0.5 + 0.5 * float(m.get('delta_precision', 0.0)), min_value=-1.0, max_value=1.0)}"
        "</div>"
        "</div>"
    )


def _ci_instrument(ci_block: dict[str, Any], preferred: str | None) -> str:
    if not ci_block:
        return "<div class='dash-empty'>No CI data.</div>"
    selected_name = _pick_selected_name(ci_block, preferred)
    if not selected_name:
        return "<div class='dash-empty'>No CI data.</div>"
    metrics = ci_block.get(selected_name, {})

    def _ci_bar(metric_name: str, metric_payload: dict[str, Any]) -> str:
        low = float(metric_payload.get("low", 0.0))
        high = float(metric_payload.get("high", 0.0))
        lo = max(0.0, min(1.0, low))
        hi = max(0.0, min(1.0, high))
        width = max(1.0, (hi - lo) * 100.0)
        return (
            "<div class='ci-row'>"
            f"<span class='ci-label'>{metric_name}</span>"
            "<div class='ci-track'>"
            f"<div class='ci-band' style='left:{lo*100:.2f}%; width:{width:.2f}%'></div>"
            "</div>"
            f"<span class='ci-val'>{_fmt(low)}-{_fmt(high)}</span>"
            "</div>"
        )

    return (
        "<div class='instrument-grid'>"
        "<div class='instrument-card'>"
        f"<div class='dash-title'>Tolerance Bars ({selected_name})</div>"
        f"{_ci_bar('Precision CI', metrics.get('precision', {}))}"
        f"{_ci_bar('Recall CI', metrics.get('recall', {}))}"
        f"{_ci_bar('F1 CI', metrics.get('f1', {}))}"
        "</div>"
        "</div>"
    )


def _traffic_light_icon(is_ok: Any) -> str:
    if is_ok is True:
        return "<span class='tl tl-green' aria-label='deployable'></span>"
    if is_ok is False:
        return "<span class='tl tl-red' aria-label='not-deployable'></span>"
    return "<span class='tl tl-yellow' aria-label='unknown'></span>"


def _go_no_go_banner(deploy: dict[str, Any]) -> str:
    statuses: list[bool] = []
    for key, value in deploy.items():
        if not (isinstance(value, dict) and "deployable" in value):
            continue
        required = value.get("required")
        if required is None:
            # Backward-compatible default: temporal primary is binding.
            required = key == "temporal_primary"
        if required:
            statuses.append(bool(value.get("deployable")))
    if statuses and all(s is True for s in statuses):
        return "<div class='go-banner go'>GO - Binding deployment readiness checks passed.</div>"
    if any(s is False for s in statuses):
        return "<div class='go-banner no-go'>NO-GO - At least one binding deployment check failed.</div>"
    return "<div class='go-banner caution'>CAUTION - Deployment readiness is incomplete or unavailable.</div>"


def _deploy_cards(deploy: dict[str, Any]) -> str:
    temporal = deploy.get("temporal_primary", {}) if isinstance(deploy, dict) else {}
    recent = deploy.get("recent_24_week", {}) if isinstance(deploy, dict) else {}
    t_ok = temporal.get("deployable")
    r_ok = recent.get("deployable")
    t_required = temporal.get("required", True)
    r_required = recent.get("required", False)
    return (
        f"<div class='card deploy-card {'ok' if t_ok is True else 'bad' if t_ok is False else 'warn'}'>"
        f"<strong>Deploy: Temporal Primary ({'Required' if t_required else 'Advisory'})</strong>"
        f"<div class='deploy-state'>{_traffic_light_icon(t_ok)} <span>{t_ok}</span></div></div>"
        f"<div class='card deploy-card {'ok' if r_ok is True else 'bad' if r_ok is False else 'warn'}'>"
        f"<strong>Deploy: Recent Operational Window ({'Required' if r_required else 'Advisory'})</strong>"
        f"<div class='deploy-state'>{_traffic_light_icon(r_ok)} <span>{r_ok}</span></div></div>"
    )


def _section(title: str, split_block: dict[str, Any], section_id: str, selected_model_name: str | None) -> str:
    models = _ordered_models(split_block.get("models", {}) or {})
    baselines = split_block.get("baselines", {})
    lift = split_block.get("model_vs_baseline_lift", {})
    return f"""
<section class="panel">
  <h2>{title}</h2>
  <div class="meta">
    <span>Test rows: {split_block.get('test_rows', '-')}</span>
    <span>Test positives: {split_block.get('test_positives', '-')}</span>
    <span>Positive rate: {_fmt(split_block.get('test_positive_rate'))}</span>
  </div>
  <div class="table-view">
    <h3>Model Metrics</h3>
    <table>
      <thead><tr><th>Model</th><th>Precision</th><th>Recall</th><th>F1</th><th>ROC-AUC</th><th>PR-AUC</th></tr></thead>
      <tbody>{_models_table_rows(models)}</tbody>
    </table>
    <h3>Baselines</h3>
    <table>
      <thead><tr><th>Baseline</th><th>Accuracy</th><th>F1</th><th>PR-AUC</th><th>Recall</th><th>Precision</th></tr></thead>
      <tbody>{_baseline_rows(baselines)}</tbody>
    </table>
    <h3>Lift vs Baseline</h3>
    <table>
      <thead><tr><th>Model</th><th>Baseline</th><th>Delta F1</th><th>Delta PR-AUC</th><th>Delta Recall</th><th>Delta Precision</th></tr></thead>
      <tbody>{_lift_rows(lift)}</tbody>
    </table>
  </div>
  <div class="dash-view">
    <h3>Model Metrics</h3>
    {_models_dash(models, section_id)}
    <h3>Baselines</h3>
    {_baselines_dash(baselines)}
    <h3>Lift vs Baseline</h3>
    {_lift_dash(lift)}
  </div>
  <div class="instrument-view">
    <h3>Primary Diagnostics</h3>
    {_instrument_model_panel(models, selected_model_name)}
    <h3>Delta Diagnostics</h3>
    {_instrument_lift_panel(lift, selected_model_name)}
  </div>
</section>
"""


def _ci_rows(ci_block: dict[str, Any], model_names: list[str]) -> str:
    """One row per evaluated model; missing CI shows as dash (no silent omission)."""
    if not model_names:
        return "<tr><td colspan='4'>No models evaluated.</td></tr>"
    rows: list[str] = []
    for model_name in model_names:
        metrics = ci_block.get(model_name) if isinstance(ci_block, dict) else None
        if not isinstance(metrics, dict):
            rows.append(
                "<tr>"
                f"<td>{model_name}</td>"
                "<td>-</td><td>-</td><td>-</td>"
                "</tr>"
            )
            continue
        p = metrics.get("precision", {}) or {}
        r = metrics.get("recall", {}) or {}
        f = metrics.get("f1", {}) or {}
        rows.append(
            "<tr>"
            f"<td>{model_name}</td>"
            f"<td>{_fmt(p.get('low'))} - {_fmt(p.get('high'))}</td>"
            f"<td>{_fmt(r.get('low'))} - {_fmt(r.get('high'))}</td>"
            f"<td>{_fmt(f.get('low'))} - {_fmt(f.get('high'))}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def _ci_dash(ci_block: dict[str, Any], model_names: list[str]) -> str:
    if not model_names:
        return "<div class='dash-empty'>No models evaluated.</div>"
    cards: list[str] = []
    for model_name in model_names:
        metrics = ci_block.get(model_name) if isinstance(ci_block, dict) else None
        if not isinstance(metrics, dict):
            cards.append(
                "<div class='dash-card'>"
                f"<div class='dash-title'>{model_name}</div>"
                "<div class='kpi-chip'><div class='kpi-label'>Precision CI</div><div class='kpi-value'>-</div></div>"
                "<div class='kpi-chip'><div class='kpi-label'>Recall CI</div><div class='kpi-value'>-</div></div>"
                "<div class='kpi-chip'><div class='kpi-label'>F1 CI</div><div class='kpi-value'>-</div></div>"
                "</div>"
            )
            continue
        p = metrics.get("precision", {}) or {}
        r = metrics.get("recall", {}) or {}
        f = metrics.get("f1", {}) or {}
        cards.append(
            "<div class='dash-card'>"
            f"<div class='dash-title'>{model_name}</div>"
            f"<div class='kpi-chip'><div class='kpi-label'>Precision CI</div><div class='kpi-value'>{_fmt(p.get('low'))} - {_fmt(p.get('high'))}</div></div>"
            f"<div class='kpi-chip'><div class='kpi-label'>Recall CI</div><div class='kpi-value'>{_fmt(r.get('low'))} - {_fmt(r.get('high'))}</div></div>"
            f"<div class='kpi-chip'><div class='kpi-label'>F1 CI</div><div class='kpi-value'>{_fmt(f.get('low'))} - {_fmt(f.get('high'))}</div></div>"
            "</div>"
        )
    return "<div class='dash-grid'>" + "".join(cards) + "</div>"


def _figure_card(rel_path: str, title: str) -> str:
    return f"""
<figure class="chart">
  <figcaption>{title}</figcaption>
  <img src="{rel_path}" alt="{title}" loading="lazy" />
</figure>
"""


def _png_data_uri(path: Path) -> str:
    raw = path.read_bytes()
    b64 = base64.b64encode(raw).decode("ascii")
    return f"data:image/png;base64,{b64}"


def generate_dashboard(project_root: str | Path | None = None) -> Path:
    root = Path(project_root) if project_root else Path(__file__).resolve().parents[1]
    metrics_path = root / "models" / "classification_metrics_v2_ordertime.json"
    if not metrics_path.exists():
        raise FileNotFoundError(f"Missing metrics artifact: {metrics_path}")
    metrics = json.loads(metrics_path.read_text())

    output_dir = root / "output" / "dashboard"
    output_dir.mkdir(parents=True, exist_ok=True)
    out_html = output_dir / "model_health_dashboard.html"

    selected = metrics.get("selected_model", {})
    selected_name = selected.get("name") if isinstance(selected, dict) else None
    gate = metrics.get("diagnostics", {}).get("label_maturity_gate", {})
    deploy = metrics.get("deployment_readiness", {})
    ci = metrics.get("confidence_intervals", {})
    temporal = metrics.get("temporal_holdout", {})
    group = metrics.get("group_holdout", {})
    recent = metrics.get("recent_24_week_temporal_holdout", {})
    recent_weeks = recent.get("window_weeks", 24)
    all_model_names = _all_cockpit_model_names(metrics)

    chart_categories = {
        "Governance & Readiness": [
            ("evidence_ci_errorbars_v2_ordertime.png", "95% Bootstrap CI Error Bars"),
            ("evidence_decision_curve_v2_ordertime.png", "Decision Curve Analysis"),
            ("evidence_calibration_ci_v2_ordertime.png", "Calibration with Confidence Bands"),
            ("temporal_positive_rate_drift_v2_ordertime.png", "Temporal Positive Rate Drift"),
            ("evidence_drift_performance_overlay_v2_ordertime.png", "Drift / Performance Overlay"),
            ("target_balance_v2_ordertime.png", "Target Balance"),
            ("evidence_temporal_snapshot_live_v2_ordertime.png", "Temporal Snapshot (Live)"),
        ],
        "Performance & Discrimination": [
            ("roc_curves_temporal_v2_ordertime.png", "Temporal ROC Curves"),
            ("pr_curves_temporal_v2_ordertime.png", "Temporal PR Curves"),
            ("evidence_pr_gain_v2_ordertime.png", "PR-Gain Curve"),
            ("evidence_det_curve_v2_ordertime.png", "DET Curve"),
            ("evidence_lift_gains_v2_ordertime.png", "Lift and Cumulative Gains"),
            ("evidence_ks_curve_v2_ordertime.png", "KS Curve"),
            ("evidence_precision_recall_scatter_v2_ordertime.png", "Precision-Recall Scatter by Split"),
            ("classification_confusion_matrices_v2_ordertime.png", "Confusion Matrices"),
            ("score_distribution_temporal_v2_ordertime.png", "Temporal Score Distribution"),
            ("evidence_model_comparison_heatmap_live_v2_ordertime.png", "Model Comparison Heatmap (Live)"),
            ("evidence_pvalue_bootstrap_hist_v2_ordertime.png", "Bootstrap F1 Lift Distribution (p-value style)"),
        ],
        "Interpretability & Feature Evidence": [
            ("classification_feature_importance_v2_ordertime.png", "Feature Importance"),
            ("evidence_permutation_importance_ci_v2_ordertime.png", "Permutation Importance with CI"),
            ("evidence_pdp_ice_top3_v2_ordertime.png", "PDP + ICE (Top 3 Numeric Features)"),
            ("evidence_brier_decomposition_v2_ordertime.png", "Brier Decomposition"),
        ],
    }
    figures_dir = root / "output" / "figures"
    category_sections: list[str] = []
    for category, charts in chart_categories.items():
        cards: list[str] = []
        for filename, title in charts:
            figure_path = figures_dir / filename
            if not figure_path.exists():
                continue
            cards.append(_figure_card(_png_data_uri(figure_path), title))
        if cards:
            category_sections.append(f"<div class='category'><h3>{category}</h3><div class='grid'>{''.join(cards)}</div></div>")
    chart_html = "\n".join(category_sections)

    gen_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    html = f"""<!doctype html>
<html lang="en">
<head>
  <!-- Snapshot: metrics baked at {gen_ts}. Source JSON: models/classification_metrics_v2_ordertime.json (local artifact). Champion rule: docs/md/modeling_experiment_protocol.md section 9. -->
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Model Health Cockpit</title>
  <style>
    :root {{ color-scheme: dark; }}
    body {{ font-family: Inter, Segoe UI, Arial, sans-serif; margin: 0; background: #0d1117; color: #e6edf3; }}
    .wrap {{ padding: 18px; display: grid; gap: 16px; }}
    .kpis {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }}
    .card, .panel {{ background: #161b22; border: 1px solid #30363d; border-radius: 12px; padding: 14px; }}
    h1, h2, h3 {{ margin: 0 0 10px; }}
    h1 {{ font-size: 34px; line-height: 1.1; letter-spacing: 0.4px; font-weight: 800; }}
    h2 {{ font-size: 24px; line-height: 1.2; font-weight: 700; margin-bottom: 14px; }}
    h3 {{ font-size: 17px; color: #c3ccd7; margin-top: 18px; margin-bottom: 10px; font-weight: 650; }}
    .meta {{ display: flex; gap: 16px; flex-wrap: wrap; color: #9da7b3; font-size: 12px; margin-bottom: 8px; }}
    .meta-row {{ display:flex; align-items:center; justify-content:space-between; gap:12px; flex-wrap:wrap; }}
    .control-bar {{ position: sticky; top: 0; z-index: 1000; display:flex; justify-content:space-between; align-items:center; gap:10px; flex-wrap:wrap; background:#0f1622; border:1px solid #2f3a4a; border-radius:10px; padding:10px 12px; margin-top:12px; }}
    .control-right {{ display:flex; align-items:center; gap:8px; }}
    .mode-badge {{ font-size:12px; font-weight:700; border:1px solid #385177; color:#d5e8ff; background:#1f314d; border-radius:999px; padding:6px 10px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
    th, td {{ border-bottom: 1px solid #30363d; padding: 6px 8px; text-align: left; }}
    th {{ color: #9da7b3; font-weight: 600; }}
    td.metric-cell {{ transition: background-color 0.2s ease; border-radius: 4px; }}
    .grid {{ display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); }}
    .chart {{ margin: 0; background: #161b22; border: 1px solid #3f4a57; border-radius: 10px; padding: 10px; }}
    figcaption {{ margin-bottom: 8px; font-size: 12px; color: #9da7b3; }}
    img {{ width: 100%; height: auto; border-radius: 6px; background: #0d1117; }}
    .category {{ border-top: 2px solid #2a3350; padding-top: 14px; margin-top: 14px; }}
    .category:first-of-type {{ border-top: 0; padding-top: 0; margin-top: 0; }}
    .hero-sub {{ font-size: 13px; color: #9da7b3; margin-top: 6px; }}
    .champion-note {{ margin-top: 12px; padding: 10px 12px; border-radius: 8px; border: 1px solid #385177; background: #141c2e; color: #c8d7ea; font-size: 12px; line-height: 1.45; max-width: 920px; }}
    .toggle-btn {{ background:#24304a; border:1px solid #3a4e7a; color:#e6edf3; border-radius:8px; padding:8px 12px; font-size:12px; cursor:pointer; }}
    .toggle-btn.active {{ background:#2f6a42; border-color:#3d8c59; color:#e8fff0; }}
    .go-banner {{ border-radius:10px; padding:10px 12px; margin-top:12px; font-size:13px; font-weight:700; border:1px solid transparent; }}
    .go-banner.go {{ background:#173a24; border-color:#2f6a42; color:#d6f4df; }}
    .go-banner.no-go {{ background:#4b1f24; border-color:#8d3c47; color:#ffd9dd; }}
    .go-banner.caution {{ background:#4a3a16; border-color:#8a7133; color:#ffeec2; }}
    .dash-view, .instrument-view {{ display:none; }}
    body.grid-mode .table-view {{ display:block; }}
    body.grid-mode .dash-view, body.grid-mode .instrument-view {{ display:none; }}
    body.dash-mode .table-view {{ display:none; }}
    body.dash-mode .dash-view {{ display:block; }}
    body.dash-mode .instrument-view {{ display:none; }}
    body.instrument-mode .table-view, body.instrument-mode .dash-view {{ display:none; }}
    body.instrument-mode .instrument-view {{ display:block; }}
    .dash-grid {{ display:grid; gap:10px; grid-template-columns:repeat(auto-fit, minmax(220px,1fr)); }}
    .dash-card {{ border:1px solid #3a4250; border-radius:10px; padding:10px; background:#121820; }}
    .dash-title {{ font-size:13px; font-weight:700; margin-bottom:8px; }}
    .dash-sub {{ font-size:11px; color:#9da7b3; margin-bottom:8px; }}
    .kpi-chip {{ border:1px solid #333b49; border-radius:8px; padding:8px; margin-bottom:8px; background:#171f2b; }}
    .kpi-label {{ font-size:11px; color:#9da7b3; }}
    .kpi-value {{ font-size:14px; font-weight:700; margin-top:2px; }}
    .dash-empty {{ font-size:12px; color:#9da7b3; padding:8px 0; }}
    .gauge-chip {{ border:1px solid #333b49; border-radius:8px; padding:8px; margin-bottom:8px; background:#171f2b; }}
    .gauge-label {{ font-size:11px; color:#9da7b3; margin-bottom:6px; }}
    .gauge {{ position:relative; width:100%; max-width:180px; margin:0 auto; aspect-ratio: 2 / 1; border-radius: 180px 180px 0 0; overflow:hidden; background: conic-gradient(from 180deg, var(--gauge-color) calc(var(--pct) * 180deg), #2c3646 0deg); }}
    .gauge::before {{ content:''; position:absolute; left:12%; right:12%; bottom:0; top:24%; border-radius: 160px 160px 0 0; background:#121820; }}
    .gauge-ticks {{ position:absolute; inset:0; z-index:2; pointer-events:none; }}
    .tick {{ position:absolute; width:2px; height:14px; background:#93a4bc; left:50%; bottom:0; transform-origin: 50% 100%; opacity:0.85; }}
    .tick.t0 {{ transform: rotate(-90deg) translateY(-78px); }}
    .tick.t25 {{ transform: rotate(-45deg) translateY(-78px); }}
    .tick.t50 {{ transform: rotate(0deg) translateY(-78px); }}
    .tick.t75 {{ transform: rotate(45deg) translateY(-78px); }}
    .tick.t100 {{ transform: rotate(90deg) translateY(-78px); }}
    .gauge-needle {{ position:absolute; left:50%; bottom:0; width:3px; height:72px; background:linear-gradient(180deg,#f8fafc,#c8d2df); border-radius:4px; transform-origin: 50% 100%; transform: translateX(-50%) rotate(var(--needle-angle)); z-index:4; box-shadow: 0 0 4px rgba(0,0,0,0.6); }}
    .gauge-hub {{ position:absolute; left:50%; bottom:0; width:12px; height:12px; border-radius:50%; background:#d6dee9; border:2px solid #0f1723; transform:translate(-50%,50%); z-index:5; }}
    .gauge-value {{ position:absolute; left:0; right:0; bottom:8px; text-align:center; font-size:14px; font-weight:700; z-index:1; }}
    .gauge-scale {{ display:flex; justify-content:space-between; color:#7f8ba0; font-size:10px; margin-top:5px; }}
    .dash-sort-row {{ display:flex; align-items:center; gap:8px; margin-bottom:8px; font-size:12px; color:#9da7b3; }}
    .sort-btn {{ background:#1f2531; border:1px solid #3b465b; color:#d5ddeb; border-radius:7px; padding:6px 10px; font-size:12px; cursor:pointer; }}
    .deploy-card {{ display:flex; flex-direction:column; gap:8px; }}
    .deploy-state {{ display:flex; align-items:center; gap:8px; font-weight:700; }}
    .tl {{ width:10px; height:10px; border-radius:50%; display:inline-block; border:1px solid rgba(255,255,255,0.25); }}
    .tl-green {{ background:#39d98a; }}
    .tl-red {{ background:#ff5d5d; }}
    .tl-yellow {{ background:#ffd166; }}
    .deploy-card.ok {{ border-color:#2f6a42; }}
    .deploy-card.bad {{ border-color:#8d3c47; }}
    .deploy-card.warn {{ border-color:#8a7133; }}
    .instrument-grid {{ display:grid; gap:10px; grid-template-columns:repeat(auto-fit, minmax(300px,1fr)); align-items:stretch; }}
    .instrument-card {{ border:1px solid #3a4250; border-radius:10px; padding:10px; background:#121820; display:flex; flex-direction:column; min-height:0; height:100%; box-sizing:border-box; }}
    .rail-row, .ci-row {{ display:grid; grid-template-columns: 130px 1fr 95px; align-items:center; gap:8px; margin:6px 0; }}
    .instrument-card--rails .rails-body {{ flex:1 1 auto; display:flex; flex-direction:column; gap:clamp(6px, 1.2vh, 14px); min-height:0; justify-content:space-between; }}
    .instrument-card--rails .rail-row {{ flex:1 1 0; margin:0; min-height:48px; grid-template-columns:minmax(108px, 32%) 1fr minmax(76px, 22%); gap:10px; align-items:center; }}
    .rail-label, .ci-label {{ font-size:11px; color:#b6c1cf; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }}
    .instrument-card--rails .rail-label {{ font-size:clamp(12px, 1.05vw, 14px); font-weight:600; }}
    .rail-track, .ci-track {{ height:10px; border-radius:999px; background:#1d2734; position:relative; }}
    .instrument-card--rails .rail-track {{ height:clamp(20px, 4.2vmin, 44px); box-shadow: inset 0 1px 0 rgba(255,255,255,0.06); }}
    .rail-fill {{ height:100%; border-radius:999px; background:linear-gradient(90deg, #8252C7, #00B5E2); }}
    .rail-val, .ci-val {{ font-size:11px; color:#dce5f3; text-align:right; }}
    .instrument-card--rails .rail-val {{ font-size:clamp(12px, 1.05vw, 15px); font-weight:700; font-variant-numeric: tabular-nums; }}
    .ci-band {{ position:absolute; top:0; bottom:0; border-radius:999px; background:linear-gradient(90deg, #9D581F, #00B5E2); }}
  </style>
</head>
<body class="grid-mode">
  <div class="wrap">
    <div class="card">
      <h1>Model Health Cockpit</h1>
      <div class="hero-sub">Single-pane operational model health, governance, and evidence.</div>
      <div class="champion-note"><strong>Champion (defensible):</strong> choose the primary model using <em>inner temporal validation</em> on temporal-train rows only—never because it wins group holdout or another diagnostic slice alone. <strong>Temporal holdout</strong> reports forward-time behavior at the frozen threshold. Diagnostic splits are context, not a second crown.</div>
      {_go_no_go_banner(deploy)}
      <div class="meta">
        <span>Updated: {gen_ts}</span>
        <span>Selected model: {selected.get('name', '-')}</span>
        <span>Temporal split strategy: {temporal.get('strategy', '-')}</span>
        <span>Maturity gate pass: {gate.get('passed', '-')}</span>
        <span>Gate profile: {gate.get('profile', '-')}</span>
        <span>Models in cockpit (union): {", ".join(all_model_names)}</span>
      </div>
      <div class="control-bar">
        <div class="control-left">View Controls</div>
        <div class="control-right">
          <span id="modeBadge" class="mode-badge">Mode: Grid</span>
          <button id="gridToggle" class="toggle-btn active" type="button">Grid</button>
          <button id="dashToggle" class="toggle-btn" type="button">Dash</button>
          <button id="instrumentToggle" class="toggle-btn" type="button">Instrument</button>
        </div>
      </div>
      <div class="meta-row">
        <div class="meta">
          <span>Toggle affects metric sections only.</span>
          <span>Evidence charts remain unchanged.</span>
        </div>
      </div>
    </div>

    <div class="kpis">
      <div class="card"><strong>Temporal test rows</strong><div>{temporal.get('test_rows', '-')}</div></div>
      <div class="card"><strong>Temporal positives</strong><div>{temporal.get('test_positives', '-')}</div></div>
      <div class="card"><strong>Temporal positive rate</strong><div>{_fmt(temporal.get('test_positive_rate'))}</div></div>
      <div class="card"><strong>Dataset positive rate</strong><div>{_fmt(metrics.get('dataset_summary', {}).get('positive_rate'))}</div></div>
      {_deploy_cards(deploy)}
    </div>

    {_section("Temporal Holdout (Primary)", temporal, "dash-models-temporal", selected_name)}
    {_section("Group Holdout (Diagnostic)", group, "dash-models-group", selected_name)}
    {_section(f"Recent Operational Window ({recent_weeks}-Week) Holdout", recent, "dash-models-recent", selected_name)}

    <section class="panel">
      <h2>Bootstrap Confidence Intervals (95%)</h2>
      <div class="table-view">
        <h3>Temporal Primary</h3>
        <table>
          <thead><tr><th>Model</th><th>Precision CI</th><th>Recall CI</th><th>F1 CI</th></tr></thead>
          <tbody>{_ci_rows(ci.get('temporal_primary', {}), all_model_names)}</tbody>
        </table>
        <h3>Recent Operational Window</h3>
        <table>
          <thead><tr><th>Model</th><th>Precision CI</th><th>Recall CI</th><th>F1 CI</th></tr></thead>
          <tbody>{_ci_rows(ci.get('recent_24_week', {}), all_model_names)}</tbody>
        </table>
      </div>
      <div class="dash-view">
        <h3>Temporal Primary</h3>
        {_ci_dash(ci.get('temporal_primary', {}), all_model_names)}
        <h3>Recent Operational Window</h3>
        {_ci_dash(ci.get('recent_24_week', {}), all_model_names)}
      </div>
      <div class="instrument-view">
        <h3>Temporal Primary</h3>
        {_ci_instrument(ci.get('temporal_primary', {}), selected_name)}
        <h3>Recent Operational Window</h3>
        {_ci_instrument(ci.get('recent_24_week', {}), selected_name)}
      </div>
    </section>

    <section class="panel">
      <h2>Evidence Charts</h2>
      {chart_html if chart_html else "<div class='card'>No chart artifacts found.</div>"}
    </section>
  </div>
  <script>
    (function() {{
      const gridBtn = document.getElementById("gridToggle");
      const dashBtn = document.getElementById("dashToggle");
      const instrumentBtn = document.getElementById("instrumentToggle");
      const badge = document.getElementById("modeBadge");
      function setMode(mode) {{
        document.body.classList.remove("grid-mode", "dash-mode", "instrument-mode");
        document.body.classList.add(mode + "-mode");
        if (badge) badge.textContent = "Mode: " + (mode === "grid" ? "Grid" : mode === "dash" ? "Dash" : "Instrument");
        [gridBtn, dashBtn, instrumentBtn].forEach(function(b) {{
          if (!b) return;
          b.classList.remove("active");
        }});
        if (mode === "grid" && gridBtn) gridBtn.classList.add("active");
        if (mode === "dash" && dashBtn) dashBtn.classList.add("active");
        if (mode === "instrument" && instrumentBtn) instrumentBtn.classList.add("active");
      }}
      if (gridBtn) gridBtn.addEventListener("click", function() {{ setMode("grid"); }});
      if (dashBtn) dashBtn.addEventListener("click", function() {{ setMode("dash"); }});
      if (instrumentBtn) instrumentBtn.addEventListener("click", function() {{ setMode("instrument"); }});
      document.querySelectorAll(".sort-btn").forEach(function(sortBtn) {{
        sortBtn.addEventListener("click", function() {{
          const target = sortBtn.getAttribute("data-target");
          const metric = sortBtn.getAttribute("data-metric");
          if (!target || !metric) return;
          const grid = document.getElementById(target);
          if (!grid) return;
          const cards = Array.from(grid.querySelectorAll(".dash-card.sortable"));
          cards.sort(function(a, b) {{
            const av = parseFloat(a.getAttribute("data-" + metric) || "0");
            const bv = parseFloat(b.getAttribute("data-" + metric) || "0");
            return bv - av;
          }});
          cards.forEach(function(c) {{ grid.appendChild(c); }});
        }});
      }});
      setMode("grid");
    }})();
  </script>
</body>
</html>
"""

    out_html.write_text(html)
    return out_html


if __name__ == "__main__":
    path = generate_dashboard()
    print(f"Wrote dashboard: {path}")
