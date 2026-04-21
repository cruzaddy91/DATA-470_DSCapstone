#!/usr/bin/env python3
"""Write a simple `v2` model comparison report."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
METRICS = ROOT / "models" / "classification_metrics_v2_ordertime.json"
OUT = ROOT / "docs" / "html" / "Model-Performance-SideBySide.html"

SPLITS = [
    ("temporal_holdout", "Temporal holdout"),
    ("group_holdout", "Grouped holdout"),
    ("recent_24_week_temporal_holdout", "Recent 24-week temporal"),
]

MODEL_ORDER = ["logistic_regression", "lightgbm"]
MODEL_LABELS = {
    "logistic_regression": "Logistic regression",
    "lightgbm": "LightGBM",
}


def fmt(value: object) -> str:
    if value is None:
        return "—"
    return f"{float(value):.4f}"


def main() -> int:
    if not METRICS.exists():
        print(f"Missing {METRICS}; run scripts/run_modeling.py first.", file=sys.stderr)
        return 1

    metrics = json.loads(METRICS.read_text(encoding="utf-8"))
    rows_html: list[str] = []

    for model_name in MODEL_ORDER:
        cells = [f"<td><strong>{MODEL_LABELS[model_name]}</strong></td>"]
        for split_key, _ in SPLITS:
            block = metrics.get(split_key) or {}
            split_metrics = (block.get("models") or {}).get(model_name) or {}
            cells.append(f"<td>{fmt(split_metrics.get('f1'))}</td>")
            cells.append(f"<td>{fmt(split_metrics.get('pr_auc'))}</td>")
            cells.append(f"<td>{fmt(split_metrics.get('roc_auc'))}</td>")
        rows_html.append("<tr>" + "".join(cells) + "</tr>")

    header_cells = ["<th>Model</th>"]
    for _, label in SPLITS:
        header_cells.append(
            f'<th colspan="3">{label}<br/><span style="font-weight:400;font-size:0.85em">'
            "F1 · PR-AUC · ROC-AUC</span></th>"
        )

    selected = metrics.get("selected_model") or {}
    selected_metrics = selected.get("metrics") or {}

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>V2 model comparison</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; background: #f5f5f5; }}
    .wrap {{ max-width: 1200px; margin: 0 auto; background: #fff; padding: 28px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }}
    h1 {{ color: #112467; font-size: 1.5rem; margin-bottom: 8px; }}
    p {{ color: #444; line-height: 1.5; margin-bottom: 18px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.94rem; }}
    th, td {{ border: 1px solid #e0e0e0; padding: 8px 10px; text-align: center; }}
    th {{ background: #f0f2f5; color: #112467; }}
    tr:nth-child(even) td {{ background: #fafafa; }}
    td:first-child, th:first-child {{ text-align: left; }}
    .meta {{ font-size: 0.92rem; color: #555; margin-top: 20px; }}
    code {{ background: #eee; padding: 2px 6px; border-radius: 4px; font-size: 0.88em; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>V2 backorder model comparison</h1>
    <p>The official comparison is limited to the two `v2` classifiers: logistic regression and LightGBM.</p>
    <table>
      <thead>
        <tr>{"".join(header_cells)}</tr>
        <tr><th></th>{"<th>F1</th><th>PR-AUC</th><th>ROC-AUC</th>" * len(SPLITS)}</tr>
      </thead>
      <tbody>
        {"".join(rows_html)}
      </tbody>
    </table>
    <div class="meta">
      <p><strong>Selected model:</strong> {selected.get("name", "—")} on {selected.get("selection_split", "—")}.</p>
      <p><strong>Selected metrics:</strong> F1 {fmt(selected_metrics.get("f1"))}, PR-AUC {fmt(selected_metrics.get("pr_auc"))}, ROC-AUC {fmt(selected_metrics.get("roc_auc"))}.</p>
      <p>Source: <code>models/classification_metrics_v2_ordertime.json</code></p>
    </div>
  </div>
</body>
</html>
"""

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(html, encoding="utf-8")
    print(f"Wrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
