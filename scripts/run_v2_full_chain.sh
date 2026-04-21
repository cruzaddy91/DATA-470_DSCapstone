#!/usr/bin/env bash
# Full v2 backorder pipeline in dependency order:
#   1) data pipeline (master tables + BRD)
#   2) build_targets (v2 ordertime CSV + optional snapshot/v3 side tables)
#   3) modeling (LR + LightGBM metrics + figures)
#   4) HTML side-by-side report
#
# Usage (from repo root):
#   ./scripts/run_v2_full_chain.sh
#   V2_PYTHON=/path/to/python ./scripts/run_v2_full_chain.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -n "${V2_PYTHON:-}" ]]; then
  PY="$V2_PYTHON"
elif [[ -x "$ROOT/.venv-v2/bin/python" ]]; then
  PY="$ROOT/.venv-v2/bin/python"
else
  PY="${PYTHON:-python3}"
fi

echo "==> v2 full chain using: $PY"
echo "==> [1/4] Data pipeline (run_pipeline)"
"$PY" "$ROOT/run_pipeline.py"

echo "==> [2/4] Build targets (v2 modeling table + related outputs)"
"$PY" -m src.features.build_targets

echo "==> [3/4] Train/evaluate classifiers"
"$PY" "$ROOT/scripts/run_modeling.py"

echo "==> [4/4] Model comparison HTML"
"$PY" "$ROOT/scripts/generate_model_performance_side_by_side_html.py"

echo ""
echo "Done."
echo "  Metrics:  $ROOT/models/classification_metrics_v2_ordertime.json"
echo "  Report:   $ROOT/docs/html/Model-Performance-SideBySide.html"
echo "  Figures:  $ROOT/output/figures/"
echo "  Temporal scores (ROC/PR inputs): $ROOT/models/temporal_holdout_test_scores_v2_ordertime.json"
