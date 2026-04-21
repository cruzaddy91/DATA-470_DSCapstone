#!/usr/bin/env bash
# Regenerate poster PNGs (when inputs exist) + PowerPoint + font check.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export PYTHONPATH="$ROOT"

export MPLBACKEND="${MPLBACKEND:-Agg}"

# Prefer project venv so matplotlib/numpy stay compatible (system conda may break).
if [[ -x "$ROOT/.venv-v2/bin/python" ]]; then
  PY="$ROOT/.venv-v2/bin/python"
elif [[ -x "$ROOT/.venv_pr/bin/python" ]]; then
  PY="$ROOT/.venv_pr/bin/python"
else
  PY="python3"
fi
echo "Using Python (figures): $PY"

# python-pptx may only be on system Python — use it for the deck if venv lacks it.
if "$PY" -c "import pptx" 2>/dev/null; then
  PPT_PY="$PY"
else
  PPT_PY="${PPT_PY:-python3}"
  echo "Using Python (PowerPoint): $PPT_PY (pptx not in venv)"
fi

echo "== Canonical visual spec (YAML from CSV/JSON truth) =="
if [[ -f "data/processed/master_order_fulfillment_modeling_v2_ordertime.csv" \
      && -f "output/tables/classification_model_comparison_v2_ordertime.csv" \
      && -f "models/temporal_holdout_test_scores_v2_ordertime.json" ]]; then
  if ! "$PY" scripts/build_canonical_poster_visual_spec.py; then
    echo "  warning: build_canonical_poster_visual_spec failed; falling back to legacy figure scripts"
    if [[ -f "data/processed/master_order_fulfillment_modeling_v2_ordertime.csv" ]]; then
      "$PY" scripts/generate_target_balance.py
    fi
    if [[ -f "output/tables/classification_model_comparison_v2_ordertime.csv" ]]; then
      "$PY" scripts/generate_showcase_metrics_visuals.py || true
    fi
    if [[ -f "models/temporal_holdout_test_scores_v2_ordertime.json" ]]; then
      "$PY" scripts/generate_poster_figures_v2.py
    fi
  else
    echo "== Template renders (Seaborn/mako from canonical_poster_visual_spec.yaml) =="
    "$PY" scripts/render_poster_visuals_from_canonical_spec.py
  fi
else
  echo "  skip: need master CSV + classification table + temporal_holdout JSON for spec pipeline"
  if [[ -f "data/processed/master_order_fulfillment_modeling_v2_ordertime.csv" ]]; then
    "$PY" scripts/generate_target_balance.py
  fi
  if [[ -f "output/tables/classification_model_comparison_v2_ordertime.csv" ]]; then
    "$PY" scripts/generate_showcase_metrics_visuals.py || true
  fi
  if [[ -f "models/temporal_holdout_test_scores_v2_ordertime.json" ]]; then
    "$PY" scripts/generate_poster_figures_v2.py
  fi
fi

echo "== Mermaid diagrams (poster/diagrams → PNG) =="
bash "$ROOT/scripts/render_poster_diagrams.sh"

echo "== PowerPoint =="
"$PPT_PY" build_poster.py

echo "== Poster manifest + PosterCraft input bundle (JSON/YAML from model outputs) =="
"$PY" scripts/generate_poster_visual_manifest.py
"$PY" scripts/export_postercraft_poster_inputs.py

echo "== Font guardrail =="
"$PPT_PY" scripts/verify_poster_fonts.py

echo "== Layout guardrail (heading vs body boxes) =="
"$PPT_PY" scripts/verify_poster_layout.py

echo "Done: $ROOT/DS_Capstone_Poster_FINAL.pptx"
