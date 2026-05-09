#!/usr/bin/env bash
# Regenerate poster PNGs (when inputs exist) + PowerPoint + font check.
#
# Byte identity note: a byte-for-byte clone of a hand deck is only guaranteed by
#   python tools/poster/scripts/poster_replicate_byte_identical.py (plain file copy).
# build_poster.py uses python-pptx + zip media overlay; OOXML bytes will differ from FINAL
# even when layout and fonts match. Truth layout is preserved by cloning FINAL then swapping
# rasters and applying poster_deck_text run text.
#
# Dependency order (each step consumes outputs of the prior):
#   1. build_canonical_poster_visual_spec.py — YAML from master CSV, comparison table, temporal JSON
#   2. render_poster_visuals_from_canonical_spec.py — matplotlib PNGs into output/figures/
#   3. render_poster_diagrams.sh — tools/poster/mermaid/diagrams/*.mmd → *.png (Mermaid CLI)
#   4. build_poster_truth_overlay.py — clones POSTER_TRUTH_PPTX (default: DS_Capstone_Poster_FINAL.pptx)
#      and replaces only ppt/media poster rasters (layout-preserving). See script for MEDIA_REPLACE map.
#   5. generate_poster_visual_manifest.py + export_postercraft_poster_inputs.py — manifests
#   6. verify_poster_fonts.py + verify_poster_layout.py
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
POSTER_SCRIPTS="$ROOT/tools/poster/scripts"
cd "$ROOT"
export PYTHONPATH="$ROOT:$ROOT/tools/poster"

# Script-built deck (does not overwrite FINAL fact unless you set this to that name).
export POSTER_PPTX_OUTPUT="${POSTER_PPTX_OUTPUT:-$ROOT/tools/poster/exports/DS_Capstone_Poster_FINAL_SCRIPT_COPY.pptx}"
# Fact deck to clone before media overlay (never modified by this script).
export POSTER_TRUTH_PPTX="${POSTER_TRUTH_PPTX:-$ROOT/tools/poster/exports/DS_Capstone_Poster_FINAL.pptx}"

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
  if ! "$PY" "$POSTER_SCRIPTS/build_canonical_poster_visual_spec.py"; then
    echo "  warning: build_canonical_poster_visual_spec failed; falling back to legacy figure scripts"
    if [[ -f "data/processed/master_order_fulfillment_modeling_v2_ordertime.csv" ]]; then
      "$PY" scripts/generate_target_balance.py
    fi
    if [[ -f "output/tables/classification_model_comparison_v2_ordertime.csv" ]]; then
      "$PY" scripts/generate_showcase_metrics_visuals.py || true
    fi
    if [[ -f "models/temporal_holdout_test_scores_v2_ordertime.json" ]]; then
      "$PY" "$POSTER_SCRIPTS/generate_poster_figures_v2.py"
    fi
  else
    echo "== Template renders (Westminster palette from canonical_poster_visual_spec.yaml) =="
    "$PY" "$POSTER_SCRIPTS/render_poster_visuals_from_canonical_spec.py"
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
    "$PY" "$POSTER_SCRIPTS/generate_poster_figures_v2.py"
  fi
fi

echo "== Mermaid diagrams (tools/poster/mermaid/diagrams → PNG) =="
bash "$POSTER_SCRIPTS/render_poster_diagrams.sh"

echo "== PowerPoint (truth clone + deterministic media overlay) =="
"$PY" "$POSTER_SCRIPTS/build_poster_truth_overlay.py"

echo "== Apply poster_deck_text + final media package (writes DS_Capstone_Poster_FULL_RENDER.pptx by default) =="
POSTER_FACT_PPTX="$ROOT/tools/poster/exports/DS_Capstone_Poster_FINAL_SCRIPT_COPY.pptx" \
POSTER_PPTX_OUTPUT="$ROOT/tools/poster/exports/DS_Capstone_Poster_FULL_RENDER.pptx" \
"$PPT_PY" "$ROOT/tools/poster/build_poster.py"

export POSTER_PPTX_OUTPUT="$ROOT/tools/poster/exports/DS_Capstone_Poster_FULL_RENDER.pptx"

echo "== Poster manifest + PosterCraft input bundle (JSON/YAML from model outputs) =="
"$PY" "$POSTER_SCRIPTS/generate_poster_visual_manifest.py"
"$PY" "$POSTER_SCRIPTS/export_postercraft_poster_inputs.py"

echo "== Font guardrail (FULL_RENDER) =="
"$PPT_PY" "$POSTER_SCRIPTS/verify_poster_fonts.py"

echo "== Layout guardrail (heading vs body boxes; FULL_RENDER) =="
# Fact FINAL OOXML may use spacing tighter than HEAD_BODY_GAP_EMU; overlay preserves that layout.
if ! "$PPT_PY" "$POSTER_SCRIPTS/verify_poster_layout.py"; then
  echo "  warning: layout verify exited non-zero (often OK for truth-cloned decks)."
fi

echo "Done: overlay+deck-text → $ROOT/tools/poster/exports/DS_Capstone_Poster_FULL_RENDER.pptx (media-only clone: $ROOT/tools/poster/exports/DS_Capstone_Poster_FINAL_SCRIPT_COPY.pptx)"

# Open the deck on macOS after a successful build (set OPEN_POSTER_AFTER_BUILD=0 to skip).
if [[ "${OPEN_POSTER_AFTER_BUILD:-1}" == "1" ]] && command -v open >/dev/null 2>&1; then
  if [[ "$POSTER_PPTX_OUTPUT" = /* ]]; then
    open "$POSTER_PPTX_OUTPUT" || true
  else
    open "$ROOT/$POSTER_PPTX_OUTPUT" || true
  fi
fi
