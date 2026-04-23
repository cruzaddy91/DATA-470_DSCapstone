#!/usr/bin/env bash
# Assemble poster-ready PNGs into output/poster/ (numbered + manifest).
# Run from repo root after: .venv-v2/bin/python scripts/run_modeling.py

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

FIG="$ROOT/output/figures"
OUT="$ROOT/output/poster"
mkdir -p "$OUT"

cp "$FIG/target_balance_v2_ordertime.png" \
  "$OUT/01_column1_target_balance.png"
cp "$FIG/showcase_model_comparison_heatmap.png" \
  "$OUT/02_hero_model_comparison_heatmap.png"
cp "$FIG/roc_curves_temporal_v2_ordertime.png" \
  "$OUT/03_roc_curves_temporal_holdout.png"
cp "$FIG/pr_curves_temporal_v2_ordertime.png" \
  "$OUT/04_pr_curves_temporal_holdout.png"
cp "$FIG/classification_feature_importance_v2_ordertime.png" \
  "$OUT/05_feature_importance.png"
cp "$FIG/temporal_positive_rate_drift_v2_ordertime.png" \
  "$OUT/06_temporal_positive_rate_drift.png"
cp "$FIG/score_distribution_temporal_v2_ordertime.png" \
  "$OUT/07_score_distribution_thresholds_optional.png"

cat > "$OUT/POSTER_MANIFEST.txt" <<'EOF'
Poster bundle (v2 order-time) — suggested layout

  01_column1_target_balance.png
      Class imbalance (backorder vs not).

  02_hero_model_comparison_heatmap.png
      Hero: metrics across splits / models.

  03_roc_curves_temporal_holdout.png
      Required: ROC on temporal holdout test (LR + XGBoost + CatBoost + others when fitted).

  04_pr_curves_temporal_holdout.png
      Required: Precision–recall on temporal holdout (honest rare-class).

  05_feature_importance.png
      What drives predictions (selected / primary model pipeline).

  06_temporal_positive_rate_drift.png
      Why the problem is hard: label rate over time (from diagnostics).

  07_score_distribution_thresholds_optional.png
      Optional Q&A: predicted probabilities + decision thresholds.

Not included in this bundle (EDA / backup): discovery_*.png, showcase_temporal_snapshot,
classification_confusion_matrices_v2_ordertime.png — use only if you need them.

Regenerate figures: .venv-v2/bin/python scripts/run_modeling.py
Rebuild this folder: ./scripts/ship_poster.sh
EOF

echo "Poster bundle ready: $OUT"
ls -la "$OUT"
