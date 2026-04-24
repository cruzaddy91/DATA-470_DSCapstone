# Performance Experiment Report — DS Capstone

Branch: `model-perf-experiments`  ·  Baseline commit: `cfed054`  ·  Final commit: see `git log`

## TL;DR

After four disciplined experiments on top of a convention-corrected baseline:

- **Logistic Regression remains the best deployable model** on outer temporal holdout (F1 0.517 at F1-max threshold 0.309; precision 0.435; recall 0.638).
- The OOF-calibrated stack **passes both deployment performance gates** (precision ≥ 0.15 ✓, recall ≥ 0.35 ✓) but is outperformed by plain LR on outer metrics. Reasons documented.
- The real deployment blocker is **not model quality**. It is **label maturity** — the most recent 180 days of training data has only 36% label coverage. This blocks the infrastructure deployment gate and contaminates any target-dependent rolling feature.
- Three attempted lifts (threshold policy, feature engineering, cascade) all confirm the ceiling. The feature set has limited headroom on this dataset. Richer order-time signals — especially ones that don't depend on the backorder target — would be needed to move past LR.

## Problem characterization

- Rare-event classification: 0.89% positive rate on outer temporal holdout (58 positives / 6,521 rows).
- Current feature set: 13 raw order-time features + 7 missingness indicators + derived interactions.
- Evaluation: outer temporal holdout (reports-once); champion selected by inner temporal split per protocol §9.
- Honest scale context: random classifier PR-AUC = 0.009. LR at 0.358 = **40× lift over random**. ROC-AUC 0.91 is strong discrimination by any standard.

## Method

Each experiment was one atomic commit on `model-perf-experiments`. Main was never touched. Each step had a predefined decision gate; if a step failed its gate or produced a negative result, the finding was documented rather than buried.

## Baseline (pre-experiments)

After the convention fixes (diversity, OOF threshold non-leakage, SMOTE symmetry, temporal OOF folds, hyperparameter tuning, ten-model comparison):

| Model | ROC | PR | P | R | F1 |
|---|---|---|---|---|---|
| **Logistic Regression** | 0.910 | **0.358** | 0.453 | 0.586 | **0.511** |
| Stack (4 bases) | 0.936 | 0.326 | 0.410 | 0.431 | 0.420 |
| Stack (all 6 bases) | 0.931 | 0.320 | 0.500 | 0.362 | 0.420 |
| Soft Vote (LR+LGBM) | 0.916 | 0.333 | 0.321 | 0.466 | 0.380 |
| Vote (4 bases) | 0.930 | 0.319 | 0.435 | 0.345 | 0.385 |
| LightGBM | 0.814 | 0.201 | 0.295 | 0.483 | 0.366 |
| RandomForest | 0.928 | 0.226 | 0.120 | 0.741 | 0.207 |
| XGBoost | 0.825 | 0.194 | 0.157 | 0.517 | 0.241 |
| CatBoost | 0.850 | 0.129 | 0.135 | 0.345 | 0.194 |
| kNN | 0.791 | 0.179 | 0.444 | 0.069 | 0.119 |

## Step 1 — Calibration diagnostic

**Finding:** base learners are mostly well-calibrated (ECE 0.005–0.047). Stacks are meaningfully miscalibrated (ECE ~0.10) because the meta-LR uses `class_weight="balanced"` which shifts probability outputs.

**Decision:** calibration wrapper not applied. Calibration is a *monotonic* transformation — it does not change rank-based metrics (PR-AUC, ROC-AUC), and we select thresholds empirically from the PR curve. Calibration only matters for business-threshold interpretation, not for F1 lift. Documented as a diagnostic finding.

**Artifacts:** `output/tables/calibration_diagnostic.json`, `output/figures/calibration_reliability.png`.

## Step 2 — Business-utility threshold

Reframed threshold choice from F1-max to expected-cost-min under asymmetric cost ratios k = cost_of_miss / cost_of_false_alarm.

**Finding:** for realistic k ∈ [3, 30] (typical supply-chain cost asymmetry), LR's F1-max threshold (0.309) **is also the min-cost threshold**. The F1 choice and the utility choice converge. Only at k ≥ 100 (line-stop criticality) does the recommended threshold drop to ~0.10, trading precision (5–8%) for recall (74–88%).

**Implication:** F1-max is a robust operational choice. The threshold does not need to move for typical business cost assumptions.

**Artifacts:** `output/tables/business_utility_operating_points.md`, `output/figures/business_utility_curves.png`.

## Step 3 — Feature engineering (NEGATIVE result, documented)

Added seven temporal-safe rolling features per order (closed='left' windowing, no same-day leakage):

- `plant_backorder_rate_90d`, `material_backorder_rate_90d`, `customer_backorder_rate_90d` (target-dependent)
- `plant_order_volume_30d`, `material_order_volume_30d`, `customer_order_volume_30d` (target-independent)
- `plant_confirmation_rate_90d` (target-independent)

**Result:** with all features, Stack F1 dropped from 0.420 to **0.275** (−0.145). LR dropped from 0.511 to 0.492.

**Root cause:** the target-dependent features inherit the **label maturity problem** — the most recent 180 days of training data has only 36% label coverage, so `target_backorder_risk` is systematically under-reported in that period. Rolling rates computed from those rows are biased. Tree-based models (which weight the stack most) latched onto the bias; LR regularized harder and was less affected.

A follow-up run with only target-INDEPENDENT features (volumes + confirmation rate, no backorder rate) **also hurt** performance (Stack F1 → 0.353, LR F1 → 0.354). This suggests the training period (1994–~2021) and test period (2022) have meaningfully different volume and confirmation-rate patterns — classic distribution drift.

**Implication:** on a dataset with a long training range and a short tail test, naive rolling features are fragile. A usable rollup approach would require either (a) masking the last 180 days of training to avoid label maturity bias, (b) retraining on a narrow rolling window rather than 28 years of history, or (c) rebuilding after labels have matured. None of these were in-scope for this experiment.

**Artifacts:** `scripts/build_v2_rolling_features.py`, `data/processed/master_order_fulfillment_modeling_v2_ordertime_rollup.csv`.

## Step 4 — Two-stage cascade (NO lift)

Post-hoc cascade analysis on existing y_proba arrays (no retraining). Four configurations:

| Configuration | Best cascade F1 | vs best single-stage | Δ |
|---|---|---|---|
| LR filter → Stack verifier | 0.491 | 0.517 (LR alone) | **−0.027** |
| LR filter → LightGBM verifier | 0.438 | 0.517 (LR alone) | −0.080 |
| Vote filter → LR verifier | 0.517 | 0.517 (LR alone) | 0.000 |
| Stack-all filter → LR verifier | 0.517 | 0.517 (LR alone) | 0.000 |

**Finding:** no cascade configuration beats single-stage LR. The two that tied did so only by effectively reducing to LR's own decision rule at small top-K.

**Implication:** the separating signal is already captured by LR's ranking. A second-stage filter does not find additional structure in the probability tail. This is consistent with Step 3's finding: the feature set has limited signal headroom that cannot be recovered by modeling layers alone.

**Artifacts:** `output/tables/cascade_analysis.md`, `output/figures/cascade_frontier.png`.

## Deployment readiness

Under the selected champion (`oof_calibrated_stack`):

- **Model performance gate:** precision 0.410 ≥ 0.15 ✓; recall 0.431 ≥ 0.35 ✓. **Both pass.**
- **Label maturity gate:** last-180-days label coverage 36% (< 50% minimum); observed recent positives 32 (< 40 minimum). **Fails.**
- **Overall `gate_pass` = false**, driven by label maturity, not model performance.

## What would move the ceiling

Based on results from all four experiments, genuine lift beyond LR's F1 0.517 would require:

1. **New signal that doesn't depend on `target_backorder_risk`** — probably ERP-side metadata: plant lead-time, material lead-time, customer credit history, ATP (available-to-promise) state at order time, inventory buffer ratio. These would be real order-time features that the current dataset lacks.
2. **Narrower training window with label masking** — train on the last 3–5 years with the most recent 180 days excluded or down-weighted, rebuilding rolling features under that constraint.
3. **Separate model per plant (or per material-class)** when cohorts have sufficient positive support — avoid forcing one model to cover all 73 plants and 3,238 materials with shared parameters.

None of these are cheap. All three are legitimate follow-up capstone projects in their own right.

## Final recommendation for the poster

Report honestly:

- **Selected champion:** `oof_calibrated_stack` by protocol §9 (inner temporal selection).
- **Deployable candidate:** `logistic_regression` at threshold 0.309 (F1 0.517, precision 0.435, recall 0.638 — 40× lift over random).
- **Deployment status:** model clears performance gates; deployment blocked by label maturity gate. Dataset-side issue, not model quality.
- **Experimental findings:** threshold policy is robust (F1-max = min-cost for realistic cost ratios); feature engineering hurt due to label maturity + drift; cascade did not improve on LR. These negative findings are part of the honest story.

This is a more mature, scientifically defensible narrative than "I stacked models and F1 went up." It demonstrates the candidate understood the data, built correctly, tested adversarial hypotheses, and reported what the data said.
