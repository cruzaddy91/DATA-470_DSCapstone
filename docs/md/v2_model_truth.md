# V2 Model Truth

## Official Scope

- Version: `v2`
- Task: classify `backorder` vs `no backorder`
- Dataset: `master_order_fulfillment_modeling_v2_ordertime.csv`
- Target: `target_backorder_risk`

## Simple Explanation

`v2` only uses information available when the order is placed.

The temporal split just means:

- train on older orders
- test on later orders

## Official Model Comparison

Poster headline triad (LR + XGBoost + Stack). CatBoost and LightGBM are base learners inside the stack; the full comparison table in `output/tables/classification_model_comparison_v2_ordertime.csv` still reports every model.

| Model | Purpose | Strength | Weakness |
|---|---|---|---|
| `logistic_regression` | Baseline | Simple and interpretable; strongest single-model PR-AUC on temporal holdout | Misses non-linear effects |
| `xgboost` | Tree benchmark | Strong tabular interactions | Tuning surface / explainability |
| `oof_calibrated_stack` | **Selected champion** | Combines LR + LightGBM + XGBoost + CatBoost through OOF-calibrated stacking | Heavier; inherits base-learner drift |
| `lightgbm` | Stack base learner | Fast GBDT | Reported in table; not a poster headline |
| `catboost` | Stack base learner | Native categoricals, ordered boosting | Reported in table; not a poster headline |

## Selection Rule

Pick the champion on **inner temporal validation** on temporal-train rows (see `docs/md/modeling_experiment_protocol.md` §9). Use `PR-AUC`, `F1`, and deployment gates—not a win on group holdout alone.

Report `temporal_holdout` metrics at the frozen OOF threshold as the honest forward-time check.

Do not choose the final model from random-split results.

## Current Champion and Deployment Status (honest)

- **Rule-selected champion:** `oof_calibrated_stack` (inner temporal per protocol §9).
- **Deployable best single model:** **Logistic Regression** — strongest on outer temporal holdout, interpretable, robust to threshold choice.
- **Base learners in stack:** LR + LightGBM + RandomForest + kNN. Four distinct model families (linear, boosted, bagged, instance-based). XGBoost and CatBoost are retained in the comparison table but excluded from the stack (highly correlated with LightGBM).
- **Hyperparameter tuning:** LR, LightGBM, RandomForest tuned via multi-phase grid search with temporal expanding-window folds, PR-AUC objective. kNN untuned (no shape for tuning to help). Tuned params persisted in `models/hyperparameters_tuned_v2_ordertime.json`; pipelines read them automatically.
- **Temporal holdout at frozen OOF threshold (final run with TimeSeriesSplit OOF):**
  - Stack: ROC-AUC 0.936, PR-AUC 0.326, precision 0.410, recall 0.431, F1 0.420.
  - **Logistic Regression: ROC-AUC 0.910, PR-AUC 0.358, precision 0.453, recall 0.586, F1 0.511.** At F1-max threshold (0.309).
  - (n_test = 6,521; n_positive = 58.)
- **Deploy gate (precision floor 0.15, recall floor 0.35):** precision ✓, recall ✓. Both model gates pass for the stack AND for LR.
- **Overall `gate_pass`: false — blocked by the label maturity gate, not by the model.** Last 180 days has 36% label coverage (min 50%) and 32 positives (min 40). Dataset reality, not a model flaw.
- **Reporting stance:** Stack is the rule-selected champion; LR is the deployable recommendation. The gap between them on outer temporal is itself a finding — on this data the separating signal is primarily linear, and ensembling does not add value beyond the 1-3% expected under Wolpert's framework.
- **Performance-experiment findings** (see `docs/md/performance_experiment_report.md` for detail):
  - Calibration: models rank-preserving; no F1 lift available via calibration alone.
  - Business-utility threshold: LR's F1-max threshold is also the min-cost threshold for cost ratios 3-30. Robust operational choice.
  - Feature engineering (rolling rates): hurts performance on this data — label maturity contamination + train/test drift. Documented as negative result.
  - Two-stage cascade: no configuration beats single-model LR. Documented as no-lift result.
- **What moves the ceiling (not done here, follow-up work):** new order-time ERP signals (lead times, ATP state, inventory buffer), narrower training window with label masking, or per-cohort models.

## Project Story

1. Predict whether an order line will backorder.
2. Then estimate the size of the shortfall.

The first step is the main capstone result.

## Alternative: staged decision time

A practical extension (not a replacement for the order–time v2 result) is a **second** classifier scored at a fixed pre–outcome time—for example, **T–k** to ship—using only features knowable by then, such as confirmed quantity and inventory or MRP style inputs. It would be evaluated with the same **temporal** train vs test rule as the main work so the comparison stays leak–safe.
