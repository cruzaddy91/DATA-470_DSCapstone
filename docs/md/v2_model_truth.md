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

- **Selected champion:** `oof_calibrated_stack` (inner temporal PR-AUC dominates; tie-breaks on calibration).
- **Temporal holdout at frozen OOF threshold:** ROC-AUC 0.903, PR-AUC 0.179, precision 0.316, recall 0.310, F1 0.313 (n_test = 6,521; n_positive = 58).
- **Deploy gate (precision floor 0.15, recall floor 0.35):** precision ✓, recall ✗ — **NO-GO**.
- **Reporting stance:** report the gate failure. Do not re-run selection against the gate, do not swap gate-model to a different candidate to manufacture a GO banner. Honest evaluation is the deliverable.
- **Context:** on outer temporal holdout, logistic regression posts the highest single-model PR-AUC (0.349). The stack wins inner CV and is the rule-selected champion; the LR-vs-stack outer gap is part of the honest story, not a contradiction.

## Project Story

1. Predict whether an order line will backorder.
2. Then estimate the size of the shortfall.

The first step is the main capstone result.

## Alternative: staged decision time

A practical extension (not a replacement for the order–time v2 result) is a **second** classifier scored at a fixed pre–outcome time—for example, **T–k** to ship—using only features knowable by then, such as confirmed quantity and inventory or MRP style inputs. It would be evaluated with the same **temporal** train vs test rule as the main work so the comparison stays leak–safe.
