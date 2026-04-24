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
- **Base learners in stack:** Logistic Regression, LightGBM, RandomForest, kNN. Four distinct model families (linear, boosted tree, bagged tree, instance-based). CatBoost and XGBoost were removed from the stack — three GBDTs do not contribute three viewpoints.
- **Hyperparameter tuning:** LR, LightGBM, and RandomForest tuned via multi-phase grid search with temporal expanding-window folds, scored on PR-AUC. kNN left untuned (F1 0.119 / recall 0.069 on baseline — shape that no tuning can rescue). Tuned parameters persisted in `models/hyperparameters_tuned_v2_ordertime.json`; pipelines read them automatically.
- **Temporal holdout at frozen OOF threshold (tuned run):**
  - Stack: ROC-AUC 0.927, PR-AUC 0.244, precision 0.264, recall 0.552, F1 0.358.
  - Logistic Regression: ROC-AUC 0.910, PR-AUC 0.358, precision 0.453, recall 0.586, F1 0.511 (still the strongest single model by F1).
  - (n_test = 6,521; n_positive = 58.)
- **Deploy gate (precision floor 0.15, recall floor 0.35):** precision ✓ (0.264), recall ✓ (0.552). **Both model gates pass.**
- **Overall `gate_pass`: false — blocked by the label maturity gate, not by the model.** The last-180-days window has only 36% label coverage and 32 observed positives (min 40). Recent orders have not had time to become backorders (or not), so the operational reliability check fails on the data, not on model performance.
- **Reporting stance:** report the honest result — the trained model clears its performance floors; deployment is blocked by dataset freshness/label maturity. Possible paths forward: (1) accept a longer observation window before labeling is considered mature, (2) shorten the operational label window (e.g. T+30 days instead of T+90), (3) retrain more frequently as labels mature. Do not shop the gate to force a GO banner.
- **Context — pre-fix baseline vs tuned run:**
  - Stack PR-AUC rose from 0.111 (post-threshold-fix honest floor) to 0.244 (tuned). F1 from 0.190 to 0.358.
  - The pre-existing `0.179 PR-AUC, 0.313 F1` stack numbers were flattered by a threshold-calibration leakage bug (threshold picked against `y_test`); those numbers should not be cited as baseline.

## Project Story

1. Predict whether an order line will backorder.
2. Then estimate the size of the shortfall.

The first step is the main capstone result.

## Alternative: staged decision time

A practical extension (not a replacement for the order–time v2 result) is a **second** classifier scored at a fixed pre–outcome time—for example, **T–k** to ship—using only features knowable by then, such as confirmed quantity and inventory or MRP style inputs. It would be evaluated with the same **temporal** train vs test rule as the main work so the comparison stays leak–safe.
