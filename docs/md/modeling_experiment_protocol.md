# Modeling Experiment Protocol (v2 Order-Time Backorder)

This protocol defines conventional and ethical rules for model improvement, evaluation, and reporting.

## 1) Data and Split Policy

- Use only order-time available features for training and inference.
- Exclude known leaky columns listed in the modeling contract.
- Select model architecture using inner temporal validation on temporal-train rows only.
- Keep final temporal holdout as untouched final evaluation for each run.

## 2) Label Maturity and Stability Gates

- Run label maturity checks before final reporting.
- Enforce recent-window minimum coverage and positive-support thresholds.
- Support strict fail mode and warn mode with explicit logging.
- Record gate profile, thresholds, observed values, and pass/fail in output artifacts.

## 3) Baseline Requirements

For every reported split, include at least:

- `always_negative`: predicts no backorder for every row.
- `prevalence_random`: random Bernoulli predictions at train positive rate.

Baselines are mandatory context for rare-event tasks and must be shown beside model metrics.

## 4) Primary Optimization Objective

- Primary family of metrics: `pr_auc`, `f1`, `precision`, `recall`.
- Accuracy is reported but is not primary for imbalanced data.
- Threshold policy is chosen on train-side inner validation only.
- Threshold objective should align to operational goals, such as maximizing recall subject to a precision floor.

## 5) Model Selection and Threshold Rules

- Select the primary model from inner temporal metrics only.
- Do not use final holdout outcomes to choose model architecture or thresholds.
- Persist threshold strategy and calibration metadata in outputs.
- If calibration support is sparse, use deterministic fallback policy and log it.

## 6) Reproducibility

- Fix random seeds in all sampling and split operations.
- Persist metrics, model selection rationale, and diagnostics as versioned artifacts.
- Keep environment-managed dependencies pinned and documented.

## 7) Fairness and Risk Monitoring

- Track subgroup/cohort performance for high-impact entities (client, material, plant, month).
- Track drift and label maturity over time.
- Treat major subgroup degradation or unstable labels as release blockers until reviewed.

## 8) Prohibited Practices

- No test-set tuning.
- No post-outcome features disguised as predictors.
- No cherry-picking split windows after looking at holdout results.
- No changing target definitions during a comparison cycle.

## 9) Champion model (defensible selection)

These rules keep the **selection path** aligned with the **claim path**.

- **Where the champion is chosen:** Pick the primary model family and operating threshold using **inner temporal validation** on rows inside temporal train only (see `selected_model.selection_split` and `selected_model.selection_basis` in `models/classification_metrics_v2_ordertime.json`). Do not crown a model because it wins **group holdout** or another diagnostic view alone.
- **Where forward honesty is reported:** **Temporal holdout** is the primary forward-time evaluation for how the frozen choice behaves on later dates. Expect sparse positives; threshold metrics can be noisy—still report them plainly.
- **What diagnostic splits are for:** Group holdout and recent-window slices are **stress and comparison context**, not a second scoreboard for picking the champion. Presenting a model as “best” because it wins grouped while the narrative claims temporal integrity is inconsistent.
- **Versioned dashboard snapshot:** `output/dashboard/model_health_dashboard.html` is generated from the metrics JSON and may be committed after a good run as a **UI checkpoint** for revert or review. Regenerate with `python3 scripts/generate_model_health_dashboard.py` after metrics change; the JSON remains the numerical source of truth.
