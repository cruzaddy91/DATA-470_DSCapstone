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

| Model | Purpose | Strength | Weakness |
|---|---|---|---|
| `logistic_regression` | Baseline | Simple and interpretable | Misses non-linear effects |
| `lightgbm` | Challenger | Stronger on tabular interactions | Harder to explain |

## Selection Rule

Pick the final classifier from the `temporal_holdout` split.

Use:

- `PR-AUC`
- `F1`

Do not choose the final model from random-split results.

## Project Story

1. Predict whether an order line will backorder.
2. Then estimate the size of the shortfall.

The first step is the main capstone result.

## Alternative: staged decision time

A practical extension (not a replacement for the order–time v2 result) is a **second** classifier scored at a fixed pre–outcome time—for example, **T–k** to ship—using only features knowable by then, such as confirmed quantity and inventory or MRP style inputs. It would be evaluated with the same **temporal** train vs test rule as the main work so the comparison stays leak–safe.
