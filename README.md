# Data Science Capstone

Backorder prediction for SAP order lines.

## Version Of Truth

This repository now treats `v2` as the only official modeling path.

- Official modeling table: `data/processed/master_order_fulfillment_modeling_v2_ordertime.csv`
- Official target: `target_backorder_risk`
- Official task: binary classification
  `backorder` vs `no backorder`
- Official model comparison:
  `logistic_regression` vs `lightgbm`

Older snapshot-style experiments and `v1.1` are not part of the current project story.

## Problem Framing

The capstone answers two questions in order:

1. Will this order line become a backorder?
2. If yes, by how much?

The main project result is the first question. The regression piece is secondary.

## Why `v2`

`v2` uses only information that is available at order time. That makes it more defensible than snapshot variants that include fields too close to the label definition.

In practice, this means the model can use order-time business context such as:

- order quantity
- confirmed quantity
- net value
- requested lead time
- order calendar features
- basic categorical context like plant, division, item category, and sales organization

It does not use downstream snapshot fields like `outstanding_qty` or `saleable_inventory`.

## Temporal Holdout, Plain English

The temporal split is simple:

- train on older orders
- test on later orders

That is all it means. It is just a more realistic check of whether the model would still work on future orders.

## Model Comparison

The modeling story should stay small:

| Model | Role | Why it is here |
|---|---|---|
| `logistic_regression` | Baseline | Simple, interpretable, easy to defend |
| `lightgbm` | Challenger | Captures non-linear patterns in tabular data |

Selection should be based on the `temporal_holdout` results, with `PR-AUC` and `F1` as the main metrics.

## Canonical Workflow

1. Build pipeline tables.
2. Build the `v2` order-time modeling table.
3. Train and compare `logistic_regression` and `lightgbm`.
4. Present the better temporal-holdout model.

## Key Files

| Path | Purpose |
|---|---|
| `docs/md/v2_model_truth.md` | Short canonical summary of the official modeling story |
| `requirements-v2.txt` | Minimal package set for the official `v2` workflow |
| `src/features/build_targets.py` | Builds the official `v2` order-time modeling table |
| `src/models/backorder_modeling.py` | Trains and evaluates the official classifiers |
| `scripts/run_modeling.py` | Runs the modeling pipeline |
| `scripts/generate_model_performance_side_by_side_html.py` | Writes a simple comparison report |
| `models/classification_metrics_v2_ordertime.json` | Main saved classification metrics |

## Quick Start

```bash
python3.12 -m venv .venv-v2
source .venv-v2/bin/activate
pip install -r requirements-v2.txt
python run_pipeline.py
python -m src.features.build_targets
python scripts/run_modeling.py
python scripts/generate_model_performance_side_by_side_html.py
```

## Project Structure

```text
DATA-470_DSCapstone/
├── config/
├── data/
│   ├── raw/
│   ├── clean/
│   └── processed/
├── docs/
├── models/
├── output/
├── scripts/
├── src/
└── tests/
```

## License

MIT — see [LICENSE](LICENSE).
