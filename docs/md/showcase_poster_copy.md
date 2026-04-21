# Showcase Poster Copy

## Title

Predictive Supply Chain Analytics for Backorder Prevention and Inventory Optimization

## Byline

Addy Cruz  
DATA-470 Capstone | Data Science | Westminster University

## Problem & Goal

Backorders reduce service levels and delay customer orders. Overstock ties up working capital and increases waste. This project predicts backorder risk at order time from SAP ERP data so planning decisions can happen earlier.

## Data & Workflow

Integrated sales, delivery, billing, inventory, purchasing, and master-data tables from the SAP BigQuery dataset. Built reproducible ETL from raw CSVs to master tables, BRD metrics, and a leakage-safe v2 modeling set. Final order-time classification sample: 31,177 labeled order lines with a 3.38% positive rate.

## Model Comparison Metrics

- Production choice is based on the strict temporal split, not the easier grouped split.
- The comparison visual shows F1 and PR-AUC for logistic regression, random forest, XGBoost, and LightGBM across temporal, grouped, and recent 24-week validation.
- Selected temporal model: logistic regression.
- Temporal holdout: 6,521 rows, 58 positives, F1 0.28, ROC-AUC 0.85, PR-AUC 0.19.
- Grouped holdout: LightGBM reached F1 0.56 and PR-AUC 0.61.
- Recent 24-week holdout collapsed to zero F1 because the test window had only 14 positives.

## Figures

- `output/figures/target_balance_v2_ordertime.png`
- `output/figures/showcase_model_comparison_heatmap.png`
- `output/figures/classification_feature_importance_v2_ordertime.png`
- `output/figures/showcase_temporal_snapshot.png`
