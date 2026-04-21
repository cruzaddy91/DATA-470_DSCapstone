# Student Showcase Materials Templates

This file contains finalization-ready language for Westminster Student Showcase materials, updated for the stabilized order-time v2 modeling logic.

## Recommended Main Title

`Predictive Supply Chain Analytics for Backorder Prevention and Inventory Optimization`

## Alternate Title Options

- `Data-Driven Supply Planning for Backorder Risk and Inventory Optimization`
- `Enterprise Supply Chain Intelligence for Backorder and Overstock Decision Support`

---

## 1) Application Abstract Template (Finalization-Ready)

This project explores how data science can support more effective supply chain decision-making by identifying backorder and overstock risk and estimating demand and inventory levels from enterprise resource planning data. Using the SAP BigQuery dataset, I built a reproducible workflow that integrates data cleaning, feature engineering, exploratory analysis, and predictive modeling across sales, delivery, inventory, and purchasing records.

The goal of this work is to transform complex operational data into actionable insights that can support replenishment timing, inventory allocation, and planning decisions. I stabilized a leakage-aware, order-time modeling flow to reduce validation inflation and improve forward-test realism while preserving practical decision value. This project demonstrates how machine learning can be applied to enterprise-style business data to inform more efficient and evidence-based operations.

---

## 2) Application Abstract Template (Short Backup)

This project investigates how machine learning can improve supply chain decision support by predicting backorder and overstock risk and estimating demand and inventory levels from SAP-based ERP data. I built a reproducible pipeline spanning data cleaning, feature engineering, exploratory analysis, and predictive modeling across sales, delivery, inventory, and purchasing tables.

The modeling flow now emphasizes leakage-safe features and order-time validation to improve generalization and reduce overfitting. The intended outcome is an actionable decision-support approach for replenishment timing, inventory allocation, and operational planning.

---

## 3) Poster Template (Full Version)

Use these sections in the Westminster poster template.

### Title

Predictive Supply Chain Analytics for Backorder Prevention and Inventory Optimization

### Author and Affiliation

Addy Cruz  
DATA-470 Capstone, Westminster University

### Problem and Motivation

Backorders and overstock both create financial and operational inefficiencies. I investigated whether machine learning can identify these risks earlier and support better inventory and planning decisions from enterprise operational data.

### Data Source and Scope

I used the SAP BigQuery Dataset (Kaggle), including sales, delivery, billing, inventory, materials, customer, and purchasing tables. The project scope focuses on transforming multi-table ERP-style data into model-ready features and decision-support outputs.

### Methods and Workflow

- Data cleaning and table standardization
- Multi-table integration and key alignment
- Feature engineering for order, delivery, inventory, and purchasing context
- Exploratory analysis for missingness, duplicates, and target behavior
- Classification and regression modeling for risk and magnitude-oriented outcomes
- Reproducible pipeline and reporting workflow

### Current Technical Focus

The current technical focus is strengthening temporal robustness in low-positive windows and improving stability under late-period label sparsity and drift.

### Results Snapshot (v2 Order-Time Validation)

- Grouped best model (LightGBM): Accuracy 0.9675, Precision 0.8408, Recall 0.8287, F1 0.8347, PR-AUC 0.9353
- Temporal best model (Random Forest): Accuracy 0.9942, Precision 0.2674, Recall 0.4600, F1 0.3382, PR-AUC 0.2074
- Overfitting correction signal: grouped F1 decreased from 0.9351 to 0.8347 while temporal F1 increased from 0.0000 to 0.3382

### Emerging Outcomes

This workflow is designed to produce:

- backorder risk indicators
- overstock risk indicators
- demand and shortage-related estimates
- analysis outputs that support inventory and planning decisions

### Real-World Application

The intended use of this approach is to support actionable decisions such as replenishment timing, inventory allocation, and resource planning. The broader objective is to reduce waste, improve service levels, and enable faster evidence-based operational choices.

### Limitations and Next Steps

The v2 flow materially improved validation honesty, but limitations remain in late-period positive scarcity and regime drift. Next steps include threshold calibration under sparse positives, stress tests across time windows, and finalizing business-facing operating thresholds for deployment recommendations.

---

## 4) Poster Template (Compact Backup)

Use this version if poster space is limited.

### Problem

I address two connected supply chain risks, backorders and overstock, using SAP-style enterprise data.

### Approach

I built a reproducible pipeline for data cleaning, feature engineering, exploratory analysis, and predictive modeling across sales, delivery, inventory, and purchasing records.

### Current Stage

I stabilized a leakage-safe, order-time v2 modeling flow that reduced validation inflation and improved forward-test behavior.

### Practical Value

The intended outcome is decision support for replenishment timing, inventory allocation, and planning efficiency.

### Next Step

I will finalize operating thresholds and business decision cutoffs using additional temporal stress tests, then lock the final model card for presentation.

---

## Quick Fill Fields

- Faculty mentor: `[Name]`
- Presentation format: `Poster`
- Contact email: `[Email]`
- Optional employer or partner mention only if your program/mentor requires it (use neutral wording in public artifacts).
