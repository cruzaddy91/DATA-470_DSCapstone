# Data Science Capstone: Project Proposal

**Predicting backorder and overstock risk, plus demand and inventory levels, to reduce unfulfilled orders, excess inventory, and waste.**

**Project type:** This is both a **classification** project (predicting which products will go on backorder or become overstocked) and a **regression/forecasting** project (producing numeric estimates for actionable decisions).

This repository supports a capstone project that builds a robust predictive pipeline using the models outlined below. The pipeline will (1) **classify** products at risk of backorder or overstock and (2) **produce numeric outputs** (i.e., expected shortfall, excess units, recommended order or production quantities, demand forecasts) so that production efforts, labor, and resources can be reallocated for maximum efficiency, reduced product and labor waste, and higher revenue. The work is aligned with real-world supply chain problems (i.e., significant revenue at risk from backorders while holding inventory that could last 900+ weeks). The sole data source is the **SAP BigQuery dataset** (enterprise SAP sales, materials, inventory, and finance tables) for demand signals, inventory context, and order-to-delivery flow.

*This folder (`data/BigQuery_Dataset/`) contains the SAP BigQuery dataset CSVs used in the project.*

---

## Table of Contents

1. [Datasets: Summary & Provenance](#datasets-summary--provenance)
2. [Data Structure & Examples](#data-structure--examples)
3. [Planned Use of the Data](#planned-use-of-the-data)
4. [Preliminary Modeling Plan](#preliminary-modeling-plan)

---

## Datasets: Summary & Provenance

### SAP BigQuery Dataset (Kaggle): *tables in this folder*

| Attribute | Description |
|-----------|-------------|
| **What it is** | Enterprise SAP ERP data exported as CSV, structured like a BigQuery dataset. Covers **sales** (orders, line items, schedule lines), **materials** (master, storage locations), **purchasing** (headers, items, schedules), **deliveries**, **finance** (document headers, line items), and **master data** (customers, vendors, cost centers, plants). Tables use standard SAP naming (such as `vbak`, `vbap`, `vbep`, `mara`, `mard`, `bkpf`, `bseg`, `kna1`, `lfa1`). |
| **Where it comes from** | [Kaggle: SAP Dataset \| BigQuery Dataset](https://www.kaggle.com/datasets/mustafakeser4/sap-dataset-bigquery-dataset) by Mustafa Keser. Representative of real SAP transactional and master data. |
| **Typical use** | Demand signals (sales orders, requested delivery dates, quantities), inventory and stock by material/plant (`mard`), order-to-delivery flow, and rich joins (materials, customers, vendors) for backorder/overstock and demand-forecasting work. |
| **License / access** | Kaggle; free account required. Download via Kaggle API (such as `kagglehub`). |

**Source:**  
[SAP Dataset \| BigQuery Dataset (Kaggle)](https://www.kaggle.com/datasets/mustafakeser4/sap-dataset-bigquery-dataset)

---

## Data Structure & Examples

### SAP BigQuery Dataset: tables in this folder

*Join keys:* `mandt` (client), `vbeln` (sales doc), `matnr` (material), `bukrs` (company), `werks` (plant). Use for demand signals, inventory context, and order-to-cash flow.

#### Core tables (demand, inventory, order-to-delivery)

| Table | Description | Role in project |
|-------|-------------|-----------------|
| **vbak** | Sales Document Header. Order number, customer, order date, net value, sales org, currency. | Demand signals: order dates, customer, value. |
| **vbap** | Sales Document Item. Line-level material, quantity, value per order. | Core demand and product mix by order. |
| **vbep** | Sales Document Schedule Line. Requested/confirmed delivery dates and quantities. | Demand timing and fulfillment (requested vs confirmed). |
| **mara** | Material Master Data. Material number, base unit, material type, product hierarchy. | Join key and attributes for materials. |
| **mard** | Material Stock Data. Plant, storage location, unrestricted stock, etc. | **Inventory levels** by material/plant; critical for backorder/overstock. |
| **makt** | Material Descriptions. Names and text for materials. | Reporting and feature enrichment. |
| **likp** | Delivery Header. Delivery numbers and dates. | Order-to-delivery flow. |
| **lips** | Delivery Item. Quantities delivered, goods movement. | Fulfillment vs order (shortfall by material). |
| **vbrk** | Billing Document Header. Billing dates, invoice numbers. | Order-to-cash, fulfillment completion. |
| **vbrp** | Billing Document Item. Billed quantities and amounts. | What was actually billed (fulfillment). |
| **vbuk** | Sales Document Status. Overall order status (approval, processing). | Fulfillment and backorder status. |
| **vbup** | Sales Document Item Status. Item-level status. | Item-level fulfillment/backorder signals. |
| **vbfa** | Sales Document Flow. Links order → delivery → billing. | Document flow and tracking. |
| **vbpa** | Sales Document Partners. Sold-to, ship-to. | Customer and location context. |
| **ekko** | Purchasing Document Header. PO numbers, vendor, dates. | Replenishment context. |
| **ekpo** | Purchasing Document Item. Material, quantity, dates per PO line. | Replenishment quantities and timing. |
| **ekbe** | Purchase Order History. Quantities and values over time. | Replenishment history. |
| **eket** | Purchase Order Item Schedule. Delivery dates for PO items. | Inbound timing. |
| **kna1** | Customer Master. Customer attributes. | Demand segmentation and joins. |
| **lfa1** | Vendor Master. Vendor attributes. | Supply and purchasing context. |

#### Supporting tables (joins, units, organization)

| Table | Description | Role in project |
|-------|-------------|-----------------|
| **t001** | Company Codes. Organizational company codes. | Organizational context for plants/company. |
| **t001w** | Plants (Company Code / Currency). Plant to company and currency. | Link plants to company and currency. |
| **t006**, **t006a**, **t006t** | Units of Measure (and texts). Base and additional UoM. | Correct interpretation of quantities. |
| **t002**, **t009**, **t009b** | Currencies (and codes). Currency definitions. | Value and multi-currency context. |
| **tvko**, **tvkot** | Sales Organization (and texts). Sales org definitions. | Demand segmentation by sales org. |
| **tvtw**, **tvtwt** | Sales Office (and texts). Sales office definitions. | Finer segmentation if needed. |
| **konv** | Conditions. Pricing conditions on documents. | Optional value-based demand or filters. |

*Additional CSVs in this folder (such as address, accounting, tax, settlement) are available for enrichment but are not part of the core/supporting set used for backorder, demand, and inventory in this proposal.*

---

## Planned Use of the Data

1. **Backorder and overstock risk (SAP-derived): classification**  
   - Derive backorder/overstock targets or risk signals from SAP data (such as order vs delivery timing, shortfall by material, stock levels). Train and evaluate classifiers to predict backorder/overstock risk from inventory, lead time, order/delivery history, and schedule-line status.  
   - Use results to identify which factors drive backorder and overstock and how such models support replenishment, safety-stock, and production-allocation decisions.

2. **Demand and inventory levels (SAP): regression and forecasting**  
   - Build demand forecasts from order/delivery history by material or customer and, where applicable, numeric estimates of excess inventory or recommended order quantities to support actionable reallocation of production, labor, and resources.

3. **Actionable numeric outputs**  
   - Combine classification (who is at risk?) with regression/forecasting (how much shortfall or excess? what order quantity?) so that outputs support concrete decisions: shift production, allocate resources, reduce waste, and capture revenue.

4. **SAP BigQuery Dataset: demand, inventory, and backorder context**  
   - Use **sales documents** (`vbak`, `vbap`, `vbep`) for demand signals: order dates, requested delivery dates, quantities, and schedule-line status.  
   - Use **storage location data** (`mard`) for inventory by material and plant (such as unrestricted stock); join with **material master** (`mara`) for attributes.  
   - Derive backorder-like or fulfillment-gap signals (such as order vs delivery timing, shortfall by material) for classification and demand-forecasting work.  
   - Support **demand forecasting** (such as order/delivery history by material or customer) and **inventory-level** estimates for production and replenishment decisions, aligned with the capstone goals of reducing unfulfilled orders and excess inventory.

---

## Preliminary Modeling Plan

*To be refined after EDA and baseline results; the following are candidate approaches. The pipeline will deliver both classification outputs (backorder/overstock risk) and numeric outputs (demand, shortfall, excess, or recommended quantities) for actionable resource and production decisions.*

### Backorder and overstock risk (SAP-derived): classification

| Approach | Rationale |
|----------|-----------|
| **Logistic regression** | Interpretable baseline; coefficients show direction and relative importance of inventory, lead time, order/delivery timing, etc. |
| **Tree-based (Random Forest, XGBoost/LightGBM)** | Handle nonlinearity and interactions; often strong on tabular supply-chain data; feature importance for reporting. |
| **Class imbalance** | Backorder/overstock events may be imbalanced. Plan to try: class weights, SMOTE or similar resampling, and threshold tuning; compare precision/recall and business-oriented metrics. |
| **Evaluation** | Accuracy, precision, recall, F1, ROC-AUC; confusion matrix; cost-sensitive view if we attach rough costs to false positives vs missed backorders. |

### Demand and inventory levels (SAP): regression & forecasting

| Approach | Rationale |
|----------|-----------|
| **Regression with time features** | Linear or penalized regression with order/delivery history by material or customer; interpretable and fast; supports numeric forecasts for production and ordering. |
| **Classical time series** | ARIMA or similar for demand series by material/plant; good baseline and seasonality check. |
| **Goal** | Produce numeric demand forecasts and, where applicable, excess-inventory or recommended-order estimates so that results are actionable for production shifts, resource allocation, waste reduction, and revenue capture. |

### Workflow

1. **EDA:** Distributions, missing values, correlation, and document-flow analysis on SAP tables.  
2. **Baselines:** Logistic regression (backorder/overstock); naive or regression-based demand forecast.  
3. **Model selection:** Compare logistic vs tree-based (classification); compare forecasting approaches.  
4. **Documentation:** Methods, assumptions, limitations, and how results would translate to company data once approved.
