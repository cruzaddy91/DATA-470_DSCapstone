# Data Science Capstone

**Predicting backorder and overstock risk, plus demand and inventory levels, to reduce unfulfilled orders, excess inventory, and waste.**

---

## About Dataset

**Dataset description: SAP BigQuery data (Kaggle)**

**Source:** [SAP Dataset | BigQuery Dataset (Kaggle)](https://www.kaggle.com/datasets/mustafakeser4/sap-dataset-bigquery-dataset): enterprise SAP ERP data exported as CSV, structured like a BigQuery dataset.

**Overview:** The dataset provides a comprehensive replication of SAP (Systems, Applications, and Products in Data Processing) business data. It is designed to support data analytics and machine learning by offering structured data that mimics real-world enterprise scenarios. It includes data from sales, materials, purchasing, deliveries, and master data (customers, vendors, plants), enabling demand signals, inventory context, order-to-delivery flow, and backorder/overstock and demand-forecasting work.

**Content:**

- **Tables and schemas:** Multiple CSV tables representing SAP business operations: sales documents (headers, items, schedule lines), material master and stock, purchasing documents, deliveries, billing, and master data (customers, vendors, company codes, units of measure, sales organization).
- **Data types:** Structured data with transaction IDs, timestamps, customer and product details, quantities, amounts, and status fields.
- **Data volume:** Standard ERP grain (document headers, line items, master records); suitable for joins, pivots, and analysis.

**Usage:**

- **Business analytics:** Analyze demand trends, inventory levels, order-to-delivery timing, and fulfillment gaps.
- **Machine learning:** Build models for backorder/overstock classification and demand forecasting; derive backorder-like signals and inventory features.
- **Data processing:** ETL, joins (material master, customers, vendors), feature engineering for lead time, stock, and schedule-line status.

**Example use cases:**

- **Sales analysis:** Track and analyze sales performance by order, customer, material, and time.
- **Inventory management:** Monitor stock levels by material and plant; identify shortfall and overstock.
- **Demand forecasting:** Produce demand forecasts by material or customer from order/delivery history.
- **Order-to-cash flow:** Trace document flow from sales order → delivery → billing.

---

### Tables (core and supporting files used in this project)

Only the **core** (demand, inventory, order-to-delivery) and **supporting** (joins, units, organization) files are used. Other CSVs in the dataset (such as address, accounting, tax) are available for enrichment but are not required for this project.

#### Core tables

| File name | Description |
|-----------|-------------|
| vbak.csv | Sales Document Header. Contains headers for sales documents, such as sales order numbers and dates. |
| vbap.csv | Sales Document Item. Details items within sales documents, including quantities and product details. |
| vbep.csv | Sales Document Schedule. Provides schedule details for sales documents, including delivery dates. |
| vbfa.csv | Sales Document Flow. Contains the flow of sales documents, tracking their status and changes. |
| vbpa.csv | Sales Document Partners. Provides information on partners involved in sales documents. |
| vbrk.csv | Billing Document Header. Contains headers for billing documents, including invoice numbers and dates. |
| vbrp.csv | Billing Document Item. Details items within billing documents, including amounts and descriptions. |
| vbuk.csv | Sales Document Status. Provides status information for sales documents, such as approval and processing status. |
| vbup.csv | Sales Document Status (Update). Contains updated status information for sales documents. |
| ekbe.csv | Purchase Order History. Details history of purchase orders, including quantities and values. |
| eket.csv | Purchase Order Item History. Details changes and statuses for individual purchase order items. |
| ekko.csv | Purchasing Document Header. Contains headers for purchasing documents, such as order numbers and dates. |
| ekpo.csv | Purchasing Document Item. Contains details about items within purchasing documents, including item numbers and quantities. |
| kna1.csv | Customer Master Data. Provides detailed information about customers, including names and addresses. |
| konv.csv | Conditions. Contains pricing conditions and agreements related to sales and purchasing documents. |
| lfa1.csv | Vendor Master Data. Provides detailed information about vendors, including contact and banking details. |
| likp.csv | Delivery Header Data. Contains headers for delivery documents, such as delivery numbers and dates. |
| lips.csv | Delivery Item Data. Details items within delivery documents, including quantities and goods movement information. |
| makt.csv | Material Descriptions. Provides descriptions for materials, including names and text. |
| mara.csv | Material Master Data. Contains basic data for materials, such as material numbers and categories. |
| mard.csv | Material Stock Data. Details stock levels for materials in different storage locations. |

#### Supporting tables (joins, units, organization)

| File name | Description |
|-----------|-------------|
| t001.csv | Company Codes. Contains data about company codes used within the SAP system. |
| t001w.csv | Company Code Currency. Provides currency information for company codes (and plants). |
| t002.csv | Currencies. Contains details about currencies used in the SAP system. |
| t006.csv | Units of Measure. Provides data on units of measure used in the SAP system. |
| t006a.csv | Units of Measure (Additional). Contains additional information on units of measure. |
| t006t.csv | Units of Measure Texts. Provides textual descriptions of units of measure. |
| t009.csv | Currency Codes. Contains codes and descriptions for currencies. |
| t009b.csv | Currency Codes (Additional). Provides additional information on currency codes. |
| tvko.csv | Sales Organization. Provides data about sales organizations within the system. |
| tvkot.csv | Sales Organization Texts. Contains textual descriptions of sales organizations. |
| tvtw.csv | Sales Office. Details information about sales offices. |
| tvtwt.csv | Sales Office Texts. Provides textual descriptions for sales offices. |

*Join keys:* `mandt` (client), `vbeln` (sales doc), `matnr` (material), `bukrs` (company), `werks` (plant). See [SAP tables used in this project](#sap-tables-used-in-this-project) below for role of each table in the project.

---

## Project structure

| Folder | Purpose |
|--------|---------|
| `data/` | Raw data in `data/raw/main/` and `data/raw/supporting/`; clean in `data/clean/`; processed output in `data/processed/`. |
| `notebooks/` | Jupyter notebooks: EDA, ETL/preprocessing, modeling, conclusion. |
| `src/` | Reusable Python modules (data loaders, features, models, utils). |
| `scripts/` | Pipeline and modeling entry points (`run_pipeline.py`, `run_modeling.py`). |
| `models/` | Saved model artifacts (pickle, joblib). |
| `docs/reports/` | Source reports: HTML and Markdown in `docs/reports/html/`, `docs/reports/md/`. |
| `output/` | Generated outputs: PDFs in `output/pdf/`, tables in `output/tables/`, figures in `output/figures/`. |
| `assets/` | Images and logos (e.g., `assets/png/`). |
| `config/` | Configuration (paths, model hyperparameters). |
| `tests/` | Unit tests. |

**Pipeline:** Raw data → EDA → ETL/preprocessing → modeling → conclusion. Notebooks drive the workflow; `src` holds shared code.

---

## Quick start

1. **Environment:** `pip install -r requirements.txt`
2. **Data:** Download the [SAP BigQuery dataset (Kaggle)](https://www.kaggle.com/datasets/mustafakeser4/sap-dataset-bigquery-dataset); place core tables in `data/raw/main/` and supporting tables in `data/raw/supporting/` (or use existing clean CSVs in `data/clean/`).
3. **Pipeline:** Run `python run_pipeline.py` to build master tables, BRD metrics, and ML targets.
4. **Modeling:** Run `notebooks/02_modeling.ipynb` or `python scripts/run_modeling.py` for classification and regression/forecasting.
5. **Conclusion:** Run `notebooks/03_conclusion.ipynb` for summary and report-ready outputs.
6. **PDFs:** Run `./html-to-pdf.sh --all` to regenerate all reports from `docs/reports/html/` to `output/pdf/`.

---

## SAP tables used in this project

The SAP BigQuery dataset (Kaggle) is used for **demand signals**, **inventory context**, and **order-to-delivery flow**. Only **core** and **supporting** tables relevant to backorder, demand, and inventory are used.

### Core tables (demand, inventory, order-to-delivery)

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

### Supporting tables (joins, units, organization)

| Table | Description | Role in project |
|-------|-------------|-----------------|
| **t001** | Company Codes. Organizational company codes. | Organizational context for plants/company. |
| **t001w** | Plants (Company Code / Currency). Plant to company and currency. | Link plants to company and currency. |
| **t006**, **t006a**, **t006t** | Units of Measure (and texts). Base and additional UoM. | Correct interpretation of quantities. |
| **t002**, **t009**, **t009b** | Currencies (and codes). Currency definitions. | Value and multi-currency context. |
| **tvko**, **tvkot** | Sales Organization (and texts). Sales org definitions. | Demand segmentation by sales org. |
| **tvtw**, **tvtwt** | Sales Office (and texts). Sales office definitions. | Finer segmentation if needed. |
| **konv** | Conditions. Pricing conditions on documents. | Optional value-based demand or filters. |

*Join keys:* `mandt` (client), `vbeln` (sales doc), `matnr` (material), `bukrs` (company), `werks` (plant).

---

## Preliminary Modeling Plan

*To be refined after EDA and baseline results. The pipeline will deliver both **classification** outputs (backorder/overstock risk) and **numeric** outputs (demand, shortfall, excess, or recommended quantities) for actionable resource and production decisions.*

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
