# Data Science Capstone

**Predicting backorder and overstock risk, plus demand and inventory levels, to reduce unfulfilled orders, excess inventory, and waste.**

| | |
|---|---|
| **Institution** | Westminster University |
| **Program** | Data Science |
| **Course** | DATA-470 Capstone |
| **Class of** | 2026 |
| **Professor** | Dr. Liang Jingsai |
| **Author** | Addy Cruz |

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

## Project Structure

```
DATA-470_DSCapstone/
├── config/                 # Configuration (paths, model hyperparameters)
│   ├── paths.yaml
│   └── model_params.yaml
├── data/                    # Data at each pipeline stage
│   ├── raw/                 # Raw SAP CSVs from Kaggle
│   │   ├── main/            # Core transactional tables (vbak, vbap, vbep, etc.)
│   │   └── supporting/      # Reference tables (t001, t006, tvko, etc.)
│   ├── clean/               # Normalized, deduplicated tables
│   │   ├── main/            # Transactional tables (orders, delivery, billing, PO)
│   │   └── supporting/      # Reference tables (plant, company_code, sales_org)
│   └── processed/           # Master tables + BRD outputs (6 CSV files)
├── docs/                    # Documentation and reports
│   ├── html/                # HTML reports (view in browser or convert to PDF)
│   │   ├── data-capstone-modeling-plan.html
│   │   ├── data-capstone-pipeline-report.html
│   │   ├── final_proposal.html
│   │   └── Progress-Report-Feb-19.html
│   └── md/                  # Markdown source (PROPOSAL.md, final_proposal.md)
├── figures/                 # Generated visualizations (EDA, modeling)
├── models/                  # Saved model artifacts (joblib, pickle, JSON metrics)
├── notebooks/               # Jupyter notebooks
│   ├── 01_eda_targets.ipynb # EDA and target exploration
│   ├── 02_modeling.ipynb    # Classification and regression models
│   └── 03_conclusion.ipynb  # Summary and report-ready outputs
├── output/                  # Generated outputs
│   ├── figures/             # Output figures
│   ├── pdf/                 # PDF reports (from docs/html)
│   └── tables/              # CSV outputs (forecasts, feature importance, etc.)
├── reports/                 # Report tables
│   └── tables/
├── scripts/                 # Pipeline and modeling entry points
│   ├── run_pipeline.py      # Build master tables + BRD metrics
│   └── run_modeling.py      # Train classification and regression models
├── src/                     # Reusable Python modules
│   ├── data/                # ETL: build_master_tables, build_brd_metrics
│   ├── features/            # Feature engineering, targets
│   ├── models/              # Model training utilities
│   └── utils/               # SAP rename config, helpers
├── tests/                   # Unit tests
├── assets/                  # Images and logos (e.g., WashU logo)
├── artifacts/               # Build artifacts
├── run_pipeline.py          # Root pipeline entry point
├── html-to-pdf.sh           # Convert docs/html to output/pdf
├── requirements.txt
└── README.md
```

**Pipeline flow:** Raw data → Clean → Master tables → BRD metrics → Modeling → Conclusion.

---

## Documentation & Reports

Detailed documentation is in `docs/html/`. Open in a browser or run `./html-to-pdf.sh --all` to generate PDFs in `output/pdf/`.

| Document | Description |
|----------|-------------|
| [data-capstone-modeling-plan.html](docs/html/data-capstone-modeling-plan.html) | Model selection for backorder prediction (regression + classification); data flow; primary and Plan B models |
| [data-capstone-pipeline-report.html](docs/html/data-capstone-pipeline-report.html) | Pipeline structure, master tables, join keys, ETL flow |
| [Progress-Report-Feb-19.html](docs/html/Progress-Report-Feb-19.html) | Pipeline summary, output tables, modeling plan, next steps |
| [final_proposal.html](docs/html/final_proposal.html) | Project proposal and scope |

---

## Pipeline Overview

A two-phase ETL pipeline turns SAP ERP clean CSVs into master tables and BRD-aligned metrics (backorders, weeks of coverage).

**Phase 1 – Core master tables** (`build_master_tables.py`):
- Reads from `data/clean/main/`
- Produces: `master_order_fulfillment`, `master_inventory_material`, `master_purchase`

**Phase 2 – BRD metrics** (`build_brd_metrics.py`):
- Reads core tables + delivery data
- Produces: `master_order_fulfillment_brd`, `shipment_history`, `master_woc`

### Pipeline output tables

| Table | Rows | Description |
|-------|------|-------------|
| `master_order_fulfillment` | ~52k | Order-to-cash: SO + delivery + billing + material + customer |
| `master_order_fulfillment_brd` | ~52k | Same + outstanding_qty, saleable_inventory, backorder_units/amount, aging |
| `master_inventory_material` | ~66k | Inventory by material/plant: unrestricted_stock, blocked, etc. |
| `master_purchase` | ~862k | Purchase orders: PO + vendor + material |
| `shipment_history` | ~12k | Material × week shipments for AWD (rolling 24 weeks) |
| `master_woc` | ~26k | Weeks of Coverage: SI, net_available, AWD, WOC, woc_low_flag |

### Data flow: raw → prediction

```
SAP Raw Tables → Clean Main/Supporting → Processed Master Tables → Features + Targets → Models → Predicted Output
```

---

## Modeling Plan

### Classification: backorder risk (yes/no)

| Approach | Rationale |
|----------|-----------|
| **Primary: XGBoost / LightGBM** | Strong on tabular data; handles ~10% backorder class imbalance; feature importance for reporting |
| **Plan B: Logistic regression** | Interpretable baseline; coefficients show direction and relative importance |

### Regression: magnitude of backorder

| Approach | Rationale |
|----------|-----------|
| **Primary: Ridge / ElasticNet** | Linear, interpretable, robust to multicollinearity; layered feature groups |
| **Plan B: XGBoost / LightGBM Regressor** | Captures non-linear effects; often best performance on tabular supply-chain data |

**Workflow:** Start with Ridge/ElasticNet for regression and XGBoost/LightGBM for classification. Fall back to Plan B if needed.

---

## Quick Start

1. **Environment:** `pip install -r requirements.txt`
2. **Data:** Download the [SAP BigQuery dataset (Kaggle)](https://www.kaggle.com/datasets/mustafakeser4/sap-dataset-bigquery-dataset); place core tables in `data/raw/main/` and supporting tables in `data/raw/supporting/` (or use existing clean CSVs in `data/clean/`).
3. **Pipeline:** Run `python run_pipeline.py` to build master tables, BRD metrics, and ML targets.
4. **Modeling:** Run `notebooks/02_modeling.ipynb` or `python scripts/run_modeling.py` for classification and regression.
5. **Conclusion:** Run `notebooks/03_conclusion.ipynb` for summary and report-ready outputs.
6. **PDFs:** Run `./html-to-pdf.sh --all` to regenerate all reports from `docs/html/` to `output/pdf/`.

---

## SAP Tables Used

### Core tables (demand, inventory, order-to-delivery)

| File name | Description |
|-----------|-------------|
| vbak.csv | Sales Document Header |
| vbap.csv | Sales Document Item |
| vbep.csv | Sales Document Schedule |
| vbfa.csv | Sales Document Flow |
| vbpa.csv | Sales Document Partners |
| vbrk.csv | Billing Document Header |
| vbrp.csv | Billing Document Item |
| vbuk.csv | Sales Document Status |
| vbup.csv | Sales Document Status (Update) |
| ekbe.csv | Purchase Order History |
| eket.csv | Purchase Order Item History |
| ekko.csv | Purchasing Document Header |
| ekpo.csv | Purchasing Document Item |
| kna1.csv | Customer Master Data |
| konv.csv | Conditions |
| lfa1.csv | Vendor Master Data |
| likp.csv | Delivery Header Data |
| lips.csv | Delivery Item Data |
| makt.csv | Material Descriptions |
| mara.csv | Material Master Data |
| mard.csv | Material Stock Data |

### Supporting tables (joins, units, organization)

| File name | Description |
|-----------|-------------|
| t001.csv | Company Codes |
| t001w.csv | Company Code Currency |
| t002.csv | Currencies |
| t006.csv, t006a.csv, t006t.csv | Units of Measure |
| t009.csv, t009b.csv | Currency Codes |
| tvko.csv, tvkot.csv | Sales Organization |
| tvtw.csv, tvtwt.csv | Sales Office |

*Join keys:* `mandt` (client), `vbeln` (sales doc), `matnr` (material), `bukrs` (company), `werks` (plant).

---

## Glossary

| Term | Definition |
|------|------------|
| **AWD** | Average Weekly Demand, rolling 24-week average of units shipped |
| **BRD** | Business Requirements Document |
| **EDA** | Exploratory Data Analysis |
| **ETL** | Extract, Transform, Load |
| **PO** | Purchase Order |
| **SAP** | Systems, Applications, and Products (ERP software) |
| **SI** | Saleable Inventory, unrestricted stock available to fulfill orders |
| **SO** | Sales Order |
| **WOC** | Weeks of Coverage, net available inventory ÷ AWD |
