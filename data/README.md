# Data layout

This directory is **not tracked in git**. The pipeline expects raw SAP tables downloaded from the public Kaggle dataset below.

## Source

**[SAP Dataset (BigQuery) by Mustafa Keser](https://www.kaggle.com/datasets/mustafakeser4/sap-dataset-bigquery-dataset)**

License and attribution terms follow the Kaggle dataset license. This repository does not redistribute raw SAP extracts.

## Expected layout

After download:

```text
data/
├── raw/
│   └── main/
│       ├── ekbe.csv
│       ├── eket.csv
│       ├── ekko.csv
│       ├── ekpo.csv
│       ├── kna1.csv
│       ├── konv.csv
│       ├── lfa1.csv
│       ├── likp.csv
│       ├── lips.csv
│       ├── makt.csv
│       ├── mara.csv
│       ├── mard.csv
│       ├── vbak.csv
│       ├── vbap.csv
│       ├── vbep.csv
│       ├── vbrk.csv
│       └── vbrp.csv
└── raw/
    └── supporting/
        (optional supporting tables; see Kaggle for the full list)
```

## Generated paths

Running the pipeline populates:

- `data/clean/main/` and `data/clean/supporting/` — cleaned CSVs
- `data/processed/` — master tables and the official `v2` order-time modeling table

None of these generated files are committed to git.

## Reproducibility

Given the same raw input, fixed seeds in `src/models/v2_ordertime/` produce reproducible processed tables and model metrics.
