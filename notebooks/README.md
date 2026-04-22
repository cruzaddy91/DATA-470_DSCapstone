# Notebooks (removed)

The former Jupyter notebooks were replaced by headless scripts under `scripts/`:

| Former notebook | Replacement |
|---|---|
| `01_eda_targets.ipynb` | `scripts/run_eda_targets_analysis.py` |
| `02_modeling.ipynb` | `scripts/run_modeling_notebook_export.py` |
| `03_conclusion.ipynb` | `scripts/run_conclusion_notebook_export.py` |

Run all three: `python scripts/run_notebook_replacements.py`  
(or `./scripts/run_v2_full_chain.sh`, which includes this after modeling).

Outputs remain under `output/figures/` and `output/tables/` for the dashboard and poster pipeline.
