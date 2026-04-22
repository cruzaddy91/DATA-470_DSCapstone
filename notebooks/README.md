# Notebooks (optional)

The canonical headless replacements live under `scripts/`:

| Notebook | Replacement |
|---|---|
| `01_eda_targets.ipynb` | `scripts/run_eda_targets_analysis.py` |
| `02_modeling.ipynb` | `scripts/run_modeling_notebook_export.py` |
| `03_conclusion.ipynb` | `scripts/run_conclusion_notebook_export.py` |

Run all three in order: `python scripts/run_notebook_replacements.py`  
(or rely on `scripts/run_v2_full_chain.sh`, which runs this after modeling).

Outputs stay under `output/figures/` and `output/tables/` as before so the model health dashboard and poster tooling keep the same paths.
