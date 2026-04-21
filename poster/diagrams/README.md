# Poster diagrams (Mermaid)

Aligned with **EnableCV PROD** flow style (`graph TB`, subgraphs, `classDef` roles). See **`ENABLECV_STYLE_REFERENCE.md`** for pointers to the canonical HTML examples in the EnableCV repo.

| File | Purpose |
|------|---------|
| `01_data_and_features.mmd` | Sources → ETL → grain → order-time features → leakage guard |
| `02_validation_and_models.mmd` | Validation splits → models → metrics |
| `03_table_change_flow.mmd` | Governed **table / DDL** path (design → review → DEV → gates → PROD → verify) |

Rendered PNGs: `01_*.png` … `03_*.png`. Regenerate:

```bash
bash scripts/render_poster_diagrams.sh
```

Preview: open **`preview.html`** in a browser.
