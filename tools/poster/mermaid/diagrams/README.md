# Poster diagrams (Mermaid)

Diagrams follow a governed-pipeline visual style (`graph TB`, subgraphs, `classDef` roles). See **`DIAGRAM_STYLE_REFERENCE.md`** for layout, edge, and color conventions used across figures.

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
