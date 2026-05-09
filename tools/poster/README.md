# Poster tooling (optional)

All capstone **showcase poster** assets live under `tools/poster/` so the repository root stays focused on the DATA-470 modeling pipeline.

| Path | Role |
| --- | --- |
| `exports/` | Local PPTX/DOCX working copies (not part of the official v2 deliverable) |
| `mermaid/diagrams/` | Mermaid sources (`.mmd`), PNG exports, `preview.html`, diagram README |
| `scripts/` | Poster-only Python and shell scripts (moved from top-level `scripts/`) |
| `*.py` in this folder | Shared poster modules (`build_poster.py`, palette, PPTX helpers, …) |
| `poster_figures_v2.py` | ROC/PR/drift PNGs for temporal holdout (invoked from `src.models.backorder_modeling` via `importlib`) |

**Regenerate diagrams (from repo root):**

```bash
bash tools/poster/scripts/render_poster_diagrams.sh
```

**Full poster rebuild (requires modeling outputs + optional fact decks in `exports/`):**

```bash
bash tools/poster/scripts/rebuild_poster.sh
```

The official graded pipeline remains: `run_pipeline.py` → `build_targets` → `scripts/run_modeling.py` → reports under `docs/html/` and `report/`.
