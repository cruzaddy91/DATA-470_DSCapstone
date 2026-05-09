# Diagram style reference (Mermaid)

These diagrams follow conventions common in governed data and ML pipeline documentation so they stay legible in print and on posters.

## Layout

- Prefer `graph TB` or `flowchart TD` for top-to-bottom flow.
- Use `subgraph` blocks to separate layers (sources, ETL, storage, features, validation, models, reporting).
- Use `classDef` for repeatable node categories (for example storage, pipeline, process, reporting) so styling stays consistent across figures.

## Edges

- Solid edges for direct data movement or execution.
- Dotted or dashed edges when the relationship is definitional, optional, or cross-environment (for example design versus deployed artifact).

## Color

Palette choices align with Westminster poster brand colors where applicable. Named colors used in HTML and PDF artifacts live in [`../../poster_template_style.py`](../../poster_template_style.py) at the repository root (and related `westminster_poster_palette.py`).

## Regeneration

From the repository root:

```bash
bash scripts/render_poster_diagrams.sh
```

Open `preview.html` in this folder to review rendered output before exporting PNGs.
