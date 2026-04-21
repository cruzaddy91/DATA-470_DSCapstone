# Diagram style (aligned with EnableCV PROD visuals)

These capstone Mermaid files follow the same **visual language** as the production pipeline docs under EnableCV:

| Reference (examples) | What to copy |
|------------------------|----------------|
| `EnableCV/platform/data-engineering/workspaces/prod/docs/html/PROD_Pipeline_Flow_Visualization.html` | `graph TB`, `subgraph … [" "]`, per-node `style` fills (schedule / BC / ADLS / pipeline / SQL / views), `sequenceDiagram` for step timing |
| `EnableCV/standup/html/standup-unified-prod-target-and-cost-optimization.html` | `flowchart TD`, `classDef` roles (**storage**, **pipeline**, **dataset**, **reporting**, **process**), dotted `-.->` for “defines” vs solid for “reads/writes”, subgraphs with readable titles |
| `EnableCV/platform/data-engineering/workspaces/prod/artifacts/PROD_Pipeline_Flow_Visualization.html` | Artifact-centric subgraphs (Pipelines / Datasets / Linked services) |

**Color roles** (match standup `classDef` palette where possible):

- **Source / external** — cool gray (`#cfd8dc`)
- **Pipeline / orchestration** — amber (`#ffecb3`, stroke `#ff8f00`)
- **Storage / lake / table files** — purple (`#e1bee7`, stroke `#8e24aa`)
- **Process / SP / transform** — green (`#c8e6c9`, stroke `#2e7d32`)
- **Reporting / metrics / views** — pink (`#f8bbd0`, stroke `#c2185b`)

**Table / DDL changes** (`03_table_change_flow.mmd`) mirror a typical governed path: **design in repo → review → DEV → validate → PROD execute → verify** — analogous to how PROD flows separate **staging**, **EDW**, and **reporting** layers.
