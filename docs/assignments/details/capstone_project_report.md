# DATA 470 — Capstone Project Report (course deliverable)

| Field | Detail |
|--------|--------|
| **Points** | 100 |
| **Submit** | File upload (Word, PDF, HTML, etc.) |
| **Due** | **May 7, 5:30pm** |
| **Availability** | Submissions allowed until **May 8, 11:59pm** |
| **Length** | **Not longer than 12 pages** (body; confirm with instructor if appendices count) |
| **Project (this repo)** | **Data Science Capstone** — backorder risk for SAP order lines (`v2` order-time modeling) |

## Purpose

A **scientific-paper style** report: clear **problem → methods → results → implications** for readers who can follow technical writing and ML evaluation.

## Required sections (at minimum)

Use these **headings** in order unless your instructor allows another style; keep the same content.

### Abstract

- **Brief, non-technical** summary of the sections below.
- Problem, what you did, what you found, why it matters.
- Write **last** when the rest is stable.

### Introduction

- **Problem statement** and **context** (operations, supply chain, why backorder risk matters).
- **Why it matters** (importance, not just model accuracy).
- **Background**: prior approaches, related forecasting / classification work—**citations** for claims that are not your own.
- State the **research questions** clearly (this repo frames: (1) will this line backorder? (2) if yes, by how much?—see root `README.md`).

### Industry or organization partner (if applicable)

- If you have a partner: **organization’s role**, where the data/problem came from, **liaison name(s) and role(s)**.
- If there is **no** formal external partner: one short, explicit paragraph (or confirm with instructor you may omit the section—do not leave an empty heading without checking).

### Methods

- **Techniques**: data scope, `v2` order-time table, target definition, feature families, **temporal / holdout protocol**, model families compared, selection rule (e.g. inner-temporal champion vs holdout reporting).
- **Math/stat**: label definition, metrics (precision, recall, ROC-AUC, calibration, business-style gates), any regression formulation for the secondary quantity question—at the level a strong data-science reader expects.
- **Implementation** at a high level: `src/`, key scripts, artifacts under `models/` and `output/` (no need to dump file paths; point to the **canonical** story in `docs/md/v2_model_truth.md` and `docs/md/modeling_experiment_protocol.md`).

### Results

- **What the study produced**: champion model, **temporal holdout** (and any other reported) metrics, key figures/tables, honest deployment / gate status.
- **Interpretation**: what the metrics mean for the business question, not only numbers in a table.

### Discussion / conclusion

- **Tie results to the research questions** and to the Introduction.
- **Limitations** (data scope, label noise, covariate shift, what “order time” does and does not allow, known weaknesses).
- **Implications** and **future work** (concrete next steps: monitoring, retraining, features, process change).

### Acknowledgements (optional)

- Brief: people, orgs, funding not covered above.

### Bibliography / citations

- **One** consistent style (APA, IEEE, etc.).
- Cite external papers, product docs, and non-obvious methodology sources you rely on.

## Project-specific anchors (this repository)

- **Truth summary:** `docs/md/v2_model_truth.md`
- **Protocol (splits, champion rule, reporting):** `docs/md/modeling_experiment_protocol.md`
- **Official table (path name):** `data/processed/master_order_fulfillment_modeling_v2_ordertime.csv` (see root `README.md` for the full “version of truth” list)
- **Key metrics artifacts:** e.g. `models/classification_metrics_v2_ordertime.json`, `models/temporal_holdout_test_scores_v2_ordertime.json` (use what your final run actually produced)
- **Optional checkpoint:** `output/dashboard/model_health_dashboard.html` if you refer to a frozen health view

## Practical constraints

- **12 pages max**: plan figures and references; avoid appendix bloat unless allowed.
- **Overlap** with proposal / progress HTML / poster: you may **reuse** text and figures; the report should read as a **single coherent paper** with **current** results and limitations.

## Suggested writing order

1. **Bullet outline** of all sections.
2. **Methods** + **Results** from the frozen `v2` story and your JSON/tables.
3. **Introduction** + **Abstract**.
4. **Discussion** (limitations + future work).
5. **Citation** pass; **page** trim.

## Checklist before upload

- [ ] **≤12 pages** (or per instructor)
- [ ] All **required sections** and clear headings
- [ ] **Abstract** non-technical and consistent with the body
- [ ] **Methods** include substantial **math/stat** (labels, metrics, protocol, model comparison logic)
- [ ] **Results** **interpreted**, not only tabulated
- [ ] **Limitations** and **future work** are **specific**
- [ ] **References** complete; style consistent
- [ ] Partner section accurate or explicitly N/A
- [ ] Submitted file opens correctly (PDF/Word/HTML); figures legible in print
