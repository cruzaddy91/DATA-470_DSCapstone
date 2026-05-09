#!/usr/bin/env python3
"""
Emit a single JSON manifest of every raster/diagram used on the capstone poster, with
source paths, generator scripts, and (when inputs exist) embedded numeric summaries.

PosterCraft (arXiv:2506.10741) is a *text-to-image* aesthetic poster generator (diffusion).
It is NOT a replacement for matplotlib/seaborn for quantitative plots. This manifest ties
each data-bearing PNG to its upstream tables/JSON so aesthetic tools never become the
source of truth for metrics.

Output: output/figures/POSTER_VISUAL_MANIFEST.json
PosterCraft bundle (verbatim metrics + composite instructions): scripts/export_postercraft_poster_inputs.py
  → output/postercraft/POSTERCRAFT_POSTER_INPUT.{json,yaml}
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

TABLE_COMPARISON = REPO_ROOT / "output" / "tables" / "classification_model_comparison_v2_ordertime.csv"
MASTER_CSV = REPO_ROOT / "data" / "processed" / "master_order_fulfillment_modeling_v2_ordertime.csv"
TEMPORAL_JSON = REPO_ROOT / "models" / "temporal_holdout_test_scores_v2_ordertime.json"
FIGURES = REPO_ROOT / "output" / "figures"
DIAGRAMS = REPO_ROOT / "poster" / "diagrams"
PPTX_OUT = REPO_ROOT / os.environ.get("POSTER_PPTX_OUTPUT", "DS_Capstone_Poster_FINAL.pptx")
MANIFEST_OUT = FIGURES / "POSTER_VISUAL_MANIFEST.json"


def _sha256_file(path: Path) -> str | None:
    if not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _load_showcase_module():
    path = REPO_ROOT / "scripts" / "generate_showcase_metrics_visuals.py"
    spec = importlib.util.spec_from_file_location("showcase_metrics", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _heatmap_numeric_summary() -> dict:
    try:
        import pandas as pd

        gsm = _load_showcase_module()
        TABLE_PATH = gsm.TABLE_PATH
        MODEL_LABELS = gsm.MODEL_LABELS
        POSTER_MODEL_ORDER = gsm.POSTER_MODEL_ORDER
        POSTER_SPLIT_ORDER = gsm.POSTER_SPLIT_ORDER
        SPLIT_LABELS = gsm.SPLIT_LABELS

        df = pd.read_csv(TABLE_PATH)
        df = df[df["model"].isin(POSTER_MODEL_ORDER)].copy()
        out: dict = {"source_csv": str(Path(TABLE_PATH).relative_to(REPO_ROOT))}
        for metric in ("roc_auc", "pr_auc"):
            piv = df.pivot(index="model", columns="split", values=metric)
            cols = [s for s in POSTER_SPLIT_ORDER if s in piv.columns]
            sub = piv.loc[POSTER_MODEL_ORDER, cols]
            out[metric] = {
                str(mi): {str(c): float(sub.loc[mi, c]) for c in cols}
                for mi in POSTER_MODEL_ORDER
            }
        out["models_shown"] = POSTER_MODEL_ORDER
        out["split_labels_ui"] = {k: SPLIT_LABELS[k] for k in POSTER_SPLIT_ORDER}
        out["model_labels_ui"] = {k: MODEL_LABELS[k] for k in POSTER_MODEL_ORDER}
        return out
    except Exception as e:
        return {"error": str(e)}


def _confusion_summary() -> dict:
    try:
        import numpy as np
        from sklearn.metrics import confusion_matrix

        payload = json.loads(TEMPORAL_JSON.read_text(encoding="utf-8"))
        y_true = np.asarray(payload["y_true"], dtype=int)
        out: dict = {
            "source_json": str(TEMPORAL_JSON.relative_to(REPO_ROOT)),
            "n_test": int(len(y_true)),
            "n_positive": int(y_true.sum()),
            "baseline_positive_rate": float(payload.get("baseline_positive_rate", y_true.mean())),
            "models": {},
        }
        for name in ("logistic_regression", "xgboost", "oof_calibrated_stack"):
            m = payload.get("models", {}).get(name)
            if not m:
                continue
            y_pred = np.asarray(m["y_pred"], dtype=int)
            cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
            out["models"][name] = {
                "decision_threshold": float(m.get("decision_threshold", 0.5)),
                "roc_auc": float(m.get("roc_auc", 0)),
                "pr_auc": float(m.get("pr_auc", 0)),
                "confusion_matrix_labels_order": ["actual_0_pred_0", "actual_0_pred_1", "actual_1_pred_0", "actual_1_pred_1"],
                "confusion_matrix_counts": cm.ravel().tolist(),
                "confusion_matrix_2x2": cm.tolist(),
            }
        return out
    except Exception as e:
        return {"error": str(e)}


def _mermaid_sources() -> dict:
    out = {}
    for name in ("01_data_and_features.mmd", "02_validation_and_models.mmd", "03_table_change_flow.mmd"):
        p = DIAGRAMS / name
        png = p.with_suffix(".png")
        out[name] = {
            "mermaid_relative": str(p.relative_to(REPO_ROOT)) if p.exists() else None,
            "png_relative": str(png.relative_to(REPO_ROOT)) if png.exists() else None,
            "mmd_sha256": _sha256_file(p) if p.exists() else None,
            "png_sha256": _sha256_file(png) if png.exists() else None,
        }
    return out


def _finalize_heatmap_numeric_fallback(manifest: dict) -> None:
    hm = next((a for a in manifest["assets"] if a["id"] == "showcase_model_comparison_heatmap"), None)
    if hm and isinstance(hm.get("numeric_summary"), dict) and hm["numeric_summary"].get("error"):
        try:
            import pandas as pd

            df = pd.read_csv(TABLE_COMPARISON)
            hm["numeric_summary"] = {
                "fallback_csv_rows": len(df),
                "columns": list(df.columns),
                "source_csv": "output/tables/classification_model_comparison_v2_ordertime.csv",
            }
        except Exception as e:
            hm["numeric_summary"]["import_error"] = str(e)


def build_manifest_dict() -> dict:
    FIGURES.mkdir(parents=True, exist_ok=True)

    assets: list[dict] = [
        {
            "id": "target_balance",
            "role_on_poster": "Left column: class balance strip chart",
            "png_relative": "output/figures/target_balance_v2_ordertime.png",
            "generator_script": (
                "scripts/build_canonical_poster_visual_spec.py → scripts/render_poster_visuals_from_canonical_spec.py "
                "(fallback: scripts/generate_target_balance.py)"
            ),
            "primary_sources": [
                "data/processed/master_order_fulfillment_modeling_v2_ordertime.csv",
            ],
            "png_sha256": _sha256_file(FIGURES / "target_balance_v2_ordertime.png"),
        },
        {
            "id": "showcase_model_comparison_heatmap",
            "role_on_poster": "Center: ROC-AUC / PR-AUC grids (Logistic vs XGBoost vs Stack)",
            "png_relative": "output/figures/showcase_model_comparison_heatmap.png",
            "generator_script": (
                "scripts/build_canonical_poster_visual_spec.py → scripts/render_poster_visuals_from_canonical_spec.py "
                "(fallback: scripts/generate_showcase_metrics_visuals.py)"
            ),
            "primary_sources": [
                "output/tables/classification_model_comparison_v2_ordertime.csv",
            ],
            "numeric_summary": _heatmap_numeric_summary(),
            "png_sha256": _sha256_file(FIGURES / "showcase_model_comparison_heatmap.png"),
        },
        {
            "id": "diagram_sources_flow",
            "role_on_poster": "Center: Mermaid sources / feature contract",
            "png_relative": "tools/poster/mermaid/diagrams/01_data_and_features.png",
            "generator_script": "scripts/render_poster_diagrams.sh (mmd → png)",
            "primary_sources": ["tools/poster/mermaid/diagrams/01_data_and_features.mmd"],
            "png_sha256": _sha256_file(DIAGRAMS / "01_data_and_features.png"),
        },
        {
            "id": "diagram_validation_flow",
            "role_on_poster": "Center: Mermaid validation design",
            "png_relative": "tools/poster/mermaid/diagrams/02_validation_and_models.png",
            "generator_script": "scripts/render_poster_diagrams.sh",
            "primary_sources": ["tools/poster/mermaid/diagrams/02_validation_and_models.mmd"],
            "png_sha256": _sha256_file(DIAGRAMS / "02_validation_and_models.png"),
        },
        {
            "id": "confusion_matrices_temporal",
            "role_on_poster": "Right column: row-normalized confusion matrices",
            "png_relative": "output/figures/classification_confusion_matrices_v2_ordertime.png",
            "generator_script": (
                "scripts/build_canonical_poster_visual_spec.py → scripts/render_poster_visuals_from_canonical_spec.py "
                "(fallback: tools/poster/scripts/generate_poster_figures_v2.py → tools/poster/poster_figures_v2.py)"
            ),
            "primary_sources": ["models/temporal_holdout_test_scores_v2_ordertime.json"],
            "numeric_summary": _confusion_summary(),
            "png_sha256": _sha256_file(FIGURES / "classification_confusion_matrices_v2_ordertime.png"),
        },
    ]

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "repository_root": str(REPO_ROOT),
        "deck": {
            "builder": "build_poster.py",
            "template": "Showcase Templates.pptx",
            "output_pptx_relative": PPTX_OUT.name,
            "pptx_sha256": _sha256_file(PPTX_OUT),
        },
        "postercraft_framework": {
            "citation": "Chen et al., PosterCraft: Rethinking High-Quality Aesthetic Poster Generation, arXiv:2506.10741",
            "project_page": "https://ephemeral182.github.io/PosterCraft/",
            "code": "https://github.com/Ephemeral182/PosterCraft",
            "what_it_is": "A diffusion-based text-to-image pipeline (FLUX + fine-tuned weights) for aesthetic full-poster generation from prompts.",
            "policy_for_this_capstone": (
                "Chart rasters (heatmaps, confusion matrices, ROC/PR, etc.) are exported from matplotlib/seaborn "
                "from versioned CSV/JSON. PosterCraft is used for the full-poster aesthetic; prompts should "
                "paste verbatim metrics from POSTERCRAFT_POSTER_INPUT.yaml (generated with this manifest). "
                "Composite the listed PNG paths into the layout—do not ask diffusion to redraw plots or numeric "
                "labels. This manifest remains the source of truth for file paths and hashes."
            ),
        },
        "mermaid_diagrams": _mermaid_sources(),
        "assets": assets,
        "supplemental_figures_not_on_main_poster": {
            "showcase_temporal_snapshot": str((FIGURES / "showcase_temporal_snapshot.png").relative_to(REPO_ROOT)),
            "note": "Generated alongside heatmap script; not placed in build_poster.py center column by default.",
        },
    }

    _finalize_heatmap_numeric_fallback(manifest)
    return manifest


def _optional_matplotlib_outputs() -> list[dict]:
    """Extra pipeline PNGs (e.g. temporal curves) for optional PosterCraft composites."""
    names = (
        "roc_curves_temporal_v2_ordertime.png",
        "pr_curves_temporal_v2_ordertime.png",
        "temporal_positive_rate_drift_v2_ordertime.png",
        "score_distribution_temporal_v2_ordertime.png",
        "classification_feature_importance_v2_ordertime.png",
    )
    out: list[dict] = []
    for n in names:
        p = FIGURES / n
        if p.is_file():
            out.append(
                {
                    "relative_path": str(p.relative_to(REPO_ROOT)),
                    "sha256": _sha256_file(p),
                    "note": "Optional/supporting matplotlib export if your PosterCraft layout includes it.",
                }
            )
    return out


def _build_postercraft_prompt_snippets(manifest: dict) -> dict:
    """Human-readable blocks to paste into PosterCraft so prompts carry exact pipeline numbers."""
    hm = next((a for a in manifest["assets"] if a["id"] == "showcase_model_comparison_heatmap"), None)
    cm = next((a for a in manifest["assets"] if a["id"] == "confusion_matrices_temporal"), None)
    ns_h = (hm or {}).get("numeric_summary") or {}
    ns_c = (cm or {}).get("numeric_summary") or {}

    lines: list[str] = []
    lines.append("=== Verbatim metrics (copy exactly; do not invent values) ===")

    if ns_h.get("error"):
        lines.append(f"(Heatmap summary unavailable: {ns_h['error']})")
    if ns_c.get("error"):
        lines.append(f"(Temporal scores summary unavailable: {ns_c['error']})")

    if ns_c.get("n_test") is not None:
        lines.append(
            f"Temporal holdout test: n_test={ns_c['n_test']}, n_positive={ns_c.get('n_positive')}, "
            f"baseline_positive_rate={ns_c.get('baseline_positive_rate')}."
        )

    labels = ns_h.get("model_labels_ui") or {}
    for metric_key, title in (("roc_auc", "ROC-AUC"), ("pr_auc", "PR-AUC")):
        block = ns_h.get(metric_key)
        if not isinstance(block, dict):
            continue
        lines.append(f"{title} (from classification_model_comparison table):")
        for mkey, splits in block.items():
            if not isinstance(splits, dict):
                continue
            label = labels.get(mkey, mkey)
            parts = [f"{sk}={float(v):.4f}" for sk, v in splits.items()]
            lines.append(f"  {label}: " + "; ".join(parts))

    models_c = ns_c.get("models")
    if isinstance(models_c, dict) and models_c:
        lines.append("Temporal holdout (scores JSON), per model:")
        for name, row in models_c.items():
            if not isinstance(row, dict):
                continue
            lines.append(
                f"  {name}: roc_auc={row.get('roc_auc')}, pr_auc={row.get('pr_auc')}, "
                f"decision_threshold={row.get('decision_threshold')}"
            )

    constraints = (
        "PosterCraft may style the full poster. Do not generate replacement data charts. "
        "Composite the authoritative PNG files listed under authoritative_chart_rasters (and optional outputs) "
        "into the layout."
    )

    return {
        "constraints_for_diffusion": constraints,
        "verbatim_block": "\n".join(lines),
    }


def build_postercraft_bundle(manifest: dict) -> dict:
    """Structured inputs for PosterCraft: same numbers as the manifest, plus composite instructions."""
    hm = next((a for a in manifest["assets"] if a["id"] == "showcase_model_comparison_heatmap"), None)
    cm = next((a for a in manifest["assets"] if a["id"] == "confusion_matrices_temporal"), None)
    return {
        "schema_version": 1,
        "generated_at_utc": manifest["generated_at_utc"],
        "repository_root": manifest["repository_root"],
        "purpose": (
            "PosterCraft full-poster aesthetic generation fed by pipeline outputs. Verbatim metrics below are "
            "copied from CSV/JSON; chart images are matplotlib/seaborn exports—composite those rasters, do not "
            "redraw plots or numbers in diffusion."
        ),
        "authoritative_chart_rasters": [
            {
                "id": a["id"],
                "relative_path": a["png_relative"],
                "postercraft_instruction": (
                    "Use this file as the figure layer for this panel; do not regenerate plot geometry or labels."
                ),
                "sha256": a.get("png_sha256"),
                "role_on_poster": a.get("role_on_poster"),
                "generator_script": a.get("generator_script"),
            }
            for a in manifest["assets"]
            if a.get("png_relative")
        ],
        "verbatim_numeric_summaries": {
            "classification_model_comparison_and_heatmap": (hm or {}).get("numeric_summary"),
            "temporal_holdout_scores_json": (cm or {}).get("numeric_summary"),
        },
        "paste_ready_prompt_addendum": _build_postercraft_prompt_snippets(manifest),
        "optional_matplotlib_outputs": _optional_matplotlib_outputs(),
        "supplemental_figures_not_on_main_poster": manifest.get("supplemental_figures_not_on_main_poster"),
        "linked_manifest_relative": "output/figures/POSTER_VISUAL_MANIFEST.json",
        "linked_canonical_visual_spec_relative": "output/poster_visual_spec/canonical_poster_visual_spec.yaml",
        "postercraft_reference": manifest["postercraft_framework"],
        "mermaid_diagrams": manifest.get("mermaid_diagrams"),
        "deck": manifest.get("deck"),
    }


def main() -> int:
    manifest = build_manifest_dict()
    MANIFEST_OUT.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Wrote {MANIFEST_OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
