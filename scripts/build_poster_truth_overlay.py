#!/usr/bin/env python3
"""
Build ``POSTER_PPTX_OUTPUT`` by cloning ``POSTER_TRUTH_PPTX`` (layout / OOXML preserved)
and replacing only embedded poster rasters under ``ppt/media/``. University logo
(``image1.png``) is left unchanged.

This yields a deck identical to the fact FINAL except for swapped PNG payloads
(regenerated figures letterboxed to each embedded part’s pixel box; no non-uniform stretch).

Environment (defaults match repo layout):
  POSTER_TRUTH_PPTX   — source deck (default: DS_Capstone_Poster_FINAL.pptx)
  POSTER_PPTX_OUTPUT  — output path (default: DS_Capstone_Poster_FINAL_SCRIPT_COPY.pptx)
"""
from __future__ import annotations

import io
import os
import sys
import zipfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

try:
    from PIL import Image
except ImportError as e:
    raise SystemExit("Pillow is required: pip install Pillow") from e

from poster_pptx_fact_build import png_bytes_letterboxed_to_canvas

# Map embedded part name → repo-relative PNG source (must exist after figure pipeline).
# Derived from slide1 picture embeds + ppt/slides/_rels/slide1.xml.rels on fact FINAL.
MEDIA_REPLACE: tuple[tuple[str, str], ...] = (
    ("ppt/media/image2.png", "output/figures/target_balance_v2_ordertime.png"),
    ("ppt/media/image3.png", "output/figures/showcase_model_comparison_heatmap.png"),
    ("ppt/media/image4.png", "poster/diagrams/01_data_and_features.png"),
    ("ppt/media/image5.png", "poster/diagrams/02_validation_and_models.png"),
)


def main() -> int:
    truth = REPO / os.environ.get("POSTER_TRUTH_PPTX", "DS_Capstone_Poster_FINAL.pptx")
    out = REPO / os.environ.get("POSTER_PPTX_OUTPUT", "DS_Capstone_Poster_FINAL_SCRIPT_COPY.pptx")

    if not truth.is_file():
        print(f"Missing fact deck: {truth}", file=sys.stderr)
        return 1

    replacements: dict[str, bytes] = {}
    with zipfile.ZipFile(truth, "r") as zin:
        for arcname, rel_src in MEDIA_REPLACE:
            if arcname not in zin.namelist():
                print(f"Fact deck missing {arcname}; re-scan slide1 / rels.", file=sys.stderr)
                return 2
            old = zin.read(arcname)
            w, h = Image.open(io.BytesIO(old)).size
            src = REPO / rel_src
            if not src.is_file():
                print(f"Missing source PNG: {src}", file=sys.stderr)
                return 3
            replacements[arcname] = png_bytes_letterboxed_to_canvas(src, w, h)

    # Rewrite zip preserving each ZipInfo so structure matches truth (compression metadata kept).
    with zipfile.ZipFile(truth, "r") as zin, zipfile.ZipFile(out, "w") as zout:
        for info in zin.infolist():
            data = zin.read(info.filename)
            if info.filename in replacements:
                data = replacements[info.filename]
            zout.writestr(info, data)

    print(f"Wrote {out} (truth overlay; {len(replacements)} media parts replaced).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
