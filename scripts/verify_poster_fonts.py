#!/usr/bin/env python3
"""Verify DS_Capstone_Poster_FINAL.pptx uses only the poster font (Latin typeface in OOXML)."""
from pathlib import Path
import sys
import zipfile
import xml.etree.ElementTree as ET

NS = {"a": "http://schemas.openxmlformats.org/drawingml/2006/main"}
REPO = Path(__file__).resolve().parents[1]
PPTX = REPO / "DS_Capstone_Poster_FINAL.pptx"

# Must match build_poster.py FONT and poster_matplotlib_style.POSTER_FONT
EXPECTED = "Times New Roman"


def main() -> int:
    if not PPTX.exists():
        print(f"Not found: {PPTX}", file=sys.stderr)
        return 1
    faces: set[str] = set()
    with zipfile.ZipFile(PPTX) as z:
        for name in z.namelist():
            if not name.startswith("ppt/slides/slide") or not name.endswith(".xml"):
                continue
            root = ET.fromstring(z.read(name))
            for tag in ("latin", "ea", "cs"):
                for el in root.findall(f".//a:{tag}", NS):
                    tf = el.get("typeface")
                    if tf:
                        faces.add(tf)
    print(f"File: {PPTX}")
    if not faces:
        print("No explicit typeface attributes found (unexpected).", file=sys.stderr)
        return 2
    bad = faces - {EXPECTED}
    print("Typefaces in slide XML:")
    for f in sorted(faces):
        mark = "OK" if f == EXPECTED else "UNEXPECTED"
        print(f"  - {f}  ({mark})")
    if bad:
        print(f"Guardrail failed: expected only {EXPECTED!r}, also found {sorted(bad)!r}", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
