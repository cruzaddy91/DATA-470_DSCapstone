#!/usr/bin/env python3
"""Write a byte-for-byte identical copy of the hand-finished poster deck.

Rebuilding from ``Showcase Templates.pptx`` via ``build_poster.py`` cannot produce a
byte-identical match to ``DS_Capstone_Poster_FINAL.pptx`` (zip entry order, timestamps, rels, and
non-text edits differ). For a **guaranteed** duplicate of FINAL, use this file copy and verify
SHA-256.

Typical use after saving edits in PowerPoint:

  python scripts/poster_replicate_byte_identical.py
  python scripts/verify_poster_deck_against_pptx.py
  # optional: point verify at the copy
  POSTER_PPTX_TRUTH=DS_Capstone_Poster_FINAL_COPY.pptx python scripts/verify_poster_deck_against_pptx.py

Environment (defaults for Westminster capstone layout):

* ``POSTER_PPTX_TRUTH`` / ``--src`` — source .pptx (default: ``DS_Capstone_Poster_FINAL.pptx``)
* ``POSTER_PPTX_COPY`` / ``--dst`` — output (default: ``DS_Capstone_Poster_FINAL_COPY.pptx``; overwrites in place)
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    os.chdir(ROOT)
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--src",
        default=os.environ.get("POSTER_PPTX_TRUTH", "DS_Capstone_Poster_FINAL.pptx"),
    )
    ap.add_argument(
        "--dst",
        default=os.environ.get("POSTER_PPTX_COPY", "DS_Capstone_Poster_FINAL_COPY.pptx"),
    )
    args = ap.parse_args()
    src, dst = args.src, args.dst
    if not os.path.isfile(src):
        print(f"error: source not found: {src!r}", file=sys.stderr)
        return 1
    shutil.copy2(src, dst)
    a, b = _sha256(src), _sha256(dst)
    if a != b:
        print("error: copy digest mismatch (unexpected).", file=sys.stderr)
        return 2
    print(f"OK: {dst!r} is byte-identical to {src!r} (sha256 {a[:16]}…)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
