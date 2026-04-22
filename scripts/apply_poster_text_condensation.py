#!/usr/bin/env python3
"""Copy slide-1 text from :mod:`poster_deck_text` into a .pptx (in-place or to ``--dst``).

The canonical strings live in ``poster_deck_text.POSTER_DECK_TEXT`` (kept in lockstep with
``DS_Capstone_Poster_FINAL.pptx``). This script only applies that text; it does not rebuild
figures or layout. See :func:`build_poster` for a full template render.
"""

from __future__ import annotations

import argparse
import os
import sys

from pptx import Presentation

# Repo root
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from poster_deck_text import apply_poster_deck_text  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--src",
        default=os.environ.get("POSTER_PPTX_INPUT", "DS_Capstone_Poster_FINAL.pptx"),
    )
    ap.add_argument("--dst", default=None, help="Default: overwrite --src")
    args = ap.parse_args()
    os.chdir(ROOT)
    dst = args.dst or args.src
    prs = Presentation(args.src)
    apply_poster_deck_text(prs.slides[0])
    prs.save(dst)
    print(f"Saved: {dst}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
