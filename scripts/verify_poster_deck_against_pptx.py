#!/usr/bin/env python3
"""Fail if ``DS_Capstone_Poster_FINAL.pptx`` text does not match ``poster_deck_text.POSTER_DECK_TEXT``.

Optional: ``POSTER_PPTX_TRUTH=path`` to check another file (same structure).
"""
from __future__ import annotations

import os
import sys

# Repo root
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from poster_deck_text import assert_shapes_match_pptx  # noqa: E402
from pptx import Presentation  # noqa: E402


def main() -> int:
    os.chdir(ROOT)
    path = os.environ.get("POSTER_PPTX_TRUTH", "DS_Capstone_Poster_FINAL.pptx")
    prs = Presentation(path)
    slide = prs.slides[0]
    assert_shapes_match_pptx(slide)
    print(f"OK: text matches poster_deck_text.py ({path})")
    return 0


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print("VERIFY FAILED:", e, file=sys.stderr)
        sys.exit(1)
    except Exception as e:  # noqa: BLE001
        print("ERROR:", e, file=sys.stderr)
        sys.exit(2)
