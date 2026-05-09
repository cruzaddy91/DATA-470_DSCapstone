#!/usr/bin/env python3
"""Render the capstone poster .pptx (fact-deck path only).

This entrypoint imports almost nothing beyond ``python-pptx``, Pillow, and
``poster_pptx_fact_build`` — use it when ``build_poster.py`` feels heavy or when you only need
the fact copy + text + media overlay.

Always resolves paths against the repo root (works no matter which directory you launch from).

Environment (same as ``build_poster.py`` fact mode):
  POSTER_FACT_PPTX, POSTER_PPTX_OUTPUT, POSTER_APPLY_WESTMINSTER_BRAND, POSTER_APPLY_DECK_TEXT

If ``.venv`` exists under the repo, this process re-executes under ``.venv/bin/python`` unless
``POSTER_NO_VENV_REEXEC`` is set.

Setup: ``bash setup_venv.sh``
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = str(Path(__file__).resolve().parents[2])
_EXPORTS = Path(REPO_ROOT) / "tools" / "poster" / "exports"

if __name__ == "__main__":
    from poster_pptx_fact_build import maybe_reexec_with_repo_venv, run_poster_from_fact

    maybe_reexec_with_repo_venv(REPO_ROOT)
    _fact = _EXPORTS / "DS_Capstone_Poster_FINAL_SCRIPT_COPY.pptx"
    _out = _EXPORTS / "DS_Capstone_Poster_FULL_RENDER.pptx"
    msg = run_poster_from_fact(
        REPO_ROOT,
        os.environ.get("POSTER_FACT_PPTX", str(_fact)),
        os.environ.get("POSTER_PPTX_OUTPUT", str(_out)),
        apply_westminster=os.environ.get("POSTER_APPLY_WESTMINSTER_BRAND", "0") == "1",
        apply_deck_text=os.environ.get("POSTER_APPLY_DECK_TEXT", "1") == "1",
    )
    print(msg)
    sys.exit(0)
