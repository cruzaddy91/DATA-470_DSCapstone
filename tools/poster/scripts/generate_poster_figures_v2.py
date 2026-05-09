#!/usr/bin/env python
"""Regenerate poster PNGs from ``models/temporal_holdout_test_scores_v2_ordertime.json`` (no retrain)."""

import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_POSTER_DIR = os.path.join(PROJECT_ROOT, "tools", "poster")
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, _POSTER_DIR)
os.chdir(PROJECT_ROOT)

import importlib.util

_spec = importlib.util.spec_from_file_location(
    "poster_figures_v2",
    os.path.join(_POSTER_DIR, "poster_figures_v2.py"),
)
if _spec is None or _spec.loader is None:
    raise RuntimeError("Could not load tools/poster/poster_figures_v2.py")
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
main = _mod.main

if __name__ == "__main__":
    raise SystemExit(main())
