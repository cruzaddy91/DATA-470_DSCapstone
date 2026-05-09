#!/usr/bin/env python3
"""
Load ``output/poster_visual_spec/canonical_poster_visual_spec.yaml`` and write poster PNGs
using ``poster_visual_templates`` (Seaborn template style).

Run after: scripts/build_canonical_poster_visual_spec.py
Same output filenames as legacy matplotlib scripts so ``build_poster.py`` is unchanged.
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SPEC_DEFAULT = REPO_ROOT / "output" / "poster_visual_spec" / "canonical_poster_visual_spec.yaml"


def main() -> int:
    try:
        from poster_visual_templates import render_all_from_spec
    except ImportError as e:
        print(e, file=sys.stderr)
        return 1

    spec_path = Path(sys.argv[1]) if len(sys.argv) > 1 else SPEC_DEFAULT
    if not spec_path.is_file():
        print(f"Missing spec: {spec_path}. Run: scripts/build_canonical_poster_visual_spec.py", file=sys.stderr)
        return 1

    spec = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    out = render_all_from_spec(spec, REPO_ROOT)
    for k, p in sorted(out.items()):
        print(f"{k}: {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
