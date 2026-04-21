#!/usr/bin/env python3
"""Emit PosterCraft-oriented YAML/JSON from the same pipeline state as POSTER_VISUAL_MANIFEST.json.

Matplotlib/seaborn figures stay authoritative; this file supplies verbatim metrics and paths for
full-poster aesthetic runs (composite chart PNGs, paste prompt addendum from YAML).
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / "output" / "postercraft"


def _load_manifest_builder():
    path = REPO_ROOT / "scripts" / "generate_poster_visual_manifest.py"
    spec = importlib.util.spec_from_file_location("poster_visual_manifest", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError:
        print("PyYAML is required: pip install PyYAML (see requirements-v2.txt)", file=sys.stderr)
        return 1

    mod = _load_manifest_builder()
    manifest = mod.build_manifest_dict()
    bundle = mod.build_postercraft_bundle(manifest)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUT_DIR / "POSTERCRAFT_POSTER_INPUT.json"
    yaml_path = OUT_DIR / "POSTERCRAFT_POSTER_INPUT.yaml"
    json_path.write_text(json.dumps(bundle, indent=2), encoding="utf-8")
    yaml_path.write_text(
        yaml.dump(bundle, default_flow_style=False, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    print(f"Wrote {json_path}")
    print(f"Wrote {yaml_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
