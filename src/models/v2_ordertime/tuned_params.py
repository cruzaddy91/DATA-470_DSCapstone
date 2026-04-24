"""
Load tuned hyperparameters from models/hyperparameters_tuned_v2_ordertime.json
when present. Returns {} if the file is missing — builders then fall back to
hand-defaults. This keeps tuning optional and non-destructive.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
TUNED_PATH = REPO_ROOT / "models" / "hyperparameters_tuned_v2_ordertime.json"


def load_tuned_params(model_name: str) -> dict[str, Any]:
    if not TUNED_PATH.is_file():
        return {}
    try:
        payload = json.loads(TUNED_PATH.read_text(encoding="utf-8"))
        entry = payload.get("tuned", {}).get(model_name, {})
        params = entry.get("best_params")
        return dict(params) if isinstance(params, dict) else {}
    except Exception:
        return {}
