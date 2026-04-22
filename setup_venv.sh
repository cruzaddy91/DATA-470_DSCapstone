#!/usr/bin/env bash
# Create ``.venv`` and install ``requirements.txt`` so matplotlib/NumPy wheels match
# (avoids "compiled using NumPy 1.x" when the active interpreter has NumPy 2.x).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
PY="${PYTHON:-python3}"
"$PY" -m venv .venv
.venv/bin/python -m pip install -U pip setuptools wheel
.venv/bin/pip install -r requirements.txt
echo "Ready. Use: .venv/bin/python build_poster.py"
echo "Or: source .venv/bin/activate"
