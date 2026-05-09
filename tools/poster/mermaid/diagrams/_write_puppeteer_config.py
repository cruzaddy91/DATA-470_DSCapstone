#!/usr/bin/env python3
"""Emit puppeteer launch JSON for mmdc. Prefer a real browser on macOS to avoid bundled-Chromium hangs."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# macOS: Google Chrome (most reliable for mermaid-cli headless on Apple Silicon + newer OS)
_CANDIDATES = (
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
)

OUT = Path(__file__).resolve().parent / ".mermaid_puppeteer_launch.json"


def main() -> int:
    cfg: dict = {
        "timeout": 180_000,
        "headless": "new",
        "args": [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-software-rasterizer",
        ],
    }
    override = os.environ.get("MERMAID_PUPPETEER_EXECUTABLE", "").strip()
    if override and os.path.isfile(override):
        cfg["executablePath"] = override
    else:
        for p in _CANDIDATES:
            if os.path.isfile(p) and os.access(p, os.X_OK):
                cfg["executablePath"] = p
                break
    OUT.write_text(json.dumps(cfg, indent=2) + "\n", encoding="utf-8")
    if "executablePath" in cfg:
        print(f"wrote {OUT} (using {cfg['executablePath']})", file=sys.stderr)
    else:
        print(
            f"wrote {OUT} (no system browser found; mmdc will use bundled Chromium)",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
