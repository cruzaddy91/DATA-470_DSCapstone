#!/usr/bin/env bash
# SSVC Validate: Markdown structure (markdownlint MD036, MD060 only).
# Requires Node/npx. From repo root: ./scripts/ssvc_validate.sh  or  make validate
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
exec npx --yes markdownlint-cli2@0.17.2 --config .markdownlint-cli2.yaml
