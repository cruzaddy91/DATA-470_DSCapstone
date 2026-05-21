#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIAGRAMS="$ROOT/docs/diagrams"
CLI="@mermaid-js/mermaid-cli@11"

if ! command -v npx >/dev/null 2>&1; then
  echo "ERROR: npx not found" >&2
  exit 1
fi

render_pair() {
  local base="$1"
  local src="$DIAGRAMS/${base}.mmd"
  [[ -f "$src" ]] || { echo "ERROR: missing $src" >&2; exit 1; }
  echo "Rendering $base ..."
  npx --yes "$CLI" -i "$src" -o "$DIAGRAMS/${base}.png" -c "$DIAGRAMS/mermaid-config.json" -b transparent -s 3
  npx --yes "$CLI" -i "$src" -o "$DIAGRAMS/${base}-dark.png" -c "$DIAGRAMS/mermaid-config-dark.json" -b transparent -s 3
  npx --yes "$CLI" -i "$src" -o "$DIAGRAMS/${base}.svg" -c "$DIAGRAMS/mermaid-config.json" -b transparent
  npx --yes "$CLI" -i "$src" -o "$DIAGRAMS/${base}-dark.svg" -c "$DIAGRAMS/mermaid-config-dark.json" -b transparent
}

if (($#)); then
  for base in "$@"; do
    render_pair "${base%.mmd}"
  done
else
  for src in "$DIAGRAMS"/*.mmd; do
    [[ -f "$src" ]] || continue
    base="$(basename "$src" .mmd)"
    [[ "$base" == *-dark ]] && continue
    render_pair "$base"
  done
fi

echo "Done."
