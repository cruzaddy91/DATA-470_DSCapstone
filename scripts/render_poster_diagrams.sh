#!/usr/bin/env bash
# Render poster/diagrams/*.mmd to PNG (EnableCV-aligned palette via classDef in each .mmd).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="$ROOT/poster/diagrams"
cd "$DIR"

CFG="mermaid-poster-config.json"
OPTS=( -b white -w 3200 -H 1400 -s 2 -c "$CFG" )

for pair in \
  "01_data_and_features.mmd 01_data_and_features.png" \
  "02_validation_and_models.mmd 02_validation_and_models.png" \
  "03_table_change_flow.mmd 03_table_change_flow.png"; do
  read -r IN OUT <<<"$pair"
  npx -y @mermaid-js/mermaid-cli@10.9.1 -i "$IN" -o "$OUT" "${OPTS[@]}"
done

echo "Wrote:"
ls -la "$DIR"/*.png
