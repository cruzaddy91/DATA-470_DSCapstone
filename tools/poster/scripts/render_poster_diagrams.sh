#!/usr/bin/env bash
# Render tools/poster/mermaid/diagrams/*.mmd to PNG (see tools/poster/mermaid/diagrams/DIAGRAM_STYLE_REFERENCE.md).
#
# Canvas ``-w``/``-H`` match the aspect ratio of each slot in the fact deck so letterboxing in
# ``poster_pptx_fact_build`` does not shrink diagram text into a postage stamp:
#   image4 (01): 624×1614  → tall portrait
#   image5 (02): 956×890   → near-square landscape
#
# mmdc’s bundled Chromium often times out on macOS (Puppeteer “WS endpoint” wait). We generate a
# puppeteer launch file that prefers Google Chrome / Edge with a long timeout and safe args.
# Override: MERMAID_PUPPETEER_EXECUTABLE=/path/to/chrome
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
DIR="$ROOT/tools/poster/mermaid/diagrams"
cd "$DIR"

python3 "$DIR/_write_puppeteer_config.py" >&2
PUPPETEER_JSON="$DIR/.mermaid_puppeteer_launch.json"

# Newer mmdc: better headless + puppeteer; pin for reproducible CI.
MERMAID_CLI_VERSION="${MERMAID_CLI_VERSION:-11.4.0}"

CFG="mermaid-poster-config.json"
BASE=( -b white -s 2.25 -c "$CFG" -p "$PUPPETEER_JSON" )

render_one() {
  local IN="$1" OUT="$2" W="$3" H="$4"
  npx -y "@mermaid-js/mermaid-cli@${MERMAID_CLI_VERSION}" -i "$IN" -o "$OUT" "${BASE[@]}" -w "$W" -H "$H"
}

# 5× native slot pixels (sharp text); same aspect as fact ``image4`` / ``image5``.
render_one "01_data_and_features.mmd" "01_data_and_features.png" 3120 8070
render_one "02_validation_and_models.mmd" "02_validation_and_models.png" 4780 4450
render_one "03_table_change_flow.mmd" "03_table_change_flow.png" 4780 4450

echo "Wrote:"
ls -la "$DIR"/*.png
