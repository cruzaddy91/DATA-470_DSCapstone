#!/bin/bash
# html-to-pdf.sh — HTML → PDF with clean metadata
# Output matches Microsoft Word PDF export (Creator/Producer metadata)
# Mirrors md-to-pdf.sh workflow, minus the pandoc step (HTML is source)

set -e

# Usage
if [ $# -lt 1 ]; then
  echo "Usage: $0 <html-file> [output-name] [output-dir]"
  echo "Example: $0 docs/reports/html/data-capstone-pipeline-report.html"
  echo "         $0 docs/reports/html/data-capstone-pipeline-report.html data-capstone-pipeline-report output/pdf"
  echo "Batch:   $0 --all   (converts all docs/reports/html/*.html → output/pdf/)"
  exit 1
fi

# Batch mode: convert all docs/reports/html/*.html to output/pdf/
if [ "$1" = "--all" ]; then
  SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
  REPO_ROOT="$(cd "$SCRIPT_DIR" && pwd)"
  HTML_DIR="${REPO_ROOT}/docs/reports/html"
  PDF_DIR="${REPO_ROOT}/output/pdf"
  if [ ! -d "$HTML_DIR" ]; then
    echo "Error: docs/reports/html not found"
    exit 1
  fi
  mkdir -p "$PDF_DIR"
  for f in "$HTML_DIR"/*.html; do
    [ -f "$f" ] || continue
    name=$(basename "$f" .html)
    echo "→ Converting $name.html ..."
    "$0" "$f" "$name" "$PDF_DIR"
  done
  echo ""
  echo "Done. PDFs in $PDF_DIR"
  exit 0
fi

HTML_FILE="$1"
# Normalize path: replace backslashes with forward slashes
HTML_FILE="${HTML_FILE//\\//}"
OUTPUT_NAME="${2:-$(basename "${HTML_FILE%.html}")}"
OUTPUT_DIR="${3:-$(dirname "$HTML_FILE")}"

if [ ! -f "$HTML_FILE" ]; then
  echo "Error: File not found: $HTML_FILE"
  exit 1
fi

# Resolve to absolute path (Chrome needs file:// URL)
HTML_FILE="$(cd "$(dirname "$HTML_FILE")" && pwd)/$(basename "$HTML_FILE")"
mkdir -p "$OUTPUT_DIR"
PDF_FILE="${OUTPUT_DIR}/${OUTPUT_NAME}.pdf"
CHROME="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

echo "→ Step 1: HTML → PDF (Chrome headless)"
"$CHROME" --headless --disable-gpu --no-pdf-header-footer \
  --print-to-pdf="$PDF_FILE" "file://${HTML_FILE}"

echo "→ Step 2: Set metadata to Microsoft Word convention (exiftool)"
exiftool -Creator="Microsoft Word" -Producer="Microsoft Word" -overwrite_original "$PDF_FILE" > /dev/null 2>&1

echo "→ Step 3: Remove XMP / exiftool traces"
exiftool -XMP:all= -overwrite_original "$PDF_FILE" > /dev/null 2>&1

echo "→ Step 4: Linearize PDF (strip old Chrome/metadata blocks)"
qpdf --linearize "$PDF_FILE" "${PDF_FILE}.tmp" && mv "${PDF_FILE}.tmp" "$PDF_FILE"

echo ""
echo "Done. PDF: $PDF_FILE"
exiftool "$PDF_FILE" 2>/dev/null | grep -E "Creator|Producer|Title" || true
