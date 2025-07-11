#!/bin/bash

# Script to create compressed and split archives using 7zip
# Usage: ./create_archive.sh SOURCE_DIR OUTPUT_DIR ARCHIVE_NAME MAX_SIZE

set -e 

show_help() {
    echo "Usage: $0 SOURCE_DIR OUTPUT_DIR ARCHIVE_NAME MAX_SIZE"
    echo ""
    echo "Parameters:"
    echo "  SOURCE_DIR    Path to the directory to archive"
    echo "  OUTPUT_DIR    Directory where to create the archive parts"
    echo "  ARCHIVE_NAME  Base name for the archive files"
    echo "  MAX_SIZE      Maximum size per file (e.g., 10g, 500m, 2000m)"
    echo ""
    echo "Example:"
    echo "  $0 /mnt/arcangelo/repositories/virtuoso_utilities/virtuoso-data ./backups oc_meta_prov_06_06 10g"
    echo ""
    echo "Note: Uses 7zip with LZMA compression"
    echo ""
    echo "Options:"
    echo "  --help        Show this help"
}

if [ "$1" = "--help" ] || [ "$1" = "-h" ] || [ $# -eq 0 ]; then
    show_help
    exit 0
fi

if [ $# -ne 4 ]; then
    echo "ERROR: All 4 parameters are required!"
    echo ""
    show_help
    exit 1
fi

SOURCE_DIR="$1"
OUTPUT_DIR="$2"
ARCHIVE_NAME="$3"
MAX_SIZE="$4"

echo "=== Creating Archive ==="
echo "Source directory: $SOURCE_DIR"
echo "Output directory: $OUTPUT_DIR"
echo "Archive name: $ARCHIVE_NAME"
echo "Maximum size per file: $MAX_SIZE"
echo ""

if [ ! -d "$SOURCE_DIR" ]; then
    echo "ERROR: Source directory '$SOURCE_DIR' does not exist!"
    exit 1
fi

if ! command -v 7z &> /dev/null; then
    echo "ERROR: 7zip is not installed!"
    echo "Please install 7zip: sudo apt install p7zip-full"
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

echo "Calculating source directory size..."
SOURCE_SIZE=$(du -sh "$SOURCE_DIR" | cut -f1)
echo "Total size: $SOURCE_SIZE"
echo ""

echo "Creating 7zip archive with splitting..."
echo "Compression: LZMA"
echo ""

cd "$(dirname "$SOURCE_DIR")"
7z a -t7z -mx=5 -v"$MAX_SIZE" "${OUTPUT_DIR}/${ARCHIVE_NAME}.7z" "$(basename "$SOURCE_DIR")"

echo ""
echo "=== Created Archives ==="
ls -lh "${OUTPUT_DIR}/${ARCHIVE_NAME}.7z"*
echo ""

PART_COUNT=$(ls "${OUTPUT_DIR}/${ARCHIVE_NAME}.7z"* | wc -l)
TOTAL_SIZE=$(du -sh "${OUTPUT_DIR}/${ARCHIVE_NAME}.7z"* | tail -1 | cut -f1)
echo "Number of parts created: $PART_COUNT"
echo "Total archive size: $TOTAL_SIZE"
echo ""

echo "✅ Creation completed!"
echo ""
echo "To extract the archives use:"
echo "  ./extract_archive.sh '${OUTPUT_DIR}/${ARCHIVE_NAME}.7z.001' [EXTRACT_TO]"