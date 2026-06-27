#!/bin/bash

# SPDX-FileCopyrightText: 2025 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

# Script to create compressed and split archives using 7-Zip
# Usage: ./create_archive.sh SOURCE_PATH OUTPUT_DIR ARCHIVE_NAME MAX_SIZE

set -euo pipefail

show_help() {
    echo "Usage: $0 SOURCE_PATH OUTPUT_DIR ARCHIVE_NAME MAX_SIZE"
    echo ""
    echo "Parameters:"
    echo "  SOURCE_PATH   Path to the file or directory to archive"
    echo "  OUTPUT_DIR    Directory where to create the archive parts"
    echo "  ARCHIVE_NAME  Base name for the archive files"
    echo "  MAX_SIZE      Maximum size per file (e.g., 10g, 500m, 2000m)"
    echo ""
    echo "Example:"
    echo "  $0 /path/to/source ./archives dataset_dump 10g"
    echo ""
    echo "Note: Requires 7-Zip"
    echo ""
    echo "Options:"
    echo "  --help        Show this help"
}

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ] || [ $# -eq 0 ]; then
    show_help
    exit 0
fi

if [ $# -ne 4 ]; then
    echo "ERROR: All 4 parameters are required"
    echo ""
    show_help
    exit 1
fi

SOURCE_PATH="$1"
OUTPUT_DIR="$2"
ARCHIVE_NAME="$3"
MAX_SIZE="$4"

echo "=== Creating Archive ==="
echo "Source path: $SOURCE_PATH"
echo "Output directory: $OUTPUT_DIR"
echo "Archive name: $ARCHIVE_NAME"
echo "Maximum size per file: $MAX_SIZE"
echo ""

if [ ! -e "$SOURCE_PATH" ]; then
    echo "ERROR: Source path '$SOURCE_PATH' does not exist"
    exit 1
fi

if command -v 7z > /dev/null 2>&1; then
    SEVEN_ZIP_COMMAND="7z"
elif command -v 7zz > /dev/null 2>&1; then
    SEVEN_ZIP_COMMAND="7zz"
else
    echo "ERROR: 7-Zip is not installed"
    echo "Install it with your package manager, for example: sudo apt install p7zip-full"
    exit 1
fi

mkdir -p "$OUTPUT_DIR"
SOURCE_PATH=$(realpath "$SOURCE_PATH")
OUTPUT_DIR=$(realpath "$OUTPUT_DIR")

echo "Calculating source size..."
SOURCE_SIZE=$(du -sh "$SOURCE_PATH" | cut -f1)
echo "Total size: $SOURCE_SIZE"
echo ""

echo "Creating 7zip archive with splitting..."
echo "Compression level: mx=5"
echo ""

cd "$(dirname "$SOURCE_PATH")"
"$SEVEN_ZIP_COMMAND" a -t7z -mx=5 -v"$MAX_SIZE" "${OUTPUT_DIR}/${ARCHIVE_NAME}.7z" "$(basename "$SOURCE_PATH")"

echo ""
echo "=== Created Archives ==="
ls -lh "${OUTPUT_DIR}/${ARCHIVE_NAME}.7z"*
echo ""

PART_COUNT=$(find "$OUTPUT_DIR" -maxdepth 1 -type f -name "${ARCHIVE_NAME}.7z*" | wc -l)
TOTAL_SIZE=$(du -ch "${OUTPUT_DIR}/${ARCHIVE_NAME}.7z"* | tail -1 | cut -f1)
echo "Number of parts created: $PART_COUNT"
echo "Total archive size: $TOTAL_SIZE"
echo ""

echo "Creation completed"
echo ""
echo "To extract the archives use:"
echo "  ./extract_archive.sh '${OUTPUT_DIR}/${ARCHIVE_NAME}.7z.001' [EXTRACT_TO]"
