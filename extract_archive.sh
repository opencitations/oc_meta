#!/bin/bash

# Script to extract split 7zip archives
# Usage: ./extract_archive.sh PART_FILE [EXTRACT_TO]

set -e 

show_help() {
    echo "Usage: $0 PART_FILE [EXTRACT_TO]"
    echo ""
    echo "Parameters:"
    echo "  PART_FILE     Path to any 7z part file (e.g., archive.7z.001)"
    echo "  EXTRACT_TO    Directory to extract to (optional, default: current dir)"
    echo ""
    echo "Example:"
    echo "  $0 ./backups/oc_meta_prov_06_06.7z.001 ./restored"
    echo ""
    echo "Special options (replace EXTRACT_TO):"
    echo "  --list-only   Only list contents without extracting"
    echo "  --help        Show this help"
}

if [ "$1" = "--help" ] || [ "$1" = "-h" ] || [ $# -eq 0 ]; then
    show_help
    exit 0
fi

PART_FILE="$1"
EXTRACT_TO="$2"

if [ ! -f "$PART_FILE" ]; then
    echo "ERROR: File '$PART_FILE' does not exist!"
    exit 1
fi

if ! command -v 7z &> /dev/null;
then
        if ! command -v 7zz &> /dev/null;
        then
        echo "ERROR: 7zip is not installed!"
        echo "Please install 7zip: sudo apt install p7zip-full"
        exit 1
        else
                _7zcommand='7zz'
        fi
else
        _7zcommand='7z'
fi

if [[ "$PART_FILE" =~ (.+\.7z)\.[0-9]+ ]]; then
    BASE_ARCHIVE="${BASH_REMATCH[1]}"
    ARCHIVE_NAME=$(basename "${BASE_ARCHIVE%.7z}")
else
    echo "ERROR: '$PART_FILE' is not a valid 7z part file!"
    echo "Expected format: *.7z.001"
    exit 1
fi

PART_PATTERN="${BASE_ARCHIVE}.*"
if ! ls $PART_PATTERN >/dev/null 2>&1; then
    echo "ERROR: No part files found with pattern '$PART_PATTERN'"
    exit 1
fi

echo "=== Extracting 7zip Archive ==="
echo "Archive name: $ARCHIVE_NAME"
echo "Base archive: $BASE_ARCHIVE"
if [ -n "$EXTRACT_TO" ] && [ "$EXTRACT_TO" != "--list-only" ]; then
    echo "Extracting to: $EXTRACT_TO"
fi
echo ""

echo "Part files found:"
ls -lh $PART_PATTERN
echo ""

PART_COUNT=$(ls $PART_PATTERN | wc -l)
TOTAL_SIZE=$(du -ch $PART_PATTERN | tail -1 | cut -f1)
echo "Number of parts: $PART_COUNT"
echo "Total size: $TOTAL_SIZE"
echo ""

if [ "$EXTRACT_TO" = "--list-only" ]; then
    echo "Archive contents:"
    ${_7zcommand} l "$BASE_ARCHIVE.001"
    exit 0
fi

if [ -z "$EXTRACT_TO" ]; then
    EXTRACT_TO="."
    echo "No extraction directory specified, extracting to current directory"
fi

mkdir -p "$EXTRACT_TO"

echo "Extracting using 7zip..."
echo "7zip will automatically reassemble the split parts"
echo ""

${_7zcommand} x "$BASE_ARCHIVE.001" -o"$EXTRACT_TO"

echo ""
echo "✅ Extraction completed!"
echo "Files extracted to: $EXTRACT_TO"