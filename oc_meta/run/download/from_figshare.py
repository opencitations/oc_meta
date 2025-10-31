#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2025, Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

"""
Download files from a Figshare article using the Figshare API.

This script downloads all files associated with a Figshare article ID.
It uses the public Figshare API which works reliably unlike direct wget/curl
on Figshare URLs.
"""

import argparse
import hashlib
import sys
from pathlib import Path

import requests
from tqdm import tqdm

BASE_URL = "https://api.figshare.com/v2"
CHUNK_SIZE = 8192


def get_article_metadata(article_id):
    """Retrieve article metadata from Figshare API."""
    url = f"{BASE_URL}/articles/{article_id}"
    response = requests.get(url)
    response.raise_for_status()
    article_data = response.json()

    # Figshare API has a default limit of 10 files. We need to fetch files separately with pagination.
    files_url = f"{BASE_URL}/articles/{article_id}/files"
    files_response = requests.get(files_url, params={"page_size": 1000})
    files_response.raise_for_status()
    article_data['files'] = files_response.json()

    return article_data


def download_file(download_url, output_path, expected_size, expected_md5=None):
    """Download a file from URL with progress bar and optional MD5 verification."""
    response = requests.get(download_url, stream=True)
    response.raise_for_status()

    md5_hash = hashlib.md5()

    with open(output_path, 'wb') as f:
        with tqdm(total=expected_size, unit='B', unit_scale=True, unit_divisor=1024) as pbar:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                f.write(chunk)
                md5_hash.update(chunk)
                pbar.update(len(chunk))

    if expected_md5:
        actual_md5 = md5_hash.hexdigest()
        if actual_md5 != expected_md5:
            raise ValueError(f"MD5 mismatch: expected {expected_md5}, got {actual_md5}")
        print(f"  MD5 checksum verified: {actual_md5}")


def main():
    parser = argparse.ArgumentParser(
        description="Download files from a Figshare article"
    )
    parser.add_argument(
        "article_id",
        type=int,
        help="Figshare article ID"
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=Path,
        default=Path("."),
        help="Output directory for downloaded files (default: current directory)"
    )

    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Fetching metadata for article {args.article_id}...")
    metadata = get_article_metadata(args.article_id)

    files = metadata.get("files", [])
    if not files:
        print("No files found in this article")
        return 1

    print(f"\nFound {len(files)} file(s) to download:")
    for f in files:
        size_mb = f['size'] / (1024 * 1024)
        print(f"  - {f['name']} ({size_mb:.2f} MB)")

    print(f"\nDownloading to: {args.output_dir.absolute()}\n")

    for file_info in files:
        filename = file_info['name']
        download_url = file_info['download_url']
        size = file_info['size']
        md5 = file_info.get('supplied_md5')

        output_path = args.output_dir / filename

        print(f"Downloading {filename}...")
        download_file(download_url, output_path, size, md5)
        print(f"  Saved to {output_path}\n")

    print("All files downloaded successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
