---
title: ORCID-DOI index
description: Build a CSV index mapping DOIs to ORCID authors
---

The `orcid_process.py` script extracts DOI-author associations from ORCID XML summary files. The output is a CSV index that maps each DOI to the authors who claimed it in their ORCID profile.

## Usage

```bash
uv run python -m oc_meta.run.orcid_process \
    -out <output_path> \
    -s <summaries_path> \
    [-t <threshold>] \
    [-lm] \
    [-v]
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `-out, --output` | Output directory for CSV files |
| `-s, --summaries` | Path to ORCID summaries folder (subfolders included) |
| `-t, --threshold` | Number of files processed before saving (default: 10000) |
| `-lm, --low-memory` | Low memory mode for large datasets |
| `-v, --verbose` | Show progress bar |

## Input

The script expects ORCID public data summaries in XML format. These can be downloaded from the [ORCID public data file](https://info.orcid.org/documentation/integration-guide/working-with-bulk-data/).

Each XML file contains an ORCID profile with external identifiers. The script extracts DOIs marked with relationship type "self" (i.e., works authored by the profile owner).

## Output

CSV files with two columns:

| Column | Content |
|--------|---------|
| `id` | DOI |
| `value` | Author name and ORCID in format `Surname, Given [0000-0000-0000-0000]` |

Multiple authors can be associated with the same DOI if they all claimed it in their profiles.

## Example

```bash
uv run python -m oc_meta.run.orcid_process \
    -out ./orcid_index \
    -s ./ORCID_2023_10_summaries \
    -t 50000 \
    -v
```

This processes all XML files in `ORCID_2023_10_summaries`, saves progress every 50000 files, and shows a progress bar.

## Resume support

The script tracks processed ORCID IDs. If interrupted, it skips already processed files on the next run.
