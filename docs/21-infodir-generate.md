<!--
SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>

SPDX-License-Identifier: ISC
-->

# Generate info dir

Scans the RDF directory structure and generates filesystem counter files with the correct values based on existing entities.

## Usage

```bash
uv run python -m oc_meta.run.infodir.gen <rdf_directory> <info_dir>
```

## Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `rdf_directory` | Yes | Path to the RDF directory (containing `br/`, `ra/`, etc.) |
| `info_dir` | Yes | Base path for the info_dir where counter files will be written |

The supplier prefix "060" is hardcoded.

## Process

1. For each entity type (`br`, `ra`, `ar`, `re`, `id`), the script finds the highest numbered entity in each supplier prefix folder
2. Writes the main counter for each entity type and prefix to the corresponding counter file
3. Scans all provenance files in parallel to find the highest snapshot number for each entity
4. Updates the provenance counter files in batch

## Example

```bash
uv run python -m oc_meta.run.infodir.gen /srv/oc_meta/rdf /srv/oc_meta/info_dir
```

This reads all RDF files under `/srv/oc_meta/rdf` and writes the counter files to `/srv/oc_meta/info_dir`.