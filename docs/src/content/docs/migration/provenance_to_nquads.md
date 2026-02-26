---
title: Provenance to N-Quads
description: Convert JSON-LD provenance archives to N-Quads format
---

Recursively searches for `se.zip` provenance files, extracts JSON-LD content, converts it to N-Quads format, and verifies the quad count matches between input and output.

## Usage

```bash
uv run python -m oc_meta.run.migration.provenance_to_nquads <input_dir> <output_dir> [options]
```

## Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `input_dir` | Yes | - | Directory containing se.zip files (searched recursively) |
| `output_dir` | Yes | - | Output directory for converted .nq files |
| `-w`, `--workers` | No | CPU count | Number of worker processes |

## Process

1. Recursively finds all `se.zip` files in the input directory
2. For each archive, extracts the JSON-LD content
3. Converts JSON-LD to N-Quads using rdflib
4. Writes the output to a flat directory with filenames derived from the path
5. Verifies that the quad count matches between input and output

## Output naming

Output filenames are derived from the relative path of the source file, with path separators replaced by dashes:

- Input: `ra/0610/10000/1000/prov/se.zip`
- Output: `ra-0610-10000-1000-prov-se.nq`

## Example

```bash
uv run python -m oc_meta.run.migration.provenance_to_nquads /srv/oc_meta/rdf /data/provenance_nquads \
    --workers 8
```

## Report

At completion, the script reports the number of successfully processed files and failures:

```
----- Final Report -----
Successfully processed files: 12345
Failed files: 0
-----------------------
```
