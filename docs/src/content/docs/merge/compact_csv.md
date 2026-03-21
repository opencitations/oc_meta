---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Compact merge CSV
description: Extract completed merges into a single file
---

After running merge operations, the CSV files contain a `Done` column marking completed rows. This script extracts only successful merges and combines them into a single CSV.

## Usage

```bash
uv run python -m oc_meta.run.merge.compact_output_csv <INPUT_DIR> <OUTPUT_FILE>
```

| Argument | Description |
|----------|-------------|
| `INPUT_DIR` | Directory containing merge CSV files |
| `OUTPUT_FILE` | Path for the output CSV |

## Example

```bash
uv run python -m oc_meta.run.merge.compact_output_csv groups/ completed_merges.csv
```

## Output format

```csv
surviving_entity,merged_entities
https://w3id.org/oc/meta/br/060/1,https://w3id.org/oc/meta/br/060/2; https://w3id.org/oc/meta/br/060/3
https://w3id.org/oc/meta/br/060/100,https://w3id.org/oc/meta/br/060/101
```