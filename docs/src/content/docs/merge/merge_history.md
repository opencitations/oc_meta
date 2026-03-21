---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Merge history
description: Reconstruct merge history from provenance data
---

This script reconstructs the history of merged entities by analyzing provenance data. It finds all entities that were merged and traces the chain of merges.

## Usage

```bash
uv run python -m oc_meta.run.find.merged_entities -c <META_CONFIG> -o <OUTPUT_CSV> --entity-type <TYPE> [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `-c, --config` | - | Path to Meta config file |
| `-o, --output` | - | Output CSV file |
| `--entity-type` | - | Entity type: `br`, `ra`, `id`, `ar`, `re` |
| `--workers` | 4 | Parallel workers |

## Examples

Find all merged bibliographic resources:

```bash
uv run python -m oc_meta.run.find.merged_entities \
  -c meta_config.yaml \
  -o merged_brs.csv \
  --entity-type br \
  --workers 8
```

Find merged responsible agents:

```bash
uv run python -m oc_meta.run.find.merged_entities \
  -c meta_config.yaml \
  -o merged_ras.csv \
  --entity-type ra
```

## Output format

```csv
surviving_entity,merged_entities
https://w3id.org/oc/meta/br/060/1,https://w3id.org/oc/meta/br/060/2; https://w3id.org/oc/meta/br/060/3
https://w3id.org/oc/meta/br/060/100,https://w3id.org/oc/meta/br/060/101
```

## How it works

The script scans provenance files (`se.zip`) and looks for snapshots with `prov:wasDerivedFrom` pointing to 2+ sources—this indicates a merge operation. For each merge snapshot:

1. Extracts the surviving entity from `prov:specializationOf`
2. Extracts merged entities from `prov:wasDerivedFrom` sources (excluding the surviving entity itself)

It then reconstructs chains: if A was merged into B, and B was later merged into C, the script reports C as the final surviving entity for both A and B.