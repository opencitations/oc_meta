---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Check info dir
description: Verify filesystem counters against RDF files
---

Verifies that provenance entities in the RDF files have corresponding entries in the filesystem counter files. Reports any missing counter entries.

## Usage

```bash
uv run python -m oc_meta.run.infodir.check <directory> <info_dir>
```

## Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `directory` | Yes | Path to the RDF directory |
| `info_dir` | Yes | Base directory for counter files |

## Process

1. Finds all provenance ZIP files in the directory
2. For each provenance entity, checks if the expected counter entry exists in the filesystem
3. Reports missing entries with details: entity URI, provenance URI, expected counter file entry

## Example

```bash
uv run python -m oc_meta.run.infodir.check /srv/oc_meta/rdf /srv/oc_meta/info_dir
```

## Output

The script prints each missing entity as it's found:

```
Missing entity:
URI: https://w3id.org/oc/meta/br/06101234
Prov URI: https://w3id.org/oc/meta/br/06101234/prov/se/1
---
```

At the end, it reports the total count of missing entities.