---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Check info dir
description: Verify Redis counters against RDF files
---

Verifies that provenance entities in the RDF files have corresponding entries in Redis. Reports any missing counter entries.

## Usage

```bash
uv run python -m oc_meta.run.infodir.check <directory> [options]
```

## Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `directory` | Yes | - | Path to the RDF directory |
| `--redis-host` | No | localhost | Redis server host |
| `--redis-port` | No | 6379 | Redis server port |
| `--redis-db` | No | 6 | Redis database number |

## Process

1. Finds all provenance ZIP files in the directory
2. For each provenance entity, checks if the expected Redis key exists
3. Reports missing entries with details: entity URI, provenance URI, expected Redis key

## Example

```bash
uv run python -m oc_meta.run.infodir.check /srv/oc_meta/rdf \
    --redis-host localhost \
    --redis-port 6379 \
    --redis-db 5
```

## Output

The script prints each missing entity as it's found:

```
Entità mancante trovata:
URI: https://w3id.org/oc/meta/br/06101234
Prov URI: https://w3id.org/oc/meta/br/06101234/prov/se/1
Chiave Redis attesa: br:0610:1234:se
---
```

At the end, it reports the total count of missing entities.