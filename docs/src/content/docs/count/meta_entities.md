---
title: Meta entities
description: Count bibliographic resources, agent roles, and venues
---

Counts bibliographic resources, agent roles (authors, publishers, editors), and venues in the dataset.

## Usage

```bash
uv run python -m oc_meta.run.count.meta_entities <SPARQL_ENDPOINT> [OPTIONS]
```

## Options

| Option | Description |
|--------|-------------|
| `--csv` | Path to CSV dump directory (required for venue counting) |
| `--br` | Count bibliographic resources (fabio:Expression) |
| `--ar` | Count agent roles (pro:author, pro:publisher, pro:editor) |
| `--venues` | Count distinct venues (requires `--csv`) |

If no options are specified, all counts are computed.

## Examples

All counts:

```bash
uv run python -m oc_meta.run.count.meta_entities http://localhost:8890/sparql --csv /path/to/csv/dump
```

Only bibliographic resources and roles:

```bash
uv run python -m oc_meta.run.count.meta_entities http://localhost:8890/sparql --br --ar
```

Only venues:

```bash
uv run python -m oc_meta.run.count.meta_entities http://localhost:8890/sparql --venues --csv /path/to/csv/dump
```

## How it works

| Count | Method |
|-------|--------|
| Bibliographic resources | SPARQL query counting `fabio:Expression` entities |
| Agent roles | SPARQL query counting `pro:RoleInTime` by role type |
| Venues | CSV dump parsing with disambiguation |

Venues are counted from CSV files because the SPARQL query for venue disambiguation can exhaust memory on large datasets.

## Venue disambiguation

Venues are disambiguated based on identifiers:

- If a venue has only an OMID (no external identifiers like ISSN/ISBN), venues with the same name are counted as one
- If a venue has external identifiers, it's counted by its OMID
