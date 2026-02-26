---
title: Find duplicates
description: Scan RDF files to find duplicate identifiers and entities
---

These scripts scan RDF files in ZIP archives to find duplicates that need merging.

:::caution[Order matters]
You must find and merge duplicate **identifiers** before searching for duplicate **entities**. Since `duplicated_entities` detects duplicates by shared identifier URIs, two BR entities pointing to different ID URIs won't be detected as duplicates—even if those IDs represent the same value (e.g., the same DOI). Merge duplicate IDs first so that all references point to the same identifier entity.
:::

## Find duplicate identifiers

Finds identifier entities that share the same value, indicating duplicates in the `id/` folder.

```bash
uv run python -m oc_meta.run.find.duplicated_ids <FOLDER_PATH> <CSV_PATH> [OPTIONS]
```

| Argument | Description |
|----------|-------------|
| `FOLDER_PATH` | Path to folder containing the `id/` subfolder with ZIP files |
| `CSV_PATH` | Output CSV file for duplicates |

| Option | Default | Description |
|--------|---------|-------------|
| `--chunk-size` | 5000 | ZIP files to process per chunk (results saved to temp files between chunks) |
| `--temp-dir` | system temp | Directory for temporary files |

Example:

```bash
uv run python -m oc_meta.run.find.duplicated_ids /data/meta/rdf duplicated_ids.csv
```

### Output format

```csv
surviving_entity,merged_entities
https://w3id.org/oc/meta/id/0601,https://w3id.org/oc/meta/id/0602; https://w3id.org/oc/meta/id/0603
```

The surviving entity is arbitrarily selected from the duplicate set.

## Find duplicate entities

Finds bibliographic resources or responsible agents that share identifiers.

```bash
uv run python -m oc_meta.run.find.duplicated_entities <FOLDER_PATH> <CSV_PATH> <RESOURCE_TYPE>
```

| Argument | Description |
|----------|-------------|
| `FOLDER_PATH` | Path to RDF folder (should contain `br/` and/or `ra/`) |
| `CSV_PATH` | Output CSV file |
| `RESOURCE_TYPE` | `br` for bibliographic resources, `ra` for responsible agents, `both` for both |

Find duplicate bibliographic resources:

```bash
uv run python -m oc_meta.run.find.duplicated_entities /data/rdf dup_br.csv br
```

Find duplicate responsible agents:

```bash
uv run python -m oc_meta.run.find.duplicated_entities /data/rdf dup_ra.csv ra
```

Find both:

```bash
uv run python -m oc_meta.run.find.duplicated_entities /data/rdf dup_all.csv both
```

### Output format

```csv
surviving_entity,merged_entities
https://w3id.org/oc/meta/br/0601,https://w3id.org/oc/meta/br/0602; https://w3id.org/oc/meta/br/0603
```

The surviving entity is arbitrarily selected from the duplicate set.

## How duplicates are detected

**duplicated_ids**: Finds identifier entities (`id/`) that have the same scheme and literal value. For example, two ID entities both representing `doi:10.1234/a` are duplicates.

**duplicated_entities**: Finds BR or RA entities that reference the same identifier URI. For example:

- `br/0601` has `datacite:hasIdentifier` pointing to `id/0610`
- `br/0602` has `datacite:hasIdentifier` pointing to `id/0610`

These share the same identifier entity, so they're duplicates.

The `duplicated_entities` script uses Union-Find to handle transitive relationships. If A shares an identifier with B, and B shares an identifier with C, then A, B, and C are all grouped together even if A and C share no direct identifier.

## Expected directory structure

```
/data/meta/rdf/
├── br/
│   └── 060/
│       └── 10000/
│           ├── 1000.zip
│           └── ...
├── ra/
│   └── ...
└── id/
    └── ...
```

## Next steps

Use the output CSV with:

1. [Group entities](/oc_meta/merge/group_entities/) - Prepare for parallel merging
2. [Merge entities](/oc_meta/merge/merge_entities/) - Execute the merge
