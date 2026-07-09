<!--
SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>

SPDX-License-Identifier: ISC
-->

# Find duplicates

The duplicate finder scans RDF files in ZIP archives to find duplicates that need merging.

:::caution[Order matters]
You must find and merge duplicate **IDs** before searching for duplicate **RAs** and **BRs**. Since RA and BR detection uses shared identifier URIs, two BRs pointing to different ID URIs won't be detected as duplicates, even if those IDs represent the same value (e.g., the same DOI). Merge duplicate IDs first so that all references point to the same identifier entity.
:::

## Find duplicate IDs

Finds IDs that share the same value, indicating duplicates in the `id/` folder.

```bash
uv run python -m oc_meta.run.find.duplicates ids <FOLDER_PATH> <CSV_PATH> [OPTIONS]
```

| Argument | Description |
|----------|-------------|
| `FOLDER_PATH` | Path to folder containing the `id/` subfolder with ZIP files |
| `CSV_PATH` | Output CSV file for duplicates |

| Option | Default | Description |
|--------|---------|-------------|
| `--chunk-size` | 5000 | ZIP files to process per chunk (results saved to temp files between chunks) |

Temporary chunk files are created in the directory containing `CSV_PATH` and removed when the command finishes.

Example:

```bash
uv run python -m oc_meta.run.find.duplicates ids /data/meta/rdf duplicate_ids.csv
```

### Output format

```text
surviving_entity,merged_entities
https://w3id.org/oc/meta/id/0601,https://w3id.org/oc/meta/id/0602; https://w3id.org/oc/meta/id/0603
```

For duplicate IDs, the surviving entity is the first URI in sorted order.

## Find duplicate responsible agents

Finds responsible agents that share identifiers.

```bash
uv run python -m oc_meta.run.find.duplicates ras <FOLDER_PATH> <CSV_PATH>
```

| Argument | Description |
|----------|-------------|
| `FOLDER_PATH` | Path to RDF folder containing the `ra/` subfolder |
| `CSV_PATH` | Output CSV file |

Example:

```bash
uv run python -m oc_meta.run.find.duplicates ras /data/rdf dup_ras.csv
```

## Find duplicate bibliographic resources

Finds bibliographic resources that share identifiers.

```bash
uv run python -m oc_meta.run.find.duplicates brs <FOLDER_PATH> <CSV_PATH>
```

| Argument | Description |
|----------|-------------|
| `FOLDER_PATH` | Path to RDF folder containing the `br/` subfolder |
| `CSV_PATH` | Output CSV file |

Example:

```bash
uv run python -m oc_meta.run.find.duplicates brs /data/rdf dup_brs.csv
```

If errors occur, `error_log_find_duplicated_resources.txt` is written in the directory containing `CSV_PATH`. Empty error logs are removed when the command finishes.

### Output format

```text
surviving_entity,merged_entities
https://w3id.org/oc/meta/br/0601,https://w3id.org/oc/meta/br/0602; https://w3id.org/oc/meta/br/0603
```

For duplicate RAs and BRs, the surviving entity is selected by functional metadata, the more the better. Ties use URI order.

## How duplicates are detected

**ids mode**: Finds IDs (`id/`) that have the same scheme and literal value. For example, two IDs both representing `doi:10.1234/a` are duplicates.

**ras mode** and **brs mode**: Find RAs and BRs that reference the same identifier URI. For example:

- `br/0601` has `datacite:hasIdentifier` pointing to `id/0610`
- `br/0602` has `datacite:hasIdentifier` pointing to `id/0610`

These share the same identifier entity, so they're duplicates.

The `ras` and `brs` modes use Union-Find to handle transitive relationships. If A shares an identifier with B, and B shares an identifier with C, then A, B, and C are all grouped together even if A and C share no direct identifier.

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

Place the output CSV in a folder and run [Merge entities](14-merge-entities.md) on it.
