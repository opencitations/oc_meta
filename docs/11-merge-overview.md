<!--
SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>

SPDX-License-Identifier: ISC
-->

# Merge overview

The merge tools find duplicate entities and consolidate them, combining their data and updating all references.

## Workflow

1. **Find duplicates** - Scan RDF files to find entities sharing identifiers
2. **Execute merge** - Consolidate entities sequentially with provenance tracking
3. **Track history** - Reconstruct what was merged (optional)

Find duplicate IDs:

```bash
uv run python -m oc_meta.run.find.duplicates ids /data/rdf merges/duplicate_ids.csv
```

Find duplicate responsible agents:

```bash
uv run python -m oc_meta.run.find.duplicates ras /data/rdf merges/duplicate_ras.csv
```

Find duplicate bibliographic resources:

```bash
uv run python -m oc_meta.run.find.duplicates brs /data/rdf merges/duplicate_brs.csv
```

Merge:

```bash
uv run python -m oc_meta.run.merge.entities merges/ meta_config.yaml https://w3id.org/oc/meta/prov/pa/1
```

Production merge order: `id`, re-index, `ra`, re-index, `br`, re-index, orphan cleanup.

Optional - see what was merged:

```bash
uv run python -m oc_meta.run.find.merged_entities -c meta_config.yaml -o merged.csv --entity-type br
```

## Available tools

| Tool | Purpose |
|------|---------|
| [Find duplicates](12-find-duplicates.md) | Scan RDF files for duplicate IDs, RAs, and BRs |
| [Merge entities](14-merge-entities.md) | Execute merge operations |
| [Verify merge](15-verify-merge.md) | Check merge results and generate fix queries |
| [Compact CSV](16-compact-csv.md) | Extract completed merges into a single file |
| [Merge history](17-merge-history.md) | Reconstruct merge history from provenance |

## What happens during merge

When entity B is merged into entity A:

1. **Identifiers** from B are added to A
2. **Metadata** from B fills gaps in A (titles, dates, etc.)
3. **Relationships** pointing to B are redirected to A
4. **Author/editor chains** from A are kept; when A has none of a role type, the richest chain from B is adopted
5. **Provenance** records the merge operation
6. **Entity B** is marked as merged and invalidated

The surviving entity (A) becomes the canonical representation. The merged entity (B) is preserved in provenance for historical queries but is no longer active.
