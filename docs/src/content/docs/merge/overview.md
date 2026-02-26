---
title: Merge overview
description: Find and consolidate duplicate entities
---

The merge tools find duplicate entities and consolidate them, combining their data and updating all references.

## Workflow

1. **Find duplicates** - Scan RDF files to find entities sharing identifiers
2. **Group entities** - Prepare for parallel processing
3. **Execute merge** - Consolidate entities with provenance tracking
4. **Track history** - Reconstruct what was merged (optional)

Find duplicates:

```bash
uv run python -m oc_meta.run.find.duplicated_entities /data/rdf duplicates.csv br
```

Group for parallel processing:

```bash
uv run python -m oc_meta.run.merge.group_entities duplicates.csv groups/ meta_config.yaml
```

Merge:

```bash
uv run python -m oc_meta.run.merge.entities groups/ meta_config.yaml https://w3id.org/oc/meta/prov/pa/1
```

Optional - see what was merged:

```bash
uv run python -m oc_meta.run.find.merged_entities -c meta_config.yaml -o merged.csv --entity-type br
```

## Available tools

| Tool | Purpose |
|------|---------|
| [Find duplicates](/oc_meta/merge/duplicates/) | Scan RDF files for duplicate identifiers and entities |
| [Group entities](/oc_meta/merge/group_entities/) | Prepare duplicates for parallel merging |
| [Merge entities](/oc_meta/merge/merge_entities/) | Execute merge operations |
| [Verify merge](/oc_meta/merge/verify_merge/) | Check merge results and generate fix queries |
| [Compact CSV](/oc_meta/merge/compact_csv/) | Extract completed merges into a single file |
| [Merge history](/oc_meta/merge/merge_history/) | Reconstruct merge history from provenance |

## What happens during merge

When entity B is merged into entity A:

1. **Identifiers** from B are added to A
2. **Metadata** from B fills gaps in A (titles, dates, etc.)
3. **Relationships** pointing to B are redirected to A
4. **Author/editor chains** from A are kept (B's chains are discarded)                                                           
5. **Provenance** records the merge operation
6. **Entity B** is marked as merged and invalidated

The surviving entity (A) becomes the canonical representation. The merged entity (B) is preserved in provenance for historical queries but is no longer active.